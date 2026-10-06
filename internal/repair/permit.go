package repair

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
)

type PermitState string

const (
	PermitRequested  PermitState = "requested"
	PermitIsolating  PermitState = "isolating"
	PermitGroundable PermitState = "groundable"
	PermitGrounded   PermitState = "grounded"
	PermitReleased   PermitState = "released"
)

type Permit struct {
	ID          string      `json:"id"`
	StationID   string      `json:"station_id"`
	CircuitID   string      `json:"circuit_id"`
	OperationID string      `json:"operation_id"`
	State       PermitState `json:"state"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// PermitJournal durably records permit state changes. Implementations must be
// safe for concurrent use.
type PermitJournal interface {
	Append(kind, entityID string, value any, at time.Time) error
}

// ErrPermitActive signals that an active permit already owns the circuit; the
// returned permit is the in-progress one, so callers can adopt it instead of
// opening a second isolation.
var ErrPermitActive = errors.New("permit request already in progress for circuit")

// PermitPersistError reports that the circuit was isolated but the permit
// could not be persisted. Attempted means the circuit state was rolled back,
// so the request can safely be retried.
type PermitPersistError struct {
	Permit    Permit
	Cause     error
	Attempted bool
}

func (e *PermitPersistError) Error() string {
	return fmt.Sprintf("persist permit %s: %v", e.Permit.ID, e.Cause)
}

func (e *PermitPersistError) Unwrap() error {
	return e.Cause
}

type PermitService struct {
	mu        sync.RWMutex
	permits   map[string]Permit
	circuits  map[string]*feed.Circuit
	isolation *feed.IsolationService
	receipts  *landing.ReceiptRegistry
	journal   PermitJournal
	ids       platform.IDGenerator
	now       func() time.Time
}

func NewPermitService(isolation *feed.IsolationService, receipts *landing.ReceiptRegistry, journal PermitJournal, ids platform.IDGenerator, now func() time.Time) *PermitService {
	return &PermitService{
		permits: make(map[string]Permit), circuits: make(map[string]*feed.Circuit), isolation: isolation,
		receipts: receipts, journal: journal, ids: ids, now: now,
	}
}

func circuitKey(stationID, circuitID string) string {
	return stationID + "\x00" + circuitID
}

func (s *PermitService) RegisterCircuit(circuit *feed.Circuit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := circuit.Snapshot()
	s.circuits[circuitKey(snapshot.StationID, snapshot.ID)] = circuit
}

// activePermitLocked returns the non-released permit owning the circuit, if
// any. The caller must hold s.mu.
func (s *PermitService) activePermitLocked(stationID, circuitID string) (Permit, bool) {
	for _, permit := range s.permits {
		if permit.StationID == stationID && permit.CircuitID == circuitID && permit.State != PermitReleased {
			return permit, true
		}
	}
	return Permit{}, false
}

func (s *PermitService) Request(stationID, circuitID string) (Permit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	circuit := s.circuits[circuitKey(stationID, circuitID)]
	if circuit == nil {
		return Permit{}, fmt.Errorf("feed circuit %s/%s not found", stationID, circuitID)
	}
	// Retry/idempotency: a permit stored here is guaranteed to have been
	// persisted (failed writes are never stored), so an active permit means the
	// previous request committed and the caller should adopt it rather than
	// opening a second isolation.
	if permit, ok := s.activePermitLocked(stationID, circuitID); ok {
		return permit, ErrPermitActive
	}
	permit := Permit{
		ID: s.ids.New("permit"), StationID: stationID, CircuitID: circuitID,
		OperationID: s.ids.New("isolation"), State: PermitRequested, UpdatedAt: s.now().UTC(),
	}
	command, err := s.isolation.Begin(circuit, permit.OperationID, s.now())
	if err != nil {
		return Permit{}, err
	}
	permit.State = PermitIsolating
	// The circuit now claims to be isolating. The permit must be durable in
	// the same critical section before that state is allowed to outlive this
	// request; otherwise the circuit is stuck isolating with no queryable
	// permit and no way to compensate.
	if err := s.journal.Append("feed-isolation", permit.ID, permit, permit.UpdatedAt); err != nil {
		// Compensate: release the circuit back to its pre-isolation state
		// before reporting failure. Abort is guarded by the operation ID, so a
		// state that another actor advanced past isolating is left untouched.
		s.isolation.Abort(circuit, command, s.now())
		return Permit{}, &PermitPersistError{Permit: permit, Cause: err, Attempted: true}
	}
	s.permits[permit.ID] = permit
	return permit, nil
}

func (s *PermitService) Evaluate(permitID string) (Permit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	permit, exists := s.permits[permitID]
	if !exists {
		return Permit{}, fmt.Errorf("permit %s not found", permitID)
	}
	circuit := s.circuits[circuitKey(permit.StationID, permit.CircuitID)]
	_, matched := s.receipts.Match(permit.StationID, permit.CircuitID, permit.OperationID)
	if _, err := s.isolation.Confirm(circuit, permit.StationID, permit.CircuitID, permit.OperationID, matched, s.now()); err != nil {
		return permit, err
	}
	permit.State = PermitGroundable
	permit.UpdatedAt = s.now().UTC()
	s.permits[permit.ID] = permit
	return permit, nil
}

func (s *PermitService) Get(permitID string) (Permit, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	permit, exists := s.permits[permitID]
	return permit, exists
}

func (s *PermitService) Circuit(stationID, circuitID string) (*feed.Circuit, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	circuit, exists := s.circuits[circuitKey(stationID, circuitID)]
	return circuit, exists
}

func (s *PermitService) Ground(permitID string) (Permit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	permit, exists := s.permits[permitID]
	if !exists {
		return Permit{}, fmt.Errorf("permit %s not found", permitID)
	}
	circuit := s.circuits[circuitKey(permit.StationID, permit.CircuitID)]
	if _, err := s.isolation.Ground(circuit, permit.OperationID, s.now()); err != nil {
		return permit, err
	}
	permit.State = PermitGrounded
	permit.UpdatedAt = s.now().UTC()
	s.permits[permit.ID] = permit
	return permit, nil
}
