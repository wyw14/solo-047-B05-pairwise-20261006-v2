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

// ErrPermitPersist marks failures to durably record a permit. The in-memory
// state is rolled back when it is returned, so callers may retry safely.
var ErrPermitPersist = errors.New("permit persistence failed")

// PermitPersister durably records a permit before it becomes visible in the
// service. A nil persister rejects every state change.
type PermitPersister func(permit Permit) error

type PermitService struct {
	mu        sync.RWMutex
	permits   map[string]Permit
	circuits  map[string]*feed.Circuit
	isolation *feed.IsolationService
	receipts  *landing.ReceiptRegistry
	ids       platform.IDGenerator
	now       func() time.Time
	persist   PermitPersister
}

func NewPermitService(isolation *feed.IsolationService, receipts *landing.ReceiptRegistry, ids platform.IDGenerator, now func() time.Time, persist PermitPersister) *PermitService {
	return &PermitService{
		permits: make(map[string]Permit), circuits: make(map[string]*feed.Circuit), isolation: isolation,
		receipts: receipts, ids: ids, now: now, persist: persist,
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

// Request creates an isolation permit for a circuit. The circuit transition
// and the durable permit record form one atomic unit: the permit is persisted
// before it becomes visible, and if persistence fails the circuit is rolled
// back to its previous state and no permit is kept — a failure never leaves
// an orphaned isolating circuit behind. Retrying is safe: a retry after a
// failure starts clean, and a retry while a permit is still active for the
// circuit returns that permit instead of conflicting.
func (s *PermitService) Request(stationID, circuitID string) (Permit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	circuit := s.circuits[circuitKey(stationID, circuitID)]
	if circuit == nil {
		return Permit{}, fmt.Errorf("feed circuit %s/%s not found", stationID, circuitID)
	}
	if existing, active := s.activePermitLocked(stationID, circuitID); active {
		return existing, nil
	}
	before := circuit.Snapshot()
	permit := Permit{
		ID: s.ids.New("permit"), StationID: stationID, CircuitID: circuitID,
		OperationID: s.ids.New("isolation"), State: PermitRequested, UpdatedAt: s.now().UTC(),
	}
	if _, err := s.isolation.Begin(circuit, permit.OperationID, s.now()); err != nil {
		return Permit{}, err
	}
	permit.State = PermitIsolating
	if err := s.persistPermit(permit); err != nil {
		_, abortErr := s.isolation.Abort(circuit, permit.OperationID, before)
		return Permit{}, errors.Join(err, abortErr)
	}
	s.permits[permit.ID] = permit
	return permit, nil
}

// activePermitLocked returns the non-terminal permit covering a circuit, if
// one exists. Callers must hold s.mu.
func (s *PermitService) activePermitLocked(stationID, circuitID string) (Permit, bool) {
	for _, permit := range s.permits {
		if permit.StationID == stationID && permit.CircuitID == circuitID && permit.State != PermitReleased {
			return permit, true
		}
	}
	return Permit{}, false
}

func (s *PermitService) persistPermit(permit Permit) error {
	if s.persist == nil {
		return fmt.Errorf("%w: no persister configured", ErrPermitPersist)
	}
	if err := s.persist(permit); err != nil {
		return fmt.Errorf("%w: %v", ErrPermitPersist, err)
	}
	return nil
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
