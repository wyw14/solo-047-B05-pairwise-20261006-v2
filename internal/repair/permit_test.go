package repair

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
)

var fixedNow = func() time.Time { return time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC) }

// flakyJournal fails the first failTimes Append calls, then succeeds.
type flakyJournal struct {
	mu       sync.Mutex
	failLeft int
	appends  []string
}

func (j *flakyJournal) Append(kind, entityID string, value any, at time.Time) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.appends = append(j.appends, kind+"/"+entityID)
	if j.failLeft > 0 {
		j.failLeft--
		return fmt.Errorf("simulated disk failure")
	}
	return nil
}

func newTestPermitService(t *testing.T, journal PermitJournal) (*PermitService, *feed.Circuit) {
	t.Helper()
	circuit, err := feed.NewCircuit("C", "east", fixedNow())
	if err != nil {
		t.Fatal(err)
	}
	service := NewPermitService(
		feed.NewIsolationService(), landing.NewReceiptRegistry(),
		journal, platform.UUIDGenerator{}, fixedNow,
	)
	service.RegisterCircuit(circuit)
	return service, circuit
}

func TestRequestPersistsAndMarksCircuitIsolating(t *testing.T) {
	service, circuit := newTestPermitService(t, &flakyJournal{})

	permit, err := service.Request("east", "C")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if permit.State != PermitIsolating {
		t.Fatalf("permit state = %s, want %s", permit.State, PermitIsolating)
	}
	stored, ok := service.Get(permit.ID)
	if !ok || stored.OperationID != permit.OperationID {
		t.Fatalf("permit not stored or operation mismatch: %+v", stored)
	}
	snapshot := circuit.Snapshot()
	if snapshot.State != feed.StateIsolating || snapshot.OperationID != permit.OperationID {
		t.Fatalf("circuit snapshot = %+v, want isolating for %s", snapshot, permit.OperationID)
	}
}

func TestRequestRollsBackCircuitWhenPersistFails(t *testing.T) {
	journal := &flakyJournal{failLeft: 1}
	service, circuit := newTestPermitService(t, journal)

	_, err := service.Request("east", "C")
	var persistErr *PermitPersistError
	if !errors.As(err, &persistErr) {
		t.Fatalf("want *PermitPersistError, got %v", err)
	}
	if !persistErr.Attempted {
		t.Fatalf("rollback should be attempted, got %+v", persistErr)
	}
	if snapshot := circuit.Snapshot(); snapshot.State != feed.StateEnergized || snapshot.OperationID != "" {
		t.Fatalf("circuit left orphaned after persist failure: %+v", snapshot)
	}
	if _, ok := service.Get(persistErr.Permit.ID); ok {
		t.Fatal("failed permit must not be queryable")
	}
}

func TestRequestSafelyRetriesAfterRollback(t *testing.T) {
	journal := &flakyJournal{failLeft: 1}
	service, circuit := newTestPermitService(t, journal)

	if _, err := service.Request("east", "C"); err == nil {
		t.Fatal("first request was expected to fail")
	}
	permit, err := service.Request("east", "C")
	if err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
	snapshot := circuit.Snapshot()
	if snapshot.State != feed.StateIsolating || snapshot.OperationID != permit.OperationID {
		t.Fatalf("retry did not isolate circuit correctly: %+v", snapshot)
	}
	if _, ok := service.Get(permit.ID); !ok {
		t.Fatal("retried permit is not queryable")
	}
}

func TestRequestReturnsActivePermitInsteadOfRivalIsolation(t *testing.T) {
	service, circuit := newTestPermitService(t, &flakyJournal{})
	first, err := service.Request("east", "C")
	if err != nil {
		t.Fatalf("first request: %v", err)
	}

	second, err := service.Request("east", "C")
	if !errors.Is(err, ErrPermitActive) {
		t.Fatalf("want ErrPermitActive, got %v", err)
	}
	if second.ID != first.ID || second.OperationID != first.OperationID {
		t.Fatalf("retry should adopt existing permit, first=%+v second=%+v", first, second)
	}
	snapshot := circuit.Snapshot()
	if snapshot.State != feed.StateIsolating || snapshot.OperationID != first.OperationID {
		t.Fatalf("circuit state must stay owned by the original operation: %+v", snapshot)
	}
}

func TestAbortRestoresReleasedState(t *testing.T) {
	circuit, err := feed.NewCircuit("C", "east", fixedNow())
	if err != nil {
		t.Fatal(err)
	}
	isolation := feed.NewIsolationService()
	first, err := isolation.Begin(circuit, "op-1", fixedNow())
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationID != "op-1" {
		t.Fatalf("unexpected command: %+v", first)
	}
	if _, err := isolation.Confirm(circuit, "east", "C", "op-1", true, fixedNow()); err != nil {
		t.Fatal(err)
	}
	if _, err := isolation.Ground(circuit, "op-1", fixedNow()); err != nil {
		t.Fatal(err)
	}
	if _, err := isolation.Release(circuit, "op-1", fixedNow()); err != nil {
		t.Fatal(err)
	}

	second, err := isolation.Begin(circuit, "op-2", fixedNow())
	if err != nil {
		t.Fatal(err)
	}
	if second.Restore != feed.StateReleased {
		t.Fatalf("recorded restore state = %s, want released", second.Restore)
	}
	snapshot := isolation.Abort(circuit, second, fixedNow())
	if snapshot.State != feed.StateReleased || snapshot.OperationID != "" {
		t.Fatalf("abort should restore released state, got %+v", snapshot)
	}
}

func TestAbortDoesNotClobberAdvancedOperation(t *testing.T) {
	circuit, err := feed.NewCircuit("C", "east", fixedNow())
	if err != nil {
		t.Fatal(err)
	}
	isolation := feed.NewIsolationService()
	command, err := isolation.Begin(circuit, "op-1", fixedNow())
	if err != nil {
		t.Fatal(err)
	}
	// The field confirms isolation before the compensating abort runs.
	if _, err := isolation.Confirm(circuit, "east", "C", "op-1", true, fixedNow()); err != nil {
		t.Fatal(err)
	}

	snapshot := isolation.Abort(circuit, command, fixedNow())
	if snapshot.State != feed.StateIsolated || snapshot.OperationID != "op-1" {
		t.Fatalf("abort must not clobber an operation past isolating, got %+v", snapshot)
	}
}
