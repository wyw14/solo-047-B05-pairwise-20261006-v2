package repair

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
)

func newTestPermitService(t *testing.T, persist PermitPersister) (*PermitService, *feed.Circuit) {
	t.Helper()
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }
	service := NewPermitService(feed.NewIsolationService(), landing.NewReceiptRegistry(), platform.UUIDGenerator{}, now, persist)
	circuit, err := feed.NewCircuit("A", "east", now())
	if err != nil {
		t.Fatal(err)
	}
	service.RegisterCircuit(circuit)
	return service, circuit
}

func TestRequestPersistFailureRollsBackCircuit(t *testing.T) {
	attempts := 0
	service, circuit := newTestPermitService(t, func(Permit) error {
		attempts++
		if attempts == 1 {
			return errors.New("journal append: disk full")
		}
		return nil
	})

	if _, err := service.Request("east", "A"); !errors.Is(err, ErrPermitPersist) {
		t.Fatalf("Request error = %v, want ErrPermitPersist", err)
	}
	if snapshot := circuit.Snapshot(); snapshot.State != feed.StateEnergized || snapshot.OperationID != "" {
		t.Fatalf("circuit left orphaned after persist failure: %+v", snapshot)
	}
	if got := len(service.permits); got != 0 {
		t.Fatalf("permits leaked after persist failure: %d", got)
	}

	// A retry continues safely: the circuit was restored, so a fresh permit
	// is created and becomes queryable.
	permit, err := service.Request("east", "A")
	if err != nil {
		t.Fatalf("retry after persist failure: %v", err)
	}
	if permit.State != PermitIsolating {
		t.Fatalf("permit state = %s, want %s", permit.State, PermitIsolating)
	}
	if snapshot := circuit.Snapshot(); snapshot.State != feed.StateIsolating || snapshot.OperationID != permit.OperationID {
		t.Fatalf("circuit after retry: %+v, want isolating under %s", snapshot, permit.OperationID)
	}
	if _, exists := service.Get(permit.ID); !exists {
		t.Fatalf("permit %s not queryable after retry", permit.ID)
	}
}

func TestRequestRetryReturnsActivePermit(t *testing.T) {
	var persisted []Permit
	service, _ := newTestPermitService(t, func(permit Permit) error {
		persisted = append(persisted, permit)
		return nil
	})

	first, err := service.Request("east", "A")
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Request("east", "A")
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.OperationID != first.OperationID {
		t.Fatalf("retry created a new permit: first %+v second %+v", first, second)
	}
	if len(persisted) != 1 {
		t.Fatalf("persister called %d times, want 1", len(persisted))
	}
}

func TestRequestUnknownCircuit(t *testing.T) {
	service, _ := newTestPermitService(t, func(Permit) error { return nil })
	if _, err := service.Request("east", "missing"); err == nil {
		t.Fatal("expected error for unknown circuit")
	}
}
