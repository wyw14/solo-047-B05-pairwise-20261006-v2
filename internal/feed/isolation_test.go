package feed

import (
	"testing"
	"time"
)

func TestAbortRestoresPreviousSnapshot(t *testing.T) {
	at := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	circuit, err := NewCircuit("A", "east", at)
	if err != nil {
		t.Fatal(err)
	}
	service := NewIsolationService()
	before := circuit.Snapshot()
	if _, err := service.Begin(circuit, "op-1", at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	restored, err := service.Abort(circuit, "op-1", before)
	if err != nil {
		t.Fatal(err)
	}
	if restored != before {
		t.Fatalf("restored snapshot = %+v, want %+v", restored, before)
	}
	if snapshot := circuit.Snapshot(); snapshot != before {
		t.Fatalf("circuit snapshot = %+v, want %+v", snapshot, before)
	}
}

func TestAbortRejectsMismatchedOperation(t *testing.T) {
	at := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	circuit, err := NewCircuit("A", "east", at)
	if err != nil {
		t.Fatal(err)
	}
	service := NewIsolationService()
	before := circuit.Snapshot()
	if _, err := service.Begin(circuit, "op-1", at); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Abort(circuit, "op-2", before); err == nil {
		t.Fatal("expected abort to reject a mismatched operation")
	}
	if snapshot := circuit.Snapshot(); snapshot.State != StateIsolating || snapshot.OperationID != "op-1" {
		t.Fatalf("circuit clobbered by mismatched abort: %+v", snapshot)
	}
}
