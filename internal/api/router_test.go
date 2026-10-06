package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	return testServerWithPersister(t, nil)
}

func testServerWithPersister(t *testing.T, persist repair.PermitPersister) *Server {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	journal := store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	if persist == nil {
		persist = func(permit repair.Permit) error {
			return journal.Append("feed-isolation", permit.ID, permit, clock.Now())
		}
	}
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now, persist)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: journal,
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}

func TestStartIsolationPersistFailureRollsBack(t *testing.T) {
	attempts := 0
	server := testServerWithPersister(t, func(repair.Permit) error {
		attempts++
		if attempts == 1 {
			return errors.New("journal append: disk full")
		}
		return nil
	})
	body, _ := json.Marshal(IsolationRequest{CircuitID: "B"})

	failed := httptest.NewRecorder()
	server.Handler().ServeHTTP(failed, httptest.NewRequest(http.MethodPost, "/api/landings/east/feed-isolations", bytes.NewReader(body)))
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("first attempt status=%d body=%s", failed.Code, failed.Body.String())
	}

	// The failed attempt left no orphaned isolating circuit, so the retry
	// succeeds instead of conflicting.
	retried := httptest.NewRecorder()
	server.Handler().ServeHTTP(retried, httptest.NewRequest(http.MethodPost, "/api/landings/east/feed-isolations", bytes.NewReader(body)))
	if retried.Code != http.StatusAccepted {
		t.Fatalf("retry status=%d body=%s", retried.Code, retried.Body.String())
	}
	var permit repair.Permit
	if err := json.Unmarshal(retried.Body.Bytes(), &permit); err != nil {
		t.Fatal(err)
	}
	if permit.State != repair.PermitIsolating {
		t.Fatalf("permit state=%s body=%s", permit.State, retried.Body.String())
	}

	// A further retry while the permit is active returns the same permit.
	again := httptest.NewRecorder()
	server.Handler().ServeHTTP(again, httptest.NewRequest(http.MethodPost, "/api/landings/east/feed-isolations", bytes.NewReader(body)))
	if again.Code != http.StatusAccepted {
		t.Fatalf("second retry status=%d body=%s", again.Code, again.Body.String())
	}
	var same repair.Permit
	if err := json.Unmarshal(again.Body.Bytes(), &same); err != nil {
		t.Fatal(err)
	}
	if same.ID != permit.ID {
		t.Fatalf("retry returned permit %s, want active permit %s", same.ID, permit.ID)
	}
}
