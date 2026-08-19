package service

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Pamawas/pamawas-correlator/models"
)

func TestRankCandidatesUsesV1ScoresAndTieBreaks(t *testing.T) {
	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	event := models.Event{
		Source:        "grafana",
		SourceEventID: "alert-1",
		Timestamp:     base,
		Service:       "api",
		Environment:   "prod",
		Labels: map[string]string{
			"alert_rule": "Latency",
			"namespace":  "core",
			"workload":   "api-v2",
			"instance":   "api-1",
			"cluster":    "shared",
		},
	}

	candidates := []incidentCandidate{
		{ID: "inc_source", Status: "open", LastEventAt: base.Add(-time.Minute), StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-2 * time.Hour), Environment: "prod", Source: "grafana", SourceEventID: "alert-1"},
		{ID: "inc_labels", Status: "open", LastEventAt: base.Add(-time.Minute), StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-2 * time.Hour), Environment: "prod", Service: "api", Labels: map[string]string{"alert_rule": "Latency", "namespace": "core", "workload": "api-v2", "instance": "api-1"}},
		{ID: "inc_weak", Status: "open", LastEventAt: base, StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-3 * time.Hour), Environment: "prod", Labels: map[string]string{"cluster": "shared"}},
	}

	got, score, ok := selectCandidate(event, candidates)
	if !ok {
		t.Fatal("expected a candidate")
	}
	if got.ID != "inc_labels" || score != 150 {
		t.Fatalf("selected %s score %d, want inc_labels score 150", got.ID, score)
	}
}

func TestRankCandidatesAppliesWindowsAndDeterministicTies(t *testing.T) {
	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	event := models.Event{Timestamp: base, Service: "api", Environment: "prod"}
	candidates := []incidentCandidate{
		{ID: "inc_after_window", Status: "open", LastEventAt: base.Add(-15*time.Minute - time.Nanosecond), StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-3 * time.Hour), Environment: "prod", Service: "api"},
		{ID: "inc_before_window", Status: "open", LastEventAt: base.Add(5*time.Minute + time.Nanosecond), StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-3 * time.Hour), Environment: "prod", Service: "api"},
		{ID: "inc_span", Status: "open", LastEventAt: base, StartedAt: base.Add(-6*time.Hour - time.Nanosecond), CreatedAt: base.Add(-3 * time.Hour), Environment: "prod", Service: "api"},
		{ID: "inc_b", Status: "open", LastEventAt: base.Add(-time.Minute), StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-2 * time.Hour), Environment: "prod", Service: "api"},
		{ID: "inc_a", Status: "open", LastEventAt: base.Add(-time.Minute), StartedAt: base.Add(-time.Hour), CreatedAt: base.Add(-2 * time.Hour), Environment: "prod", Service: "api"},
	}

	got, score, ok := selectCandidate(event, candidates)
	if !ok || got.ID != "inc_a" || score != 50 {
		t.Fatalf("selected %+v score %d ok %v, want inc_a score 50", got, score, ok)
	}
}

func TestResolutionRequiresAcceptedIdentity(t *testing.T) {
	event := models.Event{Source: "grafana", SourceEventID: "alert-1", Service: "api", Labels: map[string]string{"namespace": "core"}}

	if !acceptedResolution(event, incidentCandidate{Source: "grafana", SourceEventID: "alert-1"}) {
		t.Fatal("strong source identity should resolve")
	}
	if !acceptedResolution(event, incidentCandidate{Service: "api", Labels: map[string]string{"namespace": "core"}}) {
		t.Fatal("service plus identity label should resolve")
	}
	if acceptedResolution(event, incidentCandidate{Service: "api"}) {
		t.Fatal("service alone must not resolve")
	}
}

func TestProcessNextEventExtendsAndResolvesCandidateAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	labels := []byte(`{"namespace":"core"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_1", "grafana", "alert-1", "alert", base, "api", "prod", "high", "API latency", "resolved", labels))
	mock.ExpectQuery("(?s)SELECT i.id.*FOR UPDATE OF i SKIP LOCKED").
		WithArgs("prod", base.Add(-15*time.Minute), base.Add(5*time.Minute), base.Add(-6*time.Hour)).
		WillReturnRows(sqlmock.NewRows(candidateColumns()).
			AddRow("inc_b", "open", base.Add(-time.Hour), base.Add(-time.Minute), nil, "warning", "prod", "{api}", base.Add(-2*time.Hour), "api", "grafana", "other", labels).
			AddRow("inc_a", "investigating", base.Add(-time.Hour), base.Add(-time.Minute), nil, "warning", "prod", "{api}", base.Add(-3*time.Hour), "api", "grafana", "alert-1", labels))
	mock.ExpectExec("UPDATE incidents").
		WithArgs("API latency", "resolved", base, "high", sqlmock.AnyArg(), base, "inc_a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_a", "evt_1", "resolution_of_open_incident", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	requestKey := investigationRequestKey("inc_a", "evt_1")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_a", 1, requestKey, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{db: db, now: func() time.Time { return base }, newIncidentID: func(time.Time) string { return "inc_new" }}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessNextEventCreatesIncidentAndOutbox(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_2", "prometheus", "", "alert", base, "db", "prod", "critical", "DB down", "firing", []byte(`{}`)))
	mock.ExpectQuery("(?s)SELECT i.id.*FOR UPDATE OF i SKIP LOCKED").
		WithArgs("prod", base.Add(-15*time.Minute), base.Add(5*time.Minute), base.Add(-6*time.Hour)).
		WillReturnRows(sqlmock.NewRows(candidateColumns()))
	mock.ExpectExec("INSERT INTO incidents").
		WithArgs("inc_new", "DB down", base, nil, "critical", "prod", sqlmock.AnyArg(), correlationPolicy, correlationVersion, base, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_new", "evt_2", "new_incident", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	requestKey := investigationRequestKey("inc_new", "evt_2")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_new", 1, requestKey, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{db: db, now: func() time.Time { return base }, newIncidentID: func(time.Time) string { return "inc_new" }}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || !created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessNextEventRollsBackWhenOutboxInsertFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_3", "prometheus", "", "alert", base, "db", "prod", "high", "DB slow", "firing", []byte(`{}`)))
	mock.ExpectQuery("(?s)SELECT i.id.*FOR UPDATE OF i SKIP LOCKED").
		WithArgs("prod", base.Add(-15*time.Minute), base.Add(5*time.Minute), base.Add(-6*time.Hour)).
		WillReturnRows(sqlmock.NewRows(candidateColumns()))
	mock.ExpectExec("INSERT INTO incidents").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO investigation_outbox").WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	c := &Correlator{db: db, now: func() time.Time { return base }, newIncidentID: func(time.Time) string { return "inc_new" }}
	if _, _, err := c.processNextEvent(context.Background()); err == nil {
		t.Fatal("expected outbox error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimNoEventCommitsEmptyTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("FOR UPDATE SKIP LOCKED")).WillReturnRows(sqlmock.NewRows(eventColumns()))
	mock.ExpectCommit()
	c := &Correlator{db: db}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || processed || created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
}

func eventColumns() []string {
	return []string{"id", "source", "source_event_id", "type", "occurred_at", "service", "environment", "severity", "title", "status", "labels"}
}

func candidateColumns() []string {
	return []string{"id", "status", "started_at", "last_event_at", "resolved_at", "severity", "environment", "affected_services", "created_at", "service", "source", "source_event_id", "labels"}
}
