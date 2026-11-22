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

// TestEventIdentitySeparatesAlertsThatShareAServiceAndTitle pins the defect
// that made incidents merge across hosts. koba-labs "Monitoring Target Down"
// on docker_cadvisor and on mariadb share both service and title; Grafana
// distinguishes them by fingerprint, and so must the correlator.
func TestEventIdentitySeparatesAlertsThatShareAServiceAndTitle(t *testing.T) {
	base := models.Event{Service: "koba-labs", Title: "Monitoring Target Down", Environment: "production"}

	cadvisor := base
	cadvisor.Labels = map[string]string{"grafana_fingerprint": "7aaa5e768ad620f4"}
	mariadb := base
	mariadb.Labels = map[string]string{"grafana_fingerprint": "ccfb7cc2cb3a3c54"}

	if eventIdentity(cadvisor) == eventIdentity(mariadb) {
		t.Fatalf("different alerts on the same host must not share an identity: %q", eventIdentity(cadvisor))
	}
}

// TestEventIdentityPairsFiringWithResolution is the property the whole design
// rests on: a firing and its resolution describe the same condition.
func TestEventIdentityPairsFiringWithResolution(t *testing.T) {
	labels := map[string]string{"grafana_fingerprint": "5ef6d3ea2e13d8e8"}
	firing := models.Event{Service: "koba-labs", Title: "RAM", Status: "firing", Labels: labels}
	resolved := models.Event{Service: "koba-labs", Title: "RAM", Status: "resolved", Labels: labels}

	if eventIdentity(firing) != eventIdentity(resolved) {
		t.Fatalf("firing identity %q != resolved identity %q", eventIdentity(firing), eventIdentity(resolved))
	}
}

// TestEventIdentityFallsBackToServiceAndTitle covers sources that carry no
// fingerprint. The fallback is coarser but still deterministic, so it never
// merges two alerts that differ in either field.
func TestEventIdentityFallsBackToServiceAndTitle(t *testing.T) {
	a := models.Event{Service: "api", Title: "Latency", Labels: map[string]string{}}
	b := models.Event{Service: "api", Title: "Errors", Labels: map[string]string{}}
	c := models.Event{Service: "web", Title: "Latency", Labels: map[string]string{}}

	if eventIdentity(a) != eventIdentity(models.Event{Service: " API ", Title: "latency"}) {
		t.Fatal("fallback identity must normalise case and whitespace")
	}
	if eventIdentity(a) == eventIdentity(b) || eventIdentity(a) == eventIdentity(c) {
		t.Fatal("different title or service must not share an identity")
	}
}

func TestProcessNextEventExtendsAndResolvesOpenIncident(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	labels := []byte(`{"grafana_fingerprint":"fp-1"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_1", "grafana", "", "alert", base, "api", "prod", "high", "API latency", "resolved", labels))
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WithArgs("prod", "fp:fp-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "severity", "affected_services"}).AddRow("inc_1", "open", "warning", `{"api"}`))
	mock.ExpectExec("(?s)UPDATE incidents SET status = \\$1, last_event_at = GREATEST.*WHERE id = \\$6").
		WithArgs("resolved", base, "high", `{"api"}`, base, "inc_1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_1", "evt_1", "resolution_of_open_incident", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_1", 1, "resolved_incident", "inc_1", base).
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
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WithArgs("prod", "cond:db|db down").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO incidents").
		WithArgs("inc_new", "DB down", "cond:db|db down", base, "critical", "prod", sqlmock.AnyArg(), correlationPolicy, correlationVersion, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_new", "evt_2", "new_incident", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_2", 1, "new_incident", "inc_new", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	requestKey := investigationRequestKey("inc_new", "evt_2")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_new", 1, requestKey, base, "incident_created", base).
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

// TestResolutionWithoutOpenIncidentCreatesNoIncident is the regression test for
// the contradiction the daily digest showed: one condition listed as open
// "since 20:52" while the recovered section said it was down 15 minutes. The
// cause was a resolution with no open incident creating a new open incident.
//
// It also pins the operator contract: an unlinked resolution is recorded and
// counted, never silently dropped, because in steady state it should not
// happen at all.
func TestResolutionWithoutOpenIncidentCreatesNoIncident(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	labels := []byte(`{"grafana_fingerprint":"fp-orphan"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_9", "grafana", "", "alert", base, "api", "prod", "high", "API latency", "resolved", labels))
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WithArgs("prod", "fp:fp-orphan").
		WillReturnError(sql.ErrNoRows)
	// No INSERT INTO incidents, no INSERT INTO incident_events, no outbox.
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_9", 1, "ignored", "", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{db: db, now: func() time.Time { return base }, newIncidentID: func(time.Time) string { return "inc_must_not_exist" }}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestExtendingAnIncidentDoesNotRewriteItsIdentity guards the column that made
// incidents drift: a later event may move status, last-seen time and severity,
// but never title or identity_key.
func TestExtendingAnIncidentDoesNotRewriteItsIdentity(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_3", "prometheus", "", "alert", base, "db", "prod", "high", "DB slow", "firing", []byte(`{}`)))
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WithArgs("prod", "cond:db|db slow").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "severity", "affected_services"}).AddRow("inc_1", "investigating", "warning", `{"db"}`))
	mock.ExpectExec("(?s)UPDATE incidents SET status = \\$1, last_event_at = GREATEST.*WHERE id = \\$6").
		WithArgs("investigating", base, "high", `{"db"}`, base, "inc_1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_1", "evt_3", "same_condition", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_3", 1, "extended_incident", "inc_1", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	requestKey := investigationRequestKey("inc_1", "evt_3")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_1", 1, requestKey, base, "incident_created", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{db: db, now: func() time.Time { return base }}
	if _, created, err := c.processNextEvent(context.Background()); err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
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
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_4", "prometheus", "", "alert", base, "db", "prod", "high", "DB slow", "firing", []byte(`{}`)))
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO incidents").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").WillReturnResult(sqlmock.NewResult(0, 1))
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

func TestComputeOutboxBackoffExponentialAndCapped(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, time.Minute},
		{1, 2 * time.Minute},
		{2, 4 * time.Minute},
		{3, 8 * time.Minute},
		{4, 16 * time.Minute},
		{5, 32 * time.Minute},
		{6, 60 * time.Minute}, // capped at 1 hour
		{7, 60 * time.Minute},
		{100, 60 * time.Minute}, // very large attempts must not overflow
	}
	for _, tc := range cases {
		got := computeOutboxBackoff(tc.attempts)
		if got != tc.want {
			t.Fatalf("computeOutboxBackoff(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
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

func TestInitialAffectedServices(t *testing.T) {
	// Captures entity target and service
	e1 := models.Event{Service: "my-svc", Labels: map[string]string{"pod": "my-pod"}}
	if got := initialAffectedServices(e1); len(got) != 2 || got[0] != "my-pod" || got[1] != "my-svc" {
		t.Fatalf("expected [my-pod my-svc], got %v", got)
	}

	// Falls back to pod label when service is empty
	e2 := models.Event{Service: "", Labels: map[string]string{"pod": "my-pod"}}
	if got := initialAffectedServices(e2); len(got) != 1 || got[0] != "my-pod" {
		t.Fatalf("expected [my-pod], got %v", got)
	}

	// Falls back to instance
	e3 := models.Event{Service: "", Labels: map[string]string{"instance": "10.0.0.1"}}
	if got := initialAffectedServices(e3); len(got) != 1 || got[0] != "10.0.0.1" {
		t.Fatalf("expected [10.0.0.1], got %v", got)
	}

	// Empty when no candidate labels
	e4 := models.Event{Service: "", Labels: map[string]string{}}
	if got := initialAffectedServices(e4); len(got) != 0 {
		t.Fatalf("expected [], got %v", got)
	}
}

func TestSeverityRankAndMaxSeverity(t *testing.T) {
	cases := []struct {
		current  string
		incoming string
		wantMax  string
	}{
		{"warning", "critical", "critical"},
		{"critical", "warning", "critical"},
		{"critical", "critical", "critical"},
		{"low", "medium", "medium"},
		{"info", "high", "high"},
		{"medium", "warning", "medium"},
		{"", "critical", "critical"},
		{"high", "", "high"},
		{"UNKNOWN", "warning", "warning"},
		{"critical", "high", "critical"},
	}

	for _, tc := range cases {
		got := maxSeverity(tc.current, tc.incoming)
		if got != tc.wantMax {
			t.Fatalf("maxSeverity(%q, %q) = %q, want %q", tc.current, tc.incoming, got, tc.wantMax)
		}
	}
}

func TestMergeAffectedServices(t *testing.T) {
	// Deduplicates case-insensitively and preserves order
	existing := []string{"docker_cadvisor", "mariadb"}
	incoming := []string{"MARIADB", "redis", "  ", ""}
	merged := mergeAffectedServices(existing, incoming)

	expected := []string{"docker_cadvisor", "mariadb", "redis"}
	if len(merged) != len(expected) {
		t.Fatalf("expected len %d, got %d (%v)", len(expected), len(merged), merged)
	}
	for i, v := range expected {
		if merged[i] != v {
			t.Fatalf("at index %d expected %q, got %q", i, v, merged[i])
		}
	}

	// Empty inputs
	if got := mergeAffectedServices(nil, nil); len(got) != 0 {
		t.Fatalf("expected empty slice, got %v", got)
	}
}

func TestProcessNextEventSteadyStateScrapeThrottlesOutbox(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	labels := []byte(`{"grafana_fingerprint":"fp-steady"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_steady", "grafana", "", "alert", base, "api", "prod", "critical", "API latency", "firing", labels))
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WithArgs("prod", "fp:fp-steady").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "severity", "affected_services"}).AddRow("inc_steady", "investigating", "critical", `{"api"}`))
	// Updates incident with same severity and merged services
	mock.ExpectExec("(?s)UPDATE incidents SET status = \\$1, last_event_at = GREATEST.*WHERE id = \\$6").
		WithArgs("investigating", base, "critical", `{"api"}`, base, "inc_steady").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_steady", "evt_steady", "same_condition", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_steady", 1, "extended_incident", "inc_steady", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Crucial: NO INSERT INTO investigation_outbox expectation, because severity did not escalate
	mock.ExpectCommit()

	c := &Correlator{db: db, now: func() time.Time { return base }}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessNextEventSeverityEscalationTriggersOutbox(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	labels := []byte(`{"grafana_fingerprint":"fp-escalate"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_esc", "grafana", "", "alert", base, "api", "prod", "critical", "API latency", "firing", labels))
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*FOR UPDATE").
		WithArgs("prod", "fp:fp-escalate").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "severity", "affected_services"}).AddRow("inc_esc", "investigating", "warning", `{"api"}`))
	// Updates incident with escalated severity (warning -> critical)
	mock.ExpectExec("(?s)UPDATE incidents SET status = \\$1, last_event_at = GREATEST.*WHERE id = \\$6").
		WithArgs("investigating", base, "critical", `{"api"}`, base, "inc_esc").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_esc", "evt_esc", "same_condition", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_esc", 1, "extended_incident", "inc_esc", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Outbox entry MUST be queued because severity escalated
	requestKey := investigationRequestKey("inc_esc", "evt_esc")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_esc", 1, requestKey, base, "incident_created", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{db: db, now: func() time.Time { return base }}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessNextEventFlappingCooldownReopensResolvedIncident(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 10, 0, 0, time.UTC)
	labels := []byte(`{"grafana_fingerprint":"fp-flap"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_flap", "grafana", "", "alert", base, "cadvisor", "prod", "critical", "Target Down", "firing", labels))
	// 1. Open incident query returns nothing
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*status IN \\('open', 'investigating'\\).*FOR UPDATE").
		WithArgs("prod", "fp:fp-flap").
		WillReturnError(sql.ErrNoRows)
	// 2. Resolved incident query within 15m cooldown window finds the recently resolved incident
	resolvedAt := base.Add(-5 * time.Minute)
	cutoff := base.Add(-15 * time.Minute)
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*status = 'resolved'.*FOR UPDATE").
		WithArgs("prod", "fp:fp-flap", cutoff).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "severity", "affected_services"}).AddRow("inc_flap", "resolved", "critical", `{"cadvisor"}`))
	// 3. Incident is re-opened: status -> 'open', resolved_at -> NULL
	mock.ExpectExec("(?s)UPDATE incidents SET status = \\$1, last_event_at = GREATEST.*WHERE id = \\$6").
		WithArgs("open", base, "critical", `{"cadvisor"}`, base, "inc_flap").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 4. Incident event recorded with reason 'flapping_reopened'
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_flap", "evt_flap", "flapping_reopened", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 5. Decision recorded as extended_incident
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_flap", 1, "extended_incident", "inc_flap", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 6. Outbox dispatched for renewed investigation of reopened flap
	requestKey := investigationRequestKey("inc_flap", "evt_flap")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_flap", 1, requestKey, base, "incident_created", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{
		db:                     db,
		now:                    func() time.Time { return base },
		flappingCooldownWindow: 15 * time.Minute,
	}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	_ = resolvedAt
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessNextEventFlappingCooldownExpiredCreatesNewIncident(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2026, 8, 19, 10, 30, 0, 0, time.UTC)
	labels := []byte(`{"grafana_fingerprint":"fp-expired"}`)
	mock.ExpectBegin()
	mock.ExpectQuery("(?s)SELECT e.id.*FOR UPDATE SKIP LOCKED.*LIMIT 1").
		WillReturnRows(sqlmock.NewRows(eventColumns()).AddRow("evt_exp", "grafana", "", "alert", base, "cadvisor", "prod", "critical", "Target Down", "firing", labels))
	// 1. Open incident query returns nothing
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*status IN \\('open', 'investigating'\\).*FOR UPDATE").
		WithArgs("prod", "fp:fp-expired").
		WillReturnError(sql.ErrNoRows)
	// 2. Resolved incident query returns nothing (outside cooldown window)
	cutoff := base.Add(-15 * time.Minute)
	mock.ExpectQuery("(?s)SELECT id, status, severity, COALESCE.*FROM incidents.*status = 'resolved'.*FOR UPDATE").
		WithArgs("prod", "fp:fp-expired", cutoff).
		WillReturnError(sql.ErrNoRows)
	// 3. New incident is created
	mock.ExpectExec("INSERT INTO incidents").
		WithArgs("inc_new", "Target Down", "fp:fp-expired", base, "critical", "prod", sqlmock.AnyArg(), correlationPolicy, correlationVersion, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO incident_events").
		WithArgs("inc_new", "evt_exp", "new_incident", 1, base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO correlation_decisions").
		WithArgs("evt_exp", 1, "new_incident", "inc_new", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	requestKey := investigationRequestKey("inc_new", "evt_exp")
	mock.ExpectExec("INSERT INTO investigation_outbox").
		WithArgs(outboxID(requestKey), "inc_new", 1, requestKey, base, "incident_created", base).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	c := &Correlator{
		db:                     db,
		now:                    func() time.Time { return base },
		newIncidentID:          func(time.Time) string { return "inc_new" },
		flappingCooldownWindow: 15 * time.Minute,
	}
	processed, created, err := c.processNextEvent(context.Background())
	if err != nil || !processed || !created {
		t.Fatalf("processed=%v created=%v err=%v", processed, created, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
