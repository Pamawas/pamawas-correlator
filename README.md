# pamawas-correlator

**Deterministic Correlation Engine** — Groups raw events into incidents using the frozen `pamawas-correlation-v1` policy. No LLM involved.

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)](https://docker.com/)

---

## Purpose

Reduces alert noise by correlating related events into single incidents. Runs **before** any LLM investigation. Target: 183 raw alerts → 2 correlated incidents.

## Correlation Algorithm (v1)

For each unassigned event, the worker claims it with `FOR UPDATE SKIP LOCKED` and evaluates candidate open incidents:

1. **Candidate selection**: Only incidents in the same environment with status `open` or `investigating`.
2. **Adjacency window**: Event `occurred_at` must be ≤ 15 minutes after `last_event_at` and ≤ 5 minutes before it (allowing bounded out-of-order delivery).
3. **Maximum incident span**: Event must be ≤ 6 hours after `started_at`.
4. **Scoring**:
   - Explicit source identity (`source` + `source_event_id` lineage): **100**
   - Same `service`: **50**
   - Identity labels: `alert_rule` **40**, `namespace` **20**, `workload` **20**, `instance` **20**
   - Weak labels (`environment`, `cluster`, `team`, `region`, `severity`): **0** (cannot independently join incidents)
5. **Threshold**: Score must be ≥ 50.
6. **Tie-break**: Highest score → most recent `last_event_at` → earliest `created_at` → lexicographically smallest incident ID.
7. **Resolution**: A `resolved` event may resolve an incident only when it has strong source identity, or `service` plus one matching identity label. Sets `resolved_at` to event `occurred_at` (never moves it backward).
8. **Transactional persistence**: Incident creation/update, `incident_events` membership (with `correlation_reason` and `policy_version`), and exactly one `investigation_outbox` record are committed atomically. Dispatch occurs only after commit.
9. **If no candidate matches**: Create a new incident with status `open`, `resolved_at = NULL`, `last_event_at = started_at = event.occurred_at`.

## Quick Start

```bash
# Docker
docker run -e DATABASE_URL="postgres://user:***@host:5432/db" \
  -p 8080:8080 ghcr.io/yoganovvaindra/pamawas-correlator:latest

# Local development
go run main.go

# Manual trigger
curl -X POST http://localhost:8080/trigger
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | **Required** |
| `PORT` | HTTP server port | `8080` |
| `CORRELATION_TIME_WINDOW` | Grouping time window (e.g., 10m, 15m) | `10m` |
| `CORRELATION_INTERVAL` | Background worker interval | `1m` |
| `CORRELATOR_MODE` | Set to `manual` to disable background worker | `auto` |
| `LOG_LEVEL` | debug, info, warn, error | `info` |
| `ENVIRONMENT` | deployment, staging, production | `development` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Tempo OTLP gRPC endpoint | `tempo:4317` |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check with DB connectivity |
| `/ready` | GET | Readiness probe |
| `/trigger` | POST | Manually trigger correlation run |
| `/status` | GET | Current status and last run info |
| `/metrics` | GET | Prometheus metrics |

## Database Schema (v1)

```sql
-- Events (written by pamawas-ingest)
CREATE TABLE events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_event_id TEXT,
    fingerprint TEXT,
    type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    service TEXT,
    environment TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('debug','info','warning','high','critical')),
    title TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('firing','resolved','informational')),
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_payload JSONB,
    schema_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_event_id)
);

-- Incidents (written by correlator)
CREATE TABLE incidents (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open','investigating','resolved','suppressed')),
    started_at TIMESTAMPTZ NOT NULL,
    last_event_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    severity TEXT NOT NULL CHECK (severity IN ('debug','info','warning','high','critical')),
    environment TEXT NOT NULL,
    affected_services TEXT[] NOT NULL DEFAULT '{}',
    correlation_policy TEXT NOT NULL,
    correlation_version INTEGER NOT NULL CHECK (correlation_version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'resolved') = (resolved_at IS NOT NULL)),
    CHECK (last_event_at >= started_at)
);

-- Links (with correlation reason and policy version)
CREATE TABLE incident_events (
    incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE RESTRICT,
    correlation_reason TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK (policy_version > 0),
    attached_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (incident_id, event_id),
    UNIQUE (event_id, policy_version)
);

-- Investigation dispatch outbox (inserted in same transaction)
CREATE TABLE investigation_outbox (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    contract_version INTEGER NOT NULL CHECK (contract_version > 0),
    request_key_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending','leased','delivered','retryable','failed_terminal')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    safe_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Observability

| Feature | Endpoint |
|---------|----------|
| Prometheus Metrics | `/metrics` — `CorrelationCyclesTotal`, `EventsProcessedTotal`, `IncidentsCreatedTotal`, `CycleDuration` |
| JSON Logging | stdout — trace_id, span_id, service, method, path, status_code, duration_ms |
| OpenTelemetry | OTLP gRPC → Tempo:4317 |

## Building

```bash
docker build -t pamawas-correlator .
go build -o pamawas-correlator main.go
```

## Testing

```bash
# Unit tests
go test ./service -v

# With race detector
go test -race ./service

# All packages
go test ./... -count=1
```

## Related

- **Root README**: [../README.md](../README.md)
- **Ingest**: [../pamawas-ingest/README.md](../pamawas-ingest/README.md)
- **Investigator**: [../pamawas-investigator/README.md](../pamawas-investigator/README.md)
- **Database Schema**: [../pamawas-schema/README.md](../pamawas-schema/README.md)
- **Normative contract**: [../docs/system-design/domain-data-model.md](../docs/system-design/domain-data-model.md)
- **Decision log**: [../docs/system-design/decision-log-v1.md](../docs/system-design/decision-log-v1.md)