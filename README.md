# pamawas-correlator

**Deterministic Correlation Engine** — Time window + service/label grouping, no LLM involvement

Language: Go 1.26

## Purpose

Groups raw events from the `events` table into correlated `incidents` using deterministic rules only (time window, service/label overlap). This runs before any LLM involvement. Goal: 183 alerts → 2 incidents (MVP §8).

## MVP Reference

- **MVP §10 Build Order #2**: Correlator — group events into incidents by time window + service/label overlap
- **MVP §8 Correlation Engine (Deterministic)**: Time window (10 min), overlapping service/label/namespace
- **MVP §8 Architecture Overview**: Correlator (Go, deterministic) component

## Responsibilities

- Poll PostgreSQL for uncorrelated events
- Apply deterministic grouping: time window (configurable, default 10 min) + service/label overlap
- Write incidents to `incidents` table and links to `incident_events`
- Provide HTTP endpoints for health, manual trigger, status, metrics
- Background worker with configurable interval (default 1 min)
- Graceful shutdown handling

## Correlation Algorithm

1. Fetch all events not yet in `incident_events` table
2. Sort by timestamp ascending
3. Group consecutive events if they:
   - Fall within the time window (default 10 minutes) of the previous event in group
   - Share the same service name, OR
   - Share at least one label key-value pair
4. For each group, create an Incident with:
   - Title from first event
   - Severity = highest in group (critical > high > warning > info > debug)
   - Status = most severe (firing > resolved)
   - Affected services = unique services in group
   - Event IDs = all events in group
5. Persist incident and incident_events links

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Health check with DB connectivity |
| `/ready` | GET | Readiness check |
| `/trigger` | POST | Manual correlation trigger |
| `/status` | GET | Current status and last run info |
| `/metrics` | GET | Prometheus metrics |

## Configuration (Environment Variables)

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `PORT` | HTTP server port | `8080` |
| `CORRELATION_TIME_WINDOW` | Time window for grouping (e.g., 10m) | `10m` |
| `CORRELATION_INTERVAL` | Background worker interval | `1m` |
| `CORRELATOR_MODE` | `manual` to disable background worker | (auto) |
| `LOG_LEVEL` | Log level | `info` |

## Database Schema (from pamawas-schema)

```sql
-- Events table (written by pamawas-ingest)
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    service TEXT,
    environment TEXT,
    severity TEXT,
    title TEXT,
    status TEXT,
    labels JSONB,
    raw_payload JSONB
);

-- Incidents table (written by correlator)
CREATE TABLE IF NOT EXISTS incidents (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    severity TEXT,
    affected_services TEXT[]
);

-- Link table
CREATE TABLE IF NOT EXISTS incident_events (
    incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    PRIMARY KEY (incident_id, event_id)
);
```

## Current Implementation Status

- ✅ Core correlation engine with time window + service/label grouping
- ✅ Background worker with configurable interval
- ✅ Manual trigger endpoint (`/trigger`)
- ✅ Health (`/healthz`) and readiness (`/ready`) endpoints
- ✅ Status (`/status`) and metrics (`/metrics`) endpoints
- ✅ Graceful shutdown with context cancellation
- ✅ Multi-stage Dockerfile (Go 1.26-alpine builder, alpine runtime)
- ✅ GitHub Actions workflow (main + dev branches, GHCR publishing)
- ⬜ Prometheus metrics with proper labels
- ⬜ Structured JSON logging
- ⬜ Configuration management (YAML + ENV)
- ⬜ Unit tests for grouping logic (target 80%+ coverage)

## Kanban Tasks

- `t_0805522f` — Implement deterministic event grouping engine (backend-dev)
- `t_67d3f607` — Write unit tests for grouping logic (qa-dev)

## Dependencies

- **PostgreSQL** — events, incidents, incident_events tables (via pamawas-schema)
- **pamawas-schema** — Shared types and migrations (parent: `t_d1cdd7a9`)
- **pamawas-ingest** — Produces events to correlate

## Build & Run

```bash
# Local development
go run main.go

# Docker
docker build -t pamawas-correlator .
docker run -e DATABASE_URL="postgres://..." -p 8080:8080 pamawas-correlator

# Manual trigger
curl -X POST http://localhost:8080/trigger
```

## Testing

```bash
# Health check
curl http://localhost:8080/healthz

# Status
curl http://localhost:8080/status

# Metrics
curl http://localhost:8080/metrics
```