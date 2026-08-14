# pamawas-correlator

Deterministic correlation engine (time window, service/label grouping)

Language: Go

## Purpose
Groups raw events into incidents based on time windows and overlapping service/label information.

## Responsibilities
- Read events from PostgreSQL
- Apply deterministic correlation rules
- Write incident groups to PostgreSQL
- Provide health/metrics endpoints

## TODO
- Implement Go service
- Define correlation algorithms
- Connect to PostgreSQL
- Add logging and metrics

