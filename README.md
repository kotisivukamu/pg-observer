# pg-observer

A Go binary that streams PostgreSQL WAL (Write-Ahead Log) changes and writes them as jobs to graphile-worker. It acts as a dumb bridge — all business logic lives in the worker tasks.

## How it works

1. Connects to PostgreSQL using the logical replication protocol
2. Streams changes from a publication (`notification_pub`) via a replication slot
3. For each INSERT/UPDATE/DELETE, calls `graphile_worker.add_job('wal_event', payload)`
4. The `wal_event` task in the worker router decides what to do

## Configuration

All environment variables are required (no defaults):

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `SLOT_NAME` | Replication slot name |
| `PUBLICATION_NAME` | Publication to subscribe to |
| `WORKER_SCHEMA` | Graphile worker schema name |
| `TASK_NAME` | Task name to queue (typically `wal_event`) |
| `PORT` | Health check HTTP port |

## Prerequisites

Tables with `WHERE` row filters on non-PK columns need a replica identity that includes the filtered column:

```sql
CREATE UNIQUE INDEX table_id_col ON table (id, filtered_column);
ALTER TABLE table REPLICA IDENTITY USING INDEX table_id_col;
```

### Database user setup

Create a dedicated `cdc_user` with replication and the necessary grants:

```sql
CREATE USER cdc_user WITH REPLICATION BYPASSRLS PASSWORD 'strong_password';

-- Read access
GRANT USAGE ON SCHEMA public, auth TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public, auth TO cdc_user;

-- Graphile worker schemas (needs write for add_job)
GRANT USAGE ON SCHEMA graphile_worker, graphile_worker_builder TO cdc_user;
GRANT ALL ON ALL TABLES IN SCHEMA graphile_worker, graphile_worker_builder TO cdc_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA graphile_worker, graphile_worker_builder TO cdc_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA graphile_worker, graphile_worker_builder TO cdc_user;

-- Future tables automatically get SELECT
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA auth GRANT SELECT ON TABLES TO cdc_user;
```

## Deployment

Runs on Fly.io as a single instance (`max_machines_running = 1`). Only one consumer can use a replication slot at a time.

Deploys automatically on push to `main` when files in `pg-observer/` change (`.github/workflows/deploy-pg-observer.yml`).

## Health check

`GET /health` returns:

```json
{
  "status": "ok",
  "streaming": true,
  "processed": 42,
  "errors": 0,
  "uptime_s": 3600
}
```

## Loop prevention

Three layers prevent infinite loops (e.g., observer writes a job, which triggers a notification insert, which triggers another job):

1. **Dedup window** (5s) — same table+op+pk skipped if seen recently
2. **Job keys** — graphile-worker deduplicates by key
3. **Router filtering** — `wal_event` router only handles specific table+op combinations

## Adding new event sources

See the `/wal-event` skill or add a table to the publication via a database migration, then add a `case` to `worker/src/tasks/wal_event.ts`.
