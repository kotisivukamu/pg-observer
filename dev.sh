#!/usr/bin/env bash
# Run pg-observer locally for development
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/kotisivukamu"
export SLOT_NAME="notification_slot"
export PUBLICATION_NAME="notification_pub"
export WORKER_SCHEMA="graphile_worker"
export TASK_NAME="wal_event"
export PORT="8400"

exec ./pg-observer
