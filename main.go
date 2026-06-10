package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	standbyTimeout = 10 * time.Second
	dedupWindow    = 5 * time.Second
	originName     = "wal_observer"
)

// WalEvent is the payload sent to graphile_worker.
type WalEvent struct {
	Table string         `json:"table"`
	Op    string         `json:"op"`
	Row   map[string]any `json:"row,omitempty"`
	Old   map[string]any `json:"old,omitempty"`
}

// relation metadata from the WAL stream
type relInfo struct {
	schema  string
	name    string
	columns []string
}

// health tracks the observer's runtime status for the health endpoint
type health struct {
	streaming atomic.Bool
	processed atomic.Int64
	errors    atomic.Int64
	startedAt time.Time
}

func main() {
	databaseURL := requireEnv("DATABASE_URL")
	slotName := requireEnv("SLOT_NAME")
	publicationName := requireEnv("PUBLICATION_NAME")
	workerSchema := requireEnv("WORKER_SCHEMA")
	taskName := requireEnv("TASK_NAME")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received %s, shutting down...", sig)
		cancel()
	}()

	// Regular connection pool for calling add_job
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Verify worker add_job function exists
	var fnExists bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1 AND p.proname = 'add_job'
		)`, workerSchema,
	).Scan(&fnExists)
	if err != nil {
		log.Fatalf("Failed to check worker function: %v", err)
	}
	if !fnExists {
		log.Fatalf("Function %s.add_job does not exist — is graphile-worker installed?", workerSchema)
	}
	log.Printf("Verified %s.add_job exists", workerSchema)

	// Ensure replication origin exists (for indirect loop detection).
	// Workers that process wal_event jobs should call
	//   SET LOCAL pg_replication_origin_session = 'pg_observer'
	// so their writes are tagged and we can skip them here.
	ensureReplicationOrigin(ctx, pool)

	// Set origin on pool connections so our own add_job writes are tagged too
	setupPoolOrigin(ctx, pool)

	// Replication connection
	replURL := databaseURL
	if strings.Contains(replURL, "?") {
		replURL += "&replication=database"
	} else {
		replURL += "?replication=database"
	}
	replConn, err := pgconn.Connect(ctx, replURL)
	if err != nil {
		log.Fatalf("Failed to create replication connection: %v", err)
	}
	defer replConn.Close(ctx)

	// Create replication slot if it doesn't exist
	ensureReplicationSlot(ctx, pool, slotName)

	// Get the confirmed flush LSN from the slot to resume from
	startLSN := getConfirmedLSN(ctx, pool, slotName)

	sysident, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		log.Fatalf("IdentifySystem failed: %v", err)
	}
	log.Printf("System: %s, timeline: %d, xlogpos: %s", sysident.DBName, sysident.Timeline, sysident.XLogPos)

	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", publicationName),
		"messages 'true'",
		// Request origin info so we can filter out our own writes
		"origin 'any'",
	}

	err = pglogrepl.StartReplication(ctx, replConn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		log.Fatalf("StartReplication failed: %v", err)
	}

	log.Printf("Streaming from slot=%s publication=%s startLSN=%s", slotName, publicationName, startLSN)

	// Health check server
	h := &health{startedAt: time.Now()}
	h.streaming.Store(true)
	healthPort := requireEnv("PORT")
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"status":    "ok",
				"streaming": h.streaming.Load(),
				"processed": h.processed.Load(),
				"errors":    h.errors.Load(),
				"uptime_s":  int(time.Since(h.startedAt).Seconds()),
			})
		})
		log.Printf("Health server listening on :%s", healthPort)
		if err := http.ListenAndServe(":"+healthPort, mux); err != nil {
			log.Printf("Health server error: %v", err)
		}
	}()

	relations := map[uint32]*relInfo{}
	dedupCache := map[string]time.Time{}
	nextStandby := time.Now().Add(standbyTimeout)
	lastLSN := startLSN
	skipCurrentTx := false // set when we see an Origin message matching ours
	stats := struct{ processed, deduped, originSkipped, errors int }{}

	for ctx.Err() == nil {
		if time.Now().After(nextStandby) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: lastLSN + 1,
			})
			if err != nil {
				log.Printf("SendStandbyStatusUpdate error: %v", err)
			}
			nextStandby = time.Now().Add(standbyTimeout)

			// Clean expired dedup entries
			cutoff := time.Now().Add(-dedupWindow)
			for k, t := range dedupCache {
				if t.Before(cutoff) {
					delete(dedupCache, k)
				}
			}
		}

		recvCtx, recvCancel := context.WithDeadline(ctx, nextStandby)
		rawMsg, err := replConn.ReceiveMessage(recvCtx)
		recvCancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				break
			}
			log.Fatalf("ReceiveMessage error: %v", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Printf("ParseXLogData error: %v", err)
					continue
				}
				if xld.WALStart > lastLSN {
					lastLSN = xld.WALStart
				}

				event, isOrigin, isBegin := processWALData(xld.WALData, relations)

				// Defense 1: Origin-based filtering.
				// An Origin message arrives after Begin but before data messages.
				// If it matches our origin name, skip all data in this transaction.
				if isOrigin {
					skipCurrentTx = true
					continue
				}

				// Reset skip flag on new transaction
				if isBegin {
					skipCurrentTx = false
					continue
				}

				if event == nil {
					continue
				}

				// Check if this transaction should be skipped (origin match)
				if skipCurrentTx {
					stats.originSkipped++
					continue
				}

				pk := primaryKey(event)

				// Defense 2: Dedup — skip if same table+op+pk was seen recently
				dedupKey := fmt.Sprintf("%s:%s:%s", event.Table, event.Op, pk)
				if lastSeen, ok := dedupCache[dedupKey]; ok && time.Since(lastSeen) < dedupWindow {
					stats.deduped++
					continue
				}
				dedupCache[dedupKey] = time.Now()

				// Defense 3: Job key — graphile worker deduplicates by key.
				// If a job with this key is already queued/running, add_job is a no-op.
				jobKey := fmt.Sprintf("wal:%s:%s", event.Table, pk)
				payload, _ := json.Marshal(event)
				addJobSQL := fmt.Sprintf("SELECT %s.add_job($1, $2::json, job_key := $3)", workerSchema)
				_, err = pool.Exec(ctx, addJobSQL, taskName, string(payload), jobKey)
				if err != nil {
					stats.errors++
					h.errors.Add(1)
					log.Printf("add_job error: %v", err)
					continue
				}

				stats.processed++
				h.processed.Add(1)
				log.Printf("[%s] %s pk=%s (processed=%d deduped=%d origin_skipped=%d errors=%d)",
					event.Op, event.Table, pk,
					stats.processed, stats.deduped, stats.originSkipped, stats.errors)

			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Printf("ParsePrimaryKeepaliveMessage error: %v", err)
					continue
				}
				if pkm.ServerWALEnd > lastLSN {
					lastLSN = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					nextStandby = time.Time{} // send immediately
				}
			}
		}
	}

	h.streaming.Store(false)

	// Final ACK
	_ = pglogrepl.SendStandbyStatusUpdate(context.Background(), replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lastLSN + 1,
	})
	log.Printf("Stopped. processed=%d deduped=%d origin_skipped=%d errors=%d",
		stats.processed, stats.deduped, stats.originSkipped, stats.errors)
}

// ensureReplicationOrigin creates the replication origin if it doesn't exist.
func ensureReplicationOrigin(ctx context.Context, pool *pgxpool.Pool) {
	var exists bool
	err := pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_origin WHERE roname = $1)", originName,
	).Scan(&exists)
	if err != nil {
		log.Fatalf("Failed to check replication origin: %v", err)
	}
	if !exists {
		_, err = pool.Exec(ctx,
			"SELECT pg_replication_origin_create($1)", originName,
		)
		if err != nil {
			log.Fatalf("Failed to create replication origin: %v", err)
		}
		log.Printf("Created replication origin '%s'", originName)
	}
}

// setupPoolOrigin configures pool connections to tag writes with our origin.
// This way our add_job writes are marked, so if the jobs table were ever
// in the publication, we'd skip our own events.
func setupPoolOrigin(ctx context.Context, pool *pgxpool.Pool) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire connection for origin setup: %v", err)
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, "SELECT pg_replication_origin_session_setup($1)", originName)
	if err != nil {
		// Non-fatal — origin tagging is a defense layer, not critical
		log.Printf("Warning: could not set replication origin on pool: %v", err)
	}
}

// ensureReplicationSlot creates the logical replication slot if it doesn't exist.
func ensureReplicationSlot(ctx context.Context, pool *pgxpool.Pool, slotName string) {
	var exists bool
	err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slotName,
	).Scan(&exists)
	if err != nil {
		log.Fatalf("Failed to check replication slot: %v", err)
	}
	if exists {
		return
	}
	log.Printf("Creating replication slot %s...", slotName)
	_, err = pool.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
		slotName,
	)
	if err != nil {
		log.Fatalf("Failed to create replication slot: %v", err)
	}
	log.Printf("Created replication slot %s", slotName)
}

// getConfirmedLSN reads the slot's confirmed_flush_lsn so we resume
// from where we left off instead of replaying everything.
func getConfirmedLSN(ctx context.Context, pool *pgxpool.Pool, slotName string) pglogrepl.LSN {
	var lsnStr *string
	err := pool.QueryRow(ctx,
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&lsnStr)
	if err != nil || lsnStr == nil {
		log.Printf("Could not read confirmed_flush_lsn for slot %s, starting from 0", slotName)
		return 0
	}
	lsn, err := pglogrepl.ParseLSN(*lsnStr)
	if err != nil {
		log.Printf("Could not parse LSN '%s': %v, starting from 0", *lsnStr, err)
		return 0
	}
	log.Printf("Resuming from confirmed LSN: %s", lsn)
	return lsn
}

// processWALData parses a WAL message. Returns:
//   - event: non-nil for INSERT/UPDATE/DELETE
//   - isOrigin: true if this is an Origin message matching our origin (skip tx)
//   - isBegin: true if this is a Begin message (reset tx state)
func processWALData(walData []byte, relations map[uint32]*relInfo) (event *WalEvent, isOrigin bool, isBegin bool) {
	msg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		log.Printf("ParseV2 error: %v", err)
		return nil, false, false
	}

	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		return nil, false, true

	case *pglogrepl.OriginMessage:
		if m.Name == originName {
			log.Printf("Skipping transaction from origin '%s'", originName)
			return nil, true, false
		}
		return nil, false, false

	case *pglogrepl.RelationMessageV2:
		cols := make([]string, len(m.Columns))
		for i, c := range m.Columns {
			cols[i] = c.Name
		}
		relations[m.RelationID] = &relInfo{
			schema:  m.Namespace,
			name:    m.RelationName,
			columns: cols,
		}
		return nil, false, false

	case *pglogrepl.InsertMessageV2:
		rel := relations[m.RelationID]
		if rel == nil {
			return nil, false, false
		}
		return &WalEvent{
			Table: rel.schema + "." + rel.name,
			Op:    "insert",
			Row:   decodeTuple(m.Tuple, rel),
		}, false, false

	case *pglogrepl.UpdateMessageV2:
		rel := relations[m.RelationID]
		if rel == nil {
			return nil, false, false
		}
		ev := &WalEvent{
			Table: rel.schema + "." + rel.name,
			Op:    "update",
			Row:   decodeTuple(m.NewTuple, rel),
		}
		if m.OldTuple != nil {
			ev.Old = decodeTuple(m.OldTuple, rel)
		}
		return ev, false, false

	case *pglogrepl.DeleteMessageV2:
		rel := relations[m.RelationID]
		if rel == nil {
			return nil, false, false
		}
		return &WalEvent{
			Table: rel.schema + "." + rel.name,
			Op:    "delete",
			Old:   decodeTuple(m.OldTuple, rel),
		}, false, false

	default:
		return nil, false, false
	}
}

func decodeTuple(tuple *pglogrepl.TupleData, rel *relInfo) map[string]any {
	if tuple == nil {
		return nil
	}
	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.columns) {
			break
		}
		name := rel.columns[i]
		switch col.DataType {
		case pglogrepl.TupleDataTypeText:
			row[name] = string(col.Data)
		case pglogrepl.TupleDataTypeNull:
			row[name] = nil
		default:
			if len(col.Data) > 0 {
				row[name] = string(col.Data)
			}
		}
	}
	return row
}

func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Required environment variable %s is not set", key)
	}
	return val
}

func primaryKey(event *WalEvent) string {
	row := event.Row
	if row == nil {
		row = event.Old
	}
	if row == nil {
		return ""
	}
	for _, col := range []string{"id", "ID"} {
		if v, ok := row[col]; ok {
			return fmt.Sprintf("%v", v)
		}
	}
	for _, v := range row {
		return fmt.Sprintf("%v", v)
	}
	return ""
}
