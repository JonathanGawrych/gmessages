// SPDX-License-Identifier: AGPL-3.0-or-later

package gmdb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
)

func openIncomingTestDatabase(t *testing.T, dialect string) *GMDB {
	t.Helper()
	var raw *sql.DB
	var err error
	if dialect == "postgres" {
		dsn := os.Getenv("GMESSAGES_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("set GMESSAGES_TEST_POSTGRES_DSN to an isolated gmessages_inbox_test database")
		}
		if strings.Contains(dsn, "://") {
			t.Fatal("PostgreSQL test DSN must use keyword=value format")
		}
		admin, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = admin.Close() })
		var databaseName string
		if err = admin.QueryRow(`SELECT current_database()`).Scan(&databaseName); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(databaseName, "gmessages_inbox_test") {
			t.Fatal("refusing database without gmessages_inbox_test name prefix")
		}
		var random [12]byte
		if _, err = rand.Read(random[:]); err != nil {
			t.Fatal(err)
		}
		schema := fmt.Sprintf("gmessages_inbox_test_%x", random)
		if _, err = admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
				t.Error(err)
			}
		})
		raw, err = sql.Open("postgres", dsn+" search_path="+schema)
		if err != nil {
			t.Fatal(err)
		}
		raw.SetMaxOpenConns(8)
	} else {
		raw, err = sql.Open("sqlite3", filepath.Join(t.TempDir(), "inbox.db")+"?_foreign_keys=on&_busy_timeout=10000")
		if err != nil {
			t.Fatal(err)
		}
		raw.SetMaxOpenConns(1)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if _, err = raw.Exec(`CREATE TABLE user_login (bridge_id TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY (bridge_id, id))`); err != nil {
		t.Fatal(err)
	}
	wrapped, err := dbutil.NewWithDB(raw, dialect)
	if err != nil {
		t.Fatal(err)
	}
	db := New(wrapped, zerolog.Nop())
	// Exercise the installed v1 schema before upgrading the fixture to v2.
	db.UpgradeTable = table[:1]
	if err = db.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.GetLoginPrefix(context.Background(), "fixture"); err != nil {
		t.Fatal(err)
	}
	db.UpgradeTable = table
	if err = db.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(context.Background(), `INSERT INTO user_login (bridge_id, id) VALUES ('bridge', 'login'), ('bridge', 'other')`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestIncomingStoreSQLite(t *testing.T)   { testIncomingStore(t, "sqlite3") }
func TestIncomingStorePostgres(t *testing.T) { testIncomingStore(t, "postgres") }

func testIncomingStore(t *testing.T, dialect string) {
	ctx := context.Background()
	db := openIncomingTestDatabase(t, dialect)
	store := &IncomingRPCStore{DB: db, BridgeID: "bridge", LoginID: "login"}
	queued := func(id string, streams ...string) *libgm.QueuedRPC {
		return &libgm.QueuedRPC{ResponseID: id, Digest: sha256.Sum256([]byte(id)), Payload: []byte("synthetic fixture"), Streams: streams}
	}
	count := func(tableName string) int {
		t.Helper()
		var count int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM `+tableName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	complete := func(stream string) {
		t.Helper()
		pending, err := store.Next(ctx, stream)
		if err != nil || pending == nil {
			t.Fatalf("next %s: %v / %v", stream, pending, err)
		}
		if err = store.Complete(ctx, pending); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("partial replay and completion", func(t *testing.T) {
		batch := queued("split", "a", "b")
		if err := store.Enqueue(ctx, batch); err != nil {
			t.Fatal(err)
		}
		complete("a")
		if err := store.Enqueue(ctx, batch); err != nil {
			t.Fatal(err)
		}
		pending, err := store.Next(ctx, "a")
		if err != nil || pending != nil {
			t.Fatal("duplicate recreated completed child", err)
		}
		complete("b")
		if err := store.Enqueue(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if count("gmessages_incoming_event") != 0 {
			t.Fatal("completed replay recreated children")
		}
		var payload []byte
		if err := db.QueryRow(ctx, `SELECT payload FROM gmessages_incoming_rpc WHERE response_id='split'`).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if payload != nil {
			t.Fatal("completed payload was not cleared")
		}
		batch.Digest = sha256.Sum256([]byte("changed"))
		if err := store.Enqueue(ctx, batch); err == nil {
			t.Fatal("changed logical content was accepted")
		}
	})
	t.Run("atomic failed enqueue", func(t *testing.T) {
		if dialect == "postgres" {
			if _, err := db.Exec(ctx, `ALTER TABLE gmessages_incoming_event ADD CONSTRAINT fixture_failure CHECK (part_index <> 1)`); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := db.Exec(ctx, `CREATE TRIGGER fixture_failure BEFORE INSERT ON gmessages_incoming_event WHEN NEW.part_index=1 BEGIN SELECT RAISE(ABORT, 'fixture failure'); END`); err != nil {
				t.Fatal(err)
			}
		}
		before := count("gmessages_incoming_rpc")
		if err := store.Enqueue(ctx, queued("failed", "a", "b")); err == nil {
			t.Fatal("expected second child insert failure")
		}
		if count("gmessages_incoming_rpc") != before || count("gmessages_incoming_event") != 0 {
			t.Fatal("partial transaction survived rollback")
		}
		if dialect == "postgres" {
			if _, err := db.Exec(ctx, `ALTER TABLE gmessages_incoming_event DROP CONSTRAINT fixture_failure`); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := db.Exec(ctx, `DROP TRIGGER fixture_failure`); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("concurrent completion clears payload", func(t *testing.T) {
		var streams []string
		for index := range 24 {
			streams = append(streams, fmt.Sprint("parallel-", index))
		}
		if err := store.Enqueue(ctx, queued("parallel", streams...)); err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		start := make(chan struct{})
		failures := make(chan error, len(streams))
		for _, stream := range streams {
			pending, err := store.Next(ctx, stream)
			if err != nil || pending == nil {
				t.Fatalf("next: %v / %v", pending, err)
			}
			workers.Add(1)
			go func() { defer workers.Done(); <-start; failures <- store.Complete(ctx, pending) }()
		}
		close(start)
		workers.Wait()
		for range streams {
			if err := <-failures; err != nil {
				t.Fatal(err)
			}
		}
		var payload []byte
		if err := db.QueryRow(ctx, `SELECT payload FROM gmessages_incoming_rpc WHERE response_id='parallel'`).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if payload != nil || count("gmessages_incoming_event") != 0 {
			t.Fatal("concurrent completion retained children or raw payload")
		}
	})
	t.Run("restart FIFO and cancellation", func(t *testing.T) {
		if err := store.Enqueue(ctx, queued("older", "ordered")); err != nil {
			t.Fatal(err)
		}
		if err := store.Enqueue(ctx, queued("newer", "ordered")); err != nil {
			t.Fatal(err)
		}
		restarted := &IncomingRPCStore{DB: db, BridgeID: "bridge", LoginID: "login"}
		streams, err := restarted.Streams(ctx)
		if err != nil || len(streams) != 1 || streams[0] != "ordered" {
			t.Fatalf("streams: %v / %v", streams, err)
		}
		pending, err := restarted.Next(ctx, "ordered")
		if err != nil || pending.ResponseID != "older" {
			t.Fatal("restart lost FIFO order", err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err = restarted.Complete(cancelled, pending); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled completion: %v", err)
		}
		if err = restarted.Enqueue(cancelled, queued("cancelled", "ordered")); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled enqueue: %v", err)
		}
		complete("ordered")
		pending, err = restarted.Next(ctx, "ordered")
		if err != nil || pending.ResponseID != "newer" {
			t.Fatal("newer event not retained", err)
		}
		complete("ordered")
	})
	t.Run("login deletion cascades and old binary rejects schema", func(t *testing.T) {
		if err := store.Enqueue(ctx, queued("delete", "a")); err != nil {
			t.Fatal(err)
		}
		other := &IncomingRPCStore{DB: db, BridgeID: "bridge", LoginID: "other"}
		if err := other.Enqueue(ctx, queued("preserve", "a")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, `DELETE FROM user_login WHERE bridge_id='bridge' AND id='login'`); err != nil {
			t.Fatal(err)
		}
		if count("gmessages_incoming_rpc") != 1 || count("gmessages_incoming_event") != 1 {
			t.Fatal("login cascade deleted unrelated rows or retained deleted login")
		}
		if err := store.Enqueue(ctx, queued("removed", "a")); err == nil {
			t.Fatal("removed login accepted RPC")
		}
		db.UpgradeTable = table[:1]
		if err := db.Upgrade(ctx); !errors.Is(err, dbutil.ErrUnsupportedDatabaseVersion) {
			t.Fatalf("v1 binary accepted v2: %v", err)
		}
		db.UpgradeTable = table
	})
}
