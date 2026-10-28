package eventstore

import (
	"context"
	"database/sql"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
	sqlite "modernc.org/sqlite"

	"github.com/smellak/gifford/migrations"
)

// testStorePath returns a fresh database path.
func testStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "gifford.db")
}

// testStore opens a store or fails the test.
func testStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.Context(), testStorePath(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// testOpenDB opens an unmigrated pool or fails the test.
func testOpenDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := openDB(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// TestOpenPragmas checks every connection setting on three connections
// held at once, and the sessions foreign key.
func TestOpenPragmas(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	conns := make([]*sql.Conn, 0, 3)
	for range 3 {
		conn, err := store.DB().Conn(ctx)
		require.NoError(t, err)
		conns = append(conns, conn)
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			require.NoError(t, conn.Close())
		}
	})
	pragmas := []struct {
		pragma string
		want   any
	}{
		{"journal_mode", "wal"},
		{"busy_timeout", 5000},
		{"foreign_keys", 1},
		{"synchronous", 1},
		{"recursive_triggers", 1},
	}
	for _, conn := range conns {
		for _, tc := range pragmas {
			switch want := tc.want.(type) {
			case string:
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA "+tc.pragma).Scan(&got))
				require.Equal(t, want, got, tc.pragma)
			case int:
				var got int
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA "+tc.pragma).Scan(&got))
				require.Equal(t, want, got, tc.pragma)
			}
		}
	}
	_, err := store.DB().ExecContext(ctx, `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json, started_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"sess-1", "agent-9", "CONNECTED", "pty_proxy", 1, `{}`, FormatTime(testEventTime), FormatTime(testEventTime))
	require.Error(t, err)
	var sqlErr *sqlite.Error
	require.ErrorAs(t, err, &sqlErr)
	require.Equal(t, 787, sqlErr.Code())
}

// TestOpenFileMode checks the database files are owner-only while open.
func TestOpenFileMode(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("file modes are POSIX")
	}
	path := testStorePath(t)
	store, err := Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(file)
		require.NoError(t, err, file)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), file)
	}
}

// TestOpenRejectsPath checks the path rules and the missing directory.
func TestOpenRejectsPath(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"", "file:gifford.db", "gifford.db?mode=ro", ":memory:"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			_, err := Open(t.Context(), path)
			require.ErrorIs(t, err, ErrInvalidArgument)
		})
	}
	t.Run("missing directory", func(t *testing.T) {
		t.Parallel()
		_, err := Open(t.Context(), filepath.Join(t.TempDir(), "nope", "gifford.db"))
		require.ErrorIs(t, err, fs.ErrNotExist)
	})
	t.Run("not a database", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "bad.db")
		require.NoError(t, os.WriteFile(path, []byte("not a database\n"), 0o600))
		_, err := Open(t.Context(), path)
		require.Error(t, err)
	})
}

// schemaObject is one sqlite_schema row for snapshot comparisons.
type schemaObject struct {
	Type  string
	Name  string
	Table string
	SQL   string
}

// schemaSnapshot reads every user object in name order.
func schemaSnapshot(ctx context.Context, t *testing.T, db *sql.DB) []schemaObject {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT type, name, tbl_name, COALESCE(sql, '')
		FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var objects []schemaObject
	for rows.Next() {
		var object schemaObject
		require.NoError(t, rows.Scan(&object.Type, &object.Name, &object.Table, &object.SQL))
		objects = append(objects, object)
	}
	require.NoError(t, rows.Err())
	return objects
}

// schemaRowCount counts the schema_migrations rows.
func schemaRowCount(ctx context.Context, t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&count))
	return count
}

// sectionFiveNames holds the 14 object names of §5.
var sectionFiveNames = []string{
	"agents", "capture_segments", "capture_segments_capture", "capture_sessions", "capture_sessions_agent",
	"events", "events_agent_time", "events_message", "events_no_delete", "events_no_update",
	"events_session_sequence", "input_guards", "sessions", "sessions_agent",
}

// TestMigrationsUpDownUp applies, reverts and restores the embedded
// migrations without assuming which files exist beyond §5.
func TestMigrationsUpDownUp(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	embedded, err := parseMigrations(migrations.FS())
	require.NoError(t, err)
	require.NotEmpty(t, embedded)

	before := schemaSnapshot(ctx, t, store.DB())
	names := make(map[string]bool, len(before))
	for _, object := range before {
		names[object.Name] = true
	}
	for _, name := range sectionFiveNames {
		require.True(t, names[name], "§5 object %s", name)
	}
	require.Equal(t, len(embedded), schemaRowCount(ctx, t, store.DB()))

	require.NoError(t, store.MigrateDown(ctx, len(embedded)))
	require.Equal(t, 0, schemaRowCount(ctx, t, store.DB()))
	after := schemaSnapshot(ctx, t, store.DB())
	require.Len(t, after, 1)
	require.Equal(t, "schema_migrations", after[0].Name)

	require.ErrorIs(t, store.MigrateDown(ctx, 1), ErrInvalidArgument)

	require.NoError(t, store.MigrateUp(ctx))
	require.Equal(t, before, schemaSnapshot(ctx, t, store.DB()))
}

// TestEmbeddedMigrations parses the embedded files with increasing versions.
func TestEmbeddedMigrations(t *testing.T) {
	t.Parallel()

	parsed, err := parseMigrations(migrations.FS())
	require.NoError(t, err)
	require.NotEmpty(t, parsed)
	require.True(t, sort.SliceIsSorted(parsed, func(i, j int) bool {
		return parsed[i].version < parsed[j].version
	}))
}

// refusalUp and refusalDown are the valid a/b bodies; cases mutate them.
const (
	refusalUp   = "-- +goose Up\nCREATE TABLE a (x INTEGER);\n"
	refusalDown = "-- +goose Down\nDROP TABLE a;\n"
)

// refusalAName and refusalBName are the two migration file names.
const (
	refusalAName = "20261028000000_a.sql"
	refusalBName = "20261028000001_b.sql"
)

// refusalFS builds a file system from name to content.
func refusalFS(files map[string]string) fstest.MapFS {
	filesystem := make(fstest.MapFS, len(files))
	for name, content := range files {
		filesystem[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return filesystem
}

// refusalFile builds one valid migration file from its sections.
func refusalFile(up, down string) string {
	return "-- +goose Up\n" + up + "-- +goose Down\n" + down
}

// requireEmptySchema asserts no user object exists.
func requireEmptySchema(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	objects := schemaSnapshot(ctx, t, db)
	require.Empty(t, objects)
}

// TestMigrationRefusals checks every runner refusal on its own database.
func TestMigrationRefusals(t *testing.T) {
	t.Parallel()

	refusalA := refusalFile("CREATE TABLE a (x INTEGER);\n", "DROP TABLE a;\n")
	refusalB := refusalFile("CREATE TABLE b (x INTEGER);\n", "DROP TABLE b;\n")

	t.Run("invalid migration", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name  string
			files map[string]string
		}{
			{"bad name", map[string]string{"init.sql": refusalA}},
			{"bad month", map[string]string{"20261328000000_a.sql": refusalA}},
			{"uppercase", map[string]string{"20261028000000_A.sql": refusalA}},
			{"bad extension", map[string]string{"20261028000000_a.txt": refusalA}},
			{"version twice", map[string]string{
				refusalAName:           refusalA,
				"20261028000000_b.sql": refusalB,
			}},
			{"without down", map[string]string{refusalAName: refusalUp}},
			{"comment-only down", map[string]string{
				refusalAName: refusalUp + "-- +goose Down\n-- just a comment\n",
			}},
			{"down before up", map[string]string{
				refusalAName: refusalDown + refusalUp,
			}},
			{"statement above up", map[string]string{
				refusalAName: "CREATE TABLE pre (x INTEGER);\n" + refusalA,
			}},
			{"no transaction", map[string]string{
				refusalAName: refusalUp + "-- +goose NO TRANSACTION\n" + refusalDown,
			}},
			{"begin without end", map[string]string{
				refusalAName: "-- +goose Up\n-- +goose StatementBegin\nCREATE TABLE a (x INTEGER);\n" + refusalDown,
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				db := testOpenDB(t, testStorePath(t))
				require.ErrorIs(t, migrateUp(ctx, db, refusalFS(tc.files)), ErrInvalidMigration)
				requireEmptySchema(ctx, t, db)
			})
		}
	})

	t.Run("migration mismatch", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		filesAB := refusalFS(map[string]string{
			refusalAName: refusalA,
			refusalBName: refusalB,
		})
		db := testOpenDB(t, testStorePath(t))
		require.NoError(t, migrateUp(ctx, db, filesAB))
		require.ErrorIs(t, migrateUp(ctx, db, refusalFS(map[string]string{
			refusalAName: refusalA,
		})), ErrMigrationMismatch)

		renamed := refusalFS(map[string]string{
			refusalAName:           refusalA,
			"20261028000001_c.sql": refusalB,
		})
		require.ErrorIs(t, migrateUp(ctx, db, renamed), ErrMigrationMismatch)

		fresh := testOpenDB(t, testStorePath(t))
		onlyB := refusalFS(map[string]string{refusalBName: refusalB})
		require.NoError(t, migrateUp(ctx, fresh, onlyB))
		require.ErrorIs(t, migrateUp(ctx, fresh, filesAB), ErrMigrationMismatch)
	})

	t.Run("invalid steps", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		db := testOpenDB(t, testStorePath(t))
		files := refusalFS(map[string]string{refusalAName: refusalA})
		require.NoError(t, migrateUp(ctx, db, files))
		require.ErrorIs(t, migrateDown(ctx, db, files, 0), ErrInvalidArgument)
		require.ErrorIs(t, migrateDown(ctx, db, files, 2), ErrInvalidArgument)
		require.Equal(t, 1, schemaRowCount(ctx, t, db))
	})

	t.Run("failing section", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		db := testOpenDB(t, testStorePath(t))
		broken := refusalFile("CREATE TABLE b (\n", "DROP TABLE b;\n")
		files := refusalFS(map[string]string{
			refusalAName: refusalA,
			refusalBName: broken,
		})
		err := migrateUp(ctx, db, files)
		require.Error(t, err)
		require.Contains(t, err.Error(), "migration 20261028000001_b.sql up:")
		var sqlErr *sqlite.Error
		require.ErrorAs(t, err, &sqlErr)
		names := make(map[string]bool)
		for _, object := range schemaSnapshot(ctx, t, db) {
			names[object.Name] = true
		}
		require.True(t, names["a"])
		require.False(t, names["b"])
		require.Equal(t, 1, schemaRowCount(ctx, t, db))
	})
}

// testEventTime is the fixed wall time of the store tests.
var testEventTime = time.Date(2026, time.October, 28, 12, 0, 0, 0, time.UTC)

// TestEventsAppendOnly proves triggers refuse UPDATE, DELETE and
// INSERT OR REPLACE on events.
func TestEventsAppendOnly(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	ctx := t.Context()
	_, err := store.DB().ExecContext(ctx, `INSERT INTO events
		(event_id, schema_version, node_id, session_id, agent_id, adapter_type,
		 source_quality, event_type, wall_time, monotonic_ns, sequence,
		 payload_json, sensitivity, redaction_applied)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"01JB0000000000000000000001", 1, "node-a", "s", "agent-1", "generic_pty",
		"synthetic", "io_chunk", FormatTime(testEventTime), 0, 0, `{}`, "normal", 0)
	require.NoError(t, err)
	for _, statement := range []string{
		"UPDATE events SET sequence = 5",
		"DELETE FROM events",
		"INSERT OR REPLACE INTO events SELECT * FROM events",
	} {
		t.Run(statement, func(t *testing.T) {
			t.Parallel()
			_, err := store.DB().ExecContext(ctx, statement)
			require.Error(t, err)
			var sqlErr *sqlite.Error
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, 1811, sqlErr.Code())
			require.Contains(t, err.Error(), "events are append-only")
		})
	}
	var sequence int64
	require.NoError(t, store.DB().QueryRowContext(ctx,
		"SELECT sequence FROM events").Scan(&sequence))
	require.Equal(t, int64(0), sequence)
}

// TestWriteTransactionsAreImmediate proves writers take the write lock at
// BEGIN (where the timeout applies) while readers stay deferred.
func TestWriteTransactionsAreImmediate(t *testing.T) {
	t.Parallel()

	path := testStorePath(t)
	first, err := Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	ctx := t.Context()
	held, err := first.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, held.Rollback()) })

	deadline, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = second.DB().BeginTx(deadline, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	reader, err := second.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	var one int
	require.NoError(t, reader.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
	require.NoError(t, reader.Rollback())
}

// TestOpenConcurrent opens six stores at once, one row per embedded file.
func TestOpenConcurrent(t *testing.T) {
	t.Parallel()

	embedded, err := parseMigrations(migrations.FS())
	require.NoError(t, err)
	const stores = 6
	errs := make(chan error, stores)
	var group sync.WaitGroup
	for i := range stores {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			path := filepath.Join(t.TempDir(), fmt.Sprintf("gifford-%d.db", i))
			store, err := Open(t.Context(), path)
			if err != nil {
				errs <- err
				return
			}
			var count int
			if err := store.DB().QueryRowContext(t.Context(),
				"SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
				errs <- err
			} else if count != len(embedded) {
				errs <- fmt.Errorf("schema_migrations has %d rows, want %d", count, len(embedded))
			} else {
				errs <- store.Close()
			}
		}(i)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// TestTimeFormat checks the §7.5 example, the round trip, the refusals
// and the text order.
func TestTimeFormat(t *testing.T) {
	t.Parallel()

	zoned := time.Date(2026, time.October, 26, 12, 41, 2, 314000000, time.FixedZone("", 3600))
	require.Equal(t, "2026-10-26T11:41:02.314000000Z", FormatTime(zoned))

	fixed := time.Date(2026, time.October, 28, 12, 0, 0, 123456789, time.UTC)
	parsed, err := ParseTime(FormatTime(fixed))
	require.NoError(t, err)
	require.True(t, fixed.Equal(parsed))
	_, err = time.Parse(time.RFC3339Nano, FormatTime(fixed))
	require.NoError(t, err)

	for _, text := range []string{
		"2026-10-26T11:41:02.314Z",
		"2026-10-26T12:41:02.314000000+01:00",
		"2026-10-26T11:41:02Z",
		"",
	} {
		t.Run(text, func(t *testing.T) {
			t.Parallel()
			_, err := ParseTime(text)
			require.Error(t, err)
		})
	}

	// Fixed width keeps text order equal to time order; the trimmed
	// forms (…02Z, …02.5Z) would sort wrong, which is why the layout
	// keeps nine digits.
	ordered := []string{
		"2026-10-26T11:41:01.999999999Z",
		"2026-10-26T11:41:02.000000000Z",
		"2026-10-26T11:41:02.000000001Z",
		"2026-10-26T11:41:02.500000000Z",
	}
	require.True(t, sort.StringsAreSorted(ordered))
}

// TestImports keeps the package leaf-side: only stdlib, the contract
// package, the embedded migrations and the pure-Go driver.
func TestImports(t *testing.T) {
	t.Parallel()

	allowed := map[string]bool{
		"github.com/smellak/gifford/internal/protocol": true,
		"github.com/smellak/gifford/migrations":        true,
		"modernc.org/sqlite":                           true,
		"modernc.org/sqlite/lib":                       true,
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			src, err := os.ReadFile(file)
			require.NoError(t, err)
			parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.ImportsOnly)
			require.NoError(t, err)
			for _, imp := range parsed.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				first, _, _ := strings.Cut(path, "/")
				if !strings.Contains(first, ".") {
					continue
				}
				require.True(t, allowed[path], "unlisted import in %s", path)
			}
		})
	}
}
