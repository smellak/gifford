package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file holds the migration runner: parsing, version bookkeeping and
// the up and down loops. SQL statements live in Go constants; the schema
// itself lives in migrations/.

// Runner-owned statements.
const (
	schemaMigrationsDDL = `CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL
)`
	selectAppliedSQL = `SELECT version, name FROM schema_migrations`
	insertAppliedSQL = `INSERT INTO schema_migrations (version, name) VALUES (?, ?)`
	deleteAppliedSQL = `DELETE FROM schema_migrations WHERE version = ?`
)

// migration is one parsed migration file.
type migration struct {
	name    string // file name
	version int64  // the YYYYMMDDHHMMSS of the file
	up      string // the Up section text
	down    string // the Down section text
}

// parseMigrations reads every migration in fsys, split by the format of
// §4.2 and sorted by version, all before the database is touched.
func parseMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	migrations := make([]migration, 0, len(entries))
	seen := make(map[int64]bool, len(entries))
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return nil, invalidMigration(entry.Name(), "not a regular file")
		}
		parsed, err := parseMigrationFile(fsys, entry.Name())
		if err != nil {
			return nil, err
		}
		if seen[parsed.version] {
			return nil, invalidMigration(entry.Name(), "version twice")
		}
		seen[parsed.version] = true
		migrations = append(migrations, parsed)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	return migrations, nil
}

// invalidMigration reports a migration file that breaks the format.
func invalidMigration(name, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidMigration, name, reason)
}

// migrationSection tracks the parser position.
type migrationSection int

// Parser positions.
const (
	sectionNone migrationSection = iota
	sectionUp
	sectionDown
)

// parseMigrationFile validates one file's name and splits its sections.
func parseMigrationFile(fsys fs.FS, name string) (migration, error) {
	version, err := parseMigrationName(name)
	if err != nil {
		return migration{}, err
	}
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return migration{}, fmt.Errorf("read migration %s: %w", name, err)
	}
	var up, down []string
	section := sectionNone
	statementOpen := false
	seenUp := false
	seenDown := false
	for _, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if word, ok := gooseMarker(trimmed); ok {
			switch {
			case word == "Up" && section == sectionNone:
				section = sectionUp
				seenUp = true
			case word == "Down" && section == sectionUp && !statementOpen:
				section = sectionDown
				seenDown = true
			case word == "StatementBegin" && (section == sectionUp || section == sectionDown) && !statementOpen:
				statementOpen = true
			case word == "StatementEnd" && (section == sectionUp || section == sectionDown) && statementOpen:
				statementOpen = false
			default:
				return migration{}, invalidMigration(name, "unexpected marker "+word)
			}
		} else if section == sectionNone && trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			return migration{}, invalidMigration(name, "statement above Up")
		}
		switch section {
		case sectionUp:
			up = append(up, line)
		case sectionDown:
			down = append(down, line)
		default:
		}
	}
	if !seenUp || !seenDown {
		return migration{}, invalidMigration(name, "needs one Up and one Down")
	}
	if statementOpen {
		return migration{}, invalidMigration(name, "StatementBegin without StatementEnd")
	}
	if !hasStatement(up) || !hasStatement(down) {
		return migration{}, invalidMigration(name, "section without a statement")
	}
	return migration{
		name:    name,
		version: version,
		up:      strings.Join(up, "\n"),
		down:    strings.Join(down, "\n"),
	}, nil
}

// gooseMarker reports the word of a -- +goose marker line.
func gooseMarker(trimmed string) (string, bool) {
	rest, ok := strings.CutPrefix(trimmed, "-- +goose")
	if !ok {
		return "", false
	}
	word, _, _ := strings.Cut(strings.TrimSpace(rest), " ")
	if word == "" {
		return "", false
	}
	return word, true
}

// hasStatement reports whether a section holds a line that is neither
// blank nor a comment.
func hasStatement(lines []string) bool {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "--") {
			return true
		}
	}
	return false
}

// parseMigrationName validates <YYYYMMDDHHMMSS>_<name>.sql and returns the
// version: the UTC timestamp parsed with layout 20060102150405, so lexical
// order is version order.
func parseMigrationName(name string) (int64, error) {
	base, ok := strings.CutSuffix(name, ".sql")
	if !ok || len(base) < 16 || base[14] != '_' {
		return 0, invalidMigration(name, "bad name")
	}
	digits := base[:14]
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, invalidMigration(name, "bad name")
		}
	}
	if _, err := time.Parse("20060102150405", digits); err != nil {
		return 0, invalidMigration(name, "bad timestamp")
	}
	version, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, invalidMigration(name, "bad timestamp")
	}
	for _, word := range base[15:] {
		if word != '_' && (word < 'a' || word > 'z') && (word < '0' || word > '9') {
			return 0, invalidMigration(name, "bad name")
		}
	}
	words := base[15:]
	if words == "" || strings.Contains(words, "__") || words[0] == '_' || words[len(words)-1] == '_' {
		return 0, invalidMigration(name, "bad name")
	}
	return version, nil
}

// migrateUp applies every pending migration in version order. Each step
// re-reads the bookkeeping inside its own BEGIN IMMEDIATE transaction, so
// concurrent openers apply each migration once.
func migrateUp(ctx context.Context, db *sql.DB, fsys fs.FS) error {
	migrations, err := parseMigrations(fsys)
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schemaMigrationsDDL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	for {
		var done bool
		if err := inMigrationTx(ctx, db, func(tx *sql.Tx) error {
			var err error
			done, err = applyNextUp(ctx, tx, migrations)
			return err
		}); err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// applyNextUp runs the lowest pending migration. It reports done when
// none is pending; the transaction rolls back either way.
func applyNextUp(ctx context.Context, tx *sql.Tx, migrations []migration) (bool, error) {
	applied, err := readApplied(ctx, tx)
	if err != nil {
		return false, err
	}
	if err := checkConsistent(applied, migrations); err != nil {
		return false, err
	}
	var next *migration
	for i := range migrations {
		if _, ok := applied[migrations[i].version]; !ok {
			next = &migrations[i]
			break
		}
	}
	if next == nil {
		return true, nil
	}
	if _, err := tx.ExecContext(ctx, next.up); err != nil {
		return false, fmt.Errorf("migration %s up: %w", next.name, err)
	}
	if _, err := tx.ExecContext(ctx, insertAppliedSQL, next.version, next.name); err != nil {
		return false, fmt.Errorf("migration %s up: %w", next.name, err)
	}
	return false, nil
}

// migrateDown reverts the newest steps applied migrations, newest first.
func migrateDown(ctx context.Context, db *sql.DB, fsys fs.FS, steps int) error {
	migrations, err := parseMigrations(fsys)
	if err != nil {
		return err
	}
	var applied int
	if err := inMigrationTx(ctx, db, func(tx *sql.Tx) error {
		rows, err := readApplied(ctx, tx)
		if err != nil {
			return err
		}
		applied = len(rows)
		if steps < 1 || steps > applied {
			return ErrInvalidArgument
		}
		return nil
	}); err != nil {
		return err
	}
	for range steps {
		if err := inMigrationTx(ctx, db, func(tx *sql.Tx) error {
			return revertNewestDown(ctx, tx, migrations)
		}); err != nil {
			return err
		}
	}
	return nil
}

// revertNewestDown runs the newest applied Down section and deletes its row.
func revertNewestDown(ctx context.Context, tx *sql.Tx, migrations []migration) error {
	applied, err := readApplied(ctx, tx)
	if err != nil {
		return err
	}
	if err := checkConsistent(applied, migrations); err != nil {
		return err
	}
	var newest *migration
	for i := range migrations {
		if _, ok := applied[migrations[i].version]; ok {
			newest = &migrations[i]
		}
	}
	if newest == nil {
		return ErrInvalidArgument
	}
	if _, err := tx.ExecContext(ctx, newest.down); err != nil {
		return fmt.Errorf("migration %s down: %w", newest.name, err)
	}
	if _, err := tx.ExecContext(ctx, deleteAppliedSQL, newest.version); err != nil {
		return fmt.Errorf("migration %s down: %w", newest.name, err)
	}
	return nil
}

// readApplied returns the applied versions with their names.
func readApplied(ctx context.Context, tx *sql.Tx) (map[int64]string, error) {
	rows, err := tx.QueryContext(ctx, selectAppliedSQL)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()
	applied := make(map[int64]string)
	for rows.Next() {
		var version int64
		var name string
		if err := rows.Scan(&version, &name); err != nil {
			return nil, fmt.Errorf("read schema_migrations: %w", err)
		}
		applied[version] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	return applied, nil
}

// checkConsistent fails closed when the database and the embedded
// migrations disagree: an applied version missing or renamed, or a pending
// version below the highest applied one.
func checkConsistent(applied map[int64]string, migrations []migration) error {
	byVersion := make(map[int64]string, len(migrations))
	for _, m := range migrations {
		byVersion[m.version] = m.name
	}
	highest := int64(-1)
	for version, name := range applied {
		if byVersion[version] != name {
			return ErrMigrationMismatch
		}
		if version > highest {
			highest = version
		}
	}
	for _, m := range migrations {
		if _, ok := applied[m.version]; !ok && m.version < highest {
			return ErrMigrationMismatch
		}
	}
	return nil
}

// inMigrationTx runs fn in one BEGIN IMMEDIATE transaction, joining a
// rollback error onto fn's.
func inMigrationTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			err = errors.Join(err, rbErr)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit migration transaction: %w", err)
	}
	return nil
}
