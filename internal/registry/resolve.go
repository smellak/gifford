package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds process lineage: proving that a process descends from a
// live registered session of an agent. The Hub hands the registry a pid it
// obtained with SO_PEERCRED; the registry walks /proc/<pid>/stat upwards
// until an ancestor's pid and start time match the process_json of a live
// session. A live session is one with ended_at NULL.
//
// Convention: protocol.ProcessIdentity.ProcessStartTime holds the raw
// starttime field of /proc/<pid>/stat (clock ticks since boot), as read by
// CurrentProcess. Both sides of the comparison use this file, so writer
// and reader can never disagree on units.

// procFSRoot is the process filesystem the walk reads. Tests point it at a
// scratch tree; production always uses /proc.
var procFSRoot = "/proc"

// sessionLineage is one live session's claim about its process.
type sessionLineage struct {
	pid   int64
	start int64
	agent protocol.AgentID
}

// ResolveAgentPID returns the agent whose live registered session anchors
// pid: pid itself or the nearest ancestor whose pid and start time match
// the process_json of a live session. Without such an ancestor it returns
// an error wrapping ErrNoRegisteredAncestor.
func ResolveAgentPID(ctx context.Context, q Querier, pid int) (protocol.AgentID, error) {
	if pid <= 0 {
		return "", fmt.Errorf("resolve pid %d: %w", pid, ErrInvalidArgument)
	}
	agent, err := walkLineage(ctx, q, procFSRoot, pid, "")
	if err != nil {
		return "", err
	}
	return agent, nil
}

// lineageForAgent is like ResolveAgentPID but only honours sessions of
// agent. Register uses it so one agent's sessions never authorize another
// agent's key.
func lineageForAgent(
	ctx context.Context,
	q Querier,
	pid int,
	agent protocol.AgentID,
	process protocol.ProcessIdentity,
) error {
	if pid <= 0 {
		return fmt.Errorf("check lineage of pid %d: %w", pid, ErrInvalidArgument)
	}
	// The claimed identity must describe the live process first, so a
	// recycled pid cannot borrow another process's ancestry.
	_, start, err := readStat(procFSRoot, pid)
	if err != nil {
		return err
	}
	if int64(pid) != process.PID || start != process.ProcessStartTime {
		return fmt.Errorf("pid %d does not match its claimed identity: %w", pid, ErrUnrelatedProcess)
	}
	_, err = walkLineage(ctx, q, procFSRoot, pid, agent)
	return err
}

// walkLineage climbs from pid through every parent, returning the agent of
// the first ancestor with a live session. An empty agent matches any
// session's agent; a set one only matches that agent's sessions.
func walkLineage(
	ctx context.Context,
	q Querier,
	root string,
	pid int,
	agent protocol.AgentID,
) (protocol.AgentID, error) {
	sessions, err := liveSessions(ctx, q)
	if err != nil {
		return "", err
	}
	seen := make(map[int]struct{})
	current := pid
	for {
		if _, dup := seen[current]; dup {
			return "", fmt.Errorf("pid %d loops in its ancestry: %w", pid, ErrNoRegisteredAncestor)
		}
		seen[current] = struct{}{}
		ppid, start, err := readStat(root, current)
		if err != nil {
			return "", err
		}
		for _, session := range sessions {
			if agent != "" && session.agent != agent {
				continue
			}
			if session.pid == int64(current) && session.start == start {
				return session.agent, nil
			}
		}
		if ppid <= 0 {
			return "", fmt.Errorf("pid %d has no registered ancestor: %w", pid, ErrNoRegisteredAncestor)
		}
		current = ppid
	}
}

// liveSessions returns every live session's process claim. Rows whose
// process_json does not decode are skipped: a corrupt claim authenticates
// nothing.
func liveSessions(ctx context.Context, q Querier) ([]sessionLineage, error) {
	text, err := queryText("sessions_live_process.sql")
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("select live sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []sessionLineage
	for rows.Next() {
		var agent string
		var raw string
		if err := rows.Scan(&agent, &raw); err != nil {
			return nil, fmt.Errorf("scan live session: %w", err)
		}
		var identity protocol.ProcessIdentity
		if err := json.Unmarshal([]byte(raw), &identity); err != nil {
			continue
		}
		found = append(found, sessionLineage{
			pid:   identity.PID,
			start: identity.ProcessStartTime,
			agent: protocol.AgentID(agent),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live sessions: %w", err)
	}
	return found, nil
}

// readStat returns the parent pid and the start time of pid from its stat
// file. The comm field may hold spaces and parentheses, so the fields
// split after the last closing parenthesis: ppid is then the second field
// and starttime the twentieth.
func readStat(root string, pid int) (int, int64, error) {
	raw, err := os.ReadFile(root + "/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, fmt.Errorf("read stat of pid %d: %w", pid, ErrNoRegisteredAncestor)
	}
	after := raw[strings.LastIndexByte(string(raw), ')')+1:]
	fields := strings.Fields(string(after))
	if len(fields) < 20 {
		return 0, 0, fmt.Errorf("stat of pid %d holds %d fields: %w",
			pid, len(fields), ErrNoRegisteredAncestor)
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parse parent of pid %d: %w", pid, ErrNoRegisteredAncestor)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse start time of pid %d: %w", pid, ErrNoRegisteredAncestor)
	}
	return parent, start, nil
}

// CurrentProcess describes the calling process the way process_json must:
// its pid with the raw starttime of /proc/self/stat, plus its credentials.
func CurrentProcess() (protocol.ProcessIdentity, error) {
	pid := os.Getpid()
	_, start, err := readStat(procFSRoot, pid)
	if err != nil {
		return protocol.ProcessIdentity{}, err
	}
	return protocol.ProcessIdentity{
		PID:              int64(pid),
		ProcessStartTime: start,
		UID:              int64(os.Getuid()),
		GID:              int64(os.Getgid()),
	}, nil
}

// Querier is what read-only registry calls need; *sql.DB, *sql.Tx and
// *sql.Conn satisfy it.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Execer is what registry writes need; *sql.DB and *sql.Tx satisfy it.
// Callers own the transaction: every write the package performs runs
// inside the given transaction, including the re-signatures.
type Execer interface {
	Querier
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
