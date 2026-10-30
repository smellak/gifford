//go:build linux

package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type endpointSession struct {
	id      protocol.OpaqueID
	process EndpointProcess
}

// resolveEndpointIdentity derives the session and ancestry from the kernel
// peer PID and live session rows. The client contributes no session or role.
func resolveEndpointIdentity(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
	peerPID int,
	claimedStart int64,
) (EndpointIdentity, error) {
	if !agent.Valid() || peerPID <= 0 || claimedStart < 0 {
		return EndpointIdentity{}, fmt.Errorf("resolve endpoint identity: %w", errInvalidArgument)
	}
	sessions, err := endpointSessions(ctx, q, agent)
	if err != nil {
		return EndpointIdentity{}, err
	}
	seen := make(map[int]struct{})
	path := make([]EndpointProcess, 0, 8)
	current := peerPID
	for {
		if _, duplicate := seen[current]; duplicate {
			return EndpointIdentity{}, fmt.Errorf("resolve endpoint ancestry: process cycle: %w", errInvalidArgument)
		}
		seen[current] = struct{}{}
		parent, start, err := readEndpointStat(current)
		if err != nil {
			return EndpointIdentity{}, err
		}
		if current == peerPID && start != claimedStart {
			return EndpointIdentity{}, fmt.Errorf("resolve endpoint identity: peer start time changed: %w", errInvalidArgument)
		}
		path = append(path, EndpointProcess{PID: int64(current), StartTime: start})
		var matched protocol.OpaqueID
		for _, session := range sessions {
			if session.process != path[len(path)-1] {
				continue
			}
			if matched != "" {
				return EndpointIdentity{}, fmt.Errorf("resolve endpoint identity: ambiguous session: %w", errInvalidArgument)
			}
			matched = session.id
		}
		if matched != "" {
			return EndpointIdentity{
				AgentID: agent, SessionID: matched, Process: path[0],
				Ancestors: append([]EndpointProcess(nil), path[1:]...),
			}, nil
		}
		if parent <= 0 {
			return EndpointIdentity{}, fmt.Errorf("resolve endpoint identity: no live session ancestor: %w", errInvalidArgument)
		}
		current = parent
	}
}

func endpointSessions(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
) ([]endpointSession, error) {
	rows, err := q.QueryContext(ctx, `SELECT session_id, process_json FROM sessions
		WHERE agent_id = ? AND state = 'CONNECTED' AND ended_at IS NULL`, agent)
	if err != nil {
		return nil, fmt.Errorf("select endpoint sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var sessions []endpointSession
	for rows.Next() {
		var id protocol.OpaqueID
		var raw sql.NullString
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("scan endpoint session: %w", err)
		}
		if !id.Valid() || !raw.Valid {
			continue
		}
		var process protocol.ProcessIdentity
		if err := json.Unmarshal([]byte(raw.String), &process); err != nil ||
			process.PID <= 0 || process.ProcessStartTime < 0 {
			continue
		}
		sessions = append(sessions, endpointSession{
			id: id,
			process: EndpointProcess{
				PID: process.PID, StartTime: process.ProcessStartTime,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read endpoint sessions: %w", err)
	}
	if len(sessions) == 0 {
		return nil, fmt.Errorf("resolve endpoint identity: no live session: %w", errInvalidArgument)
	}
	return sessions, nil
}

func readEndpointStat(pid int) (int, int64, error) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, fmt.Errorf("read endpoint process: process disappeared: %w", errInvalidArgument)
		}
		return 0, 0, fmt.Errorf("read endpoint process: %w", err)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return 0, 0, fmt.Errorf("read endpoint process: malformed stat: %w", errInvalidArgument)
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return 0, 0, fmt.Errorf("read endpoint process: short stat: %w", errInvalidArgument)
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, fmt.Errorf("read endpoint process parent: %w", errInvalidArgument)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("read endpoint process start time: %w", errInvalidArgument)
	}
	return parent, start, nil
}
