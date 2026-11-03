package controlapi

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/smellak/gifford/internal/protocol"
)

func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query); err != nil {
		t.Fatal(err)
	}
}

func TestBridgeRetireRecipientDispatch(t *testing.T) {
	t.Parallel()
	server, store := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	db := store.DB()
	mustExec(t, db, `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		thread_ref, priority, state, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES ('01JB0000000000000000000C1', 'cm-c1', 'node-test-a', 'bridge-relay', 'local',
		'thr-c1', 'NORMAL', 'DELIVERY_QUEUED',
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`)
	mustExec(t, db, `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		source_created_at, source_read_at, created_at, updated_at)
		VALUES ('node-test-a', 11, '01JB0000000000000000000C1', 'bridge-relay', 'retire-ctl-a',
		'2026-11-03T00:00:00Z', NULL,
		'2026-11-03T00:00:00Z', '2026-11-03T00:00:00Z')`)
	mustExec(t, db, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES ('01JB0000000000000000000C1', 'retire-ctl-a', 1, 'bridge-relay', 'thr-c1', 'QUEUED', 2,
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`)

	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	params := protocol.BridgeRetireRecipientParams{SourceNodeID: "node-test-a", AgentID: "retire-ctl-a"}
	ok := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodBridgeRetireRecipient, params))
	if ok.Error != nil {
		t.Fatalf("retire: %+v", ok.Error)
	}
	if ok.Result == nil {
		t.Fatal("empty result")
	}

	// Unknown peer cannot mutate.
	denied := server.Handle(t.Context(), Peer{}, rpcRequest(t, protocol.ControlMethodBridgeRetireRecipient, params))
	if denied.Error == nil || denied.Error.Data == nil || denied.Error.Data.ErrorCode != protocol.ErrorCodePolicyDenied {
		t.Fatalf("unverified: %+v", denied.Error)
	}
}

func TestBridgeRetireRecipientBusyMapsConflict(t *testing.T) {
	t.Parallel()
	server, store := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	db := store.DB()
	mustExec(t, db, `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		thread_ref, priority, state, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES ('01JB0000000000000000000C2', 'cm-c2', 'node-test-a', 'bridge-relay', 'local',
		'thr-c2', 'NORMAL', 'DELIVERY_QUEUED',
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`)
	mustExec(t, db, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES ('01JB0000000000000000000C2', 'retire-ctl-b', 1, 'bridge-relay', 'thr-c2', 'DELIVERING', 2,
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`)
	mustExec(t, db, `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		orchestrator_decision_id, channel_decision_id, draft_generation,
		input_guard_version, control_versions_json, state, created_at, updated_at)
		VALUES ('att-c2', '01JB0000000000000000000C2', 'retire-ctl-b', 1, 'sess', 'odec', 'cdec', 0, 0, '{}',
		'COMMIT_SENT', '2026-11-03T00:00:00Z', '2026-11-03T00:00:00Z')`)
	mustExec(t, db, `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		source_created_at, source_read_at, created_at, updated_at)
		VALUES ('node-test-a', 12, '01JB0000000000000000000C2', 'bridge-relay', 'retire-ctl-b',
		'2026-11-03T00:00:00Z', NULL,
		'2026-11-03T00:00:00Z', '2026-11-03T00:00:00Z')`)

	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	params := protocol.BridgeRetireRecipientParams{SourceNodeID: "node-test-a", AgentID: "retire-ctl-b"}
	busy := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodBridgeRetireRecipient, params))
	if busy.Error == nil || busy.Error.Data == nil || busy.Error.Data.ErrorCode != protocol.ErrorCodeReservationConflict {
		t.Fatalf("busy: %+v", busy.Error)
	}
}
