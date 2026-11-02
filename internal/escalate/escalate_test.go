package escalate

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testMessage = protocol.ULID("01JB0000000000000000000001")
	testAgent   = protocol.AgentID("test-agent")
	testOp      = protocol.AgentID("test-operator")
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.ExecContext(context.Background(), `CREATE TABLE escalations (
		message_id TEXT PRIMARY KEY,
		agent_id TEXT NOT NULL,
		operator_agent TEXT NOT NULL,
		created_at TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestEscalateMarksOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	esc, err := New(testOp, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := esc.Escalate(ctx, tx, testMessage, testAgent); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := esc.Escalate(ctx, tx, testMessage, testAgent); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ok, err := Escalated(ctx, db, testMessage)
	if err != nil || !ok {
		t.Fatalf("Escalated() = %v, %v; want true, nil", ok, err)
	}
	ok, err = Escalated(ctx, db, protocol.ULID("01JB0000000000000000000002"))
	if err != nil || ok {
		t.Fatalf("Escalated() = %v, %v; want false, nil", ok, err)
	}
}

func TestEscalateRejectsBadInput(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	esc, err := New(testOp, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := esc.Escalate(ctx, nil, testMessage, testAgent); err == nil {
		t.Fatal("nil tx accepted")
	}
	if err := esc.Escalate(ctx, tx, "!!!", testAgent); err == nil {
		t.Fatal("bad message accepted")
	}
	if err := esc.Escalate(ctx, tx, testMessage, "!!!"); err == nil {
		t.Fatal("bad agent accepted")
	}
	if _, err := New("!!!", nil); err == nil {
		t.Fatal("bad operator accepted")
	}
	if _, err := Escalated(ctx, db, "!!!"); err == nil {
		t.Fatal("bad message read accepted")
	}
}

func TestClientMsgIDValid(t *testing.T) {
	if !ClientMsgID(testMessage).Valid() {
		t.Fatalf("ClientMsgID(%q) invalid", testMessage)
	}
}

func TestControlsOfMapsChannel(t *testing.T) {
	full := protocol.AgentChannel{Mode: protocol.AgentModeFull, AutoDelivery: true, Deliverable: true}
	if got := ControlsOf(full); got != (fallback.Controls{}) {
		t.Fatalf("ControlsOf(full) = %+v, want zero", got)
	}
	off := protocol.AgentChannel{Mode: protocol.AgentModeOff}
	if got := ControlsOf(off); !got.Off {
		t.Fatalf("ControlsOf(off) = %+v, want Off", got)
	}
	manual := protocol.AgentChannel{Mode: protocol.AgentModeManual}
	if got := ControlsOf(manual); !got.Manual {
		t.Fatalf("ControlsOf(manual) = %+v, want Manual", got)
	}
	held := protocol.AgentChannel{Mode: protocol.AgentModeFull, Deliverable: false}
	if got := ControlsOf(held); !got.Hold {
		t.Fatalf("ControlsOf(held) = %+v, want Hold", got)
	}
}

func TestDeadlineMatchesFallback(t *testing.T) {
	if Deadline() != fallback.EscalateAfter {
		t.Fatalf("Deadline() = %v, fallback.EscalateAfter = %v", Deadline(), fallback.EscalateAfter)
	}
}
