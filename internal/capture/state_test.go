package capture

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/smellak/gifford/internal/protocol"
)

func stoppedEnvelope(t *testing.T, seq int64, reason protocol.CaptureStopReason) protocol.EventEnvelope {
	t.Helper()
	return testEnvelope(t, protocol.EventTypeCaptureStopped, seq, protocol.CaptureStopped{
		CaptureSessionID: storeTestSession,
		Reason:           reason,
	})
}

func TestDeriveLife(t *testing.T) {
	t.Parallel()
	live := []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
	}
	if got := DeriveLife(true, live); got != LifeLive {
		t.Fatalf("live = %d", got)
	}
	for _, tc := range []struct {
		name   string
		reason protocol.CaptureStopReason
	}{
		{testReasonManual, protocol.CaptureStopReasonManual},
		{"relaunch", protocol.CaptureStopReasonRelaunchedWithRun},
		{"quota", protocol.CaptureStopReasonQuota},
		{"error", protocol.CaptureStopReasonError},
	} {
		withStop := append(append([]protocol.EventEnvelope{}, live...), stoppedEnvelope(t, 2, tc.reason))
		if got := DeriveLife(true, withStop); got != LifeTerminal {
			t.Fatalf("%s: life = %d, want terminal", tc.name, got)
		}
	}
	if got := DeriveLife(true, nil); got != LifeIncomplete {
		t.Fatalf("row without attached = %d, want incomplete", got)
	}
	if got := DeriveLife(false, nil); got != LifeNoRow {
		t.Fatalf("no row = %d, want norow", got)
	}
	// Attached at a nonzero sequence is not an attach at zero.
	shifted := testEnvelope(t, protocol.EventTypeCaptureAttached, 3, testAttachedPayload())
	if got := DeriveLife(true, []protocol.EventEnvelope{shifted}); got != LifeIncomplete {
		t.Fatalf("shifted attached = %d, want incomplete", got)
	}
}

func TestStoppedRowState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		reason protocol.CaptureStopReason
		want   string
	}{
		{protocol.CaptureStopReasonQuota, "QUOTA"},
		{protocol.CaptureStopReasonError, "ERROR"},
		{protocol.CaptureStopReasonManual, testStateStopped},
		{protocol.CaptureStopReasonRelaunchedWithRun, "STOPPED"},
	} {
		if got := StoppedRowState(tc.reason); got != tc.want {
			t.Errorf("reason %s: state = %s, want %s", tc.reason, got, tc.want)
		}
	}
}

func TestStoppedReason(t *testing.T) {
	t.Parallel()
	ev := stoppedEnvelope(t, 2, protocol.CaptureStopReasonQuota)
	reason, err := StoppedReason(&ev)
	if err != nil {
		t.Fatal(err)
	}
	if reason != protocol.CaptureStopReasonQuota {
		t.Fatalf("reason = %s", reason)
	}
	bad := testEnvelope(t, protocol.EventTypeCaptureStopped, 2, protocol.CaptureStopped{
		CaptureSessionID: storeTestSession,
		Reason:           "bogus",
	})
	if _, err := StoppedReason(&bad); err == nil {
		t.Fatal("want error for a bogus reason")
	}
}

func testClaimSlot() Slot {
	return Slot{
		Socket: claimServer, ServerPID: 100, ServerStartTime: 200,
		SessionID: "$0", WindowIndex: 3, PaneIndex: 0,
	}
}

func TestCheckClaims(t *testing.T) {
	t.Parallel()
	slot := testClaimSlot()
	live := []LiveClaim{{
		AgentID: storeTestAgent, CaptureSessionID: "cap-01AAAAAAAAAAAAAAAAAAAAAAAA",
		Server: claimServer, Pane: "%1", Slot: slot,
	}}
	// A second attach for the same agent is refused.
	err := CheckClaims(live, ClaimWant{AgentID: storeTestAgent, Server: "/tmp/other", Pane: "%9", Slot: testClaimSlot()})
	if err == nil {
		t.Fatal("want agent conflict")
	}
	// The same %N on the same server is refused, naming the other agent.
	err = CheckClaims(live, ClaimWant{AgentID: storeTestAgent2, Server: claimServer, Pane: "%1"})
	if err == nil {
		t.Fatal("want pane conflict")
	}
	// A recreated pane keeps the slot: same slot, new %N, still refused.
	err = CheckClaims(live, ClaimWant{AgentID: storeTestAgent2, Server: claimServer, Pane: "%2", Slot: slot})
	if err == nil {
		t.Fatal("want slot conflict")
	}
	// A different server and slot is free, with no pipe.
	if err := CheckClaims(live, ClaimWant{AgentID: storeTestAgent2, Server: claimServer2, Pane: "%1"}); err != nil {
		t.Fatalf("free pane: %v", err)
	}
	// A pipe without --replace is refused.
	err = CheckClaims(nil, ClaimWant{AgentID: storeTestAgent2, Server: claimServer2, Pane: "%1", HasPipe: true})
	if err == nil {
		t.Fatal("want pipe conflict")
	}
	// --replace excuses only a pipe no live session claims.
	if err := CheckClaims(nil, ClaimWant{AgentID: storeTestAgent2, Server: claimServer2, Pane: "%1", HasPipe: true, Replace: true}); err != nil {
		t.Fatalf("replace of unclaimed pipe: %v", err)
	}
	// --replace never excuses a live Gifford capture.
	err = CheckClaims(live, ClaimWant{AgentID: storeTestAgent2, Server: claimServer, Pane: "%1", Replace: true})
	if err == nil {
		t.Fatal("want pane conflict despite replace")
	}
}

func TestSlotOf(t *testing.T) {
	t.Parallel()
	slot := SlotOf(SessionTmux{
		Socket: claimServer, ServerPID: 100, ServerStartTime: 200,
		SessionID: "$0", SessionName: "s", WindowIndex: 3, PaneIndex: 0,
	})
	if slot != testClaimSlot() {
		t.Fatalf("slot = %+v", slot)
	}
}

func TestReadStopRequest(t *testing.T) {
	root, data := openTestRoot(t)
	makeSession(t, root, testAgent, testSessionA)
	dir := testAgent + "/" + testSessionA
	// Missing file is no request.
	got, err := readStopRequest(root, testAgent, testSessionA)
	if err != nil || got != nil {
		t.Fatalf("missing request: got=%+v err=%v", got, err)
	}
	putFile(t, root, dir, "stop.request", []byte(
		`{"schema":"gifford.capture.stop.v1","reason":"manual","detail":"halt","requested_at":"2026-01-02T03:04:05Z"}`))
	got, err = readStopRequest(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Reason != "manual" || got.Detail != "halt" {
		t.Fatalf("request = %+v", got)
	}
	// A corrupt request fails closed.
	corruptPath := filepath.Join(data, "captures", testAgent, testSessionA, "stop.request")
	if err := os.WriteFile(corruptPath, []byte(`{"schema":"gifford.capture.stop.v1","reason":"bogus"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readStopRequest(root, testAgent, testSessionA); err == nil {
		t.Fatal("want error for a corrupt request")
	}
}
