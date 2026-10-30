//go:build unix

package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// TestCoreDisconnectKeepsKeysFlowing closes the Core's connection with
// nothing pending: the guard goes on versioning the keys typed after
// it, and a line the human types reaches the harness whole and on time.
func TestCoreDisconnectKeepsKeysFlowing(t *testing.T) {
	t.Parallel()
	// No delivery runs here: the keys flow on an unmeasured profile,
	// whose cancel closes the draft at once.
	dr, k, _ := startSocketDelivery(t, "sock-gone", newWindowClock(false), func(o *Options) {
		o.Profile = testProfile()
	})
	before := dr.session.InputGuard().InputGuardVersion
	require.NoError(t, k.c.Close())

	// A draft opened and cancelled: two versions, and nothing for the
	// harness, since the cancel drops the key in its tty.
	writeKeys(t, dr.master, "a\x03")
	require.Eventually(t, func() bool {
		g := dr.session.InputGuard()
		return g.DraftState == protocol.DraftStateClosed && g.InputGuardVersion == before+2
	}, 10*time.Second, time.Millisecond, "the guard never took the keys")

	start := time.Now()
	writeKeys(t, dr.master, "typed-after-close\r")
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "typed-after-close\n"
	}, 10*time.Second, time.Millisecond, "the line never reached the harness")
	require.Less(t, time.Since(start), 2*time.Second, "the disconnect delayed the human")
	require.Eventually(t, func() bool {
		return dr.session.InputGuard().InputGuardVersion > before+2
	}, 10*time.Second, time.Millisecond, "the guard stopped versioning the keys")

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "typed-after-close\n__done__\n", harnessOut(t, dr.out))
}

// TestCoreDisconnectWithAPreparedDelivery closes the Core's connection
// with a delivery prepared: the session cancels it, and a line the human
// types next reaches the harness whole, with no byte of the notice.
func TestCoreDisconnectWithAPreparedDelivery(t *testing.T) {
	t.Parallel()
	dr, k, _ := startSocketDelivery(t, "sock-gone-prepared", newWindowClock(false), nil)

	const id protocol.OpaqueID = "dlv-gone"
	const canary = "canary-k3v9"
	notice := protocol.NoticeLine("[GIFFORD] " + canary + " pending")
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare(id, notice, 0, 0)).Result)
	require.NoError(t, k.c.Close())
	require.Eventually(t, func() bool {
		cancelled := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled)
		if len(cancelled) != 1 {
			return false
		}
		c := cancelled[0].(DeliveryCancelled)
		return c.DeliveryID == id && c.Reason == string(protocol.CancelReasonStateChanged)
	}, 10*time.Second, 10*time.Millisecond, "the delivery outlived its Core")
	dr.session.mu.Lock()
	_, held := dr.session.prepared[id]
	dr.session.mu.Unlock()
	require.False(t, held, "the session dropped the prepare")

	start := time.Now()
	writeKeys(t, dr.master, "typed-after-close\r")
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "typed-after-close\n"
	}, 10*time.Second, time.Millisecond, "the line never reached the harness")
	require.Less(t, time.Since(start), 2*time.Second, "the disconnect delayed the human")

	endDeliverySession(t, dr.master, dr.done)
	out := harnessOut(t, dr.out)
	require.Equal(t, "typed-after-close\n__done__\n", out, "the human's lines, whole")
	require.NotContains(t, out, canary, "no byte of the notice")
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted), "no commit ever started")
}
