package renotify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
)

// The runner must honour controls changed after a notice covered the current
// slot. Decide stays nil: this exercises the production fallback decision and
// the real durable escalation writer, not an injected decision callback.
func TestRunnerControlsAfterCoveredNoticePreventEscalation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		controls  fallback.Controls
		uncertain bool
	}{
		{name: "hold", controls: fallback.Controls{Hold: true}},
		{name: "manual", controls: fallback.Controls{Manual: true}},
		{name: "off", controls: fallback.Controls{Off: true}},
		{name: "hold_uncertain", controls: fallback.Controls{Hold: true}, uncertain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := baseSeed()
			seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
			db, loader := seedLoopDB(t, seed)
			engine, file := &fakeEngine{}, &fakeFiler{}
			runner := openTurnRunner(db, engine, file, escalatorMark(t, db))
			require.Nil(t, runner.Decide, "the runner must use the production decision")
			item := loadItem(t, loader)
			require.True(t, item.Facts.GuardsPassed)
			require.False(t, item.Facts.Escalated)
			require.Equal(t, seed.notices, item.Facts.Notices)

			// The active agent's 15-minute notice already covers the slot.
			_, err := runner.RunOnce(t.Context(), loadT0.Add(16*time.Minute), []Item{item})
			require.NoError(t, err)
			require.Empty(t, file.keys)
			require.False(t, isEscalated(t, db, loadMsg))

			if tc.uncertain {
				stamp := eventstore.FormatTime(loadT0.Add(20 * time.Minute))
				_, err = db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
					(delivery_id, message_id, agent_id, delivery_generation,
					 adapter_session_id, orchestrator_decision_id, channel_decision_id,
					 draft_generation, input_guard_version, control_versions_json,
					 state, created_at, updated_at)
					VALUES ('covered-uncertain', ?, ?, 1, 'pane-1', 'dec-1', 'ch-1',
					        0, 0, '{}', 'UNCERTAIN', ?, ?)`, loadMsg, loadAgent, stamp, stamp)
				require.NoError(t, err)
			}
			for _, after := range []time.Duration{30 * time.Minute, 31 * time.Minute} {
				item = loadItem(t, loader)
				require.Equal(t, tc.uncertain, item.Facts.Uncertain)
				// Controls are the runner's caller-supplied input, changed
				// after the covered notice. No decision is substituted.
				item.Facts.Controls = tc.controls
				_, err = runner.RunOnce(t.Context(), loadT0.Add(after), []Item{item})
				require.NoError(t, err)

				var marks int
				require.NoError(t, db.QueryRowContext(t.Context(),
					"SELECT COUNT(*) FROM escalations WHERE message_id = ?", loadMsg).Scan(&marks))
				got := [3]int{len(engine.generations), len(file.keys), marks}
				t.Logf("at %s: requeues=%d operator_notices=%d escalation_marks=%d",
					after, got[0], got[1], got[2])
				require.Equal(t, [3]int{}, got, "controls suppress both notification and escalation")
			}
			require.Empty(t, queuedGenerations(t, db, loadMsg))
		})
	}
}
