package orchestrator

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
)

func waitingChoiceFixture(
	t *testing.T,
	sequence int,
	reason string,
) (*choiceFixture, daemon.QueueDelivery) {
	t.Helper()
	f := newChoiceFixture(t)
	candidate := seedReadableDelivery(t, f.executorFixture, sequence, "wait for fresh analysis")
	f.states.set(func(s *choiceStates) {
		s.eligible, s.reason = false, reason
	})
	return f, candidate
}

func TestAutomaticStateWaitExpiresOnceWithoutTriageOrDecision(t *testing.T) {
	t.Parallel()
	f, candidate := waitingChoiceFixture(t, 90, StateEligibilityUnknownPhase)
	db := f.store.DB()

	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, 1, f.states.demandCount())
	require.Zero(t, f.triager.count())
	require.Zero(t, f.model.total())
	require.Empty(t, decisionsFor(t, db, candidate.MessageID))
	require.Empty(t, pendingNotes(t, db, candidate.MessageID))
	require.Zero(t, attemptCount(t, db, candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, db, candidate))
	require.Zero(t, countRows(t, db, "SELECT count(*) FROM orchestrator_process_bindings"),
		"the read-only precheck never binds a process")

	f.advance(automaticStateWaitWindow - time.Nanosecond)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, 2, f.states.demandCount())
	require.Zero(t, f.triager.count())
	require.Empty(t, pendingNotes(t, db, candidate.MessageID))

	f.advance(time.Nanosecond)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, 2, f.states.demandCount(), "the elapsed window is never renewed")
	require.Equal(t, 1, f.triager.count())
	require.Empty(t, f.model.calls(choiceChoose))
	decision := lastDecision(t, db, candidate.MessageID)
	require.Equal(t, choiceDefer, decision.selected)
	require.Equal(t, []string{
		notePrefix + "p_idle_threshold: state is not eligible: " + StateEligibilityUnknownPhase,
	}, pendingNotes(t, db, candidate.MessageID))
	require.NotContains(t, f.executor.stateWait, candidate, "a pending approval resolves the wait identity")
}

func TestAutomaticStateWaitStartsAtFirstEligibleSelection(t *testing.T) {
	t.Parallel()
	for name, age := range map[string]time.Duration{
		"low after batching delay": 31 * time.Second,
		"old backlog":              20 * time.Minute,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, candidate := waitingChoiceFixture(t, 91, StateEligibilityStale)
			if name == "low after batching delay" {
				_, err := f.store.DB().ExecContext(t.Context(), `UPDATE delivery_queue SET priority = 3
					WHERE message_id = ? AND agent_id = ?`, candidate.MessageID, candidate.AgentID)
				require.NoError(t, err)
			}
			f.advance(age)
			firstSelection := f.clock.Now()

			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Equal(t, firstSelection, f.executor.stateWait[candidate].started)
			require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
			f.advance(automaticStateWaitWindow - time.Nanosecond)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Equal(t, 2, f.states.demandCount())
			require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID),
				"queue age before first selection does not spend the analysis window")
		})
	}
}

func TestAutomaticStateWaitSurvivesNewEvidence(t *testing.T) {
	t.Parallel()
	f, candidate := waitingChoiceFixture(t, 92, StateEligibilityObservationNewer)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	started := f.executor.stateWait[candidate].started

	f.advance(5 * time.Second)
	f.states.set(func(s *choiceStates) {
		s.onRead = func(state *StateRead) {
			state.ObservationGeneration++
			state.EligibilityReason = StateEligibilityObservationNewer
		}
	})
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, started, f.executor.stateWait[candidate].started)
	f.advance(5 * time.Second)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Equal(t, 2, f.states.demandCount())
	require.Len(t, pendingNotes(t, f.store.DB(), candidate.MessageID), 1,
		"new evidence never opens a second window")
}

func TestAutomaticStateWaitIncludesSessionAndProfileRefreshes(t *testing.T) {
	t.Parallel()
	for index, reason := range []string{
		StateEligibilitySessionChanged,
		StateEligibilityGuardChanged,
		StateEligibilityProfileChanged,
		StateEligibilityCallInvalid,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			f, candidate := waitingChoiceFixture(t, 93+index, reason)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Equal(t, 1, f.states.demandCount())
			require.Zero(t, f.triager.count())
			require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
		})
	}
}

func TestAutomaticStateValidVetoesNeverWait(t *testing.T) {
	t.Parallel()
	for index, reason := range []string{
		StateEligibilityNotIdle,
		StateEligibilityBelowThreshold,
		StateEligibilityDeliveryUnsafe,
		StateEligibilityHumanInput,
		StateEligibilityPersistence,
		StateEligibilityUnhealthy,
		StateEligibilitySessionInactive,
		StateEligibilityNativeInformative,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			f, candidate := waitingChoiceFixture(t, 100+index, reason)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Zero(t, f.states.demandCount())
			require.Equal(t, 1, f.triager.count())
			require.Len(t, pendingNotes(t, f.store.DB(), candidate.MessageID), 1)
		})
	}
}

func TestAutomaticStateWaitNeverMasksExistingGuards(t *testing.T) {
	t.Parallel()
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator}
	tests := map[string]struct {
		arrange func(*testing.T, *choiceFixture, daemon.QueueDelivery)
		queue   string
	}{
		"manual": {
			arrange: func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				t.Helper()
				off := false
				_, err := f.controller.SetAgent(t.Context(), protocol.SetAgentChannelParams{
					AgentID: executorTestAgent, AutoDelivery: &off, ExpectedVersion: 0,
				}, actor)
				require.NoError(t, err)
			},
			queue: queueQueued,
		},
		"off": {
			arrange: func(t *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				t.Helper()
				_, err := f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
					Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
				}, actor)
				require.NoError(t, err)
			},
			queue: queueQueued,
		},
		"expired": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.advance(2 * time.Hour)
			},
			queue: "EXPIRED",
		},
		"duplicate": {
			arrange: func(t *testing.T, f *choiceFixture, candidate daemon.QueueDelivery) {
				t.Helper()
				seedCommittedEvidence(t, f.executorFixture, candidate, "dlv-wait-duplicate")
			},
			queue: "DELIVERED",
		},
		"human draft": {
			arrange: func(_ *testing.T, f *choiceFixture, _ daemon.QueueDelivery) {
				f.setReport(func(report *protocol.InputGuardReport) {
					report.DraftState = protocol.DraftStateOpen
				})
			},
			queue: queueQueued,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, candidate := waitingChoiceFixture(t, 110, StateEligibilityUnknownPhase)
			test.arrange(t, f, candidate)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Zero(t, f.states.demandCount())
			require.Equal(t, test.queue, queueStateOf(t, f.store.DB(), candidate))
		})
	}
}

func TestAutomaticStateWaitForgetsResolvedAndNonCandidates(t *testing.T) {
	t.Parallel()
	for name, arrange := range map[string]func(*testing.T, *choiceFixture){
		"state veto": func(_ *testing.T, f *choiceFixture) {
			f.states.set(func(s *choiceStates) {
				s.eligible, s.reason = false, StateEligibilityNotIdle
			})
		},
		"channel off": func(t *testing.T, f *choiceFixture) {
			t.Helper()
			_, err := f.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{
				Mode: protocol.ChannelModeOff, ExpectedVersion: 0,
			}, channel.Actor{Type: protocol.ActorTypeHuman, ID: executorTestOperator})
			require.NoError(t, err)
		},
		"human draft": func(_ *testing.T, f *choiceFixture) {
			f.setReport(func(report *protocol.InputGuardReport) {
				report.DraftState = protocol.DraftStateOpen
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, candidate := waitingChoiceFixture(t, 115, StateEligibilityUnknownPhase)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Contains(t, f.executor.stateWait, candidate)

			arrange(t, f)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.NotContains(t, f.executor.stateWait, candidate)
		})
	}
}

func TestAutomaticStateDemandRequiresARealCandidate(t *testing.T) {
	t.Parallel()
	f := newChoiceFixture(t)
	f.states.set(func(s *choiceStates) {
		s.eligible, s.reason = false, StateEligibilityUnknownPhase
	})
	err := f.executor.EvaluateAutomatic(t.Context(), daemon.QueueDelivery{
		MessageID: "01ARZ3NDEKTSV4RRFFQ69G0999", AgentID: executorTestAgent, DeliveryGeneration: 1,
	})
	require.Error(t, err)
	require.Zero(t, f.states.demandCount())
}

func TestAutomaticStateWaitMapEvictsTheOldestEntry(t *testing.T) {
	t.Parallel()
	f := newExecutorFixture(t)
	identities := make([]daemon.QueueDelivery, automaticStateWaitLimit+1)
	for index := range identities {
		identities[index] = daemon.QueueDelivery{
			MessageID: protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ6%06d", index)),
			AgentID:   executorTestAgent, DeliveryGeneration: 1,
		}
		require.True(t, f.executor.withinAutomaticStateWindow(identities[index], f.clock.Now()))
	}
	require.Len(t, f.executor.stateWait, automaticStateWaitLimit)
	require.NotContains(t, f.executor.stateWait, identities[0])
	require.Contains(t, f.executor.stateWait, identities[len(identities)-1])
}

func TestAutomaticStateDemandFailureStopsBeforeTriage(t *testing.T) {
	t.Parallel()
	f, candidate := waitingChoiceFixture(t, 120, StateEligibilityNoModelEvidence)
	f.states.set(func(s *choiceStates) { s.demandErr = errors.New("demand unavailable") })
	err := f.executor.EvaluateAutomatic(t.Context(), candidate)
	require.ErrorContains(t, err, "demand automatic state")
	require.Zero(t, f.triager.count())
	require.Zero(t, f.model.total())
}
