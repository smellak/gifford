package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParamsRules covers the cross-field rules of the §6.8 params roots.
func TestParamsRules(t *testing.T) {
	t.Parallel()

	on := true
	gen := int64(2)
	t.Run("set target", func(t *testing.T) {
		t.Parallel()

		params := SetGlobalParams{Mode: ChannelModeActive, ExpectedVersion: 3}
		require.NoError(t, params.Validate())

		draining := SetNodeParams{
			NodeID: testNode, Mode: ChannelModeDraining,
			ThenMode: ChannelModeOff, ExpectedVersion: 3,
		}
		require.NoError(t, draining.Validate())

		early := SetGlobalParams{Mode: ChannelModeActive, ThenMode: ChannelModeOff}
		require.ErrorIs(t, early.Validate(), ErrInvalid)

		other := SetGlobalParams{Mode: ChannelModeDraining, ThenMode: ChannelModePaused}
		require.ErrorIs(t, other.Validate(), ErrInvalid)
	})
	t.Run("effective plane", func(t *testing.T) {
		t.Parallel()

		params := EffectiveParams{FromAgent: testAgentID, ToAgent: testAgent2, Plane: PlaneMessage}
		require.NoError(t, params.Validate())

		params.Plane = PlaneAll
		require.ErrorIs(t, params.Validate(), ErrInvalid)
	})
	t.Run("explain forms", func(t *testing.T) {
		t.Parallel()

		pair := ExplainParams{FromAgent: testAgentID, ToAgent: testAgent2, Plane: PlaneMessage}
		require.NoError(t, pair.Validate())

		message := ExplainParams{MessageID: testMsgID}
		require.NoError(t, message.Validate())

		artifact := ExplainParams{ArtifactID: testArtifactID}
		require.NoError(t, artifact.Validate())

		both := ExplainParams{MessageID: testMsgID, ArtifactID: testArtifactID}
		require.ErrorIs(t, both.Validate(), ErrInvalid)

		mixed := ExplainParams{FromAgent: testAgentID, ToAgent: testAgent2, MessageID: testMsgID}
		require.ErrorIs(t, mixed.Validate(), ErrInvalid)

		half := ExplainParams{FromAgent: testAgentID, MessageID: testMsgID}
		require.ErrorIs(t, half.Validate(), ErrInvalid)

		threaded := ExplainParams{MessageID: testMsgID, ThreadID: testThread}
		require.ErrorIs(t, threaded.Validate(), ErrInvalid)
	})
	t.Run("agent flags", func(t *testing.T) {
		t.Parallel()

		params := SetAgentChannelParams{AgentID: testAgentID, TX: &on}
		require.NoError(t, params.Validate())

		bare := SetAgentChannelParams{AgentID: testAgentID}
		require.ErrorIs(t, bare.Validate(), ErrInvalid)
	})
	t.Run("link endpoints", func(t *testing.T) {
		t.Parallel()

		params := LinkParams{FromAgent: testAgentID, ToAgent: testAgent2}
		require.NoError(t, params.Validate())

		params.ToAgent = testAgentID
		require.ErrorIs(t, params.Validate(), ErrInvalid)
	})
	t.Run("link versions", func(t *testing.T) {
		t.Parallel()

		msgKey := "link:" + testAgentID + linkArrow + testAgent2 + ":message"
		artKey := "link:" + testAgentID + linkArrow + testAgent2 + ":artifact"
		params := SetLinkParams{
			FromAgent: testAgentID, ToAgent: testAgent2,
			Mode: LinkModeHold, Plane: PlaneMessage,
			ExpectedVersions: ControlVersions{msgKey: 0},
		}
		require.NoError(t, params.Validate())

		params.Plane = PlaneAll
		require.ErrorIs(t, params.Validate(), ErrInvalid)

		params.ExpectedVersions[artKey] = 0
		require.NoError(t, params.Validate())

		params.ExpectedVersions = ControlVersions{}
		require.ErrorIs(t, params.Validate(), ErrInvalid)

		params.ExpectedVersions = ControlVersions{string(ScopeTypeGlobal): 3}
		require.ErrorIs(t, params.Validate(), ErrInvalid)

		deleted := DeleteLinkOverrideParams{
			FromAgent: testAgentID, ToAgent: testAgent2,
			Plane: PlaneMessage, ExpectedVersions: ControlVersions{msgKey: 7},
		}
		require.NoError(t, deleted.Validate())

		deleted.ExpectedVersions = ControlVersions{}
		require.ErrorIs(t, deleted.Validate(), ErrInvalid)
	})
	t.Run("loop creation", func(t *testing.T) {
		t.Parallel()

		params := CreateLoopParams{
			ThreadID: testThread, Participants: []AgentID{testAgentID, testAgent2},
			Mode: AutonomyAuto,
		}
		require.NoError(t, params.Validate())

		params.BudgetProfile = BudgetProfileCustom
		require.ErrorIs(t, params.Validate(), ErrInvalid)
	})
	t.Run("loop mode subset", func(t *testing.T) {
		t.Parallel()

		params := SetLoopModeParams{LoopID: testLoop, Mode: LoopModeStep, ExpectedVersion: 1}
		require.NoError(t, params.Validate())

		params.Mode = LoopModeStopped
		require.ErrorIs(t, params.Validate(), ErrInvalid)
	})
	t.Run("budget extension", func(t *testing.T) {
		t.Parallel()

		params := ExtendLoopBudgetParams{LoopID: testLoop, AddMessages: &gen, ExpectedVersion: 1}
		require.NoError(t, params.Validate())

		params.AddMessages = nil
		require.ErrorIs(t, params.Validate(), ErrInvalid)
	})
	t.Run("release exactly one", func(t *testing.T) {
		t.Parallel()

		params := ReleaseHoldParams{MessageID: testMsgID}
		require.NoError(t, params.Validate())

		params.ArtifactID = testArtifactID
		require.ErrorIs(t, params.Validate(), ErrInvalid)

		params.MessageID = ""
		params.ArtifactID = ""
		require.ErrorIs(t, params.Validate(), ErrInvalid)
	})
}

// TestLinkVersionKeysMatchRows requires expected_versions to hold exactly
// the keys of the rows written: one link: key per row, no more, no less.
func TestLinkVersionKeysMatchRows(t *testing.T) {
	t.Parallel()

	set := func(plane Plane, bidirectional bool, versions ControlVersions) SetLinkParams {
		return SetLinkParams{
			FromAgent: testAgentID, ToAgent: testAgent2,
			Mode: LinkModeAllow, Plane: plane, Bidirectional: bidirectional,
			ExpectedVersions: versions,
		}
	}
	link := func(from, to AgentID, plane Plane) string {
		return "link:" + string(from) + "→" + string(to) + ":" + string(plane)
	}

	for _, tc := range []struct {
		name   string
		params SetLinkParams
		valid  bool
	}{
		{"one plane", set(PlaneMessage, false, ControlVersions{link("agent-1", "agent-2", PlaneMessage): 0}), true},
		{"plane all", set(PlaneAll, false, ControlVersions{
			link("agent-1", "agent-2", PlaneMessage):  0,
			link("agent-1", "agent-2", PlaneArtifact): 0,
		}), true},
		{"bidirectional", set(PlaneMessage, true, ControlVersions{
			link("agent-1", "agent-2", PlaneMessage): 0,
			link("agent-2", "agent-1", PlaneMessage): 0,
		}), true},
		{"both", set(PlaneAll, true, ControlVersions{
			link("agent-1", "agent-2", PlaneMessage):  0,
			link("agent-1", "agent-2", PlaneArtifact): 0,
			link("agent-2", "agent-1", PlaneMessage):  0,
			link("agent-2", "agent-1", PlaneArtifact): 0,
		}), true},
		{"other agents", set(PlaneMessage, false, ControlVersions{link("agent-3", "agent-4", PlaneMessage): 0}), false},
		{"missing key", set(PlaneAll, false, ControlVersions{link("agent-1", "agent-2", PlaneMessage): 0}), false},
		{"global key", set(PlaneMessage, false, ControlVersions{string(ScopeTypeGlobal): 0}), false},
		{"agent key", set(PlaneMessage, false, ControlVersions{"agent:agent-1": 0}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.valid {
				require.NoError(t, tc.params.Validate())
			} else {
				require.ErrorIs(t, tc.params.Validate(), ErrInvalid)
			}
		})
	}

	t.Run("delete", func(t *testing.T) {
		t.Parallel()
		del := func(plane Plane, versions ControlVersions) DeleteLinkOverrideParams {
			return DeleteLinkOverrideParams{
				FromAgent: "agent-1", ToAgent: "agent-2",
				Plane: plane, ExpectedVersions: versions,
			}
		}
		require.NoError(t, del(PlaneMessage, ControlVersions{link("agent-1", "agent-2", PlaneMessage): 0}).Validate())
		require.NoError(t, del(PlaneAll, ControlVersions{
			link("agent-1", "agent-2", PlaneMessage):  0,
			link("agent-1", "agent-2", PlaneArtifact): 0,
		}).Validate())
		require.ErrorIs(t, del(PlaneMessage, ControlVersions{string(ScopeTypeGlobal): 0}).Validate(), ErrInvalid)
		require.ErrorIs(t, del(PlaneMessage, ControlVersions{
			link("agent-1", "agent-2", PlaneMessage): 0,
			link("agent-2", "agent-1", PlaneMessage): 0,
		}).Validate(), ErrInvalid)
	})
}

// TestAgentAndLinkTTLRange requires ttl_ms of the agent and link roots
// to stay within 0–2592000000, like set_global and set_node. An omitted
// value stays valid: omission keeps its existing semantics.
func TestAgentAndLinkTTLRange(t *testing.T) {
	t.Parallel()

	ttl := func(v int64) *int64 { return &v }
	on := true
	linkKey := "link:" + testAgentID + linkArrow + testAgent2 + ":message"
	check := func(t *testing.T, params nestedValidator, valid bool) {
		t.Helper()
		if valid {
			require.NoError(t, params.Validate())
		} else {
			require.ErrorIs(t, params.Validate(), ErrInvalid)
		}
	}
	for _, ms := range []struct {
		name  string
		value *int64
		valid bool
	}{
		{"minus one", ttl(-1), false},
		{"over maximum", ttl(2592000001), false},
		{"zero", ttl(0), true},
		{"maximum", ttl(2592000000), true},
		{"omitted", nil, true},
	} {
		t.Run("channel "+ms.name, func(t *testing.T) {
			t.Parallel()
			check(t, SetAgentChannelParams{AgentID: testAgentID, TX: &on, TTLMS: ms.value}, ms.valid)
		})
		t.Run("mode "+ms.name, func(t *testing.T) {
			t.Parallel()
			check(t, SetAgentModeParams{AgentID: testAgentID, Mode: AgentModeFull, TTLMS: ms.value}, ms.valid)
		})
		t.Run("link "+ms.name, func(t *testing.T) {
			t.Parallel()
			check(t, SetLinkParams{
				FromAgent: testAgentID, ToAgent: testAgent2,
				Mode: LinkModeAllow, Plane: PlaneMessage,
				TTLMS:            ms.value,
				ExpectedVersions: ControlVersions{linkKey: 0},
			}, ms.valid)
		})
	}
}
