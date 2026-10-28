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

		key := "link:" + testAgentID + linkArrow + testAgent2 + ":message"
		params := SetLinkParams{
			FromAgent: testAgentID, ToAgent: testAgent2,
			Mode: LinkModeHold, Plane: PlaneAll,
			ExpectedVersions: ControlVersions{key: 0},
		}
		require.NoError(t, params.Validate())

		params.ExpectedVersions = ControlVersions{}
		require.ErrorIs(t, params.Validate(), ErrInvalid)

		params.ExpectedVersions = ControlVersions{"global": 3}
		require.ErrorIs(t, params.Validate(), ErrInvalid)

		deleted := DeleteLinkOverrideParams{
			FromAgent: testAgentID, ToAgent: testAgent2,
			Plane: PlaneMessage, ExpectedVersions: ControlVersions{key: 7},
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
