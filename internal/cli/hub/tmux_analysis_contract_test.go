package hub

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

func TestTmuxAnalysisNodeSharesConfiguredProjection(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("starting an empty node must not call inference")
	}))
	t.Cleanup(endpoint.Close)
	cfg := configuredAutomaticTestConfig(t, endpoint.URL)
	cfg.ML.ScreenLines = 7
	node := newAutomaticTestNode(t)
	wiring, err := node.build(t, cfg)
	require.NoError(t, err)
	require.Same(t, wiring.producer, wiring.states)
	attached, err := newTmuxAttachNode(t.Context(), node.store.DB(), automaticTestNodeID,
		node.clock, cfg, t.TempDir(), io.Discard)
	require.NoError(t, err)
	require.NotNil(t, attached.options)
	// This behavioral contract compiles before the option is implemented;
	// a node without it demonstrably cannot project its tmux observations.
	field := reflect.ValueOf(attached.options).Elem().FieldByName("Projection")
	require.True(t, field.IsValid(), "the tmux node must wire the configured analyst/dataset projection")
	projection, ok := field.Interface().(screenproj.Policies)
	require.True(t, ok)
	require.NoError(t, projection.Validate())
	want, err := cfg.ML.ProjectionPolicies()
	require.NoError(t, err)
	input := screenproj.Input{
		Rows: []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "$ "},
		Cols: 80, Draft: protocol.DraftStateClosed,
	}
	expected, err := screenproj.Project(input, want)
	require.NoError(t, err)
	actual, err := screenproj.Project(input, projection)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestTmuxAnalysisNodeWithoutConfigurationStillStarts(t *testing.T) {
	node := newAutomaticTestNode(t)
	cfg := config.DefaultConfig()
	require.Nil(t, cfg.StateAnalysis)
	attached, err := newTmuxAttachNode(t.Context(), node.store.DB(), automaticTestNodeID,
		node.clock, cfg, t.TempDir(), io.Discard)
	require.NoError(t, err, "the deterministic path must not require ML configuration")
	require.NotNil(t, attached.options)
	require.Equal(t, protocol.NoticeLine(": [GIFFORD] 1 pending message: check your inbox"),
		attached.bridgeNotice)
	require.Empty(t, attached.responseRequiredNotice,
		"a separate response notice must remain disabled without explicit configuration")
}

func TestTmuxNodeRejectsInvalidConfiguredWriterLog(t *testing.T) {
	node := newAutomaticTestNode(t)
	cfg := config.DefaultConfig()
	cfg.TmuxLocks = config.TmuxLocks{
		Enabled: true, WriterLogSocket: "relative/writer.sock",
	}

	_, err := newTmuxAttachNode(t.Context(), node.store.DB(), automaticTestNodeID,
		node.clock, cfg, t.TempDir(), io.Discard)
	require.ErrorIs(t, err, writerlog.ErrInvalid)
}
