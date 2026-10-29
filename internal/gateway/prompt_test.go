package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// echoPrompts renders the sanitised payload into the prompt and keeps
// what it was given.
type echoPrompts struct {
	mu      sync.Mutex
	version string
	seen    []string
	err     error
}

func (e *echoPrompts) RenderPrompt(c protocol.ModelContract, clean []byte) (Prompt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen = append(e.seen, string(clean))
	if e.err != nil {
		return Prompt{}, e.err
	}
	if c != protocol.ModelContractAnalyzeAgentState {
		return Prompt{}, nil
	}
	return Prompt{Version: e.version, Messages: []Message{
		{Role: RoleSystem, Content: "classify"},
		{Role: RoleUser, Content: string(clean)},
	}}, nil
}

func TestPromptRenderedAfterSanitising(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	tr := &captureTransport{answer: []byte(unknownAnswer)}
	pr := &echoPrompts{version: "analyze_agent_state.v1"}
	f := newFixture(t, "http://127.0.0.1:1", tr, func(d *Deps) { d.Prompts = pr })
	res, err := f.gw.Infer(context.Background(), analyzeRequest(secret))
	require.NoError(t, err)

	require.Len(t, pr.seen, 1)
	require.NotContains(t, pr.seen[0], secret, "the template sees the sanitised payload only")
	sent := tr.sent()
	require.Len(t, sent, 1)
	require.NotContains(t, string(sent[0]), secret)
	var wire wireRequest
	require.NoError(t, json.Unmarshal(sent[0], &wire))
	require.Equal(t, "analyze_agent_state.v1", wire.PromptVersion)
	require.Equal(t, []Message{{Role: RoleSystem, Content: "classify"}, {Role: RoleUser, Content: string(wire.Payload)}}, wire.Messages)
	require.Equal(t, sha256Hex(sent[0]), res.CallRecord.InputSHA256, "the digest covers the prompt as sent")

	// Another template version is another input.
	pr2 := &echoPrompts{version: "analyze_agent_state.v2"}
	g := newFixture(t, "http://127.0.0.1:1", &captureTransport{answer: []byte(unknownAnswer)}, func(d *Deps) { d.Prompts = pr2 })
	res2, err := g.gw.Infer(context.Background(), analyzeRequest(secret))
	require.NoError(t, err)
	require.NotEqual(t, res.CallRecord.InputSHA256, res2.CallRecord.InputSHA256)
}

// fixedPrompts returns one prompt, or one error, for every contract.
type fixedPrompts struct {
	p   Prompt
	err error
}

func (f fixedPrompts) RenderPrompt(protocol.ModelContract, []byte) (Prompt, error) { return f.p, f.err }

func TestPromptFailureSendsNothing(t *testing.T) {
	t.Parallel()
	for name, pr := range map[string]PromptRenderer{
		"renderer error":       fixedPrompts{err: errors.New("sentinel template detail")},
		"version, no messages": fixedPrompts{p: Prompt{Version: "v1"}},
		"messages, no version": fixedPrompts{p: Prompt{Messages: []Message{{Role: RoleUser, Content: "sentinel"}}}},
		"bad version":          fixedPrompts{p: Prompt{Version: "has spaces", Messages: []Message{{Role: RoleUser, Content: "x"}}}},
		"unknown role":         fixedPrompts{p: Prompt{Version: "v1", Messages: []Message{{Role: "assistant", Content: "x"}}}},
		"empty content":        fixedPrompts{p: Prompt{Version: "v1", Messages: []Message{{Role: RoleUser}}}},
		"too many":             fixedPrompts{p: Prompt{Version: "v1", Messages: make([]Message, maxPromptMessages+1)}},
		"too large": fixedPrompts{p: Prompt{Version: "v1", Messages: []Message{
			{Role: RoleSystem, Content: strings.Repeat("x", maxPromptBytes)},
			{Role: RoleUser, Content: "x"},
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := &captureTransport{answer: []byte(unknownAnswer)}
			f := newFixture(t, "http://127.0.0.1:1", tr, func(d *Deps) { d.Prompts = pr })
			res, err := f.gw.Infer(context.Background(), analyzeRequest("x"))
			require.ErrorIs(t, err, ErrCallFailed)
			require.NotContains(t, err.Error(), "sentinel")
			require.Empty(t, tr.sent(), "nothing is sent")
			require.Equal(t, protocol.GlimmerCallStatusError, res.CallRecord.Status)
			require.NoError(t, res.CallRecord.Validate())
		})
	}
}
