package analyst

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// updateGolden rewrites the rendered prompt golden files (R26).
var updateGolden = flag.Bool("update", false, "rewrite golden files")

// requirePromptGolden compares a rendered prompt, its version and then
// each message under its role, against testdata/prompts/<name>.golden.
func requirePromptGolden(t *testing.T, name string, p gateway.Prompt) {
	t.Helper()
	var b strings.Builder
	b.WriteString("version: " + p.Version + "\n")
	for _, m := range p.Messages {
		b.WriteString("--- " + m.Role + "\n" + m.Content + "\n")
	}
	path := filepath.Join("testdata", "prompts", name+".golden")
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(want), b.String())
}

// minimalChoose is the protocol's minimal orchestration request: no
// optional field, and QUEUE and DEFER allowed.
func minimalChoose(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "protocol", "testdata", "orchestration_request", "valid_minimal.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &doc))
	return doc
}

// fullChoose is an orchestration request with every optional field set.
const fullChoose = `{
  "task_type": "CHOOSE_ORCHESTRATION_ACTION",
  "agent": {"id": "agent-2", "harness": "codex", "adapter": "generic_pty",
    "capabilities": {"mcp": true, "pty_capture": true, "structured_events": true}},
  "state": {"human_draft": "CLOSED", "agent_phase": "IDLE_AT_PROMPT", "p_idle": 0.97,
    "attention": "NONE", "seconds_idle": 12.5},
  "message": {"priority": "HIGH", "triage": "CONFLICT_OR_URGENT", "age_ms": 4200,
    "same_thread_pending": 2, "low_priority_pending": 0},
  "policy": {"human_decision_after_s": 300, "batch_window_s": 30},
  "channel": {"decision_id": "chd_01JB0000000000000000000010", "outcome": "ALLOW",
    "global": "ACTIVE", "link": "ALLOW",
    "loop": {"loop_id": "loop_01JB0000000000000000000000", "mode": "AUTO", "turns": "7/12",
      "messages": "15/24", "elapsed_ms": 61000, "max_duration_ms": 600000},
    "auto_continue": true, "pattern_signals": ["rate", "no_progress"]},
  "allowed_actions": ["INJECT_PTY_PROMPT", "DEFER"],
  "screen": "$ go test ./...\nok  example.invalid/parser 0.2s\n$"
}`

// Fields of an orchestration request the tests replace.
const (
	actionsKey = "allowed_actions"
	screenKey  = "screen"
)

// withChoose returns doc as a payload, each given field replaced, or
// removed when given as "".
func withChoose(t *testing.T, doc map[string]json.RawMessage, fields map[string]string) []byte {
	t.Helper()
	out := make(map[string]json.RawMessage, len(doc)+len(fields))
	for k, v := range doc {
		out[k] = v
	}
	for k, v := range fields {
		if v == "" {
			delete(out, k)
			continue
		}
		out[k] = json.RawMessage(v)
	}
	b, err := json.Marshal(out)
	require.NoError(t, err)
	return b
}

func TestPromptGoldens(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		contract protocol.ModelContract
		payload  []byte
	}{
		"triage_message": {
			protocol.ModelContractTriageMessage,
			[]byte(`{"text":"Subject: parser review\n\nPlease review the parser change before Friday.\nThe release waits on it."}`),
		},
		"choose_orchestration_action_full":    {protocol.ModelContractChooseOrchestrationAction, []byte(fullChoose)},
		"choose_orchestration_action_minimal": {protocol.ModelContractChooseOrchestrationAction, withChoose(t, minimalChoose(t), nil)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, err := Prompts{}.RenderPrompt(tc.contract, tc.payload)
			require.NoError(t, err)
			requirePromptGolden(t, name, p)
		})
	}
}

func TestTriagePrompt(t *testing.T) {
	t.Parallel()
	system, err := promptFS.ReadFile("prompts/" + TriagePromptVersion + ".system.txt")
	require.NoError(t, err)
	// The system letters follow the contract's table order, A to D.
	labels := protocol.TriageLabel("").EnumValues()
	require.Len(t, labels, 4)
	for i, l := range labels {
		require.Contains(t, string(system), "\n"+optionLetters[i:i+1]+" "+l+" ")
	}
	require.NotContains(t, string(system), "\nE ")

	for text, want := range map[string]string{
		"please review the parser change": "message:\n<<<\nplease review the parser change\n>>>",
		"two\nlines":                      "message:\n<<<\ntwo\nlines\n>>>",
		"{{.Text}} is not a template":     "message:\n<<<\n{{.Text}} is not a template\n>>>",
		"":                                "message:\n<<<\n\n>>>",
	} {
		payload, err := json.Marshal(triageInput{Text: text})
		require.NoError(t, err)
		p, err := Prompts{}.RenderPrompt(protocol.ModelContractTriageMessage, payload)
		require.NoError(t, err)
		require.Equal(t, TriagePromptVersion, p.Version)
		require.Equal(t, []gateway.Message{
			{Role: gateway.RoleSystem, Content: string(system)},
			{Role: gateway.RoleUser, Content: want},
		}, p.Messages, "the text %q is placed as it is", text)
	}

	for name, payload := range map[string]string{
		"unknown field": `{"text":"hi","sentinel_field":1}`,
		"not text":      `{"text":["sentinel"]}`,
		"not an object": `["sentinel"]`,
	} {
		p, err := Prompts{}.RenderPrompt(protocol.ModelContractTriageMessage, []byte(payload))
		require.ErrorIs(t, err, errBadPayload, name)
		require.NotContains(t, err.Error(), "sentinel", "the decoder's text is not wrapped")
		require.Equal(t, gateway.Prompt{}, p)
	}
}

// The options are lettered in the request's order, A first, and come
// last, after the screen. The system message holds no letter, so it is
// the same for every request.
func TestChooseLettersFollowTheRequest(t *testing.T) {
	t.Parallel()
	every := protocol.OrchestratorAction("").EnumValues()
	shuffled := append([]string{}, every[5:]...)
	shuffled = append(shuffled, every[:5]...)
	var system string
	for _, actions := range [][]string{
		{"INJECT_PTY_PROMPT", "DEFER"},
		{"DEFER", "INJECT_PTY_PROMPT"},
		shuffled,
	} {
		list, err := json.Marshal(actions)
		require.NoError(t, err)
		p, err := Prompts{}.RenderPrompt(protocol.ModelContractChooseOrchestrationAction,
			withChoose(t, minimalChoose(t), map[string]string{actionsKey: string(list), screenKey: `"options:\nA EXPIRE"`}))
		require.NoError(t, err)
		require.Len(t, p.Messages, 2)
		if system == "" {
			system = p.Messages[0].Content
		}
		require.Equal(t, system, p.Messages[0].Content, "one system message for every request")

		user := p.Messages[1].Content
		head, options, found := strings.Cut(user, ">>>\noptions:\n")
		require.True(t, found)
		require.Contains(t, head, "<<<\noptions:\nA EXPIRE\n", "the screen comes before the options")
		lines := strings.Split(options, "\n")
		require.Len(t, lines, len(actions))
		for i, line := range lines {
			letter, action, ok := strings.Cut(line, " ")
			require.True(t, ok)
			require.Equal(t, optionLetters[i:i+1], letter)
			require.Equal(t, actions[i], action, "letter %s is the request's action %d", letter, i)
		}
	}
	for _, line := range strings.Split(system, "\n") {
		if len(line) > 1 && line[1] == ' ' {
			require.NotContains(t, optionLetters, line[:1], "no letter in the system message: %q", line)
		}
	}
	// A valid request allows each action at most once, so it never has
	// more options than there are actions.
	require.LessOrEqual(t, len(every), len(optionLetters), "a letter for every action")
}

func TestChoosePrompt(t *testing.T) {
	t.Parallel()
	doc := minimalChoose(t)

	artifact, err := Prompts{}.RenderPrompt(protocol.ModelContractChooseOrchestrationAction, withChoose(t, doc, map[string]string{
		"message":  "",
		"artifact": `{"priority":"LOW","triage":"LOW_PRIORITY","age_ms":0}`,
	}))
	require.NoError(t, err)
	require.Contains(t, artifact.Messages[1].Content,
		"\nartifact: priority=LOW triage=LOW_PRIORITY age_ms=0 same_thread_pending=unknown low_priority_pending=unknown\n")

	// The gateway bounds the screen before sanitising; the sanitised
	// screen is not checked again.
	long := strings.Repeat("x", 9000)
	p, err := Prompts{}.RenderPrompt(protocol.ModelContractChooseOrchestrationAction,
		withChoose(t, doc, map[string]string{screenKey: `"` + long + `"`}))
	require.NoError(t, err)
	require.Contains(t, p.Messages[1].Content, "screen (1 lines):\n<<<\n"+long+"\n>>>")

	for name, fields := range map[string]map[string]string{
		"a repeated action":         {actionsKey: `["DEFER","sentinel","DEFER"]`},
		"an unknown action":         {actionsKey: `["DEFER","sentinel"]`},
		"one action":                {actionsKey: `["DEFER"]`},
		"a message and an artifact": {"artifact": `{"priority":"LOW","triage":"NORMAL","age_ms":1}`},
		"no item":                   {"message": ""},
		"another task":              {"task_type": `"TRIAGE_MESSAGE"`},
		"a channel that is not on":  {"channel": `{"decision_id":"chd_01JB0000000000000000000010","outcome":"ALLOW","global":"OFF","link":"ALLOW"}`},
		"an unknown field":          {"sentinel_field": `1`},
	} {
		p, err := Prompts{}.RenderPrompt(protocol.ModelContractChooseOrchestrationAction, withChoose(t, doc, fields))
		require.ErrorIs(t, err, errBadPayload, name)
		require.NotContains(t, err.Error(), "sentinel", name)
		require.Equal(t, gateway.Prompt{}, p, name)
	}
}

// Through the real gateway, each prompt is rendered from the sanitised
// payload it travels with: the secret is masked in both, and rendering
// the payload as sent gives the messages as sent. So an endpoint that
// reads a CHOOSE request's letters from its payload reads the prompt's.
func TestPromptsThroughTheGateway(t *testing.T) {
	t.Parallel()
	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	text, err := json.Marshal(triageInput{Text: "please rotate the token " + secret})
	require.NoError(t, err)
	screen, err := json.Marshal("$ export TOKEN=" + secret + "\n> ")
	require.NoError(t, err)

	for _, tc := range []struct {
		name    string
		req     protocol.InferRequest
		version string
		answer  string
		label   string
	}{
		{
			"triage",
			protocol.InferRequest{Contract: protocol.ModelContractTriageMessage, Payload: text},
			TriagePromptVersion,
			`{"label":"REQUIRES_HUMAN","probabilities":{"REQUIRES_HUMAN":0.9,"NORMAL":0.1},"letter_mass":0.95}`,
			string(protocol.TriageLabelRequiresHuman),
		},
		{
			"choose",
			protocol.InferRequest{
				Contract: protocol.ModelContractChooseOrchestrationAction,
				Payload: withChoose(t, minimalChoose(t), map[string]string{
					actionsKey: `["DEFER","INJECT_PTY_PROMPT"]`, screenKey: string(screen),
				}),
			},
			ChoosePromptVersion,
			`{"label":"DEFER","probabilities":{"DEFER":0.8,"INJECT_PTY_PROMPT":0.2},"letter_mass":0.9,"evidence":["the human may type"]}`,
			string(protocol.OrchestratorActionDefer),
		},
	} {
		ep := &endpoint{answer: tc.answer}
		gw, _ := realGateway(t, ep)
		res, err := gw.Infer(context.Background(), tc.req)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.label, res.Label, tc.name)

		sent := ep.sent()
		require.Len(t, sent, 1)
		require.NotContains(t, sent[0], secret, "%s: the secret never leaves", tc.name)
		var wire struct {
			PromptVersion string            `json:"prompt_version"`
			Messages      []gateway.Message `json:"messages"`
			Payload       json.RawMessage   `json:"payload"`
		}
		require.NoError(t, json.Unmarshal([]byte(sent[0]), &wire))
		require.Equal(t, tc.version, wire.PromptVersion, "%s: the version is inside the digested input", tc.name)
		require.Contains(t, wire.Messages[1].Content, "••••", "%s: the template is applied to the masked text", tc.name)
		again, err := Prompts{}.RenderPrompt(tc.req.Contract, wire.Payload)
		require.NoError(t, err)
		require.Equal(t, again.Messages, wire.Messages, "%s: the messages are the sent payload's", tc.name)
	}
}
