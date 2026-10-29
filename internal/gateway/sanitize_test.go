package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

// captureTransport keeps every body sent and answers with a fixed body.
type captureTransport struct {
	mu     sync.Mutex
	bodies [][]byte
	answer []byte
}

func (c *captureTransport) Send(_ context.Context, body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bodies = append(c.bodies, append([]byte(nil), body...))
	return c.answer, nil
}

func (c *captureTransport) sent() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.bodies...)
}

// decodedStrings returns every string the receiver reads from body,
// escapes decoded.
func decodedStrings(t *testing.T, body []byte) string {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal(body, &v))
	var sb strings.Builder
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			sb.WriteString(x)
			sb.WriteByte('\n')
		case []any:
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			for k, e := range x {
				sb.WriteString(k)
				sb.WriteByte('\n')
				walk(e)
			}
		}
	}
	walk(v)
	return sb.String()
}

// escapeFirst JSON-escapes the first rune of s, so a textual scan of the
// encoded payload does not see s.
func escapeFirst(s string) string {
	const hexdigits = "0123456789abcdef"
	return `\u00` + string([]byte{hexdigits[s[0]>>4], hexdigits[s[0]&0xf]}) + s[1:]
}

func orchestrationPayload(t *testing.T, screen string) protocol.Payload {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "protocol", "testdata", "orchestration_request", "valid_minimal.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &doc))
	doc["screen"] = json.RawMessage(`"` + screen + `"`)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	return protocol.Payload(out)
}

const okTriageAnswer = `{"label":"NORMAL","probabilities":{"NORMAL":1}}`

func TestSemanticJSONEscapeCannotBypassRedaction(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	escaped := escapeFirst(secret)
	require.NotContains(t, escaped, secret, "the encoded payload hides the secret from a textual scan")
	cases := map[string]protocol.InferRequest{
		"orchestration screen": {
			Contract: protocol.ModelContractChooseOrchestrationAction,
			Payload:  orchestrationPayload(t, escaped),
		},
		"analyze screen lines": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"screen_lines":["` + escaped + `"]}`),
		},
		"triage text": {
			Contract: protocol.ModelContractTriageMessage,
			Payload:  protocol.Payload(`{"text":"use ` + escaped + ` for the push"}`),
		},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := &captureTransport{answer: []byte(okTriageAnswer)}
			f := newFixture(t, "http://127.0.0.1:1", tr, nil)
			res, _ := f.gw.Infer(context.Background(), req)
			sent := tr.sent()
			require.Len(t, sent, 1)
			require.NotContains(t, decodedStrings(t, sent[0]), secret, "the receiver cannot rebuild the secret")
			require.NotContains(t, string(sent[0]), escaped)
			require.Equal(t, int64(1), res.CallRecord.RedactionsCount)
		})
	}
}

func TestWrappedScreenSecretCannotBypassRedaction(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	// A TUI hard break cuts the token inside a frame, the shape the
	// redactor's screen rules join and its text rules never do.
	head, tail := "tok "+secret[:20], "│ "+secret[20:]
	rows, err := json.Marshal([]string{head, tail, "tests: 9 passed"})
	require.NoError(t, err)
	cases := map[string]protocol.InferRequest{
		"analyze screen lines": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"screen_lines":` + string(rows) + `}`),
		},
		"orchestration screen": {
			Contract: protocol.ModelContractChooseOrchestrationAction,
			Payload:  orchestrationPayload(t, head+`\n`+tail),
		},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := &captureTransport{answer: []byte(okTriageAnswer)}
			f := newFixture(t, "http://127.0.0.1:1", tr, nil)
			res, _ := f.gw.Infer(context.Background(), req)
			sent := tr.sent()
			require.Len(t, sent, 1)
			got := decodedStrings(t, sent[0])
			require.NotContains(t, got, secret[20:], "the wrapped tail is masked")
			require.NotContains(t, got, secret[:20], "and so is the head")
			require.NotContains(t, got, secret)
			require.Positive(t, res.CallRecord.RedactionsCount)
		})
	}
}

func TestUnsanitisablePayloadSendsNothing(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	cases := map[string]protocol.InferRequest{
		"unclassified field": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"screen_lines":[],"notes":"sentinel"}`),
		},
		"unclassified nested field": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"output_bytes":{"last_1h":1}}`),
		},
		"text where a number goes": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"seconds_since_last_output":"sentinel"}`),
		},
		"object where rows go": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"screen_lines":{"a":"sentinel"}}`),
		},
		"secret in a token": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"harness":"` + secret + `"}`),
		},
		"secret in a child process": {
			Contract: protocol.ModelContractAnalyzeAgentState,
			Payload:  protocol.Payload(`{"new_child_processes":["` + secret + `"]}`),
		},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := &captureTransport{answer: []byte(okTriageAnswer)}
			f := newFixture(t, "http://127.0.0.1:1", tr, nil)
			res, err := f.gw.Infer(context.Background(), req)
			require.ErrorIs(t, err, ErrRedactionFailed)
			require.Empty(t, tr.sent(), "nothing is sent")
			require.Equal(t, protocol.GlimmerCallStatusRedactionFailed, res.CallRecord.Status)
			require.NoError(t, res.CallRecord.Validate())
			require.NotContains(t, err.Error(), "sentinel")
			require.NotContains(t, err.Error(), secret)
		})
	}
}

func TestEveryContractHasASchema(t *testing.T) {
	t.Parallel()
	for _, c := range protocol.ModelContract("").EnumValues() {
		_, ok := payloadSchema(protocol.ModelContract(c))
		require.True(t, ok, c)
	}
	tr := &captureTransport{answer: []byte(`{"label":"QUEUE","probabilities":{"QUEUE":0.9,"DEFER":0.1}}`)}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	_, err := f.gw.Infer(context.Background(), protocol.InferRequest{
		Contract: protocol.ModelContractChooseOrchestrationAction,
		Payload:  orchestrationPayload(t, "tests: 9 passed"),
	})
	require.NoError(t, err, "a full orchestration payload is sanitised field by field")
	require.Len(t, tr.sent(), 1)
}

func TestResponseRejectsTrailingJSON(t *testing.T) {
	t.Parallel()
	for name, answer := range map[string]string{
		"closing bracket": okTriageAnswer + `]`,
		"second object":   okTriageAnswer + `{}`,
		"trailing word":   okTriageAnswer + ` ok`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := &captureTransport{answer: []byte(answer)}
			f := newFixture(t, "http://127.0.0.1:1", tr, nil)
			res, err := f.gw.Infer(context.Background(), protocol.InferRequest{
				Contract: protocol.ModelContractTriageMessage,
				Payload:  protocol.Payload(`{"text":"hi"}`),
			})
			require.ErrorIs(t, err, ErrInvalidOutput)
			require.Equal(t, protocol.GlimmerCallStatusInvalid, res.CallRecord.Status)
			require.Empty(t, res.Label)
		})
	}
	tr := &captureTransport{answer: []byte(okTriageAnswer + " \n")}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	_, err := f.gw.Infer(context.Background(), protocol.InferRequest{
		Contract: protocol.ModelContractTriageMessage,
		Payload:  protocol.Payload(`{"text":"hi"}`),
	})
	require.NoError(t, err, "trailing white space is fine")
}

// The request validation refuses these shapes first; the sanitiser
// refuses them on its own as well.
func TestSanitisePayloadRefusesShapes(t *testing.T) {
	t.Parallel()
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	for name, payload := range map[string]string{
		"not an object":  `["sentinel"]`,
		"trailing value": `{"text":"hi"}{"text":"sentinel"}`,
		"trailing token": `{"text":"hi"}]`,
		"malformed":      `{"text":`,
		"too deep":       strings.Repeat(`{"agent":`, maxSanitiseDepth+2) + `1` + strings.Repeat(`}`, maxSanitiseDepth+2),
	} {
		_, _, _, err := sanitisePayload(red, protocol.ModelContractTriageMessage, []byte(payload))
		require.ErrorIs(t, err, errUnsanitisable, name)
	}
	_, _, _, err = sanitisePayload(red, "CLASSIFY", []byte(`{}`))
	require.ErrorIs(t, err, errUnsanitisable, "an unknown contract has no schema")
	out, version, count, err := sanitisePayload(red, protocol.ModelContractTriageMessage, nil)
	require.NoError(t, err)
	require.Equal(t, "{}", string(out))
	require.Equal(t, redact.PolicyVersion, version, "an empty payload still names the policy")
	require.Zero(t, count)
}

func TestAnalyzeScreenIsCutAfterRedaction(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	rows := make([]string, 0, screenproj.MaxLines+3)
	rows = append(rows, "tok "+secret[:20], "│ "+secret[20:])
	for i := range screenproj.MaxLines - 2 {
		rows = append(rows, fmt.Sprintf("log %02d", i))
	}
	rows = append(rows, strings.Repeat("y", screenproj.MaxRowRunes+5), "", "")
	payload, err := json.Marshal(map[string]any{"screen_lines": rows})
	require.NoError(t, err)

	tr := &captureTransport{answer: []byte(unknownAnswer)}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	res, err := f.gw.Infer(context.Background(), protocol.InferRequest{Contract: protocol.ModelContractAnalyzeAgentState, Payload: payload})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.CallRecord.RedactionsCount, "the whole screen was redacted: the cut-off head still counts")

	var wire struct {
		Payload struct {
			ScreenLines []string `json:"screen_lines"`
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(tr.sent()[0], &wire))
	got := wire.Payload.ScreenLines
	require.Len(t, got, screenproj.MaxLines, "trailing empty rows dropped, then the last 40 kept")
	require.True(t, strings.HasPrefix(got[0], "│ ••••"), "the kept tail is masked")
	require.NotContains(t, strings.Join(got, "\n"), secret[20:])
	require.Len(t, []rune(got[len(got)-1]), screenproj.MaxRowRunes, "each row bounded")

	require.Equal(t, []string{"a", "b", "", ""}, cutRows([]string{"a", "b", "", ""}, 0, 0), "no cut configured, nothing changes")
}

// TestProjectedScreenPassesUnchanged pins the gateway as the second
// defence: a screen the Analyst already projected comes out of the
// sanitiser byte for byte, with no new substitution.
func TestProjectedScreenPassesUnchanged(t *testing.T) {
	t.Parallel()
	secret := testSecret(t)
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	tall := make([]string, 0, screenproj.MaxLines+7)
	tall = append(tall, "tok "+secret[:20], "│ "+secret[20:])
	for i := range screenproj.MaxLines + 5 {
		tall = append(tall, fmt.Sprintf("log %02d", i))
	}
	for name, rows := range map[string][]string{
		"a secret":                 {"$ go test ./...", "token " + secret, "ok"},
		"a control rune inside":    {"token " + secret[:12] + "\a" + secret[12:] + "\x00 "},
		"a secret at the rune cut": {strings.Repeat("x", screenproj.MaxRowRunes-11) + " " + secret},
		"a wrapped secret, cut":    tall,
		"trailing empty rows":      {"done", "", "  ", ""},
	} {
		proj, err := screenproj.Project(screenproj.Input{Rows: rows, Cols: 80, Draft: protocol.DraftStateClosed},
			screenproj.Policies{Lines: screenproj.MaxLines, Redactor: red, Mask: screenproj.NoMask()})
		require.NoError(t, err, name)
		payload, err := json.Marshal(map[string]any{"screen_lines": proj.Lines})
		require.NoError(t, err)
		out, version, count, err := sanitisePayload(red, protocol.ModelContractAnalyzeAgentState, payload)
		require.NoError(t, err, name)
		require.Equal(t, proj.Versions.Redaction, version, name)
		require.Zero(t, count, "%s: no new substitution", name)
		require.JSONEq(t, string(payload), string(out), "%s: the projected screen passes unchanged", name)
	}

	// The projection's cut is this one: on rows the redactor and the
	// cleaning leave alone, Project gives what cutRows gives.
	many := make([]string, 0, screenproj.MaxLines+10)
	for i := range screenproj.MaxLines + 10 {
		many = append(many, fmt.Sprintf("row %02d", i))
	}
	for name, rows := range map[string][]string{
		"no rows":           nil,
		"only empty rows":   {"", "", ""},
		"inner empty rows":  {"a", "", "b", "", ""},
		"more than 40 rows": append(many, "", ""),
		"long rows":         {strings.Repeat("é", screenproj.MaxRowRunes+88), strings.Repeat("y", screenproj.MaxRowRunes), "z"},
	} {
		proj, err := screenproj.Project(screenproj.Input{Rows: rows, Cols: 80, Draft: protocol.DraftStateClosed},
			screenproj.Policies{Lines: screenproj.MaxLines, Redactor: red, Mask: screenproj.NoMask()})
		require.NoError(t, err, name)
		require.Zero(t, proj.Redactions, name)
		require.Equal(t, cutRows(rows, screenproj.MaxLines, screenproj.MaxRowRunes), proj.Lines, name)
	}
}
