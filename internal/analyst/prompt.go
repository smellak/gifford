package analyst

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// PromptVersion names the ANALYZE_AGENT_STATE template, its system and
// user messages together. The gateway sends it with the rendered
// messages, so it is part of every call's input digest; a new template is
// new files and a new version, never an edit.
const PromptVersion = "analyze_agent_state.v1"

// Template files of PromptVersion.
const (
	systemFile = "prompts/analyze_agent_state.v1.system.txt"
	userFile   = "prompts/analyze_agent_state.v1.user.tmpl"
)

//go:embed prompts/analyze_agent_state.v1.system.txt prompts/analyze_agent_state.v1.user.tmpl
var promptFS embed.FS

// modelLabels are the ANALYZE_AGENT_STATE labels, in the closed A–G
// order of the model contract and of the system message's letters.
// BACKGROUND_WORK is never a model label: only a native event says so.
func modelLabels() []protocol.AgentPhase {
	return []protocol.AgentPhase{
		protocol.AgentPhaseIdleAtPrompt,
		protocol.AgentPhaseThinking,
		protocol.AgentPhaseRunningTool,
		protocol.AgentPhaseWaitingApproval,
		protocol.AgentPhaseAskingQuestion,
		protocol.AgentPhaseErrorOrExited,
		protocol.AgentPhaseUnknown,
	}
}

// Prompts renders the Analyst's prompt templates. The gateway applies it
// to the sanitised payload, after redaction, so the messages hold nothing
// redaction did not see and the input digest covers them as sent.
// gateway.PromptRenderer is satisfied by it.
type Prompts struct{}

// RenderPrompt renders the ANALYZE_AGENT_STATE system and user messages
// over a sanitised payload. Other contracts have no template yet and
// send none.
func (Prompts) RenderPrompt(c protocol.ModelContract, sanitised []byte) (gateway.Prompt, error) {
	if c != protocol.ModelContractAnalyzeAgentState {
		return gateway.Prompt{}, nil
	}
	var in analyzeInput
	dec := json.NewDecoder(bytes.NewReader(sanitised))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return gateway.Prompt{}, fmt.Errorf("prompt %s: payload is not an analyze input: %w", PromptVersion, errBadPayload)
	}
	system, err := promptFS.ReadFile(systemFile)
	if err != nil {
		return gateway.Prompt{}, fmt.Errorf("read prompt %s: %w", PromptVersion, err)
	}
	user, err := renderUser(in)
	if err != nil {
		return gateway.Prompt{}, err
	}
	return gateway.Prompt{Version: PromptVersion, Messages: []gateway.Message{
		{Role: gateway.RoleSystem, Content: string(system)},
		{Role: gateway.RoleUser, Content: user},
	}}, nil
}

// errBadPayload hides the decoder's error, which may quote the payload.
var errBadPayload = errors.New("bad payload")

// renderUser renders the user message over one analyze input. Values are
// formatted here, so the template only places them.
func renderUser(in analyzeInput) (string, error) {
	tmpl, err := template.ParseFS(promptFS, userFile)
	if err != nil {
		return "", fmt.Errorf("parse prompt %s: %w", PromptVersion, err)
	}
	num := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	harness := string(in.Harness)
	if harness == "" {
		harness = "unknown"
	}
	children := "none"
	switch {
	case in.NewChildProcesses == nil:
		children = "unknown"
	case len(in.NewChildProcesses) > 0:
		children = strings.Join(in.NewChildProcesses, ", ")
	}
	previous := "none"
	if in.PreviousState != "" {
		previous = fmt.Sprintf("%s (%ss ago)", in.PreviousState, num(in.PreviousStateAgeSeconds))
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, struct {
		analyzeInput
		Harness                string
		SecondsSinceLastOutput string
		Children               string
		Previous               string
		Screen                 string
	}{in, harness, num(in.SecondsSinceLastOutput), children, previous, strings.Join(in.ScreenLines, "\n")})
	if err != nil {
		return "", fmt.Errorf("render prompt %s: %w", PromptVersion, err)
	}
	return buf.String(), nil
}
