package analyst

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// PromptVersion names the ANALYZE_AGENT_STATE template. The gateway sends
// it with the rendered prompt, so it is part of every call's input
// digest; a new template is a new file and a new version, never an edit.
const PromptVersion = "analyze_agent_state.v1"

//go:embed prompts/analyze_agent_state.v1.tmpl
var promptFS embed.FS

// modelLabels are the ANALYZE_AGENT_STATE labels, in the closed A–G
// order of the model contract. BACKGROUND_WORK is never a model label:
// only a native event says so.
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
// to the sanitised payload, after redaction, so the prompt holds nothing
// redaction did not see and the input digest covers it as sent.
// gateway.PromptRenderer is satisfied by it.
type Prompts struct{}

// RenderPrompt renders the ANALYZE_AGENT_STATE template over a sanitised
// payload. Other contracts have no template yet and send none.
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
	text, err := renderPrompt(in)
	if err != nil {
		return gateway.Prompt{}, err
	}
	return gateway.Prompt{Version: PromptVersion, Text: text}, nil
}

// errBadPayload hides the decoder's error, which may quote the payload.
var errBadPayload = errors.New("bad payload")

// renderPrompt renders the template over one analyze input.
func renderPrompt(in analyzeInput) (string, error) {
	tmpl, err := template.ParseFS(promptFS, "prompts/analyze_agent_state.v1.tmpl")
	if err != nil {
		return "", fmt.Errorf("parse prompt %s: %w", PromptVersion, err)
	}
	state := "none"
	if in.NewChildProcesses == nil {
		state = "unknown"
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, struct {
		analyzeInput
		Labels        []protocol.AgentPhase
		Children      []string
		ChildrenState string
	}{in, modelLabels(), in.NewChildProcesses, state})
	if err != nil {
		return "", fmt.Errorf("render prompt %s: %w", PromptVersion, err)
	}
	return buf.String(), nil
}
