package analyst

import (
	"bytes"
	"embed"
	"fmt"
	"text/template"

	"github.com/smellak/gifford/internal/protocol"
)

// PromptVersion names the ANALYZE_AGENT_STATE template. It travels in
// the payload, so it is part of every call's input digest; a new template
// is a new file and a new version, never an edit.
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

// renderPrompt renders the instructions for one harness. The screen is
// never rendered into the prompt: it travels as rows, so the gateway
// redacts it with the screen rules.
func renderPrompt(h protocol.Harness) (string, error) {
	tmpl, err := template.ParseFS(promptFS, "prompts/analyze_agent_state.v1.tmpl")
	if err != nil {
		return "", fmt.Errorf("parse prompt %s: %w", PromptVersion, err)
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, struct {
		Harness protocol.Harness
		Labels  []protocol.AgentPhase
	}{h, modelLabels()})
	if err != nil {
		return "", fmt.Errorf("render prompt %s: %w", PromptVersion, err)
	}
	return buf.String(), nil
}
