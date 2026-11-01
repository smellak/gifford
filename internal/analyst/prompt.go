package analyst

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// Prompt versions. Each names one contract's template, its system and
// user messages together. The gateway sends the version with the
// rendered messages, so it is part of every call's input digest; a new
// template is new files and a new version, never an edit.
const (
	// PromptVersion is the ANALYZE_AGENT_STATE template a zero Prompts
	// renders, so the one the live Analyst uses. Its letters A–G are
	// modelLabels, in order.
	PromptVersion = analyzePromptV1
	// TriagePromptVersion is the TRIAGE_MESSAGE template. Its letters A–D
	// are the triage labels in the contract's table order.
	TriagePromptVersion = "triage_message.v1"
	// ChoosePromptVersion is the CHOOSE_ORCHESTRATION_ACTION template. Its
	// letters belong to one request: A is the first of the payload's
	// allowed_actions, B the second, and so on. The system message holds
	// no letter, so it is the same for every request. v2 says what the
	// guards have checked and when to choose each action, because v1 gave
	// the model no criterion and it deferred deliveries the guards allowed.
	ChoosePromptVersion = "choose_orchestration_action.v2"
)

// The ANALYZE_AGENT_STATE templates. They read the same input and answer
// with the same letters. v2 adds the screen features to the user message,
// and to the system message the rules that read them.
const (
	analyzePromptV1 = "analyze_agent_state.v1"
	analyzePromptV2 = "analyze_agent_state.v2"
)

// AnalyzePromptVersions are the embedded ANALYZE_AGENT_STATE templates,
// oldest first. Prompts renders any of them.
func AnalyzePromptVersions() []string { return []string{analyzePromptV1, analyzePromptV2} }

// The template files of a version are prompts/<version>.system.txt and
// prompts/<version>.user.tmpl.
//
//go:embed prompts/analyze_agent_state.v1.system.txt prompts/analyze_agent_state.v1.user.tmpl
//go:embed prompts/analyze_agent_state.v2.system.txt prompts/analyze_agent_state.v2.user.tmpl
//go:embed prompts/triage_message.v1.system.txt prompts/triage_message.v1.user.tmpl
//go:embed prompts/choose_orchestration_action.v1.system.txt prompts/choose_orchestration_action.v1.user.tmpl
//go:embed prompts/choose_orchestration_action.v2.system.txt prompts/choose_orchestration_action.v2.user.tmpl
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

// Prompts renders the prompt templates of the model contracts. The
// gateway applies it to the sanitised payload, after redaction, so the
// messages hold nothing redaction did not see and the input digest covers
// them as sent. gateway.PromptRenderer is satisfied by it.
type Prompts struct {
	// Analyze is the ANALYZE_AGENT_STATE template, one of
	// AnalyzePromptVersions; empty is PromptVersion.
	Analyze string
}

// RenderPrompt renders a contract's system and user messages over its
// sanitised payload, which must decode strictly as the contract's input.
func (p Prompts) RenderPrompt(c protocol.ModelContract, sanitised []byte) (gateway.Prompt, error) {
	switch c {
	case protocol.ModelContractAnalyzeAgentState:
		version := p.Analyze
		if version == "" {
			version = PromptVersion
		}
		if !slices.Contains(AnalyzePromptVersions(), version) {
			return gateway.Prompt{}, fmt.Errorf("prompt %q: %w", version, errNoTemplate)
		}
		var in analyzeInput
		if err := decodeStrict(sanitised, &in); err != nil {
			return gateway.Prompt{}, fmt.Errorf("prompt %s: payload is not an analyze input: %w", version, err)
		}
		return render(version, newAnalyzeView(in))
	case protocol.ModelContractTriageMessage:
		var in triageInput
		if err := decodeStrict(sanitised, &in); err != nil {
			return gateway.Prompt{}, fmt.Errorf("prompt %s: payload is not a triage input: %w", TriagePromptVersion, err)
		}
		return render(TriagePromptVersion, in)
	case protocol.ModelContractChooseOrchestrationAction:
		var in protocol.OrchestrationRequest
		if err := decodeStrict(sanitised, &in); err != nil {
			return gateway.Prompt{}, fmt.Errorf("prompt %s: payload is not an orchestration request: %w", ChoosePromptVersion, err)
		}
		view, err := newChooseView(in)
		if err != nil {
			return gateway.Prompt{}, fmt.Errorf("prompt %s: %w", ChoosePromptVersion, err)
		}
		return render(ChoosePromptVersion, view)
	default:
		return gateway.Prompt{}, errNoTemplate
	}
}

var (
	// errBadPayload hides the decoder's error, which may quote the payload.
	errBadPayload = errors.New("bad payload")
	// errNoTemplate is a contract with no prompt template.
	errNoTemplate = errors.New("no prompt template")
)

// Words the user messages show for a value the payload does not give.
const (
	unknownValue = "unknown"
	noneValue    = "none"
)

// decodeStrict decodes a sanitised payload into v and refuses unknown
// fields. Its error is errBadPayload, never the decoder's.
func decodeStrict(sanitised []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(sanitised))
	dec.DisallowUnknownFields()
	if dec.Decode(v) != nil {
		return errBadPayload
	}
	return nil
}

// render renders one template version: its fixed system message, and its
// user message over view. The view's values are formatted already, so
// the template only places them.
func render(version string, view any) (gateway.Prompt, error) {
	system, err := promptFS.ReadFile("prompts/" + version + ".system.txt")
	if err != nil {
		return gateway.Prompt{}, fmt.Errorf("read prompt %s: %w", version, err)
	}
	tmpl, err := template.ParseFS(promptFS, "prompts/"+version+".user.tmpl")
	if err != nil {
		return gateway.Prompt{}, fmt.Errorf("parse prompt %s: %w", version, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, view); err != nil {
		return gateway.Prompt{}, fmt.Errorf("render prompt %s: %w", version, err)
	}
	return gateway.Prompt{Version: version, Messages: []gateway.Message{
		{Role: gateway.RoleSystem, Content: string(system)},
		{Role: gateway.RoleUser, Content: buf.String()},
	}}, nil
}

// analyzeView is the ANALYZE_AGENT_STATE user message over one analyze
// input. Only v2 shows its Features.
type analyzeView struct {
	analyzeInput
	Harness                string
	SecondsSinceLastOutput string
	Children               string
	Previous               string
	Screen                 string
	Features               analyzeFeatures
}

func newAnalyzeView(in analyzeInput) analyzeView {
	children := noneValue
	switch {
	case in.NewChildProcesses == nil:
		children = unknownValue
	case len(in.NewChildProcesses) > 0:
		children = strings.Join(in.NewChildProcesses, ", ")
	}
	previous := noneValue
	if in.PreviousState != "" {
		previous = fmt.Sprintf("%s (%ss ago)", in.PreviousState, formatNum(in.PreviousStateAgeSeconds))
	}
	return analyzeView{
		analyzeInput: in, Harness: orUnknown(string(in.Harness)),
		SecondsSinceLastOutput: formatNum(in.SecondsSinceLastOutput),
		Children:               children, Previous: previous, Screen: strings.Join(in.ScreenLines, "\n"),
		Features: features(in),
	}
}

// triageInput is the TRIAGE_MESSAGE payload: the text of one message.
type triageInput struct {
	Text string `json:"text"`
}

// optionLetters letter the options of a CHOOSE_ORCHESTRATION_ACTION
// request, in its order. A valid request allows each action at most
// once, so there are always enough.
const optionLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// chooseView is the CHOOSE_ORCHESTRATION_ACTION user message over one
// orchestration request. The ids, the task type and the channel outcome,
// which validation holds to ALLOW, are not shown.
type chooseView struct {
	Harness, Adapter, Capabilities                         string
	AgentPhase, PIdle, SecondsIdle, HumanDraft, Attention  string
	Kind, Priority, Triage, AgeMS, SameThread, LowPriority string
	Policy                                                 string
	Global, Link, AutoContinue, Signals                    string
	Loop                                                   string
	ScreenLines                                            int
	Screen                                                 string
	Options                                                string
}

// newChooseView formats one orchestration request, which must validate
// with its screen aside: the gateway bounds the screen before sanitising,
// and redaction may change its length.
func newChooseView(in protocol.OrchestrationRequest) (chooseView, error) {
	unscreened := in
	unscreened.Screen = ""
	if unscreened.Validate() != nil {
		return chooseView{}, errBadPayload
	}
	kind, item := "message", in.Message
	if item == nil {
		kind, item = "artifact", in.Artifact
	}
	options := make([]string, len(in.AllowedActions))
	for i, action := range in.AllowedActions {
		options[i] = optionLetters[i:i+1] + " " + string(action)
	}
	policy := noneValue
	if p := in.Policy; p != nil {
		policy = fmt.Sprintf("human_decision_after_s=%d batch_window_s=%d", p.HumanDecisionAfterS, p.BatchWindowS)
	}
	loop := noneValue
	if l := in.Channel.Loop; l != nil {
		loop = fmt.Sprintf("mode=%s turns=%s messages=%s elapsed_ms=%s max_duration_ms=%s", l.Mode, l.Turns, l.Messages,
			optional(l.ElapsedMS, formatInt), optional(l.MaxDurationMS, formatInt))
	}
	signals := make([]string, len(in.Channel.PatternSignals))
	for i, s := range in.Channel.PatternSignals {
		signals[i] = string(s)
	}
	screenLines := 0
	if in.Screen != "" {
		screenLines = strings.Count(in.Screen, "\n") + 1
	}
	return chooseView{
		Harness: orUnknown(string(in.Agent.Harness)), Adapter: string(in.Agent.Adapter),
		Capabilities: capabilityNames(in.Agent.Capabilities),
		AgentPhase:   string(in.State.AgentPhase), PIdle: formatNum(in.State.PIdle),
		SecondsIdle: optional(in.State.SecondsIdle, formatNum),
		HumanDraft:  string(in.State.HumanDraft), Attention: string(in.State.Attention),
		Kind: kind, Priority: string(item.Priority), Triage: string(item.Triage), AgeMS: formatInt(item.AgeMS),
		SameThread:  optional(item.SameThreadPending, formatInt),
		LowPriority: optional(item.LowPriorityPending, formatInt),
		Policy:      policy,
		Global:      string(in.Channel.Global), Link: string(in.Channel.Link),
		AutoContinue: optional(in.Channel.AutoContinue, strconv.FormatBool),
		Signals:      listOrNone(signals), Loop: loop,
		ScreenLines: screenLines, Screen: in.Screen, Options: strings.Join(options, "\n"),
	}, nil
}

// capabilityNames lists the capabilities an agent has, by their JSON
// names in the protocol's order.
func capabilityNames(c protocol.Capabilities) string {
	v := reflect.ValueOf(c)
	var names []string
	for i := range v.NumField() {
		if f := v.Field(i); f.Kind() == reflect.Bool && f.Bool() {
			name, _, _ := strings.Cut(v.Type().Field(i).Tag.Get("json"), ",")
			names = append(names, name)
		}
	}
	return listOrNone(names)
}

// listOrNone joins a list of names with commas, or says none.
func listOrNone(names []string) string {
	if len(names) == 0 {
		return noneValue
	}
	return strings.Join(names, ",")
}

// optional formats *v, or says unknown when the payload leaves it out.
func optional[T any](v *T, format func(T) string) string {
	if v == nil {
		return unknownValue
	}
	return format(*v)
}

// orUnknown is s, or unknown when it is empty.
func orUnknown(s string) string {
	if s == "" {
		return unknownValue
	}
	return s
}

func formatNum(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func formatInt(v int64) string { return strconv.FormatInt(v, 10) }
