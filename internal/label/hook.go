package label

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// maxHookBytes bounds one hook event.
const maxHookBytes = 1 << 20

var (
	// ErrEmptyHook means the hook sent nothing.
	ErrEmptyHook = errors.New("hook event is empty")
	// ErrBadHook means the hook sent something other than one JSON object.
	ErrBadHook = errors.New("hook event is not one JSON object")
	// ErrHookTooLarge means the hook event is over 1 MiB.
	ErrHookTooLarge = errors.New("hook event is too large")
	// ErrBadHookHarness means the command did not name a supported harness.
	ErrBadHookHarness = errors.New("hook harness is invalid")
	// ErrBadHookCapture means the local capture id is absent or malformed.
	ErrBadHookCapture = errors.New("hook capture session is invalid")
)

// HookRecord is one line of HooksFile: what align needs of an event,
// and when it arrived.
type HookRecord struct {
	ReceivedAt       time.Time         `json:"received_at"`
	Harness          protocol.Harness  `json:"harness"`
	CaptureSessionID protocol.OpaqueID `json:"capture_session_id"`
	Event            HookEvent         `json:"event"`
}

// HookEvent is the closed projection of a hook event that is kept: its event
// and session ids, wait/error enums and the minimum native root metadata.
// Nothing else survives, the prompt, tool input and output, paths and message
// above all: text a human typed is never persisted.
type HookEvent struct {
	Name             string `json:"hook_event_name"`
	SessionID        string `json:"session_id,omitempty"`
	RequestID        string `json:"request_id,omitempty"`
	NotificationType string `json:"notification_type,omitempty"`
	ErrorType        string `json:"error_type,omitempty"`
	ToolName         string `json:"tool_name,omitempty"`
	ReplyType        string `json:"reply_type,omitempty"`
	HasParent        *bool  `json:"has_parent,omitempty"`
	PostStop         *bool  `json:"post_stop,omitempty"`
	Source           string `json:"source,omitempty"`
}

const (
	// unknownHookEnum replaces an unrecognised enum without retaining its
	// value. An absent or ill-shaped member remains absent instead.
	unknownHookEnum = "unknown"
	// permissionPrompt is the Claude or Muse notification that confirms an
	// approval prompt has remained visible long enough to be observed.
	permissionPrompt = "permission_prompt"
	// questionPrompt is the Muse notification that confirms an interactive
	// question has remained visible long enough to be observed.
	questionPrompt = "question_prompt"
	// openCodeAborted is the one OpenCode session error that is not a failed
	// agent state: it records cancellation by the user or caller.
	openCodeAborted     = "message_aborted"
	sessionStartStartup = "startup"
	sessionStartResume  = "resume"
	sessionStartClear   = "clear"
	sessionStartCompact = "compact"
)

// RecordHook reads one harness hook event from r and appends its projection,
// harness and arrival time to HooksFile in dir. Its errors never quote the
// event.
func RecordHook(dir string, now time.Time, harness protocol.Harness, captureID protocol.OpaqueID, r io.Reader) error {
	if !harness.Valid() {
		return ErrBadHookHarness
	}
	if !ValidCaptureSessionID(captureID) {
		return ErrBadHookCapture
	}
	raw, err := io.ReadAll(io.LimitReader(r, maxHookBytes+1))
	if err != nil {
		return errors.New("read hook event")
	}
	if len(raw) > maxHookBytes {
		return ErrHookTooLarge
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ErrEmptyHook
	}
	var obj map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	var extra any
	if dec.Decode(&obj) != nil || obj == nil || !errors.Is(dec.Decode(&extra), io.EOF) {
		return ErrBadHook
	}
	ev, ok := projectHook(harness, obj)
	if !ok {
		return ErrBadHook
	}
	line, err := json.Marshal(HookRecord{
		ReceivedAt: now.UTC(), Harness: harness, CaptureSessionID: captureID, Event: ev,
	})
	if err != nil {
		return fmt.Errorf("encode hook record: %w", err)
	}
	return appendLine(dir, HooksFile, append(line, '\n'))
}

// projectHook keeps the allowed members, each only when it has the
// expected shape; an event without a well-formed name is refused.
func projectHook(harness protocol.Harness, obj map[string]json.RawMessage) (HookEvent, bool) {
	properties := map[string]json.RawMessage{}
	propertiesOK := json.Unmarshal(obj["properties"], &properties) == nil && properties != nil
	name, ok := projectedHookName(harness, obj)
	if !ok {
		return HookEvent{}, false
	}
	ev := HookEvent{Name: name}
	id := hookString(obj, "session_id")
	if id == "" {
		id = hookString(obj, "sessionID")
	}
	if id == "" {
		id = hookString(properties, "sessionID")
	}
	if protocol.OpaqueID(id).Valid() {
		ev.SessionID = id
	}
	if harness == protocol.HarnessOpencode && ev.Name == openCodeSessionCreated {
		ev.SessionID, ev.HasParent = projectOpenCodeSession(properties, propertiesOK)
	}
	if harness == protocol.HarnessOpencode {
		var requestID string
		switch ev.Name {
		case openCodeAsked, openCodeQuestionAsked:
			requestID = hookString(properties, "id")
		case openCodeReplied, openCodeQuestionReplied, openCodeQuestionRejected:
			requestID = hookString(properties, "requestID")
		}
		if protocol.OpaqueID(requestID).Valid() {
			ev.RequestID = requestID
		}
	}
	if ev.Name == "Notification" {
		if kind := hookString(obj, "notification_type"); kind != "" {
			ev.NotificationType = normalisedNotification(harness, kind)
		}
	}
	if ev.Name == "SessionStart" {
		ev.Source = normalisedSessionStartSource(harness, hookString(obj, "source"))
	}
	if ev.Name == "StopFailure" || ev.Name == "session.error" {
		rawError := projectedHookError(obj, properties)
		if rawError != "" {
			ev.ErrorType = normalisedError(harness, rawError)
		}
	}
	tool := hookString(obj, "tool_name")
	if tool == "" {
		tool = hookString(properties, "tool_name")
	}
	if questionTool(harness, tool) {
		ev.ToolName = tool
	}
	if harness == protocol.HarnessOpencode && ev.Name == openCodeReplied {
		if reply := hookString(properties, "reply"); reply != "" {
			ev.ReplyType = normalisedOpenCodeReply(reply)
		}
	}
	var postStop bool
	if json.Unmarshal(obj["post_stop"], &postStop) == nil {
		ev.PostStop = &postStop
	}
	return ev, true
}

// projectedHookName accepts only the event vocabulary used by each harness.
// OpenCode's native plugin envelope is the sole source whose event name is in
// type; a type member on any other harness is never treated as a hook event.
func projectedHookName(harness protocol.Harness, obj map[string]json.RawMessage) (string, bool) {
	nameRaw, hasHookName := obj["hook_event_name"]
	name := ""
	switch {
	case harness == protocol.HarnessOpencode:
		if hasHookName {
			return "", false
		}
		name = hookString(obj, "type")
	case hasHookName:
		if json.Unmarshal(nameRaw, &name) != nil {
			return "", false
		}
	default:
		return "", false
	}
	if !matches(name, 64, func(c byte) bool {
		return isLetter(c) || isDigit(c) || c == '_' || c == '.'
	}) || !knownHookName(harness, name) {
		return "", false
	}
	return name, true
}

func knownHookName(harness protocol.Harness, name string) bool {
	switch harness {
	case protocol.HarnessClaudeCode:
		switch name {
		case hookSessionStart, hookUserPromptSubmit, hookPreToolUse, hookPermission,
			hookPostToolUse, hookPostToolUseFailure, hookNotification, hookSubagentStart,
			hookStop, hookStopFailure, hookInterrupt, hookPreCompact, hookSessionEnd:
			return true
		}
	case protocol.HarnessCodex:
		switch name {
		case hookSessionStart, hookUserPromptSubmit, hookPreToolUse, hookPermission,
			hookPostToolUse, hookSubagentStart, hookStop, hookInterrupt,
			hookPreCompact, hookSessionEnd:
			return true
		}
	case protocol.HarnessMuseCode:
		switch name {
		case hookSessionStart, hookUserPromptSubmit, hookPreToolUse, museToolUseStart,
			hookPermission, hookPostToolUse, hookPostToolUseFailure, hookNotification,
			hookSubagentStart, hookStop, hookStopFailure, hookInterrupt,
			hookPreCompact, hookSessionEnd:
			return true
		}
	case protocol.HarnessOpencode:
		switch name {
		case openCodeSessionCreated, openCodeAsked, openCodeReplied,
			openCodeQuestionAsked, openCodeQuestionReplied, openCodeQuestionRejected,
			openCodeSessionError:
			return true
		}
	default:
	}
	return false
}

// projectOpenCodeSession keeps only the native session id and whether the
// session.created info object carried a valid parent id. A missing parent is
// represented by a non-nil false pointer; null, malformed and incomplete
// shapes stay unknown, so they can never turn into root evidence by default.
func projectOpenCodeSession(properties map[string]json.RawMessage, propertiesOK bool) (string, *bool) {
	if !propertiesOK {
		return "", nil
	}
	var info map[string]json.RawMessage
	if json.Unmarshal(properties["info"], &info) != nil || info == nil {
		return "", nil
	}
	id := hookString(info, "id")
	if !protocol.OpaqueID(id).Valid() {
		return "", nil
	}
	outer := hookString(properties, "sessionID")
	if !protocol.OpaqueID(outer).Valid() || outer != id {
		return "", nil
	}
	rawParent, present := info["parentID"]
	if !present {
		hasParent := false
		return id, &hasParent
	}
	parent := hookString(info, "parentID")
	if !protocol.OpaqueID(parent).Valid() || len(rawParent) == 0 {
		return id, nil
	}
	hasParent := true
	return id, &hasParent
}

func hookString(from map[string]json.RawMessage, key string) string {
	var v string
	if json.Unmarshal(from[key], &v) != nil {
		return ""
	}
	return v
}

func projectedHookError(obj, properties map[string]json.RawMessage) string {
	if value := hookString(obj, "error"); value != "" {
		return value
	}
	if value := hookString(properties, "error"); value != "" {
		return value
	}
	var errorObject map[string]json.RawMessage
	if json.Unmarshal(obj["error"], &errorObject) != nil {
		_ = json.Unmarshal(properties["error"], &errorObject)
	}
	if value := hookString(errorObject, "name"); value != "" {
		return value
	}
	return hookString(errorObject, "type")
}

// ValidCaptureSessionID reports the exact local id shape generated by run.
func ValidCaptureSessionID(id protocol.OpaqueID) bool {
	if !id.Valid() {
		return false
	}
	rest, ok := strings.CutPrefix(string(id), "run-")
	if !ok {
		return false
	}
	pid, nanos, ok := strings.Cut(rest, "-")
	if !ok || strings.Contains(nanos, "-") || !canonicalPositiveDecimal(pid) || !canonicalPositiveDecimal(nanos) {
		return false
	}
	if _, err := strconv.ParseUint(pid, 10, 64); err != nil {
		return false
	}
	n, err := strconv.ParseInt(nanos, 10, 64)
	return err == nil && n > 0
}

func canonicalPositiveDecimal(s string) bool {
	if len(s) == 0 || s[0] == '0' {
		return false
	}
	for i := range len(s) {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

func normalisedSessionStartSource(harness protocol.Harness, value string) string {
	// These three harnesses expose the same closed source vocabulary. A
	// source is only a candidate: Align still requires an exact-version root
	// source enabled after a pilot.
	if harness == protocol.HarnessClaudeCode || harness == protocol.HarnessCodex ||
		harness == protocol.HarnessMuseCode {
		switch value {
		case sessionStartStartup, sessionStartResume, sessionStartClear, sessionStartCompact:
			return value
		}
	}
	return ""
}

// normalisedNotification admits only the notification types documented by
// the named harness. Unknown values collapse to one fixed enum and therefore
// cannot smuggle notification text into the labels file.
func normalisedNotification(harness protocol.Harness, value string) string {
	switch harness {
	case protocol.HarnessClaudeCode:
		switch value {
		case permissionPrompt, "idle_prompt", "auth_success", "elicitation_dialog",
			"elicitation_url_dialog", "elicitation_complete", "elicitation_response",
			"agent_needs_input", "agent_completed", "quota_auto_resume_fired",
			"quota_auto_resume_stale", "quota_auto_resume_disabled", "plugin_notification":
			return value
		}
	case protocol.HarnessMuseCode:
		switch value {
		case permissionPrompt, questionPrompt:
			return value
		}
	default:
	}
	return unknownHookEnum
}

// normalisedError keeps only documented error categories. The spelling is
// canonical snake_case across harnesses; arbitrary details never survive.
func normalisedError(harness protocol.Harness, value string) string {
	switch harness {
	case protocol.HarnessClaudeCode:
		switch value {
		case "rate_limit", "overloaded", "authentication_failed", "oauth_org_not_allowed",
			"account_on_hold", "billing_error", "invalid_request", "model_not_found",
			"server_error", "max_output_tokens", "cloud_credential_error", unknownHookEnum:
			return value
		}
	case protocol.HarnessOpencode:
		switch value {
		case "ProviderAuthError":
			return "provider_auth"
		case "MessageOutputLengthError":
			return "message_output_length"
		case "MessageAbortedError":
			return openCodeAborted
		case "StructuredOutputError":
			return "structured_output"
		case "ContextOverflowError":
			return "context_overflow"
		case "ContentFilterError":
			return "content_filter"
		case "APIError":
			return "api"
		case "UnknownError":
			return unknownHookEnum
		}
	default:
		return unknownHookEnum
	}
	return unknownHookEnum
}

func normalisedOpenCodeReply(value string) string {
	switch value {
	case "once", "always", "reject":
		return value
	default:
		return unknownHookEnum
	}
}

// questionTool admits only the interactive-question tool of each harness.
// Every other tool name is content-bearing metadata and is omitted.
func questionTool(harness protocol.Harness, tool string) bool {
	switch harness {
	case protocol.HarnessClaudeCode:
		return tool == "AskUserQuestion"
	case protocol.HarnessCodex:
		return tool == "request_user_input"
	case protocol.HarnessMuseCode:
		return tool == "request_user_input"
	case protocol.HarnessOpencode:
		return tool == "question"
	default:
		return false
	}
}

func matches(s string, maxLen int, ok func(byte) bool) bool {
	if len(s) == 0 || len(s) > maxLen {
		return false
	}
	for i := range len(s) {
		if !ok(s[i]) {
			return false
		}
	}
	return true
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
