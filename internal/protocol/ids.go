package protocol

import (
	"strings"
)

// This file holds the shared identifier and scalar types of §3. Every type
// exposes JSONPattern for the schema generator and Valid for the Go walker.
// The Go checkers below use small character-class functions, never
// package-level regexp values.

// AgentID identifies an agent.
type AgentID string

// JSONPattern returns the ECMA-262 pattern of an agent id.
func (AgentID) JSONPattern() string { return `^[a-z0-9][a-z0-9._-]{0,63}$` }

// Valid reports whether the id matches its pattern.
func (v AgentID) Valid() bool { return checkAgentNode(string(v)) }

// NodeID identifies a node.
type NodeID string

// JSONPattern returns the ECMA-262 pattern of a node id.
func (NodeID) JSONPattern() string { return `^[a-z0-9][a-z0-9._-]{0,63}$` }

// Valid reports whether the id matches its pattern.
func (v NodeID) Valid() bool { return checkAgentNode(string(v)) }

func checkAgentNode(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	if !isLowerAlnum(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !isLowerAlnum(c) && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func isLowerAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

// ULID is a 26-character Crockford base32 identifier.
type ULID string

// JSONPattern returns the ECMA-262 pattern of a ULID.
func (ULID) JSONPattern() string { return `^[0-9A-HJKMNP-TV-Z]{26}$` }

// Valid reports whether the value is a 26-character ULID.
func (v ULID) Valid() bool {
	if len(v) != 26 {
		return false
	}
	for i := range 26 {
		if !isULIDChar(v[i]) {
			return false
		}
	}
	return true
}

func isULIDChar(c byte) bool {
	switch {
	case c >= '0' && c <= '9':
		return true
	case c >= 'A' && c <= 'H', c == 'J' || c == 'K' || c == 'M' || c == 'N':
		return true
	case c >= 'P' && c <= 'T', c == 'V' || c == 'W' || c == 'X' || c == 'Y' || c == 'Z':
		return true
	default:
		return false
	}
}

// ChannelDecisionID identifies a channel gate decision.
type ChannelDecisionID string

// JSONPattern returns the ECMA-262 pattern of a channel decision id.
func (ChannelDecisionID) JSONPattern() string { return `^chd_[0-9A-HJKMNP-TV-Z]{26}$` }

// Valid reports whether the value matches its pattern.
func (v ChannelDecisionID) Valid() bool {
	return strings.HasPrefix(string(v), "chd_") && ULID(v[4:]).Valid()
}

// LoopID identifies a governed loop.
type LoopID string

// JSONPattern returns the ECMA-262 pattern of a loop id.
func (LoopID) JSONPattern() string { return `^loop_[0-9A-HJKMNP-TV-Z]{26}$` }

// Valid reports whether the value matches its pattern.
func (v LoopID) Valid() bool {
	return strings.HasPrefix(string(v), "loop_") && ULID(v[5:]).Valid()
}

// ThreadRef is the opaque thread id carried in the wire envelope.
type ThreadRef string

// JSONPattern returns the ECMA-262 pattern of a thread reference.
func (ThreadRef) JSONPattern() string { return `^thr_[0-9A-HJKMNP-TV-Z]{26}$` }

// Valid reports whether the value matches its pattern.
func (v ThreadRef) Valid() bool {
	return strings.HasPrefix(string(v), "thr_") && ULID(v[4:]).Valid()
}

// ThreadID is the readable thread id.
type ThreadID string

// JSONPattern returns the ECMA-262 pattern of a thread id.
func (ThreadID) JSONPattern() string { return `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` }

// Valid reports whether the value matches its pattern.
func (v ThreadID) Valid() bool { return checkThreadID(string(v), false) }

// OpaqueID is a free-form opaque identifier.
type OpaqueID string

// JSONPattern returns the ECMA-262 pattern of an opaque id.
func (OpaqueID) JSONPattern() string { return `^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$` }

// Valid reports whether the value matches its pattern.
func (v OpaqueID) Valid() bool { return checkThreadID(string(v), true) }

func checkThreadID(s string, extra bool) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	if !isAlnum(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isThreadChar(s[i], extra) {
			return false
		}
	}
	return true
}

func isThreadChar(c byte, extra bool) bool {
	if isAlnum(c) || c == '.' || c == '_' || c == '-' {
		return true
	}
	return extra && (c == ':' || c == '@')
}

func isAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// SHA256Hex holds 64 lowercase hex characters.
type SHA256Hex string

// JSONPattern returns the ECMA-262 pattern of a hex digest.
func (SHA256Hex) JSONPattern() string { return `^[0-9a-f]{64}$` }

// Valid reports whether the value is 64 lowercase hex characters.
func (v SHA256Hex) Valid() bool {
	if len(v) != 64 {
		return false
	}
	for i := range 64 {
		if !isHexLower(v[i]) {
			return false
		}
	}
	return true
}

func isHexLower(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f'
}

// Digest holds "sha256:" plus 64 lowercase hex characters.
type Digest string

// JSONPattern returns the ECMA-262 pattern of a digest.
func (Digest) JSONPattern() string { return `^sha256:[0-9a-f]{64}$` }

// Valid reports whether the value matches its pattern.
func (v Digest) Valid() bool {
	return strings.HasPrefix(string(v), "sha256:") && SHA256Hex(v[7:]).Valid()
}

// SigningKeyID identifies an agent signing key.
type SigningKeyID string

// JSONPattern returns the ECMA-262 pattern of a signing key id.
func (SigningKeyID) JSONPattern() string {
	return `^ed25519:[a-z0-9][a-z0-9._-]{0,63}:v[1-9][0-9]{0,8}$`
}

// Valid reports whether the value matches its pattern.
func (v SigningKeyID) Valid() bool { return checkKeyID(string(v), "ed25519:") }

// EncryptionKeyID identifies an agent encryption key.
type EncryptionKeyID string

// JSONPattern returns the ECMA-262 pattern of an encryption key id.
func (EncryptionKeyID) JSONPattern() string {
	return `^x25519:[a-z0-9][a-z0-9._-]{0,63}:v[1-9][0-9]{0,8}$`
}

// Valid reports whether the value matches its pattern.
func (v EncryptionKeyID) Valid() bool { return checkKeyID(string(v), "x25519:") }

func checkKeyID(s, prefix string) bool {
	if !strings.HasPrefix(s, prefix) {
		return false
	}
	rest := s[len(prefix):]
	name, ver, found := strings.Cut(rest, ":")
	if !found || !checkAgentNode(name) {
		return false
	}
	if len(ver) < 2 || ver[0] != 'v' || ver[1] == '0' || len(ver) > 10 {
		return false
	}
	for i := 1; i < len(ver); i++ {
		if ver[i] < '0' || ver[i] > '9' {
			return false
		}
	}
	return true
}

// BlobRef references raw bytes stored outside the document.
type BlobRef string

// JSONPattern returns the ECMA-262 pattern of a blob reference.
func (BlobRef) JSONPattern() string { return `^blob://[A-Za-z0-9._/-]{1,256}$` }

// Valid reports whether the value matches its pattern.
func (v BlobRef) Valid() bool {
	const prefix = "blob://"
	if !strings.HasPrefix(string(v), prefix) {
		return false
	}
	rest := string(v[len(prefix):])
	if len(rest) < 1 || len(rest) > 256 {
		return false
	}
	for i := range len(rest) {
		c := rest[i]
		if !isAlnum(c) && c != '.' && c != '_' && c != '/' && c != '-' {
			return false
		}
	}
	return true
}

// TmuxPaneID identifies a tmux pane.
type TmuxPaneID string

// JSONPattern returns the ECMA-262 pattern of a tmux pane id.
func (TmuxPaneID) JSONPattern() string { return `^%[0-9]{1,9}$` }

// Valid reports whether the value matches its pattern.
func (v TmuxPaneID) Valid() bool {
	if len(v) < 2 || len(v) > 10 || v[0] != '%' {
		return false
	}
	for i := 1; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

// UsageRatio is "used/limit", e.g. "7/12".
type UsageRatio string

// JSONPattern returns the ECMA-262 pattern of a usage ratio.
func (UsageRatio) JSONPattern() string { return `^[0-9]{1,9}/[0-9]{1,9}$` }

// Valid reports whether the value matches its pattern.
func (v UsageRatio) Valid() bool {
	used, limit, found := strings.Cut(string(v), "/")
	return found && checkUint(used, 9) && checkUint(limit, 9)
}

func checkUint(s string, maxDigits int) bool {
	if len(s) < 1 || len(s) > maxDigits {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// NoticeLine is the single line injected into a harness.
type NoticeLine string

// JSONPattern returns the ECMA-262 pattern of a notice line.
func (NoticeLine) JSONPattern() string { return "^[^\x00-\x1f\x7f]{1,512}$" }

// Valid reports whether the value holds 1–512 runes without controls.
func (v NoticeLine) Valid() bool {
	n := 0
	for _, r := range string(v) {
		if r < 0x20 || r == 0x7f {
			return false
		}
		n++
	}
	return n >= 1 && n <= 512
}

// EventType names an event family and name, e.g. "capture.chunk".
type EventType string

// JSONPattern returns the ECMA-262 pattern of an event type.
func (EventType) JSONPattern() string {
	return `^(input|output|io|process|pty|session|adapter|prompt|turn|tool|approval|question|notification|background_task|subagent|context|human_notification|control|channel|loop|message|artifact|capture)\.[a-z][a-z0-9_]{0,63}$`
}

// Valid reports whether the value matches its pattern.
func (v EventType) Valid() bool {
	family, name, found := strings.Cut(string(v), ".")
	if !found || strings.Contains(name, ".") {
		return false
	}
	switch family {
	case "input", "output", "io", familyProcess, familyPTY, "session", "adapter",
		"prompt", "turn", "tool", "approval", "question", "notification",
		"background_task", "subagent", "context", "human_notification",
		"control", "channel", "loop", familyMessage, "artifact", familyCapture:
	default:
		return false
	}
	if len(name) < 2 || len(name) > 65 {
		return false
	}
	if name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !isEventNameChar(name[i]) {
			return false
		}
	}
	return true
}

func isEventNameChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_'
}

// Shared vocabulary words: several enums freeze the same JSON word, so it
// is declared once here instead of repeating the literal.
const (
	unknownWord   = "UNKNOWN"
	pausedWord    = "PAUSED"
	noneWord      = "none"
	familyPTY     = "pty"
	familyCapture = "capture"
	familyProcess = "process"
	familyMessage = "message"
)

// ControlID identifies a channel control row (v1.1 use; pattern frozen in v1).
type ControlID string

// JSONPattern returns the ECMA-262 pattern of a control id.
func (ControlID) JSONPattern() string { return `^ctl_[0-9A-HJKMNP-TV-Z]{26}$` }

// Valid reports whether the value matches its pattern.
func (v ControlID) Valid() bool {
	return strings.HasPrefix(string(v), "ctl_") && ULID(v[4:]).Valid()
}

// ApprovalID identifies an approval request (v1.1 use; pattern frozen in v1).
type ApprovalID string

// JSONPattern returns the ECMA-262 pattern of an approval id.
func (ApprovalID) JSONPattern() string { return `^apr_[0-9A-HJKMNP-TV-Z]{26}$` }

// Valid reports whether the value matches its pattern.
func (v ApprovalID) Valid() bool {
	return strings.HasPrefix(string(v), "apr_") && ULID(v[4:]).Valid()
}

// ApprovalOption is a human approval choice (grammar checked in v1.1).
type ApprovalOption string

// JSONPattern returns the ECMA-262 pattern of an approval option.
func (ApprovalOption) JSONPattern() string {
	return `^(approve_once|approve_thread:[1-9][0-9]{0,5}[smh]|set_link:allow|reject|extend:\+[1-9][0-9]{0,5}|set_mode:step|steps:[1-9][0-9]{0,5}|resume|stop|deliver_now|hold)$`
}

// Valid reports whether the value matches the option grammar.
func (v ApprovalOption) Valid() bool {
	s := string(v)
	switch s {
	case "approve_once", "reject", "resume", "stop", "deliver_now", "hold",
		"set_link:allow", "set_mode:step":
		return true
	}
	kind, param, found := strings.Cut(s, ":")
	if !found {
		return false
	}
	switch kind {
	case "approve_thread":
		return checkDurationParam(param)
	case "extend":
		return len(param) > 1 && param[0] == '+' && checkCountParam(param[1:])
	case "steps":
		return checkCountParam(param)
	default:
		return false
	}
}

// checkDurationParam checks [1-9][0-9]{0,5}[smh].
func checkDurationParam(s string) bool {
	if len(s) < 2 || len(s) > 7 {
		return false
	}
	unit := s[len(s)-1]
	if unit != 's' && unit != 'm' && unit != 'h' {
		return false
	}
	return checkCountParam(s[:len(s)-1])
}

// checkCountParam checks [1-9][0-9]{0,5}.
func checkCountParam(s string) bool {
	if len(s) < 1 || len(s) > 6 || s[0] == '0' {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
