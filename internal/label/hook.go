package label

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
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
)

// HookRecord is one line of HooksFile: what align needs of an event,
// and when it arrived.
type HookRecord struct {
	ReceivedAt time.Time `json:"received_at"`
	Event      HookEvent `json:"event"`
}

// HookEvent is the projection of a hook event that is kept: its name, its
// session and, for a Notification, its kind. Nothing else survives, the
// prompt, the tool input and output, the paths and the message above all:
// the text a human typed is never persisted.
type HookEvent struct {
	Name             string `json:"hook_event_name"`
	SessionID        string `json:"session_id,omitempty"`
	NotificationType string `json:"notification_type,omitempty"`
}

// permissionPrompt is the kind of the Notification that asks for a
// permission.
const permissionPrompt = "permission_prompt"

// RecordHook reads one Claude Code hook event (UserPromptSubmit,
// PreToolUse, PostToolUse, Notification, Stop) from r and appends its
// projection, with its arrival time, to HooksFile in dir. Its errors
// never quote the event.
func RecordHook(dir string, now time.Time, r io.Reader) error {
	raw, err := io.ReadAll(io.LimitReader(r, maxHookBytes+1))
	if err != nil {
		return fmt.Errorf("read hook event: %w", err)
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
	ev, ok := projectHook(obj)
	if !ok {
		return ErrBadHook
	}
	line, err := json.Marshal(HookRecord{ReceivedAt: now.UTC(), Event: ev})
	if err != nil {
		return fmt.Errorf("encode hook record: %w", err)
	}
	return appendLine(dir, HooksFile, append(line, '\n'))
}

// projectHook keeps the allowed members, each only when it has the
// expected shape; an event without a well-formed name is refused.
func projectHook(obj map[string]json.RawMessage) (HookEvent, bool) {
	str := func(key string) string {
		var v string
		if json.Unmarshal(obj[key], &v) != nil {
			return ""
		}
		return v
	}
	ev := HookEvent{Name: str("hook_event_name")}
	if !matches(ev.Name, 64, isLetter) {
		return HookEvent{}, false
	}
	if id := str("session_id"); matches(id, 128, func(c byte) bool { return isLetter(c) || isDigit(c) || c == '-' || c == '_' }) {
		ev.SessionID = id
	}
	if ev.Name == "Notification" {
		kind := str("notification_type")
		switch {
		case matches(kind, 64, func(c byte) bool { return c >= 'a' && c <= 'z' || c == '_' }):
			ev.NotificationType = kind
		case kind == "" && strings.Contains(strings.ToLower(str("message")), "permission"):
			// Older events name the kind only in the message, which is
			// not kept.
			ev.NotificationType = permissionPrompt
		}
	}
	return ev, true
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
