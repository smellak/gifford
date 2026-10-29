package label

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// HookRecord is one line of HooksFile: the event as the hook sent it and
// when it arrived.
type HookRecord struct {
	ReceivedAt time.Time       `json:"received_at"`
	Event      json.RawMessage `json:"event"`
}

// RecordHook reads one Claude Code hook event (UserPromptSubmit,
// PreToolUse, PostToolUse, Notification, Stop) from r and appends it,
// with its arrival time, to HooksFile in dir. Its errors never quote the
// event.
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
	var compact bytes.Buffer
	if json.Compact(&compact, raw) != nil {
		return ErrBadHook
	}
	line, err := json.Marshal(HookRecord{ReceivedAt: now.UTC(), Event: compact.Bytes()})
	if err != nil {
		return fmt.Errorf("encode hook record: %w", err)
	}
	return appendLine(dir, HooksFile, append(line, '\n'))
}
