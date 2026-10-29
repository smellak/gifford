package label

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/protocol"
)

// ErrBadAlignConfig means the align configuration is invalid.
var ErrBadAlignConfig = errors.New("invalid align config")

// EventWindow says which instants of a window are matched against the
// label timeline. S1 (10.14) fixes the value; until then it is a
// parameter.
type EventWindow string

// EventWindow values.
const (
	// EventWindowAnchor matches the instant the window describes,
	// Provenance.AnchorAt.
	EventWindowAnchor EventWindow = "anchor"
	// EventWindowSpan matches the whole window, Provenance.WindowStart to
	// WindowEnd.
	EventWindowSpan EventWindow = "span"
)

// AlignConfig tunes align. DefaultAlignConfig documents the defaults.
type AlignConfig struct {
	EventWindow EventWindow
	// Margin drops a window that comes this close to a transition.
	Margin time.Duration
	// MaxInterval bounds how long one state lasts without a new event,
	// so a missed hook does not label hours.
	MaxInterval time.Duration
	// HookHarnesses have hooks: their windows get hook labels and never
	// weak ones. The others get weak labels.
	HookHarnesses []protocol.Harness
	// Silence is the quiet time that ends a turn; a burst of at least
	// BurstBytes in the trailing 2 s after it starts the next.
	Silence    time.Duration
	BurstBytes int64
}

// DefaultAlignConfig returns the defaults:
//
//	event_window: anchor          # provisional until S1 (10.14)
//	margin_ms: 300                # drop windows within ±300 ms of a transition
//	max_interval_seconds: 300     # a state without events lasts at most 5 min
//	hook_harnesses: [claude-code] # hook labels for these, weak for the rest
//	weak:
//	  silence_seconds: 3          # this much quiet ends a turn
//	  burst_bytes: 64             # this much output in 2 s after it starts one
func DefaultAlignConfig() AlignConfig {
	return AlignConfig{
		EventWindow:   EventWindowAnchor,
		Margin:        300 * time.Millisecond,
		MaxInterval:   5 * time.Minute,
		HookHarnesses: []protocol.Harness{protocol.HarnessClaudeCode},
		Silence:       3 * time.Second,
		BurstBytes:    64,
	}
}

type rawAlignConfig struct {
	EventWindow        *string   `yaml:"event_window"`
	MarginMS           *int64    `yaml:"margin_ms"`
	MaxIntervalSeconds *float64  `yaml:"max_interval_seconds"`
	HookHarnesses      *[]string `yaml:"hook_harnesses"`
	Weak               *struct {
		SilenceSeconds *float64 `yaml:"silence_seconds"`
		BurstBytes     *int64   `yaml:"burst_bytes"`
	} `yaml:"weak"`
}

// LoadAlignConfig reads the YAML file at path over the defaults. An empty
// path or a missing file gives the defaults.
func LoadAlignConfig(path string) (AlignConfig, error) {
	if path == "" {
		return DefaultAlignConfig(), nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return DefaultAlignConfig(), nil
	}
	if err != nil {
		return AlignConfig{}, fmt.Errorf("read align config: %w", err)
	}
	return ParseAlignConfig(bytes.NewReader(b))
}

// ParseAlignConfig reads YAML over the defaults. Unknown keys and
// out-of-range values are errors.
func ParseAlignConfig(r io.Reader) (AlignConfig, error) {
	var raw rawAlignConfig
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		return AlignConfig{}, fmt.Errorf("%w: %w", ErrBadAlignConfig, err)
	}
	cfg := DefaultAlignConfig()
	if raw.EventWindow != nil {
		cfg.EventWindow = EventWindow(*raw.EventWindow)
	}
	if raw.MarginMS != nil {
		cfg.Margin = time.Duration(*raw.MarginMS) * time.Millisecond
	}
	if raw.MaxIntervalSeconds != nil {
		cfg.MaxInterval = seconds(*raw.MaxIntervalSeconds)
	}
	if raw.HookHarnesses != nil {
		cfg.HookHarnesses = make([]protocol.Harness, 0, len(*raw.HookHarnesses))
		for _, h := range *raw.HookHarnesses {
			cfg.HookHarnesses = append(cfg.HookHarnesses, protocol.Harness(h))
		}
	}
	if w := raw.Weak; w != nil {
		if w.SilenceSeconds != nil {
			cfg.Silence = seconds(*w.SilenceSeconds)
		}
		if w.BurstBytes != nil {
			cfg.BurstBytes = *w.BurstBytes
		}
	}
	return cfg, cfg.Validate()
}

// seconds converts, saturating instead of overflowing; Validate bounds
// the result.
func seconds(s float64) time.Duration {
	if s > 1e6 || s < -1e6 || s != s {
		return -1
	}
	return time.Duration(s * float64(time.Second))
}

// Validate checks the ranges.
func (c AlignConfig) Validate() error {
	bad := func(what string) error { return fmt.Errorf("%w: %s", ErrBadAlignConfig, what) }
	switch {
	case c.EventWindow != EventWindowAnchor && c.EventWindow != EventWindowSpan:
		return bad("event_window must be anchor or span")
	case c.Margin < 0 || c.Margin > time.Minute:
		return bad("margin_ms must be 0-60000")
	case c.MaxInterval <= 0 || c.MaxInterval > 24*time.Hour:
		return bad("max_interval_seconds must be above 0 and at most 86400")
	case c.Silence <= 0 || c.Silence > time.Hour:
		return bad("weak.silence_seconds must be above 0 and at most 3600")
	case c.BurstBytes < 1:
		return bad("weak.burst_bytes must be at least 1")
	}
	known := map[string]bool{}
	for _, h := range protocol.Harness("").EnumValues() {
		known[h] = true
	}
	for _, h := range c.HookHarnesses {
		if !known[string(h)] {
			return bad("unknown harness in hook_harnesses")
		}
	}
	return nil
}
