// Package setup connects the coding-agent harnesses installed on a machine
// to Gifford: it detects each harness and its exact version, reports whether
// Gifford has a measured terminal profile for it, and installs the MCP
// server, the observe-only adapter and, with consent, the pre-approval of
// Gifford's four message tools. Every file it changes is backed up first and
// merged without removing anything already there; Undo restores the backup.
package setup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/profiles"
)

// MessageTools are Gifford's four MCP message tools, the only tools setup
// ever pre-approves.
var MessageTools = []string{"send_message", "list_pending_messages", "read_message", "ack_message"}

// Runner runs one external command and returns its combined output.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// Env is everything setup reads from the machine. Tests replace each part.
type Env struct {
	Home       string
	ConfigHome string
	DataHome   string
	// CodexHome is $CODEX_HOME, or empty for ~/.codex.
	CodexHome string
	// Gifford is the absolute path of the running gifford binary.
	Gifford  string
	LookPath func(string) (string, error)
	Run      Runner
	Now      func() time.Time
	Profiles profiles.Profiles
}

// Harness is one detected harness.
type Harness struct {
	Name     string
	Binary   string
	Version  string
	Measured bool
	// Observer says the installed version is the one the observe-only
	// adapter supports.
	Observer bool
}

// AutomaticDelivery says Gifford may deliver a notice on its own: only a
// measured terminal profile allows it. Any other build stays on approval.
func (h Harness) AutomaticDelivery() bool { return h.Measured }

// Step is one change setup proposes.
type Step struct {
	Harness string
	// Consent marks a step that pre-approves tools; it runs only when the
	// operator agreed.
	Consent     bool
	Description string
	// Files are the files the step may change; each is backed up first.
	Files []string
	apply func(context.Context, *applyState) error
	// done reports the step already in place, so nothing changes.
	done bool
}

// Done reports whether the change is already in place.
func (s Step) Done() bool { return s.done }

// Plan is what setup would do on this machine.
type Plan struct {
	Harnesses []Harness
	Missing   []string
	Steps     []Step
	Notes     []string
}

type harnessSpec struct {
	name   string
	binary string
	// observer lists the exact versions the observe-only adapter supports.
	observer []string
	plan     func(Env, Harness) ([]Step, []string, error)
}

func specs() []harnessSpec {
	return []harnessSpec{
		{name: "claude-code", binary: "claude", observer: []string{"2.1.280"}, plan: claudePlan},
		{name: "codex", binary: "codex", observer: []string{"0.154.0"}, plan: codexPlan},
		{name: "opencode", binary: "opencode", observer: []string{"1.18.29"}, plan: opencodePlan},
		{
			name: "muse-code", binary: "muse",
			observer: []string{"1.4.13-R6996.1", "1.4.14-R7309.1"}, plan: musePlan,
		},
	}
}

// Detect finds the supported harnesses on PATH and their exact versions.
func Detect(ctx context.Context, env Env) ([]Harness, []string) {
	var found []Harness
	var missing []string
	for _, spec := range specs() {
		path, err := env.LookPath(spec.binary)
		if err != nil {
			missing = append(missing, spec.name)
			continue
		}
		h := Harness{Name: spec.name, Binary: path}
		versionCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		out, err := env.Run(versionCtx, path, "--version")
		cancel()
		if err == nil {
			h.Version = versionOf(out)
		}
		if profile, err := env.Profiles.Profile(spec.name, h.Version); err == nil {
			h.Measured = profile.Measured && profile.Harness == spec.name
		}
		h.Observer = h.Version != "" && slices.Contains(spec.observer, h.Version)
		found = append(found, h)
	}
	return found, missing
}

// versionOf is the first version any output line states: a harness may
// print a warning before its version.
func versionOf(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if version := profiles.ParseVersion(strings.TrimSpace(line)); version != "" {
			return version
		}
	}
	return ""
}

func firstLine(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) != "" {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// NewPlan detects the harnesses and lists every change setup would make.
func NewPlan(ctx context.Context, env Env) (Plan, error) {
	if !filepath.IsAbs(env.Gifford) || !filepath.IsAbs(env.Home) {
		return Plan{}, errors.New("setup needs absolute home and gifford paths")
	}
	harnesses, missing := Detect(ctx, env)
	plan := Plan{Harnesses: harnesses, Missing: missing}
	for _, h := range harnesses {
		for _, spec := range specs() {
			if spec.name != h.Name {
				continue
			}
			steps, notes, err := spec.plan(env, h)
			if err != nil {
				return Plan{}, fmt.Errorf("plan %s: %w", h.Name, err)
			}
			for i := range steps {
				steps[i].Harness = h.Name
			}
			plan.Steps = append(plan.Steps, steps...)
			for _, note := range notes {
				plan.Notes = append(plan.Notes, h.Name+": "+note)
			}
		}
	}
	return plan, nil
}

// WriteReport prints the detected harnesses and the planned changes.
func WriteReport(w io.Writer, plan Plan, applied bool) {
	_, _ = fmt.Fprintf(w, "%-12s %-18s %-11s %s\n", "HARNESS", "VERSION", "PROFILE", "AUTOMATIC_DELIVERY")
	for _, h := range plan.Harnesses {
		version := h.Version
		if version == "" {
			version = "unknown"
		}
		profile := "UNMEASURED"
		if h.Measured {
			profile = "MEASURED"
		}
		_, _ = fmt.Fprintf(w, "%-12s %-18s %-11s %t\n", h.Name, version, profile, h.AutomaticDelivery())
	}
	for _, name := range plan.Missing {
		_, _ = fmt.Fprintf(w, "%-12s %-18s %-11s %s\n", name, "not installed", "-", "-")
	}
	_, _ = fmt.Fprintln(w)
	verb := "would"
	if applied {
		verb = "will"
	}
	for _, step := range plan.Steps {
		state := verb
		switch {
		case step.done:
			state = "already"
		case step.Consent:
			state = verb + " (asks first)"
		}
		_, _ = fmt.Fprintf(w, "[%s] %s: %s\n", state, step.Harness, step.Description)
	}
	for _, note := range plan.Notes {
		_, _ = fmt.Fprintf(w, "note: %s\n", note)
	}
}

func fileMode(path string) os.FileMode {
	if info, err := os.Stat(path); err == nil {
		return info.Mode().Perm()
	}
	return 0o600
}
