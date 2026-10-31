package notifications

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const maxAdapterDiagnosticBytes = 4096

type limitedBuffer struct {
	buffer    bytes.Buffer
	remaining int
}

func (w *limitedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if w.remaining > 0 {
		keep := min(len(p), w.remaining)
		_, _ = w.buffer.Write(p[:keep])
		w.remaining -= keep
	}
	return written, nil
}

func (w *limitedBuffer) String() string { return w.buffer.String() }

// TMUXOptions configures one explicit tmux socket and client. Command exists
// so package tests can execute a recording fake; production leaves it empty.
type TMUXOptions struct {
	Command  string
	Socket   string
	Client   string
	Duration time.Duration
	Timeout  time.Duration
	Template string
}

type tmuxAdapter struct {
	command  string
	socket   string
	client   string
	duration time.Duration
	timeout  time.Duration
	template string
}

// NewTMUXAdapter validates all metadata rendering and process options.
func NewTMUXAdapter(opts TMUXOptions) (Adapter, error) {
	if opts.Command == "" {
		opts.Command = "tmux"
	}
	if opts.Template == "" {
		opts.Template = DefaultTemplate
	}
	if !filepath.IsAbs(opts.Socket) || !validProcessValue(opts.Socket, 512) ||
		!validProcessValue(opts.Client, 256) || opts.Duration < time.Millisecond ||
		opts.Duration > time.Minute || opts.Timeout < time.Millisecond || opts.Timeout > time.Minute {
		return nil, ErrInvalidOptions
	}
	if err := ValidateTemplate(opts.Template); err != nil {
		return nil, err
	}
	return &tmuxAdapter{
		command: opts.Command, socket: opts.Socket, client: opts.Client,
		duration: opts.Duration, timeout: opts.Timeout, template: opts.Template,
	}, nil
}

func validProcessValue(value string, maxRunes int) bool {
	if value == "" || utf8.RuneCountInString(value) > maxRunes {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func (*tmuxAdapter) Name() AdapterKind { return AdapterTMUX }

func (a *tmuxAdapter) Render(ctx context.Context, notification Notification) error {
	if err := notification.validate(); err != nil || notification.Class == classService {
		return &AdapterError{Reason: FailureAdapterFailed}
	}
	notice, err := renderTemplate(a.template, notification)
	if err != nil {
		return err
	}
	durationMS := a.duration.Milliseconds()
	if durationMS <= 0 {
		return &AdapterError{Reason: FailureAdapterFailed}
	}
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	args := []string{
		"-S", a.socket,
		"display-message",
		"-c", a.client,
		"-l",
		"-N",
		"-d", strconv.FormatInt(durationMS, 10),
		notice,
	}
	// #nosec G204 -- production fixes command to tmux; tests inject only a local fake.
	command := exec.CommandContext(callCtx, a.command, args...)
	stdout := &limitedBuffer{remaining: maxAdapterDiagnosticBytes}
	stderr := &limitedBuffer{remaining: maxAdapterDiagnosticBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	err = command.Run()
	if err == nil {
		return nil
	}
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		return &AdapterError{Reason: FailureAdapterTimeout, Retryable: true}
	}
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return &AdapterError{Reason: FailureAdapterUnavailable}
	}
	var execErr *exec.Error
	if errors.As(err, &execErr) {
		return &AdapterError{Reason: FailureAdapterUnavailable}
	}
	diagnostic := strings.ToLower(stdout.String() + stderr.String())
	if strings.Contains(diagnostic, "unknown option") || strings.Contains(diagnostic, "usage:") {
		return &AdapterError{Reason: FailureUnsupportedFlags}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return &AdapterError{Reason: FailureClientUnavailable}
	}
	return &AdapterError{Reason: FailureAdapterFailed, Retryable: true}
}

func (a *tmuxAdapter) String() string {
	return fmt.Sprintf("tmux adapter %s", a.Name())
}
