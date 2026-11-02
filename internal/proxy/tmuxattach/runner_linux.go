//go:build linux

package tmuxattach

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"time"
)

const commandWaitDelay = 100 * time.Millisecond

const maxDiagnosticBytes = 4096

type limitedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		keep := min(len(p), remaining)
		_, _ = b.buffer.Write(p[:keep])
	}
	if len(p) > remaining {
		b.overflow = true
	}
	return written, nil
}

func (b *limitedBuffer) Bytes() []byte { return b.buffer.Bytes() }

type execCommandRunner struct{}

func newCommandRunner() commandRunner { return execCommandRunner{} }

func (execCommandRunner) Run(ctx context.Context, server Server, args ...string) ([]byte, error) {
	argv := commandArguments(server, args...)
	// #nosec G204 -- every executable is fixed and all variable values are
	// validated argv elements; no shell is involved.
	command := exec.CommandContext(ctx, "sudo", argv...)
	command.WaitDelay = commandWaitDelay
	stdout := &limitedBuffer{limit: maxCaptureBytes + 1}
	stderr := &limitedBuffer{limit: maxDiagnosticBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrCommandTimeout
		}
		return nil, ErrCommand
	}
	if stdout.overflow || len(stdout.Bytes()) > maxCaptureBytes {
		return nil, ErrCaptureTooLarge
	}
	return stdout.Bytes(), nil
}
