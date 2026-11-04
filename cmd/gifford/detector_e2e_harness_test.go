package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

const (
	// e2eCodexMode draws a synthetic screen of the measured Codex build, so
	// the real codex profile and the real detector both read it.
	e2eCodexMode    = "codex"
	e2eCodexVersion = "0.154.0"
	// e2eCodexVersionEnv overrides the version the synthetic Codex reports.
	e2eCodexVersionEnv = "GIFFORD_E2E_CODEX_VERSION"
)

// runE2ECodexHarness is a synthetic Codex at rest: output, the composer
// with its faint placeholder, and the footer. Typed text replaces the
// placeholder, a bracketed paste is unwrapped, and Enter logs a non-empty
// draft and clears it.
func runE2ECodexHarness() int {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return 2
	}
	defer func() { _ = term.Restore(int(os.Stdin.Fd()), oldState) }()
	logPath := os.Getenv(e2eRecipientLogEnv)
	draft := []byte{}
	draw := func() bool {
		composer := "› \x1b[2mAsk Codex to do anything\x1b[0m"
		if len(draft) > 0 {
			composer = "› " + string(draft)
		}
		_, err := fmt.Fprintf(os.Stdout,
			"\x1b[?2026h\x1b[H\x1b[2Jsynthetic output\r\n\r\n\r\n%s\r\n\r\n  gpt-x high · ~/work\x1b[4;%dH\x1b[?2026l",
			composer, 3+len(draft))
		return err == nil
	}
	if !draw() {
		return 2
	}
	buf := make([]byte, 256)
	escape := false
	for {
		n, readErr := os.Stdin.Read(buf)
		for _, b := range buf[:n] {
			switch {
			case escape:
				// A CSI sequence such as a paste bracket ends on its
				// final byte.
				escape = b < 0x40 || b > 0x7e || b == '['
			case b == 0x1b:
				escape = true
			case b == '\r' || b == '\n':
				if len(draft) > 0 && logPath != "" {
					file, openErr := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
					if openErr != nil {
						return 2
					}
					_, writeErr := fmt.Fprintln(file, string(draft))
					closeErr := file.Close()
					if writeErr != nil || closeErr != nil {
						return 2
					}
				}
				draft = draft[:0]
			case b == 0x03 || b == 0x15:
				draft = draft[:0]
			case b == 0x7f || b == 0x08:
				if len(draft) > 0 {
					draft = draft[:len(draft)-1]
				}
			case b >= 0x20:
				draft = append(draft, b)
			}
		}
		if n > 0 && !draw() {
			return 2
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return 0
			}
			return 2
		}
	}
}
