package tmuxattach

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/tmuxhelper"
)

// helperBinary is the privileged closed tmux interface. The absolute
// install path is deployment data owned by the helper's sudoers; the
// node only ever names this binary, never a tmux option or socket.
const helperBinary = "gifford-tmux"

// helperFrameVersion is the only output framing the node accepts from
// the helper.
const helperFrameVersion = "GTMX/1"

// helperVerbs is the closed request grammar measured on the helper
// branch. The pane flag marks verbs that address one registered pane;
// the freeze may only shrink this table through helperArgv.
var helperVerbs = map[string]bool{
	"panes":        false,
	"observe":      true,
	"clients":      false,
	"capture":      true,
	"notice-full":  true,
	"notice-short": true,
	"enter":        true,
}

// helperArgv builds the helper request argv after the binary:
// "<verb> <sha256-manifest> <bank> [pane]". It is the single grammar
// point: a freeze only changes this function and its table test.
func helperArgv(revision, bank, verb, pane string) ([]string, error) {
	needsPane, ok := helperVerbs[verb]
	if !ok {
		return nil, fmt.Errorf("helper verb: %w", ErrInvalidOptions)
	}
	if !validHex64(revision) {
		return nil, fmt.Errorf("helper revision: %w", ErrInvalidOptions)
	}
	if !validBankID(bank) {
		return nil, fmt.Errorf("helper bank: %w", ErrInvalidOptions)
	}
	argv := []string{verb, revision, bank}
	if needsPane {
		if !validPaneID(pane) {
			return nil, fmt.Errorf("helper pane: %w", ErrInvalidOptions)
		}
		return append(argv, pane), nil
	}
	if pane != "" {
		return nil, fmt.Errorf("helper pane: %w", ErrInvalidOptions)
	}
	return argv, nil
}

func validHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for index := 0; index < len(value); index++ {
		char := value[index]
		digit := char >= '0' && char <= '9'
		lower := char >= 'a' && char <= 'f'
		if !digit && !lower {
			return false
		}
	}
	return true
}

func validBankID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for index := 0; index < len(value); index++ {
		char := value[index]
		if char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validPaneID(value string) bool {
	if value == "%0" {
		return true
	}
	if len(value) < 2 || value[0] != '%' || value[1] < '1' || value[1] > '9' {
		return false
	}
	for index := 2; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

// decodeHelperFrame parses one GTMX/1 frame: "GTMX/1 <code> <len>\n"
// followed by the base64 body and a trailing newline. Truncated or
// malformed output is an error, never a guess.
func decodeHelperFrame(output []byte) (tmuxhelper.ExitCode, []byte, error) {
	line, rest, found := strings.Cut(string(output), "\n")
	if !found {
		return 0, nil, errors.New("helper frame without header")
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != helperFrameVersion {
		return 0, nil, errors.New("helper frame without version")
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil || code < int(tmuxhelper.ExitOK) || code > int(tmuxhelper.ExitBinding) {
		return 0, nil, errors.New("helper frame without code")
	}
	size, err := strconv.Atoi(fields[2])
	if err != nil || size < 0 {
		return 0, nil, errors.New("helper frame without size")
	}
	encoded, tail, found := strings.Cut(rest, "\n")
	if !found || tail != "" {
		return 0, nil, errors.New("helper frame truncated")
	}
	body, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, nil, errors.New("helper frame without body")
	}
	if len(body) != size {
		return 0, nil, errors.New("helper frame size mismatch")
	}
	return tmuxhelper.ExitCode(code), body, nil
}
