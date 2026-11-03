package tmuxhelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// ManifestPath is fixed because a caller-controlled path would turn the
	// registry revision into attacker-selected authority.
	ManifestPath = "/etc/gifford-tmux/manifest.json"

	frameVersion       = "GTMX/1"
	maxManifestBytes   = 64 << 10
	maxListBytes       = 64 << 10
	maxObserveBytes    = 256
	maxCaptureBytes    = 1 << 20
	maxEffectBytes     = 0
	maxSocketPathBytes = 107
	helperTimeout      = 2 * time.Second
	// MaxFrameBytes is the largest complete GTMX/1 stdout frame. The body
	// maximum is maxCaptureBytes; the rest is its canonical base64 encoding,
	// the longest header, and the final newline.
	MaxFrameBytes = len("GTMX/1 0 1048576\n") + ((maxCaptureBytes+2)/3)*4 + 1

	formatPanes   = "#{pane_id}\t#{pane_pid}"
	formatObserve = "#{pane_id}\t#{pane_pid}\t#{pane_width}\t#{pane_height}"
	formatClients = "#{pane_id}\t#{client_activity}"

	commandDisplayMessage = "display-message"
	commandSendKeys       = "send-keys"
)

// ExitCode is both the process status and the status carried by the output
// frame. Values are part of the privileged helper protocol.
type ExitCode int

// ErrInvalidFrame means stdout is not one exact canonical GTMX/1 frame.
var ErrInvalidFrame = errors.New("gifford tmux: invalid frame")

const (
	// ExitOK means the requested fixed operation completed.
	ExitOK ExitCode = iota
	// ExitTmux means tmux failed or wrote diagnostics.
	ExitTmux
	// ExitGrammar means the closed request grammar or bank selection failed.
	ExitGrammar
	// ExitEnvironment means trusted local authority could not be established.
	ExitEnvironment
	// ExitTimeout means the helper deadline or a termination signal fired.
	ExitTimeout
	// ExitOutputLimit means stdout or stderr exceeded its verb-specific cap.
	ExitOutputLimit
	// ExitRevision means the raw registry digest differs from the request.
	ExitRevision
	// ExitBinding means the pane is absent or its process incarnation changed.
	ExitBinding
)

type verb string

const (
	verbPanes          verb = "panes"
	verbObserve        verb = "observe"
	verbClients        verb = "clients"
	verbCapture        verb = "capture"
	verbNoticeFull     verb = "notice-full"
	verbNoticeResponse verb = "notice-response"
	verbNoticeShort    verb = "notice-short"
	verbEnter          verb = "enter"
)

type request struct {
	verb     verb
	revision string
	bankID   string
	pane     string
}

func (r request) hasPane() bool {
	return r.verb != verbPanes && r.verb != verbClients
}

type registryAccess interface {
	EffectiveUID() uint32
	LoadManifest() ([]byte, error)
}

type tmuxAccess interface {
	ValidateSocket(uint32, string) error
	RunTmux(context.Context, string, []string, int) ([]byte, ExitCode)
}

type processAccess interface {
	ProcessStart(int64) (uint64, error)
	BootID() (string, error)
}

type authorization struct {
	registry Registry
	bank     Bank
	binding  PaneBinding
}

// Main runs the production helper and returns its process exit status.
func Main(args []string) int {
	ctx, cancel := context.WithTimeout(context.Background(), helperTimeout)
	defer cancel()
	if err := prepareProcess(); err != nil {
		return writeFailureFrame(os.Stdout, ExitEnvironment)
	}
	ctx, stopSignals := signal.NotifyContext(ctx, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	host := newPlatform()
	return runCLI(ctx, host, host, host, args, os.Stdout)
}

func writeFailureFrame(output io.Writer, code ExitCode) int {
	if err := writeFrame(output, code, nil); err != nil {
		return int(ExitEnvironment)
	}
	return int(code)
}

func runCLI(
	ctx context.Context,
	registry registryAccess,
	tmux tmuxAccess,
	processes processAccess,
	args []string,
	stdout io.Writer,
) int {
	code, body := execute(ctx, registry, tmux, processes, args)
	if ctx.Err() != nil {
		code, body = ExitTimeout, nil
	}
	if err := writeFrame(stdout, code, body); err != nil {
		code = ExitEnvironment
	}
	return int(code)
}

func execute(
	ctx context.Context,
	registry registryAccess,
	tmux tmuxAccess,
	processes processAccess,
	args []string,
) (ExitCode, []byte) {
	if ctx == nil || registry == nil || tmux == nil || processes == nil {
		return ExitEnvironment, nil
	}
	req, ok := parseRequest(args)
	if !ok {
		return ExitGrammar, nil
	}
	authority, code := authorize(registry, tmux, req)
	if code != ExitOK {
		return code, nil
	}
	if req.verb == verbNoticeResponse && authority.registry.NoticeResponse == "" {
		return ExitGrammar, nil
	}

	switch req.verb {
	case verbPanes:
		return callTmux(ctx, registry, tmux, req,
			[]string{"list-panes", "-a", "-F", formatPanes}, maxListBytes)
	case verbClients:
		return callTmux(ctx, registry, tmux, req,
			[]string{"list-clients", "-F", formatClients}, maxListBytes)
	case verbObserve, verbCapture, verbNoticeFull, verbNoticeResponse, verbNoticeShort, verbEnter:
		// Pane verbs first execute the fixed binding observation below.
	default:
		return ExitGrammar, nil
	}

	observed, code := observe(ctx, registry, tmux, processes, req, authority.binding)
	if code != ExitOK {
		return code, nil
	}
	if req.verb == verbObserve {
		return ExitOK, observed
	}

	var argv []string
	limit := maxEffectBytes
	switch req.verb {
	case verbCapture:
		argv = []string{"capture-pane", "-p", "-e", "-t", req.pane}
		limit = maxCaptureBytes
	case verbNoticeFull:
		argv = []string{commandSendKeys, "-l", "-t", req.pane, authority.registry.NoticeFull}
	case verbNoticeResponse:
		argv = []string{commandSendKeys, "-l", "-t", req.pane, authority.registry.NoticeResponse}
	case verbNoticeShort:
		argv = []string{commandSendKeys, "-l", "-t", req.pane, authority.registry.NoticeShort}
	case verbEnter:
		argv = []string{commandSendKeys, "-t", req.pane, "Enter"}
	default:
		return ExitGrammar, nil
	}
	return callTmux(ctx, registry, tmux, req, argv, limit)
}

func authorize(registry registryAccess, tmux tmuxAccess, req request) (authorization, ExitCode) {
	uid := registry.EffectiveUID()
	if uid == 0 {
		return authorization{}, ExitEnvironment
	}
	raw, err := registry.LoadManifest()
	if err != nil || len(raw) == 0 || len(raw) > maxManifestBytes {
		return authorization{}, ExitEnvironment
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != req.revision {
		return authorization{}, ExitRevision
	}
	parsed, err := ParseRegistry(raw)
	if err != nil {
		return authorization{}, ExitEnvironment
	}
	bank, found := parsed.Banks[req.bankID]
	if !found {
		return authorization{}, ExitGrammar
	}
	if bank.UID != uid {
		return authorization{}, ExitEnvironment
	}
	result := authorization{registry: parsed, bank: bank}
	if req.hasPane() {
		binding, registered := bank.Panes[req.pane]
		if !registered {
			return authorization{}, ExitBinding
		}
		result.binding = binding
	}
	if err := tmux.ValidateSocket(uid, bank.Socket); err != nil {
		return authorization{}, ExitEnvironment
	}
	return result, ExitOK
}

func parseRequest(args []string) (request, bool) {
	if len(args) != 3 && len(args) != 4 {
		return request{}, false
	}
	req := request{verb: verb(args[0]), revision: args[1], bankID: args[2]}
	switch req.verb {
	case verbPanes, verbClients:
		if len(args) != 3 {
			return request{}, false
		}
	case verbObserve, verbCapture, verbNoticeFull, verbNoticeResponse, verbNoticeShort, verbEnter:
		if len(args) != 4 || !canonicalPane(args[3]) {
			return request{}, false
		}
		req.pane = args[3]
	default:
		return request{}, false
	}
	return req, validRevision(req.revision) && validBankID(req.bankID)
}

func validRevision(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			if value[index] < 'a' || value[index] > 'f' {
				return false
			}
		}
	}
	return true
}

func callTmux(
	ctx context.Context,
	registry registryAccess,
	tmux tmuxAccess,
	req request,
	argv []string,
	limit int,
) (ExitCode, []byte) {
	if ctx.Err() != nil {
		return ExitTimeout, nil
	}
	authority, code := authorize(registry, tmux, req)
	if code != ExitOK {
		return code, nil
	}
	body, code := tmux.RunTmux(ctx, authority.bank.Socket, argv, limit)
	if code != ExitOK {
		return code, nil
	}
	return ExitOK, body
}

func observe(
	ctx context.Context,
	registry registryAccess,
	tmux tmuxAccess,
	processes processAccess,
	req request,
	binding PaneBinding,
) ([]byte, ExitCode) {
	argv := []string{commandDisplayMessage, "-p", "-t", req.pane, formatObserve}
	code, body := callTmux(ctx, registry, tmux, req, argv, maxObserveBytes)
	if code != ExitOK {
		return nil, code
	}
	observedPane, pid, ok := parseObservation(body)
	if !ok || observedPane != req.pane || pid != binding.PanePID ||
		!sameProcess(processes, binding) {
		return nil, ExitBinding
	}
	return body, ExitOK
}

func sameProcess(processes processAccess, binding PaneBinding) bool {
	start, err := processes.ProcessStart(binding.PanePID)
	if err != nil || start != binding.PaneStart {
		return false
	}
	bootID, err := processes.BootID()
	return err == nil && bootID == binding.BootID
}

func parseObservation(raw []byte) (string, int64, bool) {
	line, ok := oneLine(raw)
	if !ok {
		return "", 0, false
	}
	fields := strings.Split(line, "\t")
	if len(fields) != 4 {
		return "", 0, false
	}
	pid, pidErr := strconv.ParseInt(fields[1], 10, 64)
	cols, colsErr := strconv.ParseUint(fields[2], 10, 16)
	rows, rowsErr := strconv.ParseUint(fields[3], 10, 16)
	if !canonicalPane(fields[0]) || pidErr != nil || pid < 1 ||
		colsErr != nil || rowsErr != nil || cols == 0 || rows == 0 {
		return "", 0, false
	}
	return fields[0], pid, true
}

func oneLine(raw []byte) (string, bool) {
	if len(raw) < 2 || raw[len(raw)-1] != '\n' {
		return "", false
	}
	line := string(raw[:len(raw)-1])
	return line, line != "" && !strings.ContainsAny(line, "\r\n")
}

func writeFrame(output io.Writer, code ExitCode, body []byte) error {
	if code < ExitOK || code > ExitBinding || len(body) > maxCaptureBytes ||
		code != ExitOK && len(body) != 0 {
		return errors.New("invalid helper frame")
	}
	encoded := base64.StdEncoding.EncodeToString(body)
	var frame bytes.Buffer
	_, _ = fmt.Fprintf(&frame, "%s %d %d\n", frameVersion, code, len(body))
	frame.WriteString(encoded)
	frame.WriteByte('\n')
	return writeAll(output, frame.Bytes())
}

// DecodeFrame decodes one complete canonical GTMX/1 stdout frame. It accepts
// no leading, trailing, alternate-base64 or non-canonical numeric forms. The
// caller must separately require the process exit status to equal code.
func DecodeFrame(raw []byte) (ExitCode, []byte, error) {
	if len(raw) < len("GTMX/1 0 0\n\n") || len(raw) > MaxFrameBytes || raw[len(raw)-1] != '\n' {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	headerEnd := bytes.IndexByte(raw, '\n')
	if headerEnd < 0 || bytes.IndexByte(raw[headerEnd+1:len(raw)-1], '\n') >= 0 {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	fields := strings.Split(string(raw[:headerEnd]), " ")
	if len(fields) != 3 || fields[0] != frameVersion {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	codeRaw, codeErr := strconv.ParseUint(fields[1], 10, 8)
	if codeErr != nil || codeRaw > uint64(ExitBinding) ||
		fields[1] != strconv.FormatUint(codeRaw, 10) {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	sizeRaw, sizeErr := strconv.ParseUint(fields[2], 10, 32)
	if sizeErr != nil || sizeRaw > maxCaptureBytes ||
		fields[2] != strconv.FormatUint(sizeRaw, 10) {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	size := int(sizeRaw)
	encoded := raw[headerEnd+1 : len(raw)-1]
	if len(encoded) != base64.StdEncoding.EncodedLen(size) {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	var body []byte
	if size > 0 {
		body = make([]byte, size)
	}
	decoded, decodeErr := base64.StdEncoding.Strict().Decode(body, encoded)
	if decodeErr != nil || decoded != size ||
		!bytes.Equal(encoded, []byte(base64.StdEncoding.EncodeToString(body))) {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	code := ExitCode(codeRaw)
	if code != ExitOK && len(body) != 0 {
		return ExitEnvironment, nil, ErrInvalidFrame
	}
	return code, body, nil
}

func writeAll(output io.Writer, raw []byte) error {
	for len(raw) > 0 {
		written, err := output.Write(raw)
		if err != nil {
			return fmt.Errorf("write helper frame: %w", err)
		}
		if written < 1 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}
