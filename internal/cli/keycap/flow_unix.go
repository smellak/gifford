//go:build unix

package keycap

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy"
)

const (
	defaultEscWait       = 150 * time.Millisecond
	defaultPasteIdle     = 800 * time.Millisecond
	defaultVersionTimout = 5 * time.Second
	// defaultDrainWait caps the final drain: a harness ignoring SIGHUP
	// would hold the middle slave open forever.
	defaultDrainWait = 5 * time.Second
	// maxPasteBytes caps one paste: past it the step fails instead of
	// growing memory without bound.
	maxPasteBytes = 1 << 20
	// profilePerm matches the checked-in profiles/*.yaml mode.
	profilePerm = os.FileMode(0o644)
)

// runFlow measures one harness: version detection, the guided tour under
// the PTY core, and the profile write. It returns nil once the profile
// is written, complete or not; measured tells which.
func runFlow(ctx context.Context, p flowParams) error {
	escWait := p.EscWait
	if escWait <= 0 {
		escWait = defaultEscWait
	}
	pasteIdle := p.PasteIdle
	if pasteIdle <= 0 {
		pasteIdle = defaultPasteIdle
	}
	versionTimeout := p.VersionTimeout
	if versionTimeout <= 0 {
		versionTimeout = defaultVersionTimout
	}
	drainWait := p.DrainWait
	if drainWait <= 0 {
		drainWait = defaultDrainWait
	}
	version := detectVersion(p.Command, versionTimeout)
	inputLine, versionRange := readExisting(p.OutDir, p.Harness, p.ErrW)

	say := func(format string, args ...any) {
		_, _ = fmt.Fprintf(p.ErrW, "keycap: "+format+"\n", args...)
	}
	say("midiendo %s con %q.", p.Harness, p.Command)
	say("tus teclas no llegan al harness durante la medicion.")
	if version == "" {
		say("sin version detectada: measured quedara en false.")
	}
	say("para interrumpir, mata el proceso desde otra terminal; se guarda lo medido.")

	old, err := term.MakeRaw(int(p.In.Fd()))
	if err != nil {
		return fmt.Errorf("raw outer terminal: %w", err)
	}
	defer func() { _ = term.Restore(int(p.In.Fd()), old) }()

	midM, midS, err := pty.Open()
	if err != nil {
		return fmt.Errorf("middle pty: %w", err)
	}
	defer func() { _ = midM.Close() }()
	defer func() { _ = midS.Close() }()
	if size, err := pty.GetsizeFull(p.Out); err == nil {
		_ = pty.Setsize(midM, size)
	}

	sigC := make(chan os.Signal, 16)
	signal.Notify(sigC, unix.SIGWINCH, unix.SIGHUP, unix.SIGINT, unix.SIGQUIT, unix.SIGTERM, unix.SIGUSR1, unix.SIGUSR2)
	defer signal.Stop(sigC)
	sigCore := make(chan os.Signal, 16)

	coreCtx, coreCancel := context.WithCancel(ctx)
	defer coreCancel()
	type coreResult struct {
		exit proxy.Exit
		err  error
	}
	coreDone := make(chan coreResult, 1)
	go func() {
		exit, err := proxy.Run(coreCtx, proxy.Options{
			Command: p.Command,
			In:      midS,
			Out:     midS,
			Notice:  nil,
			Signals: sigCore,
		})
		coreDone <- coreResult{exit: exit, err: err}
	}()

	done := make(chan struct{})
	defer close(done)
	chOuter := make(chan []byte)
	chMid := make(chan []byte)
	go pump(p.In, chOuter, done)
	go pump(midM, chMid, done)

	f := &flow{
		params:       p,
		escWait:      escWait,
		pasteIdle:    pasteIdle,
		measure:      &measurement{version: version},
		version:      version,
		inputLine:    inputLine,
		versionRange: versionRange,
		say:          say,
		observer:     &observer{},
	}
	f.say(prompt(tour[0]))
	// stopping marks teardown: the tour is over or the harness is gone,
	// but the loop keeps forwarding harness output until the core ends
	// and the middle slave closes. completed marks a full tour: it
	// always reports success, even if the core ends right after.
	stopping := false
	completed := false
	stopReason := ""
	stop := func(reason string) {
		if stopping {
			return
		}
		stopping = true
		stopReason = reason
		f.timer = nil
		f.buf = nil
		coreCancel()
	}
	var drainTimer <-chan time.Time
loop:
	for {
		select {
		case data, ok := <-chOuter:
			if !ok {
				chOuter = nil
				stop("se cerro la terminal")
				continue
			}
			if stopping {
				continue
			}
			if f.onKeys(data) {
				completed = true
				stop("")
			}
		case data, ok := <-chMid:
			if !ok {
				chMid = nil
				continue
			}
			if _, err := p.Out.Write(data); err != nil {
				stop("no se pudo escribir en la terminal")
				continue
			}
			f.observer.scan(data)
		case <-f.timer:
			f.timer = nil
			if stopping {
				continue
			}
			if f.onTimeout() {
				completed = true
				stop("")
			}
		case sig := <-sigC:
			if sig == unix.SIGWINCH {
				if size, err := pty.GetsizeFull(p.Out); err == nil {
					_ = pty.Setsize(midM, size)
				}
			}
			select {
			case sigCore <- sig:
			default:
			}
		case res := <-coreDone:
			_ = res
			// The core is gone: close our slave so the master pump
			// reaches EOF once drained. Bytes already written stay
			// readable; nothing is lost.
			_ = midS.Close()
			drainTimer = time.After(drainWait)
			if completed {
				stop("")
			} else {
				stop("el harness termino antes de completar el tour")
			}
		case <-ctx.Done():
			stop("contexto cancelado")
		case <-drainTimer:
			f.say("el drenaje final no termino a tiempo; puede faltar salida.")
			break loop
		}
		if stopping && chMid == nil {
			// The closed pump channel proves every output byte was
			// forwarded; the slave close above lets it end.
			break
		}
	}
	f.restoreModes(p.Out)
	return f.finish(stopReason)
}

// pump forwards one side's bytes to the loop until the descriptor fails
// or the flow ends.
func pump(r *os.File, ch chan<- []byte, done <-chan struct{}) {
	defer close(ch)
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			cp := append([]byte(nil), buf[:n]...)
			select {
			case ch <- cp:
			case <-done:
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// flow holds the tour state: which step is open, the bytes gathered for
// it, and one rearmed timer for lone escapes and raw pastes.
type flow struct {
	params       flowParams
	escWait      time.Duration
	pasteIdle    time.Duration
	measure      *measurement
	version      string
	inputLine    string
	versionRange string
	say          func(string, ...any)
	observer     *observer
	step         int
	buf          []byte
	timer        <-chan time.Time
}

// onKeys folds outer-terminal bytes into the open step. It reports true
// when the tour is complete.
func (f *flow) onKeys(data []byte) bool {
	f.buf = append(f.buf, data...)
	if len(f.buf) > maxPasteBytes+1024 {
		f.say("demasiados bytes sin una tecla reconocible: repite el paso.")
		f.buf = nil
		f.timer = nil
		f.say(prompt(tour[f.step]))
		return false
	}
	return f.drain()
}

// drain processes the gathered bytes until the open step needs more of
// them. Overrun bytes are attempted on the same step again; leftovers
// after a recorded unit belong to whatever the human typed next and are
// attempted on the following step, never recorded. It reports true when
// the tour is complete.
func (f *flow) drain() bool {
	for {
		if tour[f.step] == StepPaste {
			if f.pasteStep() {
				return f.advance()
			}
			return false
		}
		unit, rest, ok := completeStep(f.buf)
		if !ok {
			f.timer = time.After(f.escWait)
			return false
		}
		f.timer = nil
		if len(rest) > 0 {
			f.say("llegaron varias teclas a la vez; proceso la primera.")
		}
		if !verify(tour[f.step], unit) {
			f.say("eso no parece la tecla pedida: repite el paso.")
			f.buf = rest
			if len(rest) > 0 {
				continue
			}
			f.say(prompt(tour[f.step]))
			return false
		}
		f.measure.record(tour[f.step], unit)
		f.confirm(tour[f.step], unit)
		restKept := rest
		if f.advance() {
			return true
		}
		if f.buf = restKept; len(restKept) > 0 {
			// The overrun belongs to the next step now: it was
			// typed after the recorded unit, never sent anywhere.
			continue
		}
		return false
	}
}

// pasteStep closes the paste step: a wrapped paste ends at its closer,
// a raw one only at the idle timer. It reports true when recorded.
func (f *flow) pasteStep() bool {
	if bytes.HasPrefix(f.buf, []byte(pasteOn)) {
		if i := bytes.Index(f.buf, []byte(pasteOff)); i >= 0 {
			end := i + len(pasteOff)
			if end > maxPasteBytes {
				f.say("pegado demasiado largo: repite el paso.")
				f.buf = nil
				f.timer = nil
				f.say(prompt(StepPaste))
				return false
			}
			wrapped := true
			f.measure.pasteWrapped = &wrapped
			f.confirm(StepPaste, f.buf[:end])
			f.buf = f.buf[end:]
			return true
		}
		if len(f.buf) > maxPasteBytes {
			f.say("pegado demasiado largo: repite el paso.")
			f.buf = nil
			f.timer = nil
			f.say(prompt(StepPaste))
			return false
		}
		f.timer = time.After(f.pasteIdle)
		return false
	}
	f.timer = time.After(f.pasteIdle)
	return false
}

// onTimeout closes what the timer guarded: a lone ESC, a raw paste with
// no new bytes, or bytes that never completed. An empty buffer only
// re-arms: the timer may fire after the bytes were already consumed. It
// reports true when the tour is complete.
func (f *flow) onTimeout() bool {
	if len(f.buf) == 0 {
		if tour[f.step] == StepPaste {
			f.timer = time.After(f.pasteIdle)
		}
		return false
	}
	if tour[f.step] == StepPaste {
		if bytes.HasPrefix(f.buf, []byte(pasteOn)) {
			f.timer = time.After(f.pasteIdle)
			return false
		}
		wrapped := false
		f.measure.pasteWrapped = &wrapped
		f.confirm(StepPaste, f.buf)
		f.buf = nil
		return f.advance()
	}
	if len(f.buf) == 1 && f.buf[0] == 0x1b {
		if !verify(tour[f.step], f.buf) {
			f.say("llego solo Escape y el paso pide otra tecla: repite.")
			f.buf = nil
			f.say(prompt(tour[f.step]))
			return false
		}
		unit := append([]byte(nil), f.buf...)
		f.measure.record(tour[f.step], unit)
		f.confirm(tour[f.step], unit)
		f.buf = nil
		return f.advance()
	}
	f.say("no se reconocio una tecla completa: repite el paso.")
	f.buf = nil
	f.say(prompt(tour[f.step]))
	return false
}

// advance moves to the next step, prompting it, and reports whether the
// tour is complete.
func (f *flow) advance() bool {
	f.step++
	f.buf = nil
	f.timer = nil
	if f.step >= len(tour) {
		return true
	}
	f.say(prompt(tour[f.step]))
	return false
}

// confirm echoes what was recorded without ever showing bytes: steps
// are named, pastes counted, so stray secrets never reach stderr.
func (f *flow) confirm(step StepKind, unit []byte) {
	if step == StepPaste {
		wrapped := "sin marcadores"
		if bytes.HasPrefix(unit, []byte(pasteOn)) {
			wrapped = "con marcadores"
		}
		f.say("registrado: pegado de %d bytes %s.", len(unit), wrapped)
		return
	}
	f.say("registrado: %s.", step)
}

// finish closes the measurement. An incomplete tour, a missing version
// or an inconsistent mode never touches the profile on disk and fails:
// a failed run must not corrupt a valid profile, and keycap reports no
// success. A complete tour renders a candidate, validates it with the
// production profile rules plus a replay check, and only then replaces
// the file atomically. reason names why the tour ended early, "" when
// it completed.
func (f *flow) finish(reason string) error {
	m := f.measure
	ext, extReason := m.extended()
	if reason != "" {
		m.incomplete = append(m.incomplete, reason)
	}
	if m.version == "" {
		m.incomplete = append(m.incomplete, "sin version detectada")
	}
	if extReason != "" {
		m.incomplete = append(m.incomplete, extReason)
	}
	if !m.complete() {
		m.incomplete = append(m.incomplete, "faltan pasos del tour")
	}
	f.say("harness observo: %s.", f.observer.describe())
	if len(m.incomplete) > 0 {
		f.say("medicion incompleta (%s); el perfil existente queda intacto.",
			joinReasons(m.incomplete))
		return fmt.Errorf("keycap: incomplete measurement (%s)", joinReasons(m.incomplete))
	}
	versionRange := f.versionRange
	if m.version != "" {
		versionRange = m.version
	}
	content := renderProfile(f.params.Harness, m, f.inputLine, versionRange, ext)
	if err := validateCandidate(f.params.Harness, content, m); err != nil {
		f.say("candidato invalido; el perfil existente queda intacto.")
		return err
	}
	path := f.params.OutDir + "/" + f.params.Harness + ".yaml"
	if err := writeAtomic(f.params.OutDir, f.params.Harness, content); err != nil {
		return fmt.Errorf("write profile: %w", err)
	}
	f.say("perfil escrito en %s (measured: true).", path)
	return nil
}

// restoreModes switches off the terminal modes the harness negotiated
// and left active, so the human shell keeps working after keycap. Only
// modes observed active are touched, which keeps the restore idempotent:
// running it twice, or after a harness that cleaned up, changes nothing.
func (f *flow) restoreModes(out *os.File) {
	var restored []string
	if f.observer.paste != nil && *f.observer.paste {
		if _, err := out.Write([]byte("\x1b[?2004l")); err == nil {
			restored = append(restored, "bracketed paste")
		}
	}
	if f.observer.kitty {
		if _, err := out.Write([]byte("\x1b[<1u")); err == nil {
			restored = append(restored, "teclado kitty")
		}
	}
	if f.observer.modifyKeys {
		if _, err := out.Write([]byte("\x1b[>4;0m")); err == nil {
			restored = append(restored, "modifyOtherKeys")
		}
	}
	if len(restored) > 0 {
		f.say("modos restaurados: %s.", strings.Join(restored, ", "))
	}
}

// readExisting preserves what keycap never measures: the input_line of
// the profile already on disk, with its version_range as the fallback
// when no version is detected. A missing file scaffolds the xterm
// catch-all; an unreadable or invalid one warns and scaffolds too.
func readExisting(outDir, id string, errW io.Writer) (inputLine, versionRange string) {
	inputLine = "^(.*)$"
	versionRange = "*"
	raw, err := os.ReadFile(outDir + "/" + id + ".yaml")
	if err != nil {
		if !os.IsNotExist(err) {
			_, _ = fmt.Fprintf(errW, "keycap: no se pudo leer el perfil previo: %v.\n", err)
		}
		return inputLine, versionRange
	}
	prof, err := profiles.ParseFile(id, raw)
	if err != nil {
		_, _ = fmt.Fprintf(errW, "keycap: el perfil previo no valida: %v.\n", err)
		return inputLine, versionRange
	}
	return prof.InputLinePattern, prof.VersionRange
}

// detectVersion runs "<command> --version" off-terminal and picks the
// first dotted version out of it; "" when the harness says nothing
// usable in time.
func detectVersion(command []string, timeout time.Duration) string {
	if len(command) == 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	argv := append(append([]string{}, command[1:]...), "--version")
	cmd := exec.CommandContext(ctx, command[0], argv...) // #nosec G204 -- the harness command is explicit user input, like a shell's
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return parseVersion(string(out))
}

// writeAtomic replaces the profile without ever truncating it in place:
// a temp file in the same directory is filled, mode-set and renamed,
// so a crash leaves the previous profile or nothing, never a half.
func writeAtomic(dir, id, content string) error {
	tmp, err := os.CreateTemp(dir, id+"-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp profile: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.WriteString(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp profile: %w", err)
	}
	if err := tmp.Chmod(profilePerm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp profile: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp profile: %w", err)
	}
	if err := os.Rename(tmpName, dir+"/"+id+".yaml"); err != nil {
		return fmt.Errorf("replace profile: %w", err)
	}
	return nil
}

// joinReasons joins incomplete reasons for one stderr line.
func joinReasons(reasons []string) string {
	out := ""
	for i, r := range reasons {
		if i > 0 {
			out += "; "
		}
		out += r
	}
	return out
}
