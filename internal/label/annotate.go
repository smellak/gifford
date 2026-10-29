package label

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// ErrBadAnnotator means the annotator name is not 1–64 of
// [A-Za-z0-9._@-].
var ErrBadAnnotator = errors.New("annotator must be 1-64 of [A-Za-z0-9._@-]")

// maxWindows bounds one annotation run.
const maxWindows = 100000

// labelKeys maps keys 1–7 to labels in the contract's A–G order.
func labelKeys() map[byte]dataset.Label {
	return map[byte]dataset.Label{
		'1': dataset.LabelA, '2': dataset.LabelB, '3': dataset.LabelC, '4': dataset.LabelD,
		'5': dataset.LabelE, '6': dataset.LabelF, '7': dataset.LabelG,
	}
}

// Keys of the annotator besides 1–7.
const (
	keySkip  = 's'
	keyUndo  = 'u'
	keyQuit  = 'q'
	keyCtrlC = 0x03
	keyCtrlD = 0x04
)

// Annotation is one line of AnnotationsFile. It never holds the screen.
type Annotation struct {
	WindowID  protocol.ULID `json:"window_id"`
	Label     dataset.Label `json:"label"`
	Annotator string        `json:"annotator"`
	LabeledAt time.Time     `json:"labeled_at"`
}

// Clock tells the time. clock.System satisfies it.
type Clock interface {
	Now() time.Time
}

// AnnotateOptions configure one annotation run.
type AnnotateOptions struct {
	// Source yields the windows to label, io.EOF after the last.
	Source dataset.RecordingSource
	// Keys are the annotator's keystrokes, one byte each; the terminal is
	// raw. Their end saves and quits.
	Keys io.Reader
	// Screen is where each window is shown.
	Screen io.Writer
	// Annotator names who labels.
	Annotator string
	Clock     Clock
}

// decision is one step the annotator can undo: a label, or a skip.
type decision struct {
	window int
	label  dataset.Label // empty for a skip
	at     time.Time
}

// Annotate shows each window and reads one key for it: 1–7 label it,
// s skips it, u undoes the last label or skip, q (or Ctrl-C, Ctrl-D or
// the end of the keys) saves and quits. It returns the labels kept, in
// window order of decision; the caller saves them with SaveAnnotations.
func Annotate(ctx context.Context, o AnnotateOptions) ([]Annotation, error) {
	if o.Source == nil || o.Keys == nil || o.Screen == nil || o.Clock == nil {
		return nil, errors.New("annotate: missing source, keys, screen or clock")
	}
	if !validAnnotator(o.Annotator) {
		return nil, ErrBadAnnotator
	}
	keys := labelKeys()
	var windows []dataset.Recording
	var done []decision
	sourceDone := false
	note := ""
	key := make([]byte, 1)
	for i := 0; ; {
		if err := ctx.Err(); err != nil {
			return collect(o, windows, done), fmt.Errorf("annotate: %w", err)
		}
		if i == len(windows) && !sourceDone {
			rec, err := o.Source.Next(ctx)
			switch {
			case errors.Is(err, io.EOF):
				sourceDone = true
			case err != nil:
				return collect(o, windows, done), fmt.Errorf("annotate: read window: %w", err)
			case len(windows) == maxWindows:
				return collect(o, windows, done), errors.New("annotate: too many windows")
			default:
				windows = append(windows, rec)
			}
			continue
		}
		if i == len(windows) {
			render(o.Screen, nil, i, len(done), "no more windows: u undoes, any other key saves and quits")
		} else {
			render(o.Screen, &windows[i], i, len(done), note)
		}
		note = ""
		if _, err := io.ReadFull(o.Keys, key); err != nil {
			if errors.Is(err, io.EOF) {
				return collect(o, windows, done), nil
			}
			return collect(o, windows, done), fmt.Errorf("annotate: read key: %w", err)
		}
		k := key[0]
		switch {
		case k == keyUndo:
			if len(done) == 0 {
				note = "nothing to undo"
				continue
			}
			i = done[len(done)-1].window
			done = done[:len(done)-1]
		case k == keyQuit || k == keyCtrlC || k == keyCtrlD || i == len(windows):
			return collect(o, windows, done), nil
		case k == keySkip:
			done = append(done, decision{window: i})
			i++
		default:
			l, ok := keys[k]
			if !ok {
				note = "keys: 1-7 label, s skip, u undo, q save and quit"
				continue
			}
			done = append(done, decision{window: i, label: l, at: o.Clock.Now().UTC()})
			i++
		}
	}
}

// collect turns the decisions into the annotations kept.
func collect(o AnnotateOptions, windows []dataset.Recording, done []decision) []Annotation {
	out := make([]Annotation, 0, len(done))
	for _, d := range done {
		if d.label == "" {
			continue
		}
		out = append(out, Annotation{WindowID: windows[d.window].WindowID, Label: d.label, Annotator: o.Annotator, LabeledAt: d.at})
	}
	return out
}

func validAnnotator(a string) bool {
	if len(a) < 1 || len(a) > 64 {
		return false
	}
	for _, c := range a {
		if c >= unicode.MaxASCII || !unicode.IsLetter(c) && !unicode.IsDigit(c) && !strings.ContainsRune("._@-", c) {
			return false
		}
	}
	return true
}

// SaveAnnotations appends annotations to AnnotationsFile in dir in one
// write.
func SaveAnnotations(dir string, anns []Annotation) error {
	if len(anns) == 0 {
		return nil
	}
	var buf []byte
	for _, a := range anns {
		line, err := json.Marshal(a)
		if err != nil {
			return fmt.Errorf("encode annotation: %w", err)
		}
		buf = append(append(buf, line...), '\n')
	}
	return appendLine(dir, AnnotationsFile, buf)
}

// legend is the key help, in A–G order.
func legend() string {
	var sb strings.Builder
	for i, l := range []dataset.Label{dataset.LabelA, dataset.LabelB, dataset.LabelC, dataset.LabelD, dataset.LabelE, dataset.LabelF, dataset.LabelG} {
		p, _ := l.Phase()
		fmt.Fprintf(&sb, "%d %s  ", i+1, p)
	}
	return strings.TrimSpace(sb.String())
}

// render draws one window: the reconstructed screen with its control
// runes dropped (a raw screen must not drive the annotator's terminal),
// its activity, and the keys. A raw terminal needs \r\n.
func render(w io.Writer, rec *dataset.Recording, i, decided int, note string) {
	var sb strings.Builder
	sb.WriteString("\x1b[H\x1b[2J")
	line := func(format string, args ...any) {
		fmt.Fprintf(&sb, format, args...)
		sb.WriteString("\r\n")
	}
	if rec == nil {
		line("gifford label annotate: %d decided", decided)
	} else {
		in := rec.Features
		line("window %d  %s  (%d decided)", i+1, rec.WindowID, decided)
		children := "unknown"
		if in.NewChildProcesses != nil {
			children = strings.Join(in.NewChildProcesses, ", ")
			if children == "" {
				children = "none"
			}
		}
		line("harness %s  quiet %.1fs  bytes 0.5s=%d 2s=%d 10s=%d  children %s  previous %s",
			printable(string(in.Harness)), in.SecondsSinceLastOutput,
			in.OutputBytes.Last500ms, in.OutputBytes.Last2s, in.OutputBytes.Last10s,
			printable(children), printable(string(in.PreviousState)))
		line("%s", strings.Repeat("-", 40))
		for _, row := range rec.Screen.Rows {
			line("%s", printable(row))
		}
		line("%s", strings.Repeat("-", 40))
	}
	line("%s", legend())
	line("s skip  u undo  q save and quit")
	if note != "" {
		line("%s", note)
	}
	_, _ = io.WriteString(w, sb.String())
}

// printable drops control runes.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}
