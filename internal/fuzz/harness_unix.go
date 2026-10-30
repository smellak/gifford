//go:build unix

package fuzz

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

// errHarnessCut means the fake harness's terminal closed before Ctrl-D.
var errHarnessCut = errors.New("fuzz harness: terminal closed before Ctrl-D")

// Harness runs the fake harness on the terminal in: it reads keys raw,
// draws its input line on out, and appends one record per line it gets
// to the file at record, which must not exist yet. Ctrl-D ends it.
func Harness(in *os.File, out io.Writer, record string) (err error) {
	f, err := os.OpenFile(record, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304 G703 -- the record file the fuzzer named
	if err != nil {
		return fmt.Errorf("fuzz harness: record: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("fuzz harness: record: %w", cerr)
		}
	}()
	fd := int(in.Fd())
	old, err := term.MakeRaw(fd)
	if err != nil {
		return fmt.Errorf("fuzz harness: raw terminal: %w", err)
	}
	defer func() { _ = term.Restore(fd, old) }()
	var ed editor
	if _, err := out.Write(ed.start()); err != nil {
		return fmt.Errorf("fuzz harness: draw: %w", err)
	}
	buf := make([]byte, 4096)
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			screen, records := ed.feed(buf[:n])
			for _, r := range records {
				if _, err := io.WriteString(f, r.String()+"\n"); err != nil {
					return fmt.Errorf("fuzz harness: record: %w", err)
				}
			}
			if _, err := out.Write(screen); err != nil {
				return fmt.Errorf("fuzz harness: draw: %w", err)
			}
			if ed.eof {
				return nil
			}
		}
		if rerr != nil {
			return fmt.Errorf("%w: %w", errHarnessCut, rerr)
		}
	}
}
