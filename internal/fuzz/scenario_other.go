//go:build !unix

package fuzz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// supported says the fuzzer runs here: it needs Unix PTYs.
const supported = false

// runScenario cannot run a scenario without Unix PTYs.
func runScenario(_ context.Context, _ config, sc Script, _ bool) result {
	return result{index: sc.Index, err: fmt.Errorf("fuzz: %w", errors.ErrUnsupported)}
}

// Harness needs a Unix terminal.
func Harness(_ *os.File, _ io.Writer, _ string) error {
	return fmt.Errorf("fuzz harness: %w", errors.ErrUnsupported)
}
