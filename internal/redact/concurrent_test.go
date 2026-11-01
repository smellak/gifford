package redact_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact/redacttest"
)

// TestConcurrentUse shares one Redactor across exactly 8 goroutines making
// 50 calls each, cycling the 9 fixture entries per worker: every call
// equals Want with its counts, proving the v2 bundle immutable.
func TestConcurrentUse(t *testing.T) {
	t.Parallel()
	r := mustFixtureRedactor(t)
	screens := mustScreens(t)
	texts := mustTexts(t)

	var mu sync.Mutex
	var first error
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if first == nil {
			first = err
		}
	}
	checkScreen := func(w int, s redacttest.SeededScreen) {
		got, err := r.RedactScreen(s.Input)
		if err != nil {
			fail(fmt.Errorf("worker %d screen %s: %w", w, s.Name, err))
			return
		}
		if len(got.Rows) != len(s.Want) {
			fail(fmt.Errorf("worker %d screen %s rows %d, want %d", w, s.Name, len(got.Rows), len(s.Want)))
			return
		}
		for i := range s.Want {
			if got.Rows[i] != s.Want[i] {
				fail(fmt.Errorf("worker %d screen %s row %d differs", w, s.Name, i))
				return
			}
		}
		if fmt.Sprint(got.Counts) != fmt.Sprint(s.Counts) {
			fail(fmt.Errorf("worker %d screen %s counts differ", w, s.Name))
		}
	}
	checkText := func(w int, x redacttest.SeededText) {
		got, err := r.RedactText(x.Input)
		if err != nil {
			fail(fmt.Errorf("worker %d text %s: %w", w, x.Name, err))
			return
		}
		if got.Text != x.Want || fmt.Sprint(got.Counts) != fmt.Sprint(x.Counts) {
			fail(fmt.Errorf("worker %d text %s differs", w, x.Name))
		}
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range 50 {
				k := c % 9
				if k < len(screens) {
					checkScreen(w, screens[k])
				} else {
					checkText(w, texts[k-len(screens)])
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, first)
}
