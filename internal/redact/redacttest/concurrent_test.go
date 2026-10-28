package redacttest_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConcurrentUse shares one Redactor across 8 goroutines calling 50
// times each: every call equals Want, proving the v1 bundle immutable.
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
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				for _, s := range screens {
					got, err := r.RedactScreen(s.Input)
					if err != nil {
						fail(fmt.Errorf("worker %d screen %s: %w", w, s.Name, err))
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
						return
					}
				}
				for _, x := range texts {
					got, err := r.RedactText(x.Input)
					if err != nil {
						fail(fmt.Errorf("worker %d text %s: %w", w, x.Name, err))
						return
					}
					if got.Text != x.Want || fmt.Sprint(got.Counts) != fmt.Sprint(x.Counts) {
						fail(fmt.Errorf("worker %d text %s differs", w, x.Name))
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, first)
}
