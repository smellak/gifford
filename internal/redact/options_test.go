package redact_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

// TestOptions pins Options.Validate: every refusal of the sensitive path
// rules wraps ErrInvalidOptions, names the index and never the entry, and
// duplicates are accepted for New to drop. For each refusal the test
// asserts both that its text does not hold the entry and that it does not
// hold `srv`.
func TestOptions(t *testing.T) {
	t.Parallel()

	t.Run("accept", func(t *testing.T) {
		t.Parallel()
		valid := []redact.Options{
			{},
			{SensitivePaths: nil},
			{SensitivePaths: []string{"/srv/data", "~/work/x", "/a"}},
			{SensitivePaths: []string{"/dup", "/dup", "~/dup"}},
		}
		for i, opts := range valid {
			require.NoError(t, opts.Validate(), "case %d", i)
		}
	})

	t.Run("refuse", func(t *testing.T) {
		t.Parallel()
		many := make([]string, 257)
		for i := range many {
			many[i] = "/srv/share"
		}
		big := "/srv/" + strings.Repeat("a", 4096)
		cases := []struct {
			name  string
			paths []string
			// wantIndex is the entry index the refusal must name,
			// or -1 for the count refusal naming no index.
			wantIndex int
			// entry is the refused entry text the error must not hold;
			// empty when no entry text exists to leak.
			entry string
		}{
			{"too many", many, -1, "/srv/share"},
			{"empty", []string{"/srv/ok", ""}, 1, ""},
			{"oversized", []string{big}, 0, big},
			{"not utf8", []string{"/srv/ok\xff"}, 0, "/srv/ok\xff"},
			{"control", []string{"/srv/ok\x07"}, 0, "/srv/ok\x07"},
			{"surrounding spaces", []string{"/srv/ok "}, 0, "/srv/ok "},
			{"relative", []string{"srv/ok"}, 0, "srv/ok"},
			{"root", []string{"/"}, 0, "/"},
			{"home root", []string{"~/"}, 0, "~/"},
			{"unclean", []string{"/srv/../srv/ok"}, 0, "/srv/../srv/ok"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				err := redact.Options{SensitivePaths: tc.paths}.Validate()
				require.ErrorIs(t, err, redact.ErrInvalidOptions)
				if tc.wantIndex < 0 {
					require.Contains(t, err.Error(), "sensitive_paths: ")
				} else {
					require.Contains(t, err.Error(), "["+strconv.Itoa(tc.wantIndex)+"]")
				}
				if tc.entry != "" {
					require.NotContains(t, err.Error(), tc.entry)
				}
				require.NotContains(t, err.Error(), "srv")
			})
		}
	})
}
