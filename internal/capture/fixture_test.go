package capture

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// The fixture cast is synthetic: it starts at 2025-10-30T12:00:00Z, and
// the canaries mark what an import must never write.
const (
	fixtureStart = 1761825600
	typedCanary  = "typed-canary-4b7q"
	titleCanary  = "title-canary-9w2e"
	envCanary    = "env-canary-3k8z"
	markerCanary = "marker-canary-6p1d"
	pathCanary   = "path-canary-5m0x"
	// fixtureAgent is the agent the fixture is attributed to.
	fixtureAgent protocol.AgentID = "agent-1"
)

func fixtureHeader() string {
	return fmt.Sprintf(`{"version": 2, "width": 80, "height": 24, "timestamp": %d, "title": %q, "env": {"SHELL": "/bin/sh", "TERM": %q}}`,
		fixtureStart, titleCanary, envCanary)
}

// fixtureEvents hold output, typed input, a resize and a marker, and
// end with typed input: the session ends at the cast's last event.
func fixtureEvents() []string {
	return []string{
		`[0.25, "o", "fixture build ok\r\n"]`,
		`[0.5, "i", "` + typedCanary + `"]`,
		`[0.75, "o", "$ "]`,
		`[1.5, "r", "100x30"]`,
		`[2.0, "m", "` + markerCanary + `"]`,
		`[2.5, "o", "\u001b[1mdone\u001b[0m\r\n$ "]`,
		`[3.25, "i", "` + typedCanary + `"]`,
	}
}

// writeCast writes a cast of the header and event lines given.
func writeCast(t *testing.T, dir, header string, events ...string) string {
	t.Helper()
	path := filepath.Join(dir, "fixture.cast")
	body := header + "\n" + strings.Join(events, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// fixtureCast writes the fixture cast in a dir whose name is a canary.
func fixtureCast(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), pathCanary)
	require.NoError(t, os.Mkdir(dir, 0o700))
	return writeCast(t, dir, fixtureHeader(), fixtureEvents()...)
}

func fixtureOptions(dataDir, source string) Options {
	return Options{
		DataDir: dataDir, Source: source, Format: FormatAsciicastV2,
		AgentID: fixtureAgent, Harness: protocol.HarnessCodex, EnvironmentDomain: dataset.EnvironmentDomainFleetManaged,
	}
}

// tree maps every entry under dir to its mode and bytes.
func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		entry := info.Mode().String()
		if info.Mode().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			entry += " " + string(b)
		}
		out[rel] = entry
		return nil
	})
	if os.IsNotExist(err) {
		return out
	}
	require.NoError(t, err)
	return out
}

// recordings lists the names in dataDir's recordings dir.
func recordings(t *testing.T, dataDir string) []string {
	t.Helper()
	ents, err := os.ReadDir(session.RecordingsDir(dataDir))
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names
}
