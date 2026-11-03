package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// TestReleaseBundlesHelper pins the deploy contract: every release must ship
// the privileged helper next to the main binary, otherwise the S07 transport
// switch has nothing to exec and closes by default.
func TestReleaseBundlesHelper(t *testing.T) {
	t.Parallel()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	manifest, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", ".goreleaser.yaml"))
	require.NoError(t, err)

	var doc struct {
		Builds []struct {
			ID     string `yaml:"id"`
			Main   string `yaml:"main"`
			Binary string `yaml:"binary"`
		} `yaml:"builds"`
		Archives []struct {
			IDs []string `yaml:"ids"`
		} `yaml:"archives"`
	}
	require.NoError(t, yaml.Unmarshal(manifest, &doc))

	helper := -1
	for i := range doc.Builds {
		if doc.Builds[i].ID == "gifford-tmux" {
			helper = i
		}
	}
	require.NotEqual(t, -1, helper, "release must build the gifford-tmux helper")
	require.Equal(t, "./cmd/gifford-tmux", doc.Builds[helper].Main)
	require.Equal(t, "gifford-tmux", doc.Builds[helper].Binary)

	archived := false
	for _, archive := range doc.Archives {
		for _, id := range archive.IDs {
			if id == "gifford-tmux" {
				archived = true
			}
		}
	}
	require.True(t, archived, "release archives must include the helper")
}
