//go:build linux

package orchestrator

import "github.com/smellak/gifford/internal/procguard"

// defaultSampleTree reads the pane leader's process subtree from /proc.
var defaultSampleTree = procguard.SampleTree
