//go:build !linux

package main

import "os/exec"

// bindE2EChildToParent sets nothing: only Linux has a parent-death signal.
// The harness still kills and reaps its tool child when it returns.
func bindE2EChildToParent(*exec.Cmd) {}
