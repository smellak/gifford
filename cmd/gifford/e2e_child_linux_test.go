//go:build linux

package main

import (
	"os/exec"
	"syscall"
)

// bindE2EChildToParent has Linux send the harness's tool child SIGTERM when
// the harness exits.
func bindE2EChildToParent(child *exec.Cmd) {
	child.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}
