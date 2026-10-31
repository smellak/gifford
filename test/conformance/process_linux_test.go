//go:build conformance && linux

package conformance

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestSignalProcessRejectsReusedPID models a PID whose process identity no
// longer has the starttime captured by the suite. signalProcess must treat it
// as the original process already gone and leave the current process alone.
func TestSignalProcessRejectsReusedPID(t *testing.T) {
	command := exec.Command("sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatalf("start identity canary: %v", err)
	}
	var waitErr error
	waitDone := make(chan struct{})
	go func() {
		waitErr = command.Wait()
		close(waitDone)
	}()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		select {
		case <-waitDone:
		case <-time.After(time.Second):
			t.Error("identity canary did not exit during cleanup")
		}
	})

	process, ok := processByPID(command.Process.Pid)
	if !ok {
		t.Fatal("identity canary was not visible in /proc")
	}
	reused := process.identity()
	reused.startTime++
	if err := signalProcess(reused, syscall.SIGTERM); err != nil {
		t.Fatalf("reject reused PID: %v", err)
	}
	select {
	case <-waitDone:
		t.Fatalf("signal for a reused PID terminated the current process: %v", waitErr)
	case <-time.After(100 * time.Millisecond):
	}
	if !processAlive(process.identity()) {
		t.Fatal("signal for a reused PID reached the current process")
	}
}
