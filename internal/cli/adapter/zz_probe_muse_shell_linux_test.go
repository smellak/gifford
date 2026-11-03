//go:build linux

package adapter

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/nativeevents"
)

// Committed canary for H1 (miner-3 hostile review, mapper order): the real
// Muse hook command runs at the bottom of a real process tree: hook -> a
// shell standing in for Muse, which forks the hook and waits -> the
// boundary. The boundary is either a pane shell holding no socket, the
// README's "Muse resumed in tmux from a shell prompt", or the same shell
// holding one unrelated socket. The system entry is an injected scratch
// directory: the real /run/gifford is never touched. Both cases must reach
// the system entry exactly once. Removing the no-socket carve-out in
// resolve makes the shell case fail with "no socket fds" (zero arrivals).
func TestProbeMuseHookFromAShellReachesTheSystemEntry(t *testing.T) {
	if os.Getenv("PROBE_MUSE_HOOK") == "1" {
		dir := os.Getenv("PROBE_SYSTEM_DIR")
		if dir == "" || filepath.Clean(dir) == "/run/gifford" {
			t.Fatal("refusing to emit without an injected system directory")
		}
		systemNativeSocketDir = dir
		send := func(context.Context, string, nativeevents.Hello, nativeevents.Event) error {
			fmt.Println("PROBE env path used")
			return nil
		}
		fallback := func(ctx context.Context, hello nativeevents.Hello, event nativeevents.Event) error {
			err := emitMuseFallback(ctx, hello, event)
			fmt.Printf("PROBE fallback error: %v\n", err)
			return err
		}
		cmd := newMuseHookCmd(send, fallback, time.Now, rand.Reader)
		cmd.SetArgs([]string{})
		require.NoError(t, cmd.ExecuteContext(context.Background()))
		return
	}

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix",
		filepath.Join(dir, nativeevents.NodeSocketName))
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{}, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()

	self, err := os.Executable()
	require.NoError(t, err)
	inner := `"$PROBE_SELF" -test.run='^TestProbeMuseHookFromAShellReachesTheSystemEntry$' -test.v; true`
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, SocketEnvironment+"=") {
			env = append(env, kv)
		}
	}
	env = append(env, "PROBE_MUSE_HOOK=1", "PROBE_SYSTEM_DIR="+dir, "PROBE_SELF="+self, "PROBE_INNER="+inner)

	for _, tc := range []struct {
		name   string
		socket bool
	}{
		{name: "boundary is a pane shell holding no socket", socket: false},
		{name: "boundary holds one unrelated socket", socket: true},
	} {
		// The boundary forks the "Muse" shell, which forks the hook.
		boundary := exec.CommandContext(context.Background(), "/bin/sh", "-c", `/bin/sh -c "$PROBE_INNER"; true`)
		boundary.Env = env
		boundary.Stdin = strings.NewReader(`{"hook_event_name":"SessionStart","session_id":"s-probe","source":"resume"}`)
		var out bytes.Buffer
		boundary.Stdout, boundary.Stderr = &out, &out
		var pair [2]int
		if tc.socket {
			pair, err = syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
			require.NoError(t, err)
			boundary.ExtraFiles = []*os.File{os.NewFile(uintptr(pair[0]), "probe-socket")}
		}
		require.NoError(t, boundary.Run())
		if tc.socket {
			_ = boundary.ExtraFiles[0].Close()
			_ = syscall.Close(pair[1])
		}
		reached := 0
		for done := false; !done; {
			select {
			case <-accepted:
				reached++
			case <-time.After(300 * time.Millisecond):
				done = true
			}
		}
		var lines []string
		for _, line := range strings.Split(out.String(), "\n") {
			if strings.HasPrefix(line, "PROBE") {
				lines = append(lines, line)
			}
		}
		t.Logf("%s: system entry reached %d time(s); %s", tc.name, reached, strings.Join(lines, " | "))
		require.Equal(t, 1, reached, "%s", tc.name)
	}
}
