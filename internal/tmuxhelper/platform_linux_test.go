//go:build linux

package tmuxhelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	productionMainSubprocess = "GIFFORD_TMUX_TEST_PRODUCTION_MAIN"
	fakeTmuxPrefix           = "gifford-tmux-test:"
	testACLAccessName        = "system.posix_acl_access"
	testACLDefaultName       = "system.posix_acl_default"
	testACLUndefinedID       = ^uint32(0)
)

func TestMain(m *testing.M) {
	if len(os.Args) >= 3 && os.Args[1] == "-S" && strings.HasPrefix(os.Args[2], fakeTmuxPrefix) {
		os.Exit(runFakeTmux(os.Args[2], os.Args[1:]))
	}
	os.Exit(m.Run())
}

func runFakeTmux(socket string, argv []string) int {
	mode := strings.TrimPrefix(socket, fakeTmuxPrefix)
	switch {
	case mode == "inspect":
		return fakeTmuxInspect(argv)
	case strings.HasPrefix(mode, "stdout:"):
		return fakeTmuxWrite(os.Stdout, strings.TrimPrefix(mode, "stdout:"))
	case strings.HasPrefix(mode, "stderr:"):
		return fakeTmuxWrite(os.Stderr, strings.TrimPrefix(mode, "stderr:"))
	case mode == "exit-3":
		return 3
	case mode == "sleep":
		time.Sleep(30 * time.Second)
		return 0
	case strings.HasPrefix(mode, "sleep-ready:"):
		if err := os.WriteFile(strings.TrimPrefix(mode, "sleep-ready:"),
			[]byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			return 88
		}
		time.Sleep(30 * time.Second)
		return 0
	case strings.HasPrefix(mode, "sleep-group:"):
		return fakeTmuxSleepGroup(strings.TrimPrefix(mode, "sleep-group:"))
	case strings.HasPrefix(mode, "sleep-detached:"):
		return fakeTmuxSleepDetached(strings.TrimPrefix(mode, "sleep-detached:"))
	case mode == "grandchild":
		time.Sleep(30 * time.Second)
		return 0
	default:
		return 89
	}
}

func fakeTmuxInspect(argv []string) int {
	cwd, err := os.Getwd()
	if err != nil {
		return 80
	}
	input := make([]byte, 1)
	read, err := os.Stdin.Read(input)
	if read != 0 || !errors.Is(err, io.EOF) {
		return 81
	}
	inherited, ok := nonCloseOnExecDescriptors()
	if !ok {
		return 82
	}
	_, err = fmt.Fprintf(os.Stdout, "argv=%q\nenv=%q\ncwd=%q\nstdin=eof\ninherited=%d\n",
		argv, os.Environ(), cwd, inherited)
	if err != nil {
		return 83
	}
	return 0
}

func nonCloseOnExecDescriptors() (int, bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	count := 0
	for _, entry := range entries {
		fd, parseErr := strconv.ParseUint(entry.Name(), 10, 32)
		if parseErr != nil || fd < 3 {
			continue
		}
		flags, flagErr := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if errors.Is(flagErr, unix.EBADF) {
			continue
		}
		if flagErr != nil {
			return 0, false
		}
		if flags&unix.FD_CLOEXEC == 0 {
			count++
		}
	}
	return count, true
}

func fakeTmuxWrite(output io.Writer, countRaw string) int {
	count, err := strconv.Atoi(countRaw)
	if err != nil || count < 0 || count > maxDiagnostic+1 {
		return 84
	}
	if _, err := output.Write(bytes.Repeat([]byte{'x'}, count)); err != nil {
		return 85
	}
	return 0
}

func fakeTmuxSleepGroup(pidFile string) int {
	grandchild := exec.CommandContext(context.Background(), os.Args[0], "-S", fakeTmuxPrefix+"grandchild")
	grandchild.Env = []string{"LC_ALL=C.UTF-8"}
	if err := grandchild.Start(); err != nil {
		return 86
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(grandchild.Process.Pid)), 0o600); err != nil {
		_ = grandchild.Process.Kill()
		_ = grandchild.Wait()
		return 87
	}
	time.Sleep(30 * time.Second)
	return 0
}

func fakeTmuxSleepDetached(pidFile string) int {
	grandchild := exec.CommandContext(context.Background(), os.Args[0], "-S", fakeTmuxPrefix+"grandchild")
	grandchild.Env = []string{"LC_ALL=C.UTF-8"}
	grandchild.Stdout = os.Stdout
	grandchild.Stderr = os.Stderr
	grandchild.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := grandchild.Start(); err != nil {
		return 86
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(grandchild.Process.Pid)), 0o600); err != nil {
		_ = grandchild.Process.Kill()
		_ = grandchild.Wait()
		return 87
	}
	time.Sleep(30 * time.Second)
	return 0
}

func TestProductionMainClosesInheritedDescriptorsWithoutBreakingRuntime(t *testing.T) {
	if os.Getenv(productionMainSubprocess) == "1" {
		flags, err := unix.FcntlInt(3, unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC != 0 {
			os.Exit(90)
		}
		code := Main([]string{string(verbPanes), strings.Repeat("0", 64), testBankID})
		inherited, ok := nonCloseOnExecDescriptors()
		if !ok || inherited != 0 {
			os.Exit(91)
		}
		os.Exit(code)
	}

	inherited, err := os.Open("/dev/null")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, inherited.Close()) })

	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProductionMainClosesInheritedDescriptorsWithoutBreakingRuntime$")
	command.Env = append(os.Environ(), productionMainSubprocess+"=1")
	command.ExtraFiles = []*os.File{inherited}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err = command.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	exitCode := exitErr.ExitCode()
	require.Equal(t, fmt.Sprintf("GTMX/1 %d 0\n\n", exitCode), stdout.String())
	require.Empty(t, stderr.String())
	require.Contains(t, []int{int(ExitEnvironment), int(ExitRevision)}, exitCode)
}

func TestRunTmuxCommandUsesClosedProcessBoundary(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)

	args := []string{commandDisplayMessage, "-p", "literal"}
	body, code := runTmuxCommand(t.Context(), executable, fakeTmuxPrefix+"inspect", args, 4096)
	require.Equal(t, ExitOK, code)
	require.Equal(t, "argv=[\"-S\" \"gifford-tmux-test:inspect\" \"display-message\" \"-p\" \"literal\"]\n"+
		"env=[\"LC_ALL=C.UTF-8\"]\ncwd=\"/\"\nstdin=eof\ninherited=0\n", string(body))

	host := newPlatform()
	host.tmuxPath = executable
	body, code = host.RunTmux(t.Context(), fakeTmuxPrefix+"inspect", args, 4096)
	require.Equal(t, ExitEnvironment, code)
	require.Empty(t, body, "the production method never accepts the test executable")
}

func TestRunTmuxCommandMapsOutputAndChildFailures(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)

	tests := []struct {
		name     string
		mode     string
		limit    int
		wantCode ExitCode
		wantBody []byte
	}{
		{
			name: "stdout at limit", mode: "stdout:32", limit: 32, wantCode: ExitOK,
			wantBody: bytes.Repeat([]byte{'x'}, 32),
		},
		{name: "stdout over limit", mode: "stdout:33", limit: 32, wantCode: ExitOutputLimit},
		{name: "nonempty stderr", mode: "stderr:1", limit: 32, wantCode: ExitTmux},
		{name: "stderr over limit", mode: "stderr:4097", limit: 32, wantCode: ExitOutputLimit},
		{name: "nonzero exit", mode: "exit-3", limit: 32, wantCode: ExitTmux},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, code := runTmuxCommand(t.Context(), executable, fakeTmuxPrefix+test.mode,
				[]string{commandDisplayMessage}, test.limit)
			require.Equal(t, test.wantCode, code)
			require.Equal(t, test.wantBody, body)
		})
	}
}

func TestRunTmuxCommandDeadlineKillsProcessGroup(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	ctx := &triggeredDeadlineContext{Context: t.Context(), done: make(chan struct{})}
	type result struct {
		body []byte
		code ExitCode
	}
	done := make(chan result, 1)
	go func() {
		body, code := runTmuxCommand(ctx, executable, fakeTmuxPrefix+"sleep-group:"+pidFile,
			[]string{commandDisplayMessage}, 32)
		done <- result{body: body, code: code}
	}()
	pid := waitForPIDFile(t, pidFile, "same-group grandchild did not start")
	close(ctx.done)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	var got result
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tmux process group was not killed after the deadline")
	}
	body, code := got.body, got.code
	require.Equal(t, ExitTimeout, code)
	require.Empty(t, body)
	require.Eventually(t, func() bool { return processAbsent(pid) }, 5*time.Second, 10*time.Millisecond,
		"same-group grandchild %d survived the helper deadline", pid)
}

func TestRunTmuxCommandDeadlineClosesDetachedDescendantPipes(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	pidFile := filepath.Join(t.TempDir(), "detached.pid")
	ctx := &triggeredDeadlineContext{Context: t.Context(), done: make(chan struct{})}
	type result struct {
		body []byte
		code ExitCode
	}
	done := make(chan result, 1)
	go func() {
		body, code := runTmuxCommand(ctx, executable, fakeTmuxPrefix+"sleep-detached:"+pidFile,
			[]string{commandDisplayMessage}, 32)
		done <- result{body: body, code: code}
	}()
	detachedPID := waitForPIDFile(t, pidFile, "detached grandchild did not start")
	t.Cleanup(func() { _ = unix.Kill(detachedPID, unix.SIGKILL) })
	close(ctx.done)

	select {
	case got := <-done:
		require.Equal(t, ExitTimeout, got.code)
		require.Empty(t, got.body)
	case <-time.After(2 * time.Second):
		t.Fatal("detached descendant kept tmux pipes open beyond WaitDelay")
	}
	require.NoError(t, unix.Kill(detachedPID, unix.SIGKILL))
	require.Eventually(t, func() bool { return processAbsent(detachedPID) }, 5*time.Second, 10*time.Millisecond,
		"detached test grandchild %d was not reaped", detachedPID)
}

type triggeredDeadlineContext struct {
	context.Context
	done chan struct{}
}

func (c *triggeredDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *triggeredDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func TestRunTmuxCommandParentCancellationReturnsTimeout(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type result struct {
		body []byte
		code ExitCode
	}
	done := make(chan result, 1)
	go func() {
		body, code := runTmuxCommand(ctx, executable, fakeTmuxPrefix+"sleep-ready:"+pidFile,
			[]string{commandDisplayMessage}, 32)
		done <- result{body: body, code: code}
	}()
	pid := waitForPIDFile(t, pidFile, "fake tmux did not start")
	cancel()

	select {
	case got := <-done:
		require.Equal(t, ExitTimeout, got.code)
		require.Empty(t, got.body)
	case <-time.After(5 * time.Second):
		t.Fatal("tmux child was not killed and reaped after parent cancellation")
	}
	require.True(t, processAbsent(pid), "direct child must be reaped before RunTmux returns")
}

func waitForPIDFile(t *testing.T, path string, failure string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err := os.ReadFile(path)
		pid, parseErr := strconv.Atoi(string(raw))
		if err == nil && parseErr == nil && pid > 0 {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatal(failure)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func processAbsent(pid int) bool {
	_, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid)))
	return errors.Is(err, os.ErrNotExist)
}

func TestLinuxManifestLoadRejectsMutableOrRedirectedAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*testing.T, string, string)
		wantOK bool
	}{
		{name: "exact authority", wantOK: true},
		{
			name: "writable directory",
			mutate: func(t *testing.T, directory, _ string) {
				require.NoError(t, os.Chmod(directory, 0o775))
			},
		},
		{
			name: "writable ancestor",
			mutate: func(t *testing.T, directory, _ string) {
				require.NoError(t, os.Chmod(filepath.Dir(directory), 0o775))
			},
		},
		{
			name: "writable file",
			mutate: func(t *testing.T, _, file string) {
				require.NoError(t, os.Chmod(file, 0o640))
			},
		},
		{
			name: "manifest symlink",
			mutate: func(t *testing.T, _, file string) {
				outside := filepath.Join(filepath.Dir(filepath.Dir(file)), "outside.json")
				require.NoError(t, os.WriteFile(outside, validManifest(), 0o440))
				require.NoError(t, os.Remove(file))
				require.NoError(t, os.Symlink(outside, file))
			},
		},
		{
			name: "manifest hard link",
			mutate: func(t *testing.T, _, file string) {
				require.NoError(t, os.Link(file, file+".alias"))
			},
		},
		{
			name: "directory access ACL",
			mutate: func(t *testing.T, directory, _ string) {
				setExtendedACL(t, directory, testACLAccessName, 0o755)
			},
		},
		{
			name: "directory default ACL",
			mutate: func(t *testing.T, directory, _ string) {
				setExtendedACL(t, directory, testACLDefaultName, 0o755)
			},
		},
		{
			name: "file access ACL",
			mutate: func(t *testing.T, _, file string) {
				setExtendedACL(t, file, testACLAccessName, 0o440)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			host, directory, file := newManifestPlatform(t)
			if test.mutate != nil {
				test.mutate(t, directory, file)
			}
			raw, err := host.LoadManifest()
			if test.wantOK {
				require.NoError(t, err)
				require.Equal(t, validManifest(), raw)
				return
			}
			require.Error(t, err)
			require.Nil(t, raw)
		})
	}
}

func setExtendedACL(t *testing.T, path string, name string, mode uint16) {
	t.Helper()
	owner := mode >> 6 & 0o7
	group := mode >> 3 & 0o7
	other := mode & 0o7
	entries := [][3]uint32{
		{0x01, uint32(owner), testACLUndefinedID},
		{0x02, uint32(group), uint32(os.Geteuid()) + 1},
		{0x04, uint32(group), testACLUndefinedID},
		{0x10, uint32(group), testACLUndefinedID},
		{0x20, uint32(other), testACLUndefinedID},
	}
	raw := make([]byte, 4+8*len(entries))
	binary.LittleEndian.PutUint32(raw[:4], 2)
	for index, entry := range entries {
		offset := 4 + index*8
		binary.LittleEndian.PutUint16(raw[offset:offset+2], uint16(entry[0]))
		binary.LittleEndian.PutUint16(raw[offset+2:offset+4], uint16(entry[1]))
		binary.LittleEndian.PutUint32(raw[offset+4:offset+8], entry[2])
	}
	err := unix.Setxattr(path, name, raw, 0)
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		t.Skip("test filesystem does not support POSIX ACLs")
	}
	require.NoError(t, err)
}

func newManifestPlatform(t *testing.T) (*linuxPlatform, string, string) {
	t.Helper()

	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0o700))
	directory := filepath.Join(root, "etc", "gifford-tmux")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	require.NoError(t, os.Chmod(filepath.Join(root, "etc"), 0o755))
	require.NoError(t, os.Chmod(directory, 0o755))
	file := filepath.Join(directory, manifestFilename)
	require.NoError(t, os.WriteFile(file, validManifest(), 0o440))
	require.NoError(t, os.Chmod(file, 0o440))
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	return &linuxPlatform{
		manifestRoot: root, manifestEtc: manifestEtcDirectory,
		manifestDir: manifestDirectory, manifestFile: manifestFilename,
		manifestDirUID: uid, manifestDirGID: gid, manifestFileUID: uid, manifestFileGID: gid,
		manifestFileMode: 0o440,
	}, directory, file
}

func TestLinuxSocketValidationRejectsNonSocketAndSymlink(t *testing.T) {
	t.Parallel()

	t.Run("registered socket", func(t *testing.T) {
		t.Parallel()
		host, socket := newSocketPlatform(t)
		require.NoError(t, host.ValidateSocket(uint32(os.Geteuid()), socket))
	})

	t.Run("socket root mode", func(t *testing.T) {
		t.Parallel()
		host, socket := newSocketPlatform(t)
		require.NoError(t, os.Chmod(host.socketRoot, 0o777))
		require.Error(t, host.ValidateSocket(uint32(os.Geteuid()), socket))
	})

	t.Run("socket directory mode", func(t *testing.T) {
		t.Parallel()
		host, socket := newSocketPlatform(t)
		directory := filepath.Dir(socket)
		require.NoError(t, os.Chmod(directory, 0o750))
		require.Error(t, host.ValidateSocket(uint32(os.Geteuid()), socket))
	})

	t.Run("regular file", func(t *testing.T) {
		t.Parallel()
		host, socket := newSocketPlatform(t)
		require.NoError(t, host.listeners[0].Close())
		require.NoError(t, os.WriteFile(socket, []byte("not a socket"), 0o600))
		require.Error(t, host.platform.ValidateSocket(uint32(os.Geteuid()), socket))
	})

	t.Run("socket symlink", func(t *testing.T) {
		t.Parallel()
		fixture, _ := newSocketPlatform(t)
		uid := uint32(os.Geteuid())
		directory := filepath.Join(fixture.socketRoot, "tmux-"+strconv.FormatUint(uint64(uid), 10))
		alias := filepath.Join(directory, "alias")
		require.NoError(t, os.Symlink("agent", alias))
		require.Error(t, fixture.platform.ValidateSocket(uid, alias))
	})
}

type socketPlatformFixture struct {
	platform   *linuxPlatform
	socketRoot string
	listeners  []net.Listener
}

func (f *socketPlatformFixture) ValidateSocket(uid uint32, socket string) error {
	return f.platform.ValidateSocket(uid, socket)
}

func newSocketPlatform(t *testing.T) (*socketPlatformFixture, string) {
	t.Helper()

	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0o777|os.ModeSticky))
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	require.NotZero(t, uid)
	directory := filepath.Join(root, "tmux-"+strconv.FormatUint(uint64(uid), 10))
	require.NoError(t, os.Mkdir(directory, 0o700))
	require.NoError(t, os.Chmod(directory, 0o700))
	socket := filepath.Join(directory, "agent")
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	platform := &linuxPlatform{
		socketRoot: root, socketRootUID: uid, socketRootGID: gid,
	}
	return &socketPlatformFixture{
		platform: platform, socketRoot: root, listeners: []net.Listener{listener},
	}, socket
}

func TestLinuxProcessIdentityParsesCommAndPinsBoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	procDirectory := filepath.Join(root, "4242")
	require.NoError(t, os.Mkdir(procDirectory, 0o700))
	fields := make([]string, 20)
	for index := range fields {
		fields[index] = "1"
	}
	fields[0] = "S"
	fields[19] = "700"
	stat := "4242 (agent ) worker) " + strings.Join(fields, " ") + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(procDirectory, "stat"), []byte(stat), 0o600))
	bootPath := filepath.Join(root, "boot_id")
	require.NoError(t, os.WriteFile(bootPath, []byte(testBootID+"\n"), 0o600))

	host := &linuxPlatform{procRoot: root, bootIDPath: bootPath}
	start, err := host.ProcessStart(4242)
	require.NoError(t, err)
	require.Equal(t, uint64(700), start)
	bootID, err := host.BootID()
	require.NoError(t, err)
	require.Equal(t, testBootID, bootID)
}

func TestParseProcessStartRejectsMalformedStat(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		nil,
		[]byte("4242 no-parentheses"),
		[]byte("4242 (agent) S 1 2"),
		[]byte("4242 (agent) S 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 00"),
	} {
		start, ok := parseProcessStart(raw)
		require.False(t, ok)
		require.Empty(t, start)
	}
}

func TestLimitedBufferNeverReturnsPartialWrite(t *testing.T) {
	t.Parallel()

	buffer := &limitedBuffer{limit: 3}
	written, err := buffer.Write([]byte("abcdef"))
	require.NoError(t, err)
	require.Equal(t, 6, written)
	require.Equal(t, []byte("abc"), buffer.Bytes())
	require.True(t, buffer.overflow)
}
