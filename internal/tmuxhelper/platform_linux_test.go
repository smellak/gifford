//go:build linux

package tmuxhelper

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const productionMainSubprocess = "GIFFORD_TMUX_TEST_PRODUCTION_MAIN"

func TestProductionMainClosesInheritedDescriptorsWithoutBreakingRuntime(t *testing.T) {
	if os.Getenv(productionMainSubprocess) == "1" {
		if _, err := unix.FcntlInt(3, unix.F_GETFD, 0); err != nil {
			os.Exit(90)
		}
		code := Main([]string{"invalid"})
		if _, err := unix.FcntlInt(3, unix.F_GETFD, 0); err != unix.EBADF {
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
	require.Equal(t, int(ExitGrammar), exitErr.ExitCode())
	require.Equal(t, "GTMX/1 2 0\n\n", stdout.String())
	require.Empty(t, stderr.String())
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
