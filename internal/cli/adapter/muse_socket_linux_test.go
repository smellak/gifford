//go:build linux

package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	museFixtureAgentInode  = 41001
	museFixtureNativeInode = 41002
	museFixtureOtherInode  = 41003
)

type museLinuxFixture struct {
	t              *testing.T
	procRoot       string
	runtime        string
	agent          protocol.AgentID
	hookPID        int
	musePID        int
	runPID         int
	outerPID       int
	hookStart      uint64
	museStart      uint64
	runStart       uint64
	agentPath      string
	nativePath     string
	agentListener  net.Listener
	nativeListener net.Listener
	resolver       museResolver
	unixRows       []string
}

func newMuseLinuxFixture(t *testing.T) *museLinuxFixture {
	t.Helper()
	base, err := os.MkdirTemp("", "gm-") //nolint:usetesting // Unix socket paths must stay below 108 bytes.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(base)) })
	runPID := os.Getpid()
	fixture := &museLinuxFixture{
		t:         t,
		procRoot:  filepath.Join(base, "proc"),
		runtime:   filepath.Join(base, "runtime"),
		agent:     "muse-agent",
		hookPID:   runPID + 100000,
		musePID:   runPID + 100001,
		runPID:    runPID,
		outerPID:  runPID + 100002,
		hookStart: 101,
		museStart: 202,
		runStart:  303,
	}
	require.NoError(t, os.MkdirAll(filepath.Join(fixture.procRoot, "net"), 0o700))
	writeMuseProcProcess(t, fixture.procRoot, fixture.hookPID, fixture.musePID, fixture.hookStart)
	writeMuseProcProcess(t, fixture.procRoot, fixture.musePID, fixture.runPID, fixture.museStart)
	writeMuseProcProcess(t, fixture.procRoot, fixture.runPID, fixture.outerPID, fixture.runStart)
	writeMuseProcProcess(t, fixture.procRoot, fixture.outerPID, 1, 404)
	require.NoError(t, os.MkdirAll(filepath.Join(fixture.procRoot, strconv.Itoa(fixture.runPID), "fd"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(fixture.procRoot, strconv.Itoa(fixture.outerPID), "fd"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(fixture.runtime, museAgentsDirectory), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(fixture.runtime, museNativeDirectory), 0o700))
	require.NoError(t, os.Chmod(fixture.runtime, 0o700))
	require.NoError(t, os.Chmod(filepath.Join(fixture.runtime, museAgentsDirectory), 0o700))
	require.NoError(t, os.Chmod(filepath.Join(fixture.runtime, museNativeDirectory), 0o700))
	fixture.agentPath = filepath.Join(fixture.runtime, museAgentsDirectory, string(fixture.agent)+".sock")
	fixture.nativePath = filepath.Join(fixture.runtime, museNativeDirectory, string(fixture.agent)+".sock")
	fixture.agentListener = listenMuseFixtureSocket(t, fixture.agentPath)
	fixture.nativeListener = listenMuseFixtureSocket(t, fixture.nativePath)
	fixture.setBoundaryFDs(museFixtureAgentInode, museFixtureNativeInode)
	fixture.unixRows = []string{
		museFixtureUnixRow(museFixtureAgentInode, fixture.agentPath),
		museFixtureUnixRow(museFixtureNativeInode, fixture.nativePath),
	}
	fixture.writeUnixTable()
	fixture.resolver = museResolver{
		procRoot: fixture.procRoot,
		pid:      fixture.hookPID,
		euid:     os.Geteuid(),
		dial: func(ctx context.Context, path string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
	}
	return fixture
}

func listenMuseFixtureSocket(t *testing.T, path string) net.Listener {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func requireMuseNoAccept(t *testing.T, listener net.Listener) {
	t.Helper()
	unixListener, ok := listener.(*net.UnixListener)
	require.True(t, ok)
	require.NoError(t, unixListener.SetDeadline(time.Now().Add(25*time.Millisecond)))
	accepted, err := unixListener.Accept()
	if accepted != nil {
		require.NoError(t, accepted.Close())
	}
	require.Error(t, err)
}

func writeMuseProcProcess(t *testing.T, root string, pid, parent int, start uint64) {
	t.Helper()
	directory := filepath.Join(root, strconv.Itoa(pid))
	require.NoError(t, os.MkdirAll(directory, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "stat"), museFixtureProcStat(pid, parent, start), 0o600))
}

func museFixtureProcStat(pid, parent int, start uint64) []byte {
	fields := []string{
		"S", strconv.Itoa(parent), "1", "1", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "20", "0", "1", "0", strconv.FormatUint(start, 10),
	}
	return []byte(fmt.Sprintf("%d (fixture) %s\n", pid, strings.Join(fields, " ")))
}

func museFixtureUnixRow(inode uint64, path string) string {
	return fmt.Sprintf(
		"0000000000000000: 00000002 00000000 00010000 0001 01 %d %s\n",
		inode,
		path,
	)
}

func (f *museLinuxFixture) writeUnixTable() {
	f.t.Helper()
	content := "Num RefCount Protocol Flags Type St Inode Path\n" + strings.Join(f.unixRows, "")
	require.NoError(f.t, os.WriteFile(filepath.Join(f.procRoot, "net", "unix"), []byte(content), 0o600))
}

func (f *museLinuxFixture) setBoundaryFDs(inodes ...uint64) {
	f.t.Helper()
	f.setFDs(f.runPID, inodes...)
}

func (f *museLinuxFixture) setOuterFDs(inodes ...uint64) {
	f.t.Helper()
	f.setFDs(f.outerPID, inodes...)
}

func (f *museLinuxFixture) setFDs(pid int, inodes ...uint64) {
	f.t.Helper()
	directory := filepath.Join(f.procRoot, strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(directory)
	require.NoError(f.t, err)
	for _, entry := range entries {
		require.NoError(f.t, os.Remove(filepath.Join(directory, entry.Name())))
	}
	for index, inode := range inodes {
		require.NoError(f.t, os.Symlink(
			fmt.Sprintf("socket:[%d]", inode),
			filepath.Join(directory, strconv.Itoa(index+3)),
		))
	}
}

func (f *museLinuxFixture) marker(reason museMarkerReason) string {
	f.t.Helper()
	return f.markerAt(f.runtime, reason)
}

func (f *museLinuxFixture) markerAt(runtime string, reason museMarkerReason) string {
	f.t.Helper()
	return filepath.Join(
		runtime, museDiagnosticDirectory, museDiagnosticMuseFolder,
		museMarkerToken(museProcessToken{pid: f.runPID, parent: f.outerPID, start: f.runStart}),
		string(reason)+".marker",
	)
}

func museFixtureMessage(t *testing.T) (nativeevents.Hello, nativeevents.Event) {
	t.Helper()
	hello := nativeevents.Hello{
		Operation: nativeevents.OperationHello,
		Harness:   protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: museEmitterVersion,
		SchemaVersion: nativeevents.SchemaVersion,
	}
	event := nativeevents.Event{
		Operation: nativeevents.OperationEvent,
		Sequence:  1, EmitterEventID: "muse-event-1", Type: protocol.EventTypeSessionStarted,
		ObservedAt: 1, SessionRef: "session-1",
	}
	require.NoError(t, hello.Validate())
	require.NoError(t, event.Validate())
	return hello, event
}

func serveMuseFixtureEvent(t *testing.T, listener net.Listener) <-chan nativeevents.Event {
	t.Helper()
	received := make(chan nativeevents.Event, 1)
	failures := make(chan error, 1)
	t.Cleanup(func() {
		select {
		case err := <-failures:
			require.NoError(t, err)
		default:
		}
	})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			failures <- err
			return
		}
		defer func() { _ = conn.Close() }()
		message, err := nativeevents.ReadMessage(conn)
		if err != nil {
			failures <- err
			return
		}
		if _, ok := message.(nativeevents.Hello); !ok {
			failures <- errors.New("fixture expected hello")
			return
		}
		if err := nativeevents.WriteMessage(conn, nativeevents.HelloResult{
			Operation: nativeevents.OperationHelloResult,
			Accepted:  true,
		}); err != nil {
			failures <- err
			return
		}
		message, err = nativeevents.ReadMessage(conn)
		if err != nil {
			failures <- err
			return
		}
		event, ok := message.(nativeevents.Event)
		if !ok {
			failures <- errors.New("fixture expected event")
			return
		}
		received <- event
		failures <- nativeevents.WriteMessage(conn, nativeevents.Ack{
			Operation: nativeevents.OperationAck,
			Sequence:  event.Sequence, EmitterEventID: event.EmitterEventID,
		})
	}()
	return received
}

func TestMuseFallbackDiscoversAndEmitsOnAuthenticatedRun(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	// A real run may also own TCP sockets, whose inode will not appear in
	// /proc/net/unix. Discovery must ignore those unmatched descriptors.
	fixture.setBoundaryFDs(museFixtureAgentInode, museFixtureNativeInode, museFixtureOtherInode)
	received := serveMuseFixtureEvent(t, fixture.nativeListener)
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	select {
	case got := <-received:
		require.Equal(t, event, got)
	case <-time.After(time.Second):
		t.Fatal("native event was not received")
	}
	require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
}

func TestMuseFallbackAllowsWritableAncestorsBeforePrivateRuntime(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	require.NoError(t, os.Chmod(filepath.Dir(fixture.runtime), 0o775))
	received := serveMuseFixtureEvent(t, fixture.nativeListener)
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	require.Equal(t, event, <-received)
	require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
}

func TestMuseSocketFDIdentityValidatesOpenPathTarget(t *testing.T) {
	base := t.TempDir()
	euid := os.Geteuid()
	validPath := filepath.Join(base, "valid.sock")
	_ = listenMuseFixtureSocket(t, validPath)
	openPath := func(t *testing.T, path string) int {
		t.Helper()
		fd, err := unix.Open(path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, unix.Close(fd)) })
		return fd
	}

	t.Run("symlink", func(t *testing.T) {
		path := filepath.Join(base, "link.sock")
		require.NoError(t, os.Symlink(validPath, path))
		_, err := museSocketFDIdentity(openPath(t, path), euid)
		require.ErrorContains(t, err, "unsafe open socket")
	})
	t.Run("permissive socket", func(t *testing.T) {
		path := filepath.Join(base, "permissive.sock")
		_ = listenMuseFixtureSocket(t, path)
		require.NoError(t, os.Chmod(path, 0o666))
		_, err := museSocketFDIdentity(openPath(t, path), euid)
		require.ErrorContains(t, err, "unsafe open socket")
	})
	t.Run("regular file", func(t *testing.T) {
		path := filepath.Join(base, "regular")
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		_, err := museSocketFDIdentity(openPath(t, path), euid)
		require.ErrorContains(t, err, "unsafe open socket")
	})
	t.Run("wrong owner", func(t *testing.T) {
		_, err := museSocketFDIdentity(openPath(t, validPath), euid+1)
		require.ErrorContains(t, err, "unsafe open socket")
	})
	t.Run("valid socket identity", func(t *testing.T) {
		directoryFD, err := unix.Open(base, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, unix.Close(directoryFD)) })
		expected, err := museSocketIdentity(directoryFD, filepath.Base(validPath), euid)
		require.NoError(t, err)
		actual, err := museSocketFDIdentity(openPath(t, validPath), euid)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	})
}

func TestMuseFallbackIgnoresOversizedNonSocketFDLink(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	fdDirectory := filepath.Join(fixture.procRoot, strconv.Itoa(fixture.runPID), "fd")
	require.NoError(t, os.Symlink(
		strings.Repeat("x", museMaxFDLinkBytes+32),
		filepath.Join(fdDirectory, "99"),
	))
	received := serveMuseFixtureEvent(t, fixture.nativeListener)
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	require.Equal(t, event, <-received)
}

func TestMuseFallbackUsesOnlyBoundaryRunAmongParallelRuns(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	otherRuntime := filepath.Join(filepath.Dir(fixture.runtime), "parallel-runtime")
	require.NoError(t, os.MkdirAll(filepath.Join(otherRuntime, museAgentsDirectory), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(otherRuntime, museNativeDirectory), 0o700))
	require.NoError(t, os.Chmod(otherRuntime, 0o700))
	otherAgentPath := filepath.Join(otherRuntime, museAgentsDirectory, string(fixture.agent)+".sock")
	otherNativePath := filepath.Join(otherRuntime, museNativeDirectory, string(fixture.agent)+".sock")
	otherAgent := listenMuseFixtureSocket(t, otherAgentPath)
	otherNative := listenMuseFixtureSocket(t, otherNativePath)
	fixture.unixRows = append([]string{
		museFixtureUnixRow(42001, otherAgentPath),
		museFixtureUnixRow(42002, otherNativePath),
	}, fixture.unixRows...)
	fixture.writeUnixTable()
	originalDial := fixture.resolver.dial
	dials := 0
	fixture.resolver.dial = func(ctx context.Context, path string) (net.Conn, error) {
		dials++
		return originalDial(ctx, path)
	}
	received := serveMuseFixtureEvent(t, fixture.nativeListener)
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	require.Equal(t, event, <-received)
	require.Equal(t, 1, dials)
	requireMuseNoAccept(t, otherAgent)
	requireMuseNoAccept(t, otherNative)
	requireMuseNoAccept(t, fixture.nativeListener)
}

func TestMuseFallbackRejectsNativeRouteMutationBeforeProtocol(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*testing.T, *museLinuxFixture) net.Listener
		markerRuntime func(*museLinuxFixture) string
		wantErr       string
	}{
		{
			name: "renamed leaf replaced by symlink",
			mutate: func(t *testing.T, fixture *museLinuxFixture) net.Listener {
				t.Helper()
				moved := fixture.nativePath + ".moved"
				require.NoError(t, os.Rename(fixture.nativePath, moved))
				decoyPath := fixture.nativePath + ".decoy"
				decoy := listenMuseFixtureSocket(t, decoyPath)
				require.NoError(t, os.Symlink(decoyPath, fixture.nativePath))
				return decoy
			},
			wantErr: "unsafe socket",
		},
		{
			name: "substituted leaf",
			mutate: func(t *testing.T, fixture *museLinuxFixture) net.Listener {
				t.Helper()
				require.NoError(t, os.Rename(fixture.nativePath, fixture.nativePath+".moved"))
				return listenMuseFixtureSocket(t, fixture.nativePath)
			},
			wantErr: "native socket identity changed",
		},
		{
			name: "substituted directory",
			mutate: func(t *testing.T, fixture *museLinuxFixture) net.Listener {
				t.Helper()
				directory := filepath.Dir(fixture.nativePath)
				require.NoError(t, os.Rename(directory, directory+".moved"))
				require.NoError(t, os.Mkdir(directory, 0o700))
				return listenMuseFixtureSocket(t, fixture.nativePath)
			},
			wantErr: "native directory identity changed",
		},
		{
			name: "substituted runtime",
			mutate: func(t *testing.T, fixture *museLinuxFixture) net.Listener {
				t.Helper()
				require.NoError(t, os.Rename(fixture.runtime, fixture.runtime+".moved"))
				require.NoError(t, os.MkdirAll(filepath.Dir(fixture.nativePath), 0o700))
				require.NoError(t, os.Chmod(fixture.runtime, 0o700))
				require.NoError(t, os.Chmod(filepath.Dir(fixture.nativePath), 0o700))
				return listenMuseFixtureSocket(t, fixture.nativePath)
			},
			markerRuntime: func(fixture *museLinuxFixture) string {
				return fixture.runtime + ".moved"
			},
			wantErr: "runtime path identity changed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMuseLinuxFixture(t)
			originalDial := fixture.resolver.dial
			var destination net.Listener
			fixture.resolver.dial = func(ctx context.Context, path string) (net.Conn, error) {
				destination = test.mutate(t, fixture)
				return originalDial(ctx, path)
			}
			hello, event := museFixtureMessage(t)
			ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
			defer cancel()

			err := fixture.resolver.emit(ctx, hello, event)
			require.ErrorContains(t, err, test.wantErr)
			markerRuntime := fixture.runtime
			if test.markerRuntime != nil {
				markerRuntime = test.markerRuntime(fixture)
			}
			require.FileExists(t, fixture.markerAt(markerRuntime, museReasonNativeUnsafe))
			unixListener, ok := fixture.nativeListener.(*net.UnixListener)
			require.True(t, ok)
			require.NoError(t, unixListener.SetDeadline(time.Now().Add(100*time.Millisecond)))
			server, err := fixture.nativeListener.Accept()
			require.NoError(t, err)
			defer func() { require.NoError(t, server.Close()) }()
			require.NoError(t, server.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
			_, err = nativeevents.ReadMessage(server)
			require.Error(t, err)
			requireMuseNoAccept(t, destination)
		})
	}
}

func TestMuseFallbackPinnedSocketSurvivesTransientRouteChange(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	originalDial := fixture.resolver.dial
	var decoy net.Listener
	fixture.resolver.dial = func(ctx context.Context, path string) (net.Conn, error) {
		moved := fixture.nativePath + ".moved"
		if err := os.Rename(fixture.nativePath, moved); err != nil {
			return nil, err
		}
		decoy = listenMuseFixtureSocket(t, fixture.nativePath)
		if unixDecoy, ok := decoy.(*net.UnixListener); ok {
			unixDecoy.SetUnlinkOnClose(false)
		}
		conn, dialErr := originalDial(ctx, path)
		if err := os.Remove(fixture.nativePath); err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return nil, err
		}
		if err := os.Rename(moved, fixture.nativePath); err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return nil, err
		}
		return conn, dialErr
	}
	received := serveMuseFixtureEvent(t, fixture.nativeListener)
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	require.Equal(t, event, <-received)
	require.NotNil(t, decoy)
	requireMuseNoAccept(t, decoy)
	require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
}

func TestMuseFallbackNeverRescuesAnOuterRun(t *testing.T) {
	hello, event := museFixtureMessage(t)
	t.Run("inner agent without native", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		fixture.setBoundaryFDs(museFixtureAgentInode)
		fixture.setOuterFDs(museFixtureNativeInode)
		dials := 0
		fixture.resolver.dial = func(context.Context, string) (net.Conn, error) {
			dials++
			return nil, errors.New("unexpected dial")
		}
		ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
		defer cancel()

		require.Error(t, fixture.resolver.emit(ctx, hello, event))
		require.Zero(t, dials)
		marker := fixture.marker(museReasonNativeMissing)
		info, err := os.Stat(marker)
		require.NoError(t, err)
		require.Zero(t, info.Size())
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})
	t.Run("inner without either socket", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		fixture.setBoundaryFDs()
		fixture.setOuterFDs(museFixtureAgentInode, museFixtureNativeInode)
		dials := 0
		fixture.resolver.dial = func(context.Context, string) (net.Conn, error) {
			dials++
			return nil, errors.New("unexpected dial")
		}
		ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
		defer cancel()

		require.Error(t, fixture.resolver.emit(ctx, hello, event))
		require.Zero(t, dials)
		require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
	})
}

func TestMuseFallbackRejectsBrokenOrWrappedLineage(t *testing.T) {
	hello, event := museFixtureMessage(t)
	tests := []struct {
		name         string
		breakLineage func(*testing.T, *museLinuxFixture)
	}{
		{
			name: "missing direct parent",
			breakLineage: func(t *testing.T, fixture *museLinuxFixture) {
				t.Helper()
				writeMuseProcProcess(t, fixture.procRoot, fixture.hookPID, fixture.outerPID+100, fixture.hookStart)
			},
		},
		{
			name: "extra wrapper before run",
			breakLineage: func(t *testing.T, fixture *museLinuxFixture) {
				t.Helper()
				wrapperPID := fixture.outerPID + 100
				writeMuseProcProcess(t, fixture.procRoot, fixture.musePID, wrapperPID, fixture.museStart)
				writeMuseProcProcess(t, fixture.procRoot, wrapperPID, fixture.runPID, 505)
				require.NoError(t, os.MkdirAll(
					filepath.Join(fixture.procRoot, strconv.Itoa(wrapperPID), "fd"), 0o700,
				))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMuseLinuxFixture(t)
			test.breakLineage(t, fixture)
			dials := 0
			fixture.resolver.dial = func(context.Context, string) (net.Conn, error) {
				dials++
				return nil, errors.New("unexpected dial")
			}
			ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
			defer cancel()

			require.Error(t, fixture.resolver.emit(ctx, hello, event))
			require.Zero(t, dials)
			require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
		})
	}
}

func TestMuseFallbackMarksFailuresOnlyAfterAgentAuthentication(t *testing.T) {
	hello, event := museFixtureMessage(t)
	t.Run("native bind chmod window", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		require.NoError(t, os.Chmod(fixture.nativePath, 0o666))
		ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
		defer cancel()

		require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "unsafe socket")
		require.FileExists(t, fixture.marker(museReasonNativeUnsafe))
	})
	t.Run("ambiguous native", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		otherPath := filepath.Join(fixture.runtime, museNativeDirectory, "other-agent.sock")
		_ = listenMuseFixtureSocket(t, otherPath)
		fixture.setBoundaryFDs(museFixtureAgentInode, museFixtureNativeInode, museFixtureOtherInode)
		fixture.unixRows = append(fixture.unixRows, museFixtureUnixRow(museFixtureOtherInode, otherPath))
		fixture.writeUnixTable()
		ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
		defer cancel()

		require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "native entry ambiguous")
		require.FileExists(t, fixture.marker(museReasonNativeAmbiguous))
	})
	t.Run("native identity mismatch", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		otherPath := filepath.Join(fixture.runtime, museNativeDirectory, "other-agent.sock")
		_ = listenMuseFixtureSocket(t, otherPath)
		fixture.unixRows[1] = museFixtureUnixRow(museFixtureNativeInode, otherPath)
		fixture.writeUnixTable()
		ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
		defer cancel()

		require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "native identity mismatch")
		require.FileExists(t, fixture.marker(museReasonNativeUnsafe))
	})
	t.Run("connect failure", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		fixture.resolver.dial = func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("fixture connect failure")
		}
		ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
		defer cancel()

		require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "connect")
		require.FileExists(t, fixture.marker(museReasonConnect))
	})
	t.Run("expired before anchor", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.ErrorIs(t, fixture.resolver.emit(ctx, hello, event), context.Canceled)
		require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
	})
}

func TestMuseFallbackRejectsUnsafeOrAmbiguousAnchorsWithoutMarker(t *testing.T) {
	t.Run("unsafe runtime", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		require.NoError(t, os.Chmod(fixture.runtime, 0o755))
		resolution, _, err := fixture.resolver.resolve(context.Background())
		require.Error(t, err)
		require.Nil(t, resolution.anchor)
		require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
	})
	t.Run("ambiguous agents", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		otherPath := filepath.Join(fixture.runtime, museAgentsDirectory, "other-agent.sock")
		_ = listenMuseFixtureSocket(t, otherPath)
		fixture.setBoundaryFDs(museFixtureAgentInode, museFixtureNativeInode, museFixtureOtherInode)
		fixture.unixRows = append(fixture.unixRows, museFixtureUnixRow(museFixtureOtherInode, otherPath))
		fixture.writeUnixTable()

		resolution, _, err := fixture.resolver.resolve(context.Background())
		require.Error(t, err)
		require.Nil(t, resolution.anchor)
		require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
	})
	t.Run("symlinked runtime", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		linkedRuntime := filepath.Join(filepath.Dir(fixture.runtime), "linked-runtime")
		require.NoError(t, os.Symlink(fixture.runtime, linkedRuntime))
		fixture.unixRows[0] = museFixtureUnixRow(
			museFixtureAgentInode,
			filepath.Join(linkedRuntime, museAgentsDirectory, string(fixture.agent)+".sock"),
		)
		fixture.writeUnixTable()

		resolution, _, err := fixture.resolver.resolve(context.Background())
		require.Error(t, err)
		require.Nil(t, resolution.anchor)
		require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
	})
}

func TestMuseFallbackBoundsProcInputsAndDetectsPIDReuse(t *testing.T) {
	t.Run("fd limit", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		inodes := make([]uint64, museMaxFDEntries+1)
		for index := range inodes {
			inodes[index] = uint64(50000 + index)
		}
		fixture.setBoundaryFDs(inodes...)
		_, _, err := fixture.resolver.resolve(context.Background())
		require.ErrorContains(t, err, "fd limit")
	})
	t.Run("proc unix limit", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		content := "Num RefCount Protocol Flags Type St Inode Path\n" +
			strings.Repeat("x", museMaxProcUnixBytes+1)
		require.NoError(t, os.WriteFile(filepath.Join(fixture.procRoot, "net", "unix"), []byte(content), 0o600))
		_, _, err := fixture.resolver.resolve(context.Background())
		require.ErrorContains(t, err, "proc unix limit")
	})
	t.Run("incomplete proc unix record", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		content := "Num RefCount Protocol Flags Type St Inode Path\n" +
			fixture.unixRows[0] + strings.TrimSuffix(fixture.unixRows[1], "\n")
		require.NoError(t, os.WriteFile(filepath.Join(fixture.procRoot, "net", "unix"), []byte(content), 0o600))
		resolution, _, err := fixture.resolver.resolve(context.Background())
		require.ErrorContains(t, err, "incomplete proc unix record")
		require.Nil(t, resolution.anchor)
	})
	t.Run("malformed proc unix row", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		fixture.unixRows[0] = "malformed\n"
		fixture.writeUnixTable()
		resolution, _, err := fixture.resolver.resolve(context.Background())
		require.ErrorContains(t, err, "malformed proc unix")
		require.Nil(t, resolution.anchor)
	})
	t.Run("ambiguous inode mapping", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		otherPath := filepath.Join(fixture.runtime, museAgentsDirectory, "other-agent.sock")
		_ = listenMuseFixtureSocket(t, otherPath)
		fixture.unixRows = []string{
			museFixtureUnixRow(museFixtureAgentInode, fixture.agentPath),
			museFixtureUnixRow(museFixtureAgentInode, otherPath),
			museFixtureUnixRow(museFixtureNativeInode, fixture.nativePath),
		}
		fixture.writeUnixTable()
		resolution, _, err := fixture.resolver.resolve(context.Background())
		require.ErrorContains(t, err, "agent anchor unavailable")
		require.Nil(t, resolution.anchor)
	})
	t.Run("pid reuse", func(t *testing.T) {
		fixture := newMuseLinuxFixture(t)
		run, err := fixture.resolver.boundary(context.Background())
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(fixture.procRoot, strconv.Itoa(fixture.runPID), "stat"),
			museFixtureProcStat(fixture.runPID, fixture.outerPID, fixture.runStart+1),
			0o600,
		))
		require.ErrorContains(t, fixture.resolver.revalidate(run), "identity changed")
	})
}

func TestMuseFallbackAuthenticatesPeerBeforeProtocol(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})
	fixture.resolver.dial = func(context.Context, string) (net.Conn, error) {
		return client, nil
	}
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "peer is not unix")
	require.FileExists(t, fixture.marker(museReasonPeer))
}

func TestMuseFallbackRejectsPeerPIDMismatch(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := fixture.nativeListener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, err := (&net.Dialer{}).DialContext(context.Background(), "unix", fixture.nativePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	server := <-accepted
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	anchor := &museAnchor{run: museProcessToken{pid: os.Getpid() + 1}}
	require.ErrorContains(t, fixture.resolver.authenticatePeer(client, anchor), "peer credentials mismatch")
}

func TestMuseFallbackRevalidatesRunBeforeEvent(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	hello, event := museFixtureMessage(t)
	serverResult := make(chan error, 1)
	go func() {
		conn, err := fixture.nativeListener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		defer func() { _ = conn.Close() }()
		message, err := nativeevents.ReadMessage(conn)
		if err != nil {
			serverResult <- err
			return
		}
		if _, ok := message.(nativeevents.Hello); !ok {
			serverResult <- errors.New("fixture expected hello")
			return
		}
		if err := os.WriteFile(
			filepath.Join(fixture.procRoot, strconv.Itoa(fixture.runPID), "stat"),
			museFixtureProcStat(fixture.runPID, fixture.outerPID, fixture.runStart+1),
			0o600,
		); err != nil {
			serverResult <- err
			return
		}
		if err := nativeevents.WriteMessage(conn, nativeevents.HelloResult{
			Operation: nativeevents.OperationHelloResult,
			Accepted:  true,
		}); err != nil {
			serverResult <- err
			return
		}
		_, err = nativeevents.ReadMessage(conn)
		if err == nil {
			serverResult <- errors.New("fixture received event after process identity changed")
			return
		}
		serverResult <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "process identity changed")
	require.NoError(t, <-serverResult)
	require.FileExists(t, fixture.marker(museReasonProtocol))
}

func TestMuseFallbackMarksPreEventRouteChangeAsNativeUnsafe(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	hello, event := museFixtureMessage(t)
	serverResult := make(chan error, 1)
	go func() {
		conn, err := fixture.nativeListener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		defer func() { _ = conn.Close() }()
		message, err := nativeevents.ReadMessage(conn)
		if err != nil {
			serverResult <- err
			return
		}
		if _, ok := message.(nativeevents.Hello); !ok {
			serverResult <- errors.New("fixture expected hello")
			return
		}
		if err := os.Rename(fixture.nativePath, fixture.nativePath+".moved"); err != nil {
			serverResult <- err
			return
		}
		if err := nativeevents.WriteMessage(conn, nativeevents.HelloResult{
			Operation: nativeevents.OperationHelloResult,
			Accepted:  true,
		}); err != nil {
			serverResult <- err
			return
		}
		_, err = nativeevents.ReadMessage(conn)
		if err == nil {
			serverResult <- errors.New("fixture received event after native route changed")
			return
		}
		serverResult <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()

	err := fixture.resolver.emit(ctx, hello, event)
	require.ErrorIs(t, err, errMuseNativeRouteChanged)
	require.NoError(t, <-serverResult)
	require.FileExists(t, fixture.marker(museReasonNativeUnsafe))
	require.NoFileExists(t, fixture.marker(museReasonProtocol))
}

func TestMuseFallbackTimeoutHasNoLateIOOrMarker(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	hello, event := museFixtureMessage(t)
	release := make(chan struct{})
	serverResult := make(chan error, 1)
	go func() {
		conn, err := fixture.nativeListener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		defer func() { _ = conn.Close() }()
		message, err := nativeevents.ReadMessage(conn)
		if err != nil {
			serverResult <- err
			return
		}
		if _, ok := message.(nativeevents.Hello); !ok {
			serverResult <- errors.New("fixture expected hello")
			return
		}
		<-release
		_, err = nativeevents.ReadMessage(conn)
		if err == nil {
			serverResult <- errors.New("fixture received late event after timeout")
			return
		}
		serverResult <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()

	err := fixture.resolver.emit(ctx, hello, event)
	require.Error(t, err)
	var netErr net.Error
	require.Truef(t,
		errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()),
		"emit error is not a deadline: %v", err,
	)
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.Less(t, time.Since(started), 500*time.Millisecond)
	close(release)
	require.NoError(t, <-serverResult)
	require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory))
}

func TestMuseMarkerIsClosedBoundedAndIdempotent(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	resolution, _, err := fixture.resolver.resolve(context.Background())
	require.NoError(t, err)
	defer resolution.anchor.close()
	require.LessOrEqual(t, len(museMarkerReasons), museMarkerReasonLimit)
	for _, reason := range museMarkerReasons {
		fixture.resolver.mark(context.Background(), resolution.anchor, reason)
		fixture.resolver.mark(context.Background(), resolution.anchor, reason)
	}
	fixture.resolver.mark(context.Background(), resolution.anchor, "unbounded_reason")
	directory := filepath.Join(
		fixture.runtime, museDiagnosticDirectory, museDiagnosticMuseFolder,
		museMarkerToken(resolution.anchor.run),
	)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, len(museMarkerReasons))
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		require.Zero(t, info.Size())
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		require.True(t, strings.HasSuffix(entry.Name(), ".marker"))
	}
	for _, name := range []string{museDiagnosticDirectory, museDiagnosticMuseFolder, museMarkerToken(resolution.anchor.run)} {
		info, err := os.Stat(filepath.Join(fixture.runtime, museDiagnosticDirectory))
		if name == museDiagnosticMuseFolder {
			info, err = os.Stat(filepath.Join(fixture.runtime, museDiagnosticDirectory, name))
		} else if name != museDiagnosticDirectory {
			info, err = os.Stat(directory)
		}
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}
}

func TestMuseHookMarkerFailureRemainsSilent(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	diagnosticPath := filepath.Join(fixture.runtime, museDiagnosticDirectory)
	require.NoError(t, os.WriteFile(diagnosticPath, nil, 0o600))
	fixture.resolver.dial = func(context.Context, string) (net.Conn, error) {
		return nil, errors.New("fixture connect failure")
	}
	t.Setenv(SocketEnvironment, "restore-after-test")
	require.NoError(t, os.Unsetenv(SocketEnvironment))
	sendCalls := 0
	cmd := newMuseHookCmd(
		func(context.Context, string, nativeevents.Hello, nativeevents.Event) error {
			sendCalls++
			return errors.New("unexpected explicit send")
		},
		fixture.resolver.emit,
		func() time.Time { return museHookTime },
		bytes.NewReader(bytes.Repeat([]byte{0x51}, museNonceBytes)),
	)
	cmd.SetIn(strings.NewReader(museHookJSON(t, museHookInterrupt, nil)))
	cmd.SetArgs([]string{})
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	require.NoError(t, cmd.Execute())
	require.Zero(t, sendCalls)
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
	info, err := os.Lstat(diagnosticPath)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
}

// useMuseSystemEntry injects a scratch system node directory holding a
// listening shared entry. Watcher tests never touch the real /run/gifford.
func useMuseSystemEntry(t *testing.T) net.Listener {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	path := filepath.Join(dir, nativeevents.NodeSocketName)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	previous := systemNativeSocketDir
	systemNativeSocketDir = dir
	t.Cleanup(func() { systemNativeSocketDir = previous })
	return listener
}

func emitMuseSystemFixture(t *testing.T, fixture *museLinuxFixture, listener net.Listener) nativeevents.Event {
	t.Helper()
	received := serveMuseFixtureEvent(t, listener)
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()
	require.NoError(t, fixture.resolver.emit(ctx, hello, event))
	select {
	case got := <-received:
		return got
	case <-time.After(time.Second):
		t.Fatal("system native event was not received")
		return nativeevents.Event{}
	}
}

func TestMuseFallbackUsesSystemEntryWhenNativeIsMissing(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	// Drop the native side of the measured topology.
	require.NoError(t, fixture.nativeListener.Close())
	fixture.setBoundaryFDs(museFixtureAgentInode)
	fixture.unixRows = []string{museFixtureUnixRow(museFixtureAgentInode, fixture.agentPath)}
	fixture.writeUnixTable()

	systemListener := useMuseSystemEntry(t)
	_, event := museFixtureMessage(t)
	require.Equal(t, event, emitMuseSystemFixture(t, fixture, systemListener))
}

func TestMuseFallbackUsesSystemEntryWithoutARunTopology(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	// The run holds an unrelated socket: no measured agent anchor.
	fixture.setBoundaryFDs(museFixtureOtherInode)
	fixture.unixRows = []string{museFixtureUnixRow(museFixtureOtherInode, "/tmp/unrelated.sock")}
	fixture.writeUnixTable()

	systemListener := useMuseSystemEntry(t)
	_, event := museFixtureMessage(t)
	require.Equal(t, event, emitMuseSystemFixture(t, fixture, systemListener))
}

func TestMuseFallbackIgnoresAnUnsafeSystemEntry(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	require.NoError(t, fixture.nativeListener.Close())
	fixture.setBoundaryFDs(museFixtureAgentInode)
	fixture.unixRows = []string{museFixtureUnixRow(museFixtureAgentInode, fixture.agentPath)}
	fixture.writeUnixTable()

	systemListener := useMuseSystemEntry(t)
	systemDir := filepath.Dir(filepath.Join(systemNativeSocketDir, nativeevents.NodeSocketName))
	require.NoError(t, os.Chmod(systemDir, 0o777))
	hello, event := museFixtureMessage(t)
	ctx, cancel := context.WithTimeout(context.Background(), EmitTimeout)
	defer cancel()
	require.ErrorContains(t, fixture.resolver.emit(ctx, hello, event), "native entry unavailable")
	requireMuseNoAccept(t, systemListener)
}
