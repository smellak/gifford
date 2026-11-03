//go:build linux

package tmuxattach

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

type lockedPaneWriterFileProvider struct {
	path string
	file *os.File
}

func (provider *lockedPaneWriterFileProvider) DuplicateLockFile(Target) (*os.File, error) {
	file, err := os.OpenFile(provider.path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	provider.file = file
	return file, nil
}

type paneWriterPacket struct {
	body        []byte
	credentials *unix.Ucred
	descriptor  int
}

func TestPaneWriterLogOpenReleaseUseSameLockedFile(t *testing.T) {
	t.Parallel()
	listener, socketPath := newPaneWriterMediator(t)
	lockPath := filepath.Join(t.TempDir(), "pane.lock")
	require.NoError(t, os.WriteFile(lockPath, nil, 0o600))
	provider := &lockedPaneWriterFileProvider{path: lockPath}
	wantBodies := []string{
		`{"event":"acquire","operation_id":"0123456789abcdef0123456789abcdef","outcome":null,"profile":"gifford","v":1,"write_id":null}`,
		`{"event":"release","operation_id":"0123456789abcdef0123456789abcdef","outcome":null,"profile":"gifford","v":1,"write_id":null}`,
	}
	done := servePaneWriterPackets(listener, len(wantBodies), func(index int, packet paneWriterPacket) error {
		defer func() { _ = unix.Close(packet.descriptor) }()
		if string(packet.body) != wantBodies[index] {
			return fmt.Errorf("body %d differs", index)
		}
		if packet.credentials.Pid != int32(os.Getpid()) ||
			packet.credentials.Uid != uint32(os.Getuid()) ||
			packet.credentials.Gid != uint32(os.Getgid()) {
			return errors.New("unexpected peer credentials")
		}
		if err := unix.Flock(packet.descriptor, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return fmt.Errorf("descriptor does not share the retained claim: %w", err)
		}
		return nil
	})
	client, err := writerlog.New(socketPath)
	require.NoError(t, err)
	log, err := newPaneWriterLog(provider, client)
	require.NoError(t, err)

	require.NoError(t, log.Open(t.Context(), testTarget, testPaneWriterAttempt))
	require.NotNil(t, provider.file)
	require.NoError(t, log.Release(testTarget))
	require.NoError(t, <-done)
	_, err = provider.file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

func newPaneWriterMediator(t *testing.T) (*net.UnixListener, string) {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "writer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	require.NoError(t, err)
	require.NoError(t, enablePaneWriterCredentials(listener))
	require.NoError(t, listener.SetDeadline(time.Now().Add(5*time.Second)))
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	return listener, socketPath
}

func servePaneWriterPackets(
	listener *net.UnixListener,
	count int,
	handle func(int, paneWriterPacket) error,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		for index := range count {
			connection, err := listener.AcceptUnix()
			if err != nil {
				done <- err
				return
			}
			packet, receiveErr := receivePaneWriterPacket(connection)
			handleErr := receiveErr
			if handleErr == nil {
				handleErr = handle(index, packet)
			}
			if handleErr == nil {
				_, handleErr = connection.Write([]byte(fmt.Sprintf(
					`{"ok":true,"seq":%d,"v":1}`, index+1,
				)))
			}
			closeErr := connection.Close()
			if handleErr != nil || closeErr != nil {
				done <- errors.Join(handleErr, closeErr)
				return
			}
		}
		done <- nil
	}()
	return done
}

func receivePaneWriterPacket(connection *net.UnixConn) (paneWriterPacket, error) {
	if err := enablePaneWriterCredentials(connection); err != nil {
		return paneWriterPacket{}, err
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return paneWriterPacket{}, err
	}
	body := make([]byte, 4097)
	control := make([]byte, unix.CmsgSpace(unix.SizeofUcred)+unix.CmsgSpace(4))
	read, controlRead, flags, _, err := connection.ReadMsgUnix(body, control)
	if err != nil {
		return paneWriterPacket{}, err
	}
	if read == 0 || read > 4096 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return paneWriterPacket{}, errors.New("truncated request")
	}
	messages, err := unix.ParseSocketControlMessage(control[:controlRead])
	if err != nil {
		return paneWriterPacket{}, err
	}
	packet := paneWriterPacket{body: bytes.Clone(body[:read]), descriptor: -1}
	for index := range messages {
		message := &messages[index]
		switch {
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_CREDENTIALS:
			packet.credentials, err = unix.ParseUnixCredentials(message)
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS:
			var descriptors []int
			descriptors, err = unix.ParseUnixRights(message)
			if len(descriptors) == 1 {
				packet.descriptor = descriptors[0]
			} else {
				for _, descriptor := range descriptors {
					_ = unix.Close(descriptor)
				}
				err = errors.New("unexpected descriptor count")
			}
		default:
			err = errors.New("unexpected control message")
		}
		if err != nil {
			if packet.descriptor >= 0 {
				_ = unix.Close(packet.descriptor)
			}
			return paneWriterPacket{}, err
		}
	}
	if packet.credentials == nil || packet.descriptor < 0 {
		if packet.descriptor >= 0 {
			_ = unix.Close(packet.descriptor)
		}
		return paneWriterPacket{}, errors.New("missing packet authority")
	}
	var decoded map[string]json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(packet.body)).Decode(&decoded); err != nil || len(decoded) != 6 {
		_ = unix.Close(packet.descriptor)
		return paneWriterPacket{}, errors.New("invalid request body")
	}
	return packet, nil
}

func enablePaneWriterCredentials(connection syscall.Conn) error {
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	var controlErr error
	if err := raw.Control(func(descriptor uintptr) {
		controlErr = unix.SetsockoptInt(int(descriptor), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
	}); err != nil {
		return err
	}
	return controlErr
}
