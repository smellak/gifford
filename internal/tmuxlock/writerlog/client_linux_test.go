//go:build linux

package writerlog

import (
	"bytes"
	"context"
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
)

func TestClientSendsExactEventsCredentialsAndLockDescriptor(t *testing.T) {
	t.Parallel()
	listener, socketPath := newFakeMediator(t)
	lockFile := newTestLockFile(t)
	wantStat := statFile(t, lockFile)
	wantBodies := []string{
		`{"event":"acquire","operation_id":"0123456789abcdef0123456789abcdef","outcome":null,"profile":"gifford","v":1,"write_id":null}`,
		`{"event":"key_begin","operation_id":"0123456789abcdef0123456789abcdef","outcome":null,"profile":"gifford","v":1,"write_id":"1"}`,
		`{"event":"key_end","operation_id":"0123456789abcdef0123456789abcdef","outcome":"ok","profile":"gifford","v":1,"write_id":"1"}`,
		`{"event":"release","operation_id":"0123456789abcdef0123456789abcdef","outcome":null,"profile":"gifford","v":1,"write_id":null}`,
	}
	serverDone := servePackets(listener, len(wantBodies), func(index int, packet receivedPacket) ([]byte, error) {
		defer closeDescriptors(packet.descriptors)
		if string(packet.body) != wantBodies[index] {
			return nil, fmt.Errorf("body %d = %q", index, packet.body)
		}
		if packet.credentials == nil || packet.credentials.Pid != int32(os.Getpid()) ||
			packet.credentials.Uid != uint32(os.Geteuid()) ||
			packet.credentials.Gid != uint32(os.Getegid()) {
			return nil, fmt.Errorf("credentials %d = %#v", index, packet.credentials)
		}
		if len(packet.descriptors) != 1 {
			return nil, fmt.Errorf("descriptor count %d = %d", index, len(packet.descriptors))
		}
		var gotStat unix.Stat_t
		if err := unix.Fstat(packet.descriptors[0], &gotStat); err != nil {
			return nil, fmt.Errorf("stat received descriptor: %w", err)
		}
		if gotStat.Dev != wantStat.Dev || gotStat.Ino != wantStat.Ino {
			return nil, fmt.Errorf(
				"descriptor identity %d = %d/%d, want %d/%d",
				index, gotStat.Dev, gotStat.Ino, wantStat.Dev, wantStat.Ino,
			)
		}
		return []byte(fmt.Sprintf(`{"ok":true,"seq":%d,"v":1}`, index+1)), nil
	})

	client, err := New(socketPath)
	require.NoError(t, err)
	interval, err := client.Open(t.Context(), lockFile, testAttempt)
	require.NoError(t, err)
	require.NoError(t, interval.KeyBegin("1"))
	require.NoError(t, interval.KeyEnd("1", OutcomeOK))
	require.NoError(t, interval.Release())
	require.NoError(t, <-serverDone)
}

func TestConfiguredClientWithoutSocketPreventsAcquire(t *testing.T) {
	t.Parallel()
	client, err := New(filepath.Join(t.TempDir(), "missing.sock"))
	require.NoError(t, err)
	interval, err := client.Open(t.Context(), newTestLockFile(t), testAttempt)
	require.Nil(t, interval)
	require.ErrorIs(t, err, ErrUnavailable)
	require.NotErrorIs(t, err, ErrUncertain)
}

func TestLostACKAfterPossibleEffectIsUncertainAndNotRetried(t *testing.T) {
	t.Parallel()
	listener, socketPath := newFakeMediator(t)
	serverDone := servePackets(listener, 3, func(index int, packet receivedPacket) ([]byte, error) {
		defer closeDescriptors(packet.descriptors)
		if index == 2 {
			return nil, nil
		}
		return []byte(fmt.Sprintf(`{"ok":true,"seq":%d,"v":1}`, index+1)), nil
	})

	client, err := New(socketPath)
	require.NoError(t, err)
	interval, err := client.Open(t.Context(), newTestLockFile(t), testAttempt)
	require.NoError(t, err)
	require.NoError(t, interval.KeyBegin("1"))
	err = interval.KeyEnd("1", OutcomeOK)
	require.ErrorIs(t, err, ErrUncertain)
	require.ErrorIs(t, err, ErrUnavailable)
	// A second call returns the remembered uncertainty and cannot send again.
	require.ErrorIs(t, interval.KeyEnd("1", OutcomeOK), ErrUncertain)
	require.NoError(t, <-serverDone)
}

func TestClientRejectsACKWithExtraField(t *testing.T) {
	t.Parallel()
	listener, socketPath := newFakeMediator(t)
	serverDone := servePackets(listener, 1, func(_ int, packet receivedPacket) ([]byte, error) {
		defer closeDescriptors(packet.descriptors)
		return []byte(`{"extra":null,"ok":true,"seq":1,"v":1}`), nil
	})
	client, err := New(socketPath)
	require.NoError(t, err)
	interval, err := client.Open(t.Context(), newTestLockFile(t), testAttempt)
	require.Nil(t, interval)
	require.ErrorIs(t, err, ErrProtocol)
	require.NotErrorIs(t, err, ErrUncertain)
	require.NoError(t, <-serverDone)
}

func TestClosingEventsGetFreshBudgetsAfterOperationDeadline(t *testing.T) {
	t.Parallel()
	listener, socketPath := newFakeMediator(t)
	serverDone := servePackets(listener, 4, func(index int, packet receivedPacket) ([]byte, error) {
		defer closeDescriptors(packet.descriptors)
		return []byte(fmt.Sprintf(`{"ok":true,"seq":%d,"v":1}`, index+1)), nil
	})
	client, err := New(socketPath)
	require.NoError(t, err)
	operationContext, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	interval, err := client.Open(operationContext, newTestLockFile(t), testAttempt)
	require.NoError(t, err)
	require.NoError(t, interval.KeyBegin("1"))
	<-operationContext.Done()
	require.NoError(t, interval.KeyEnd("1", OutcomeOK))
	require.NoError(t, interval.Release())
	require.NoError(t, <-serverDone)
}

type receivedPacket struct {
	body        []byte
	credentials *unix.Ucred
	descriptors []int
}

func newFakeMediator(t *testing.T) (*net.UnixListener, string) {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "writer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{
		Name: socketPath,
		Net:  "unixpacket",
	})
	require.NoError(t, err)
	require.NoError(t, enablePassCredentials(listener))
	require.NoError(t, listener.SetDeadline(time.Now().Add(5*time.Second)))
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	return listener, socketPath
}

func servePackets(
	listener *net.UnixListener,
	count int,
	handle func(int, receivedPacket) ([]byte, error),
) <-chan error {
	done := make(chan error, 1)
	go func() {
		for index := range count {
			connection, err := listener.AcceptUnix()
			if err != nil {
				done <- fmt.Errorf("accept packet %d: %w", index, err)
				return
			}
			packet, receiveErr := receivePacket(connection)
			if receiveErr != nil {
				_ = connection.Close()
				done <- fmt.Errorf("receive packet %d: %w", index, receiveErr)
				return
			}
			ack, handleErr := handle(index, packet)
			if handleErr == nil && ack != nil {
				_, handleErr = connection.Write(ack)
			}
			closeErr := connection.Close()
			if handleErr != nil {
				done <- fmt.Errorf("handle packet %d: %w", index, handleErr)
				return
			}
			if closeErr != nil {
				done <- fmt.Errorf("close packet %d: %w", index, closeErr)
				return
			}
		}
		done <- nil
	}()
	return done
}

func receivePacket(connection *net.UnixConn) (receivedPacket, error) {
	if err := enablePassCredentials(connection); err != nil {
		return receivedPacket{}, err
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return receivedPacket{}, err
	}
	body := make([]byte, maxRequestLen+1)
	control := make([]byte, unix.CmsgSpace(unix.SizeofUcred)+unix.CmsgSpace(4*4))
	read, controlRead, flags, _, err := connection.ReadMsgUnix(body, control)
	if err != nil {
		return receivedPacket{}, err
	}
	if read == 0 || read > maxRequestLen || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return receivedPacket{}, errors.New("truncated request")
	}
	messages, err := unix.ParseSocketControlMessage(control[:controlRead])
	if err != nil {
		return receivedPacket{}, err
	}
	packet := receivedPacket{body: append([]byte(nil), body[:read]...)}
	for index := range messages {
		message := &messages[index]
		switch {
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_CREDENTIALS:
			if packet.credentials != nil {
				closeDescriptors(packet.descriptors)
				return receivedPacket{}, errors.New("duplicate credentials")
			}
			packet.credentials, err = unix.ParseUnixCredentials(message)
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS:
			var descriptors []int
			descriptors, err = unix.ParseUnixRights(message)
			packet.descriptors = append(packet.descriptors, descriptors...)
		default:
			err = errors.New("unexpected control message")
		}
		if err != nil {
			closeDescriptors(packet.descriptors)
			return receivedPacket{}, err
		}
	}
	if packet.credentials == nil || len(packet.descriptors) != 1 {
		closeDescriptors(packet.descriptors)
		return receivedPacket{}, errors.New("missing packet authority")
	}
	var decoded map[string]json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(packet.body)).Decode(&decoded); err != nil {
		closeDescriptors(packet.descriptors)
		return receivedPacket{}, err
	}
	if len(decoded) != 6 {
		closeDescriptors(packet.descriptors)
		return receivedPacket{}, fmt.Errorf("request fields = %d", len(decoded))
	}
	return packet, nil
}

func enablePassCredentials(connection syscall.Conn) error {
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

func statFile(t *testing.T, file *os.File) unix.Stat_t {
	t.Helper()
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(int(file.Fd()), &stat))
	return stat
}

func closeDescriptors(descriptors []int) {
	for _, descriptor := range descriptors {
		_ = unix.Close(descriptor)
	}
}
