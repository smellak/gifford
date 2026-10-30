package adapters

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

type fakeAdapter struct {
	listener net.Listener
	path     string
	done     chan error

	mu   sync.Mutex
	conn net.Conn
}

func startFakeAdapter(t *testing.T, serve func(net.Conn) error) *fakeAdapter {
	t.Helper()

	path := filepath.Join(t.TempDir(), "adapter.sock")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	fake := &fakeAdapter{listener: listener, path: path, done: make(chan error, 1)}
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			fake.done <- acceptErr
			return
		}
		fake.mu.Lock()
		fake.conn = conn
		fake.mu.Unlock()
		defer func() { _ = conn.Close() }()
		fake.done <- serve(conn)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		fake.mu.Lock()
		if fake.conn != nil {
			_ = fake.conn.Close()
		}
		fake.mu.Unlock()
		select {
		case err := <-fake.done:
			if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				t.Errorf("fake adapter: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("fake adapter did not stop")
		}
	})
	return fake
}

func connectOptions(path string) Options {
	fallback := GenericManifest("agent-1", protocol.HarnessCodex, "1.2.3")
	return Options{
		SocketPath:       path,
		AgentID:          fallback.AgentID,
		Harness:          fallback.Harness,
		Fallback:         fallback,
		HandshakeTimeout: 100 * time.Millisecond,
		HeartbeatEvery:   20 * time.Millisecond,
		HeartbeatTimeout: 40 * time.Millisecond,
	}
}

func handshake(conn net.Conn, manifest protocol.CapabilityManifest) error {
	request, err := ReadMessage(conn)
	if err != nil {
		return err
	}
	return WriteMessage(conn, Message{
		Operation: OperationManifest,
		ID:        1,
		ReplyTo:   request.ID,
		Manifest:  &manifest,
	})
}

func serveHeartbeats(conn net.Conn, manifest protocol.CapabilityManifest) error {
	if err := handshake(conn, manifest); err != nil {
		return err
	}
	for {
		request, err := ReadMessage(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if err := WriteMessage(conn, Message{Operation: OperationPong, ID: request.ID, ReplyTo: request.ID}); err != nil {
			return err
		}
	}
}

func TestConnectAcceptsVerifiedNativeManifest(t *testing.T) {
	t.Parallel()

	want := nativeManifest()
	fake := startFakeAdapter(t, func(conn net.Conn) error { return serveHeartbeats(conn, want) })
	opts := connectOptions(fake.path)
	client, got, err := Connect(t.Context(), opts)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, want, client.Manifest())
	require.NoError(t, client.Close())
}

func TestConnectRejectsUnverifiedManifests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		patch func(*protocol.CapabilityManifest)
	}{
		{name: "invalid manifest", patch: func(m *protocol.CapabilityManifest) {
			m.Capabilities.PTYCapture = false
		}},
		{name: "generic adapter", patch: func(m *protocol.CapabilityManifest) {
			m.AdapterType = protocol.AdapterTypeGenericPTY
			m.Capabilities.StructuredEvents = false
			m.Capabilities.ToolEvents = false
		}},
		{name: "crossed agent", patch: func(m *protocol.CapabilityManifest) { m.AgentID = "agent-2" }},
		{name: "crossed harness", patch: func(m *protocol.CapabilityManifest) {
			m.Harness = protocol.HarnessOpencode
		}},
		{name: "missing version", patch: func(m *protocol.CapabilityManifest) { m.HarnessVersion = "" }},
		{name: "native guard", patch: func(m *protocol.CapabilityManifest) {
			m.InputGuardProvider = protocol.InputGuardProviderNativeAdapter
			m.Capabilities.PromptBufferIntrospection = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manifest := nativeManifest()
			test.patch(&manifest)
			fake := startFakeAdapter(t, func(conn net.Conn) error { return handshake(conn, manifest) })
			opts := connectOptions(fake.path)
			client, got, err := Connect(t.Context(), opts)
			require.ErrorIs(t, err, ErrInvalidManifest)
			require.Nil(t, client)
			require.Equal(t, opts.Fallback, got)
		})
	}
}

func TestConnectRejectsWrongHandshakeReplies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		reply func(Message) Message
	}{
		{
			name: "pong",
			reply: func(request Message) Message {
				return Message{Operation: OperationPong, ID: 2, ReplyTo: request.ID}
			},
		},
		{
			name: "wrong reply_to",
			reply: func(Message) Message {
				manifest := nativeManifest()
				return Message{Operation: OperationManifest, ID: 2, ReplyTo: 2, Manifest: &manifest}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fake := startFakeAdapter(t, func(conn net.Conn) error {
				request, err := ReadMessage(conn)
				if err != nil {
					return err
				}
				return WriteMessage(conn, test.reply(request))
			})
			opts := connectOptions(fake.path)
			client, got, err := Connect(t.Context(), opts)
			require.ErrorIs(t, err, ErrInvalidManifest)
			require.Nil(t, client)
			require.Equal(t, opts.Fallback, got)
		})
	}
}

func TestConnectTimeoutFallsBack(t *testing.T) {
	t.Parallel()

	fake := startFakeAdapter(t, func(conn net.Conn) error {
		if _, err := ReadMessage(conn); err != nil {
			return err
		}
		_, err := ReadMessage(conn)
		return err
	})
	opts := connectOptions(fake.path)
	opts.HandshakeTimeout = 20 * time.Millisecond
	client, got, err := Connect(t.Context(), opts)
	require.ErrorIs(t, err, ErrUnavailable)
	require.Nil(t, client)
	require.Equal(t, opts.Fallback, got)
}

func TestConnectRequiresPrivateSocket(t *testing.T) {
	t.Parallel()

	fake := startFakeAdapter(t, func(net.Conn) error { return nil })
	require.NoError(t, os.Chmod(fake.path, 0o660))
	opts := connectOptions(fake.path)
	client, got, err := Connect(t.Context(), opts)
	require.ErrorIs(t, err, ErrInsecureSocket)
	require.Nil(t, client)
	require.Equal(t, opts.Fallback, got)
}

func TestClientDegradesWhenAdapterDies(t *testing.T) {
	t.Parallel()

	drop := make(chan struct{})
	fake := startFakeAdapter(t, func(conn net.Conn) error {
		if err := handshake(conn, nativeManifest()); err != nil {
			return err
		}
		<-drop
		return nil
	})
	opts := connectOptions(fake.path)
	client, got, err := Connect(t.Context(), opts)
	require.NoError(t, err)
	require.Equal(t, nativeManifest(), got)
	close(drop)

	select {
	case event := <-client.Changes():
		require.Equal(t, protocol.EventTypeAdapterCapabilitiesChanged, event.EventType)
		require.Equal(t, nativeManifest().Capabilities, event.Before)
		require.Equal(t, opts.Fallback.Capabilities, event.After)
	case <-time.After(time.Second):
		t.Fatal("no capability change after adapter died")
	}
	require.Equal(t, opts.Fallback, client.Manifest())
	require.NoError(t, client.Close())
}

func TestClientDegradesWhenAdapterStopsResponding(t *testing.T) {
	t.Parallel()

	fake := startFakeAdapter(t, func(conn net.Conn) error {
		if err := handshake(conn, nativeManifest()); err != nil {
			return err
		}
		if _, err := ReadMessage(conn); err != nil {
			return err
		}
		_, err := ReadMessage(conn)
		return err
	})
	opts := connectOptions(fake.path)
	client, _, err := Connect(t.Context(), opts)
	require.NoError(t, err)

	select {
	case event := <-client.Changes():
		require.Equal(t, opts.Fallback.Capabilities, event.After)
	case <-time.After(time.Second):
		t.Fatal("no capability change after adapter stopped responding")
	}
	require.Equal(t, opts.Fallback, client.Manifest())
	require.NoError(t, client.Close())
}

func TestCloseDoesNotReportDegradation(t *testing.T) {
	t.Parallel()

	fake := startFakeAdapter(t, func(conn net.Conn) error { return serveHeartbeats(conn, nativeManifest()) })
	opts := connectOptions(fake.path)
	client, _, err := Connect(t.Context(), opts)
	require.NoError(t, err)
	require.NoError(t, client.Close())
	_, ok := <-client.Changes()
	require.False(t, ok)
}
