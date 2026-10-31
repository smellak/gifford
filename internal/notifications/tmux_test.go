package notifications

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const testTMUXClient = "/dev/pts/test-client"

func writeFakeTMUX(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-tmux")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700))
	return path
}

func validAdapterNotification() Notification {
	return Notification{
		ID: "ntf_01JB0000000000000000000001", Class: ClassMessage,
		ObjectID: "01JB0000000000000000000002", Sender: notificationTestSender,
		LocalRecipients: []protocol.AgentID{notificationTestRecipient},
		Priority:        protocol.PriorityHigh, Reason: ReasonMessageAvailable,
		State: StatePending, Adapter: AdapterTMUX,
		CreatedAt: notificationTestTime, UpdatedAt: notificationTestTime,
	}
}

func TestTMUXAdapterUsesLiteralSafeArgv(t *testing.T) {
	argvPath := filepath.Join(t.TempDir(), "argv")
	t.Setenv("GIFFORD_TMUX_ARGV", argvPath)
	command := writeFakeTMUX(t, `
: > "$GIFFORD_TMUX_ARGV"
for arg do
  printf '%s\n' "$arg" >> "$GIFFORD_TMUX_ARGV"
done
`)
	socket := filepath.Join(t.TempDir(), "private.sock")
	adapter, err := NewTMUXAdapter(TMUXOptions{
		Command: command, Socket: socket, Client: testTMUXClient,
		Duration: 2500 * time.Millisecond, Timeout: time.Second,
		Template: "[GIFFORD] {class} {reason} from {sender} id {id}",
	})
	require.NoError(t, err)
	require.NoError(t, adapter.Render(t.Context(), validAdapterNotification()))
	raw, err := os.ReadFile(argvPath)
	require.NoError(t, err)
	args := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	require.Equal(t, []string{"-S", socket, "display-message", "-c", testTMUXClient}, args[:5])
	require.True(t, slices.Contains(args, "-l"))
	require.True(t, slices.Contains(args, "-N"))
	durationIndex := slices.Index(args, "-d")
	require.GreaterOrEqual(t, durationIndex, 0)
	require.Equal(t, "2500", args[durationIndex+1])
	require.NotEqual(t, "0", args[durationIndex+1])
	require.True(t, strings.HasPrefix(args[len(args)-1], FixedPrefix))
	joined := strings.Join(args, " ")
	for _, forbidden := range []string{
		"-I", "display-popup", "send" + "-keys", "BODY-CANARY", "REQUEST-NOTE-CANARY",
	} {
		require.NotContains(t, joined, forbidden)
	}
}

func TestTMUXAdapterFixedFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command func(*testing.T) string
		timeout time.Duration
		want    FailureReason
		retry   bool
	}{
		{
			name: "unsupported literal flags",
			command: func(t *testing.T) string {
				return writeFakeTMUX(t, "printf '%s\\n' 'usage: display-message [-aIp]' >&2\nexit 1")
			},
			timeout: time.Second, want: FailureUnsupportedFlags,
		},
		{
			name: "client absent",
			command: func(t *testing.T) string {
				return writeFakeTMUX(t, "printf '%s\\n' 'client unavailable' >&2\nexit 1")
			},
			timeout: time.Second, want: FailureClientUnavailable,
		},
		{
			name: "timeout",
			command: func(t *testing.T) string {
				return writeFakeTMUX(t, "exec sleep 2")
			},
			timeout: 10 * time.Millisecond, want: FailureAdapterTimeout, retry: true,
		},
		{
			name: "command absent",
			command: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "not-installed")
			},
			timeout: time.Second, want: FailureAdapterUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adapter, err := NewTMUXAdapter(TMUXOptions{
				Command: test.command(t), Socket: filepath.Join(t.TempDir(), "private.sock"),
				Client: testTMUXClient, Duration: time.Second,
				Timeout: test.timeout, Template: DefaultTemplate,
			})
			require.NoError(t, err)
			err = adapter.Render(t.Context(), validAdapterNotification())
			var adapterErr *AdapterError
			require.ErrorAs(t, err, &adapterErr)
			require.Equal(t, test.want, adapterErr.Reason)
			require.Equal(t, test.retry, adapterErr.Retryable)
			require.NotContains(t, err.Error(), "private.sock")
			require.NotContains(t, err.Error(), "test-client")
		})
	}
}

func TestAdapterFailuresRemainPending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		adapter func(*testing.T) Adapter
		want    FailureReason
	}{
		{name: "disabled", adapter: func(*testing.T) Adapter { return DisabledAdapter() }, want: FailureAdapterDisabled},
		{
			name: "client absent",
			adapter: func(t *testing.T) Adapter {
				adapter, err := NewTMUXAdapter(TMUXOptions{
					Command: writeFakeTMUX(t, "exit 1"), Socket: filepath.Join(t.TempDir(), "private.sock"),
					Client: "/dev/pts/missing", Duration: time.Second, Timeout: time.Second,
				})
				require.NoError(t, err)
				return adapter
			},
			want: FailureClientUnavailable,
		},
		{
			name: "timeout",
			adapter: func(t *testing.T) Adapter {
				adapter, err := NewTMUXAdapter(TMUXOptions{
					Command: writeFakeTMUX(t, "exec sleep 2"), Socket: filepath.Join(t.TempDir(), "private.sock"),
					Client: "/dev/pts/slow", Duration: time.Second, Timeout: 10 * time.Millisecond,
				})
				require.NoError(t, err)
				return adapter
			},
			want: FailureAdapterTimeout,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newNotificationFixture(t)
			service := newNotificationService(t, fixture, test.adapter(t), byte(20+index), nil)
			require.NoError(t, service.Initialize(t.Context()))
			messageID := notificationMessageID(50 + index)
			insertSourceMessage(t, fixture, sourceMessage{
				id: messageID, control: protocol.ControlStateAllowed,
				recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
			})
			_, err := service.RunOnce(t.Context())
			require.NoError(t, err)
			var state State
			var reason FailureReason
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state,
				last_error_reason FROM human_notifications
				WHERE class = 'MESSAGE' AND object_id = ?`, messageID).Scan(&state, &reason))
			require.Equal(t, StatePending, state)
			require.Equal(t, test.want, reason)
			require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
		})
	}
}

func TestTemplateAndEventPayloadAreStrict(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateTemplate(DefaultTemplate))
	for _, template := range []string{
		"notice {id}",
		"[GIFFORD] {unknown}",
		"[GIFFORD] {request_note}",
		"[GIFFORD] /private/path",
		"[GIFFORD] \x1b[31mred",
	} {
		require.Error(t, ValidateTemplate(template), template)
	}
	payload := EventPayload{
		NotificationID: "ntf_01JB0000000000000000000001", Class: ClassMessage,
		ObjectID: "01JB0000000000000000000002", Reason: ReasonMessageAvailable,
		State: StatePending, Adapter: AdapterTMUX,
	}
	raw, err := marshalEventPayload(protocol.EventTypeHumanNotificationCreated, payload)
	require.NoError(t, err)
	decoded, err := DecodeEventPayload(protocol.EventTypeHumanNotificationCreated, raw)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	_, err = DecodeEventPayload(protocol.EventTypeHumanNotificationCreated,
		protocol.Payload(`{"notification_id":"ntf_01JB0000000000000000000001","class":"MESSAGE","object_id":"01JB0000000000000000000002","reason":"MESSAGE_AVAILABLE","state":"PENDING","adapter":"TMUX","body":"private"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidRecord)
	invalidClassObject := payload
	invalidClassObject.ObjectID = "task-not-a-message"
	_, err = marshalEventPayload(protocol.EventTypeHumanNotificationCreated, invalidClassObject)
	require.ErrorIs(t, err, ErrInvalidRecord)
	var adapterErr *AdapterError
	require.NotErrorAs(t, err, &adapterErr)
}
