package tmuxlock

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const vectorDefaultSocket = "/tmp/tmux-1113/default"

func TestDerivePathVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		binding PanelBinding
		want    string
	}{
		{
			name:    "default pane zero",
			binding: PanelBinding{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%0"},
			want: DefaultRoot + "/1113/" +
				"eb9d236c7fa256c1c63896fd6cfbf929c61c02342bf003a3bc5de304cdf24fcf/0.lock",
		},
		{
			name:    "same socket pane twelve",
			binding: PanelBinding{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%12"},
			want: DefaultRoot + "/1113/" +
				"eb9d236c7fa256c1c63896fd6cfbf929c61c02342bf003a3bc5de304cdf24fcf/12.lock",
		},
		{
			name:    "maximum pane",
			binding: PanelBinding{OwnerUID: 0, Socket: "/tmp/tmux-0/synthetic", PaneID: "%2147483647"},
			want: DefaultRoot + "/0/" +
				"b1c8b366768eece0b6bb058d543b191aaa59873009aad79bf3c721fb2e65ebdb/2147483647.lock",
		},
		{
			name:    "exact utf8",
			binding: PanelBinding{OwnerUID: 1200, Socket: "/tmp/tmux-1200/sesión-ñ", PaneID: "%7"},
			want: DefaultRoot + "/1200/" +
				"8b41b9258ff527a0420e2300200eb1ba5dc082538824bcd3aa33473b0155e029/7.lock",
		},
		{
			name:    "trailing slash is not normalized",
			binding: PanelBinding{OwnerUID: 1113, Socket: "/tmp/tmux-1113/default/", PaneID: "%1"},
			want: DefaultRoot + "/1113/" +
				"dea87cc0bc79bc9b2426e5fc8aeb3b53d20e6ca3384b835afae307ddb48fd9a2/1.lock",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := DerivePath(test.binding)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

// Bindings that differ in a single field never share a lock: two users' panes
// with the same id, or two servers of one user, each get their own file.
func TestDerivePathSeparatesBindings(t *testing.T) {
	t.Parallel()
	bindings := []PanelBinding{
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%0"},
		{OwnerUID: 1114, Socket: vectorDefaultSocket, PaneID: "%0"},
		{OwnerUID: 1113, Socket: "/tmp/tmux-1113/other", PaneID: "%0"},
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%1"},
		{OwnerUID: 1114, Socket: "/tmp/tmux-1114/default", PaneID: "%0"},
	}
	seen := map[string]PanelBinding{}
	for _, binding := range bindings {
		path, err := DerivePath(binding)
		require.NoError(t, err)
		first, taken := seen[path]
		require.Falsef(t, taken, "%+v and %+v share %s", first, binding, path)
		seen[path] = binding
	}
}

func TestDerivePathRejectsNoncanonicalBinding(t *testing.T) {
	t.Parallel()
	invalidUTF8 := string([]byte{'/', 0xff})
	tests := []PanelBinding{
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%012"},
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "12"},
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%2147483648"},
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%"},
		{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%-1"},
		{OwnerUID: 1113, Socket: "tmp/tmux-1113/default", PaneID: "%1"},
		{OwnerUID: 1113, Socket: "/tmp/tmux-1113/default\n", PaneID: "%1"},
		{OwnerUID: 1113, Socket: "/tmp/tmux-1113/default\r", PaneID: "%1"},
		{OwnerUID: 1113, Socket: "/tmp/tmux-1113/\x00default", PaneID: "%1"},
		{OwnerUID: 1113, Socket: invalidUTF8, PaneID: "%1"},
	}
	for _, binding := range tests {
		_, err := DerivePath(binding)
		require.ErrorIs(t, err, ErrInvalidBinding)
	}
}

func TestDerivePathRejectsSocketControlBytes(t *testing.T) {
	t.Parallel()
	for control := byte(0); control <= 0x1f; control++ {
		t.Run(fmt.Sprintf("C0_%02x", control), func(t *testing.T) {
			t.Parallel()
			binding := PanelBinding{
				OwnerUID: 1113,
				Socket:   "/tmp/tmux-1113/" + string([]byte{control}) + "default",
				PaneID:   "%1",
			}
			_, err := DerivePath(binding)
			require.ErrorIs(t, err, ErrInvalidBinding)
		})
	}
	t.Run("DEL_7f", func(t *testing.T) {
		t.Parallel()
		binding := PanelBinding{
			OwnerUID: 1113,
			Socket:   "/tmp/tmux-1113/\x7fdefault",
			PaneID:   "%1",
		}
		_, err := DerivePath(binding)
		require.ErrorIs(t, err, ErrInvalidBinding)
	})
}

func TestAcquireRejectsInvalidArguments(t *testing.T) {
	t.Parallel()
	binding := PanelBinding{OwnerUID: 1113, Socket: vectorDefaultSocket, PaneID: "%1"}
	guard, outcome, err := AcquirePanelLock(t.Context(), binding, -1)
	require.Nil(t, guard)
	require.Equal(t, LockInvalid, outcome)
	require.ErrorIs(t, err, ErrInvalidBinding)

	guard, outcome, err = AcquirePanelLock(t.Context(), PanelBinding{}, 0)
	require.Nil(t, guard)
	require.Equal(t, LockInvalid, outcome)
	require.ErrorIs(t, err, ErrInvalidBinding)

	binding.OwnerUID = 0
	guard, outcome, err = AcquirePanelLock(t.Context(), binding, 0)
	require.Nil(t, guard)
	require.Equal(t, LockInvalid, outcome)
	require.ErrorIs(t, err, ErrInvalidBinding)
}
