package controlapi

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaneAdminUIDFormatIsCanonicalAndStrictlyIncreasing(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		raw  string
		want []uint32
	}{
		"empty":        {raw: "", want: []uint32{}},
		"one":          {raw: "7\n", want: []uint32{7}},
		"ordered":      {raw: "0\n7\n4294967295\n", want: []uint32{0, 7, 4294967295}},
		"no final lf":  {raw: "7"},
		"blank line":   {raw: "7\n\n"},
		"leading zero": {raw: "07\n"},
		"duplicate":    {raw: "7\n7\n"},
		"descending":   {raw: "8\n7\n"},
		"overflow":     {raw: "4294967296\n"},
		"non ascii":    {raw: "７\n"},
		"space":        {raw: "7 \n"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := parsePaneAdminUIDs([]byte(test.raw))
			if test.want == nil {
				require.ErrorIs(t, err, ErrPaneOperationPolicyInvalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestPaneOperationAuthorizerRequiresVerifiedAllowedPeer(t *testing.T) {
	t.Parallel()
	loads := 0
	authorizer, err := newPaneOperationAuthorizer("/etc/example/pane-admin-uids",
		func(string) ([]uint32, error) {
			loads++
			return []uint32{7, 42}, nil
		})
	require.NoError(t, err)
	require.ErrorIs(t, authorizer.Authorize(Peer{UID: 7}), ErrPaneOperationUnauthorized)
	require.Zero(t, loads, "an unauthenticated peer must be rejected before policy I/O")
	require.ErrorIs(t, authorizer.Authorize(Peer{UID: 8, Verified: true}), ErrPaneOperationUnauthorized)
	require.NoError(t, authorizer.Authorize(Peer{UID: 42, Verified: true}))
	require.Equal(t, 2, loads, "policy is re-read for every authenticated operation")

	authorizer.load = func(string) ([]uint32, error) { return nil, errors.New("unavailable") }
	require.ErrorIs(t,
		authorizer.Authorize(Peer{UID: 42, Verified: true}),
		ErrPaneOperationPolicyInvalid,
	)
}

func TestPaneOperationAuthorizerRejectsUnsafePath(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"", "/", "relative", "/etc/../tmp/policy", "/etc/policy\n"} {
		_, err := NewPaneOperationAuthorizer(path)
		require.ErrorIs(t, err, ErrPaneOperationPolicyInvalid, path)
	}
}
