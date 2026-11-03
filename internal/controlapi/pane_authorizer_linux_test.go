//go:build linux

package controlapi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaneAdminUIDLoaderRejectsSymlinkAndWritablePolicy(t *testing.T) {
	t.Parallel()
	uid, gid := effectivePanePolicyOwner()
	dir := t.TempDir()
	policy := filepath.Join(dir, "pane-admin-uids")
	require.NoError(t, os.WriteFile(policy, []byte("7\n42\n"), 0o600))
	uids, err := loadPaneAdminUIDsFor(policy, uid, gid, false)
	require.NoError(t, err)
	require.Equal(t, []uint32{7, 42}, uids)

	require.NoError(t, os.Chmod(policy, 0o620))
	_, err = loadPaneAdminUIDsFor(policy, uid, gid, false)
	require.ErrorIs(t, err, ErrPaneOperationPolicyInvalid)

	target := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(target, []byte("7\n"), 0o600))
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(target, link))
	_, err = loadPaneAdminUIDsFor(link, uid, gid, false)
	require.Error(t, err)
}
