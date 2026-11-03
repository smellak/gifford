//go:build linux

package tmuxlock

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type allowAuthorizer struct {
	lastPeer PanePeer
	calls    int
}

func (a *allowAuthorizer) AuthorizePane(peer PanePeer) error {
	a.lastPeer = peer
	a.calls++
	return nil
}

type fixedClock struct {
	moment time.Time
}

func (c fixedClock) Now() time.Time { return c.moment }

type denyProvisionAuthorizer struct{}

func (denyProvisionAuthorizer) AuthorizePane(peer PanePeer) error {
	return ErrInvalidBinding
}

func provisionTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o711); err != nil {
		t.Fatalf("chmod provision test root: %v", err)
	}
	return root
}

func provisionTestSpec(root string, authorizer PaneAuthorizer) ProvisionSpec {
	ownerUID := uint32(1113)
	if ownerUID == effectiveUID() {
		ownerUID++
	}
	return ProvisionSpec{
		Root:      root,
		Binding:   PanelBinding{OwnerUID: ownerUID, Socket: "/tmp/tmux-1113/default", PaneID: "%12"},
		PanePID:   424242,
		PaneStart: 987654,
		BootID:    "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
		Host:      "testhost01",
		BindUID:   1113,
		Owner:     "capycom",
		Clock:     fixedClock{moment: contractProvisionTime()},
		Peer: PanePeer{
			PID: 424242, UID: 1113, Verified: true,
			Actor: "uid:1113:pid:424242",
		},
		Authorizer: authorizer,
	}
}

func openPathDir(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return fd
}

func openPathFile(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return fd
}

// TestProvisionCreatesPanel requires a fresh provision to install the whole
// panel with the exact contract shape, and requires the panel guard to open
// the installed panel afterwards.
func TestProvisionCreatesPanel(t *testing.T) {
	root := provisionTestRoot(t)
	authorizer := &allowAuthorizer{}
	spec := provisionTestSpec(root, authorizer)

	result, err := Provision(spec)
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if !result.Created {
		t.Fatal("fresh provision did not report Created")
	}
	if authorizer.calls != 1 {
		t.Fatalf("authorizer calls = %d, want 1", authorizer.calls)
	}
	if authorizer.lastPeer.Actor != "uid:1113:pid:424242" {
		t.Fatalf("authorizer did not receive the actor: %+v", authorizer.lastPeer)
	}

	ownerUID := spec.Binding.OwnerUID
	ownerFD := openPathDir(t, result.OwnerDir)
	if err := validateManagedDirectory(ownerFD, ownerUID, effectiveUID()); err != nil {
		t.Fatalf("owner dir: %v", err)
	}
	socketFD := openPathDir(t, result.SocketDir)
	if err := validateManagedDirectory(socketFD, ownerUID, effectiveUID()); err != nil {
		t.Fatalf("socket dir: %v", err)
	}
	lockFD := openPathFile(t, result.Lock)
	if err := validateLockFile(lockFD, ownerUID, effectiveUID()); err != nil {
		t.Fatalf("lock file: %v", err)
	}
	for _, path := range []string{result.State, result.Register} {
		fd := openPathFile(t, path)
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if err := validateProvisionedFileStat(stat, recordSize); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if err := validateFileACL(fd, ownerUID); err != nil {
			t.Fatalf("%s acl: %v", path, err)
		}
	}
	stateRaw, err := os.ReadFile(result.State)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	state, err := decodeStateRecord(stateRaw)
	if err != nil {
		t.Fatalf("decode installed state: %v", err)
	}
	if state.Phase != phaseIdle || state.Writer != "provision" || state.TxID != "-" ||
		state.Owner != "capycom" || state.Gen != 0 || state.Epoch != 0 {
		t.Fatalf("installed state is not the provision vector: %+v", state)
	}
	registerRaw, err := os.ReadFile(result.Register)
	if err != nil {
		t.Fatalf("read register: %v", err)
	}
	register, err := decodeRegisterRecord(registerRaw)
	if err != nil {
		t.Fatalf("decode installed register: %v", err)
	}
	if register.Owner != "capycom" || register.Epoch != 0 || register.Pending != nil {
		t.Fatalf("installed register is not the provision record: %+v", register)
	}
	indexStat, err := os.Stat(result.IndexLock)
	if err != nil {
		t.Fatalf("stat index lock: %v", err)
	}
	if indexStat.Mode().Perm() != 0o600 || indexStat.Size() != 0 {
		t.Fatalf("index lock = %v size %d", indexStat.Mode(), indexStat.Size())
	}
	indexFD := openPathFile(t, result.IndexLock)
	if err := validateNoFileACL(indexFD); err != nil {
		t.Fatalf("index lock acl: %v", err)
	}

	guard, outcome, err := acquireAt(t.Context(), root, spec.Binding, 0)
	if err != nil {
		t.Fatalf("guard does not open the provisioned panel: %v", err)
	}
	if outcome != LockAcquired || guard == nil {
		t.Fatalf("guard outcome = %v", outcome)
	}
	if err := guard.Close(); err != nil {
		t.Fatalf("guard close: %v", err)
	}
}

// TestProvisionRepeatValidates requires a second provision of the same owner
// and binding to succeed without creating anything.
func TestProvisionRepeatValidates(t *testing.T) {
	root := provisionTestRoot(t)
	spec := provisionTestSpec(root, &allowAuthorizer{})

	first, err := Provision(spec)
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}
	if !first.Created {
		t.Fatal("first provision did not report Created")
	}
	before, err := os.ReadFile(first.State)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	second, err := Provision(spec)
	if err != nil {
		t.Fatalf("repeat provision: %v", err)
	}
	if second.Created {
		t.Fatal("repeat provision reported Created")
	}
	after, err := os.ReadFile(second.State)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if string(after) != string(before) {
		t.Fatal("repeat provision modified the state record")
	}
}

// TestProvisionRejectsOwnerMismatch requires provision for a different owner
// over installed files to fail without touching them.
func TestProvisionRejectsOwnerMismatch(t *testing.T) {
	root := provisionTestRoot(t)
	spec := provisionTestSpec(root, &allowAuthorizer{})
	result, err := Provision(spec)
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}
	before := map[string][]byte{}
	for _, path := range []string{result.State, result.Register, result.Lock} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		before[path] = raw
	}
	other := spec
	other.Owner = "gifford"
	if _, err := Provision(other); err == nil {
		t.Fatal("provision with a different owner over installed files succeeded")
	}
	for path, want := range before {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if string(raw) != string(want) {
			t.Fatalf("failed provision modified %s", path)
		}
	}
}

// TestProvisionDenyLeavesNoTrace requires a denied provision to fail before
// creating any directory or file.
func TestProvisionDenyLeavesNoTrace(t *testing.T) {
	root := provisionTestRoot(t)
	spec := provisionTestSpec(root, denyProvisionAuthorizer{})
	if _, err := Provision(spec); err == nil {
		t.Fatal("denied provision succeeded")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	// Only the index lock may exist: deny happens before any platform work,
	// so even that must be absent.
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, filepath.Base(entry.Name()))
		}
		t.Fatalf("denied provision created %v", names)
	}
}
