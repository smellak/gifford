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

func (denyProvisionAuthorizer) AuthorizePane(PanePeer) error {
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
		Binding:   PanelBinding{OwnerUID: ownerUID, Socket: provisionTestSocket, PaneID: provisionTestPaneID},
		PanePID:   424242,
		PaneStart: 987654,
		BootID:    provisionTestBootID,
		Host:      provisionTestHost,
		BindUID:   1113,
		Owner:     provisionTestOwner,
		Policy:    RecordPolicy{PeerOwner: provisionTestOwner, PeerWriters: []string{provisionTestWriter}},
		Clock:     fixedClock{moment: contractProvisionTime()},
		Peer: PanePeer{
			PID: 424242, UID: 1113, Verified: true,
			Actor: provisionTestActor,
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
	if authorizer.lastPeer.Actor != provisionTestActor {
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
	for _, file := range []struct {
		path string
		mode uint32
		acl  fileACLKind
	}{
		{result.State, provisionFileMode, fileACLWrite},
		{result.Register, provisionRegisterMode, fileACLRead},
	} {
		fd := openPathFile(t, file.path)
		if err := validateCooperativeFile(fd, file.mode, recordSize, 0, file.acl, ownerUID); err != nil {
			t.Fatalf("%s: %v", file.path, err)
		}
	}
	stateRaw, err := os.ReadFile(result.State)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	state, err := decodeStateRecord(provisionTestPolicy(t), stateRaw)
	if err != nil {
		t.Fatalf("decode installed state: %v", err)
	}
	if state.Phase != phaseIdle || state.Writer != provisionNodeWriter || state.TxID != "-" ||
		state.Owner != provisionTestOwner || state.Gen != 0 || state.Epoch != 0 {
		t.Fatalf("installed state is not the provision vector: %+v", state)
	}
	registerRaw, err := os.ReadFile(result.Register)
	if err != nil {
		t.Fatalf("read register: %v", err)
	}
	register, err := decodeRegisterRecord(provisionTestPolicy(t), registerRaw)
	if err != nil {
		t.Fatalf("decode installed register: %v", err)
	}
	if register.Owner != provisionTestOwner || register.Epoch != 0 || register.Pending != nil {
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

	parts, err := deriveParts(spec.Binding)
	if err != nil {
		t.Fatalf("derive parts: %v", err)
	}
	wantKey := parts.owner + "/" + parts.hash + "/" + parts.file
	if result.Publication.PanelKey != wantKey {
		t.Fatalf("panel key = %q, want %q", result.Publication.PanelKey, wantKey)
	}
	if result.Publication.OwnerSeq != 1 || result.Publication.EnrolledSeq != 1 {
		t.Fatalf("publication seqs = %+v, want 1/1", result.Publication)
	}
	if !result.Publication.PublishedAt.Equal(contractProvisionTime()) {
		t.Fatalf("published at = %v", result.Publication.PublishedAt)
	}
	assertOwnerIndex(t, result.OwnerDir+"/index", ownerUID, 1, []panelIndexEntry{
		{Pane: 12, SocketSHA: parts.hash},
	})
	assertEnrolledIndex(t, root+"/enrolled-uids", 1, []uint32{ownerUID})
}

// TestProvisionRepeatKeepsIndexes requires a repeat provision to leave the
// published indexes untouched with the same sequences.
func TestProvisionRepeatKeepsIndexes(t *testing.T) {
	root := provisionTestRoot(t)
	spec := provisionTestSpec(root, &allowAuthorizer{})

	first, err := Provision(spec)
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}
	ownerRaw, err := os.ReadFile(first.OwnerDir + "/index")
	if err != nil {
		t.Fatalf("read owner index: %v", err)
	}
	enrolledRaw, err := os.ReadFile(root + "/enrolled-uids")
	if err != nil {
		t.Fatalf("read enrolled index: %v", err)
	}
	second, err := Provision(spec)
	if err != nil {
		t.Fatalf("repeat provision: %v", err)
	}
	if second.Publication.OwnerSeq != 1 || second.Publication.EnrolledSeq != 1 {
		t.Fatalf("repeat publication seqs = %+v, want 1/1", second.Publication)
	}
	ownerAgain, err := os.ReadFile(second.OwnerDir + "/index")
	if err != nil {
		t.Fatalf("read owner index: %v", err)
	}
	enrolledAgain, err := os.ReadFile(root + "/enrolled-uids")
	if err != nil {
		t.Fatalf("read enrolled index: %v", err)
	}
	if string(ownerAgain) != string(ownerRaw) || string(enrolledAgain) != string(enrolledRaw) {
		t.Fatal("repeat provision rewrote the published indexes")
	}
}

// TestProvisionSecondPanelExtendsIndexes requires a second panel of the same
// owner to extend the owner index without touching the enrolled set.
func TestProvisionSecondPanelExtendsIndexes(t *testing.T) {
	root := provisionTestRoot(t)
	authorizer := &allowAuthorizer{}
	first := provisionTestSpec(root, authorizer)
	if _, err := Provision(first); err != nil {
		t.Fatalf("first provision: %v", err)
	}
	second := first
	second.Binding.PaneID = "%13"
	second.PanePID++
	result, err := Provision(second)
	if err != nil {
		t.Fatalf("second provision: %v", err)
	}
	if !result.Created {
		t.Fatal("second panel provision did not report Created")
	}
	if result.Publication.OwnerSeq != 2 {
		t.Fatalf("owner seq = %d, want 2", result.Publication.OwnerSeq)
	}
	if result.Publication.EnrolledSeq != 1 {
		t.Fatalf("enrolled seq = %d, want 1", result.Publication.EnrolledSeq)
	}
	parts, err := deriveParts(second.Binding)
	if err != nil {
		t.Fatalf("derive parts: %v", err)
	}
	firstParts, err := deriveParts(first.Binding)
	if err != nil {
		t.Fatalf("derive parts: %v", err)
	}
	assertOwnerIndex(t, result.OwnerDir+"/index", first.Binding.OwnerUID, 2, []panelIndexEntry{
		{Pane: 12, SocketSHA: firstParts.hash},
		{Pane: 13, SocketSHA: parts.hash},
	})
}

// TestProvisionRecoversMissingIndexes requires a repeat provision to
// republish indexes lost after the panel files, completing the Alta.
func TestProvisionRecoversMissingIndexes(t *testing.T) {
	root := provisionTestRoot(t)
	spec := provisionTestSpec(root, &allowAuthorizer{})
	first, err := Provision(spec)
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}
	if err := os.Remove(first.OwnerDir + "/index"); err != nil {
		t.Fatalf("remove owner index: %v", err)
	}
	if err := os.Remove(root + "/enrolled-uids"); err != nil {
		t.Fatalf("remove enrolled index: %v", err)
	}
	second, err := Provision(spec)
	if err != nil {
		t.Fatalf("repeat provision: %v", err)
	}
	if second.Created {
		t.Fatal("repeat provision reported Created for existing panel files")
	}
	if second.Publication.OwnerSeq != 1 || second.Publication.EnrolledSeq != 1 {
		t.Fatalf("recovered publication seqs = %+v, want 1/1", second.Publication)
	}
	parts, err := deriveParts(spec.Binding)
	if err != nil {
		t.Fatalf("derive parts: %v", err)
	}
	assertOwnerIndex(t, second.OwnerDir+"/index", spec.Binding.OwnerUID, 1, []panelIndexEntry{
		{Pane: 12, SocketSHA: parts.hash},
	})
	assertEnrolledIndex(t, root+"/enrolled-uids", 1, []uint32{spec.Binding.OwnerUID})
}

func assertOwnerIndex(t *testing.T, path string, ownerUID uint32, seq uint64, entries []panelIndexEntry) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	gotSeq, gotEntries, err := decodePanelIndex(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if gotSeq != seq {
		t.Fatalf("%s seq = %d, want %d", path, gotSeq, seq)
	}
	if len(gotEntries) != len(entries) {
		t.Fatalf("%s entries = %v, want %v", path, gotEntries, entries)
	}
	for index := range entries {
		if gotEntries[index] != entries[index] {
			t.Fatalf("%s entries = %v, want %v", path, gotEntries, entries)
		}
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if stat.Mode().Perm() != 0o640 {
		t.Fatalf("%s mode = %v, want 0640", path, stat.Mode())
	}
	fd := openPathFile(t, path)
	if err := validateCooperativeFile(fd, ownerIndexMode, -1, maxPanelIndexSize, fileACLRead, ownerUID); err != nil {
		t.Fatalf("%s shape: %v", path, err)
	}
}

func assertEnrolledIndex(t *testing.T, path string, seq uint64, entries []uint32) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	gotSeq, gotEntries, err := decodeEnrolledIndex(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if gotSeq != seq {
		t.Fatalf("%s seq = %d, want %d", path, gotSeq, seq)
	}
	if len(gotEntries) != len(entries) {
		t.Fatalf("%s entries = %v, want %v", path, gotEntries, entries)
	}
	for index := range entries {
		if gotEntries[index] != entries[index] {
			t.Fatalf("%s entries = %v, want %v", path, gotEntries, entries)
		}
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if stat.Mode().Perm() != 0o644 {
		t.Fatalf("%s mode = %v, want 0644", path, stat.Mode())
	}
	fd := openPathFile(t, path)
	if err := validateNoFileACL(fd); err != nil {
		t.Fatalf("%s acl: %v", path, err)
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
	other.Owner = provisionTestAltOwner
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
