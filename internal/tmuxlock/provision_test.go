package tmuxlock

import (
	"strings"
	"testing"
	"time"
)

// Shared provision test fixtures. goconst pins repeated literals, so the
// contract values used across both provision test files live here once.
const (
	provisionTestPaneID   = "%12"
	provisionTestSocket   = "/tmp/tmux-1113/default"
	provisionTestBootID   = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
	provisionTestHost     = "testhost01"
	provisionTestOwner    = "capycom"
	provisionTestAltOwner = "gifford"
	provisionTestActor    = "uid:1113:pid:424242"
	provisionTestWriter   = "notifier"
	provisionNodeWriter   = "provision"
)

// provisionTestPolicy is the deployment half the test vectors assume: the
// peer owner the vectors name, plus one deployment writer.
func provisionTestPolicy(t *testing.T) validatedRecordPolicy {
	t.Helper()
	policy, err := validateRecordPolicy(RecordPolicy{
		PeerOwner:   provisionTestOwner,
		PeerWriters: []string{provisionTestWriter},
	})
	if err != nil {
		t.Fatalf("test policy: %v", err)
	}
	return policy
}

// contractProvisionStateBody is the provision vector body from the frozen
// panel contract (enmienda 1 vector plus the mandatory "txid":"-"): every
// field at its provision value, canonical JSON. The host reads testhost01
// instead of the contract illustration: the repo pre-push gate rejects that
// literal, and the substitution keeps the golden byte-exact elsewhere.
const contractProvisionStateBody = `{"attempt":"-","boot_id":"0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0","child_pid":0,"child_start":"0","epoch":0,"gen":0,"host":"testhost01","op":"-","owner":"capycom","owner_gen":0,"pane":12,"pane_pid":424242,"pane_start":"987654","phase":"IDLE","result":"-","socket_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","step":0,"ts":"2026-11-02T21:20:00.000000000Z","txid":"-","uid":1113,"v":3,"writer":"provision","writer_pid":0,"writer_start":"0"}`

// contractProvisionRegisterBody is the register body provision installs for
// the same panel: epoch zero, no pending operation.
const contractProvisionRegisterBody = `{"binding":{"boot_id":"0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0","host":"testhost01","pane":12,"pane_pid":424242,"pane_start":"987654","socket_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","uid":1113},"epoch":0,"incorporated":1,"owner":"capycom","owner_gen":0,"pending":"-","v":1}`

func contractProvisionBinding() recordBinding {
	return recordBinding{
		BootID: provisionTestBootID, Host: provisionTestHost,
		Pane: 12, PanePID: 424242, PaneStart: "987654",
		SocketSHA: strings.Repeat("a", 64), UID: 1113,
	}
}

func contractProvisionTime() time.Time {
	moment, err := time.Parse("2006-01-02T15:04:05.000000000Z", "2026-11-02T21:20:00.000000000Z")
	if err != nil {
		panic("tmuxlock test: bad contract timestamp")
	}
	return moment
}

// TestProvisionStateMatchesContractVector requires the provision state record
// to be byte-identical to the contract vector body inside the 1024-byte
// disposition.
func TestProvisionStateMatchesContractVector(t *testing.T) {
	policy := provisionTestPolicy(t)
	raw, err := encodeStateRecord(policy, initialState(contractProvisionBinding(), provisionTestOwner, contractProvisionTime()))
	if err != nil {
		t.Fatalf("encode provision state: %v", err)
	}
	if len(raw) != recordSize {
		t.Fatalf("provision state size = %d, want %d", len(raw), recordSize)
	}
	if string(raw[:8]) != stateMagic {
		t.Fatalf("provision state magic = %q", raw[:8])
	}
	if body := string(raw[8 : 8+len(contractProvisionStateBody)]); body != contractProvisionStateBody {
		t.Fatalf("provision state body mismatch:\n got %s\nwant %s", body, contractProvisionStateBody)
	}
	rest := raw[8+len(contractProvisionStateBody) : recordBodyEnd]
	for index, char := range rest {
		if char != ' ' {
			t.Fatalf("provision state padding byte %d = %#02x", index, char)
		}
	}
	if tail := string(raw[recordBodyEnd:]); len(tail) != 72 || tail[64:] != "       \n" {
		t.Fatalf("provision state trailer = %q", tail)
	}
	if _, err := decodeStateRecord(policy, raw); err != nil {
		t.Fatalf("provision state does not strict-decode: %v", err)
	}
}

// TestProvisionRegisterMatchesContractBody requires the provision register
// record to carry the requested binding with epoch zero and no pending
// operation.
func TestProvisionRegisterMatchesContractBody(t *testing.T) {
	policy := provisionTestPolicy(t)
	raw, err := encodeRegisterRecord(policy, registerRecord{Binding: contractProvisionBinding(), Owner: provisionTestOwner})
	if err != nil {
		t.Fatalf("encode provision register: %v", err)
	}
	if len(raw) != recordSize {
		t.Fatalf("provision register size = %d, want %d", len(raw), recordSize)
	}
	if string(raw[:8]) != registerMagic {
		t.Fatalf("provision register magic = %q", raw[:8])
	}
	if body := string(raw[8 : 8+len(contractProvisionRegisterBody)]); body != contractProvisionRegisterBody {
		t.Fatalf("provision register body mismatch:\n got %s\nwant %s", body, contractProvisionRegisterBody)
	}
	register, err := decodeRegisterRecord(policy, raw)
	if err != nil {
		t.Fatalf("provision register does not strict-decode: %v", err)
	}
	if register.Binding != contractProvisionBinding() || register.Owner != provisionTestOwner ||
		register.Epoch != 0 || register.OwnerGen != 0 || register.Pending != nil {
		t.Fatalf("provision register decoded = %+v", register)
	}
}

type denyAuthorizer struct{}

func (denyAuthorizer) AuthorizePane(PanePeer) error {
	return ErrInvalidBinding
}

// TestProvisionDeniesWithoutAuthorizerOrGrant requires provision to fail
// before any platform work when unauthenticated: nil authorizer and explicit
// deny both error out.
func TestProvisionDeniesWithoutAuthorizerOrGrant(t *testing.T) {
	spec := ProvisionSpec{Root: "/nonexistent-provision-root"}
	if _, err := Provision(spec); err == nil {
		t.Fatal("provision without authorizer succeeded")
	}
	spec.Authorizer = denyAuthorizer{}
	if _, err := Provision(spec); err == nil {
		t.Fatal("provision with denying authorizer succeeded")
	}
}

// TestValidateProvisionSpecTable pins the portable request checks.
func TestValidateProvisionSpecTable(t *testing.T) {
	base := ProvisionSpec{
		Root:      "/var/lib/gifford-tmux-locks-test",
		Binding:   PanelBinding{OwnerUID: 1113, Socket: provisionTestSocket, PaneID: provisionTestPaneID},
		PanePID:   424242,
		PaneStart: 987654,
		BootID:    provisionTestBootID,
		Host:      provisionTestHost,
		BindUID:   1113,
		Owner:     provisionTestOwner,
		Policy:    RecordPolicy{PeerOwner: provisionTestOwner, PeerWriters: []string{provisionTestWriter}},
	}
	cases := map[string]func(*ProvisionSpec){
		"empty root":       func(s *ProvisionSpec) { s.Root = "" },
		"relative root":    func(s *ProvisionSpec) { s.Root = "var/lib/x" },
		"unclean root":     func(s *ProvisionSpec) { s.Root = "/var/lib/../x" },
		"zero owner uid":   func(s *ProvisionSpec) { s.Binding.OwnerUID = 0 },
		"numeric owner":    func(s *ProvisionSpec) { s.Owner = "1113" },
		"empty owner":      func(s *ProvisionSpec) { s.Owner = "" },
		"capital owner":    func(s *ProvisionSpec) { s.Owner = "Capycom" },
		"bad pane id":      func(s *ProvisionSpec) { s.Binding.PaneID = "12" },
		"zero pane pid":    func(s *ProvisionSpec) { s.PanePID = 0 },
		"huge pane pid":    func(s *ProvisionSpec) { s.PanePID = maxRecordPID + 1 },
		"zero binding uid": func(s *ProvisionSpec) { s.BindUID = 0 },
		"zero pane start":  func(s *ProvisionSpec) { s.PaneStart = 0 },
		"negative start":   func(s *ProvisionSpec) { s.PaneStart = -1 },
		"bad boot id":      func(s *ProvisionSpec) { s.BootID = "not-a-boot-id" },
		"bad host":         func(s *ProvisionSpec) { s.Host = "TESTHOST01" },
		"bad policy":       func(s *ProvisionSpec) { s.Policy.PeerWriters = nil },
		"foreign owner":    func(s *ProvisionSpec) { s.Owner = "stranger" },
		"empty socket":     func(s *ProvisionSpec) { s.Binding.Socket = "" },
		"relative socket":  func(s *ProvisionSpec) { s.Binding.Socket = "tmp/x" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec := base
			mutate(&spec)
			if _, _, err := validateProvisionSpec(spec); err == nil {
				t.Fatalf("validateProvisionSpec accepted %s", name)
			}
		})
	}
	if _, _, err := validateProvisionSpec(base); err != nil {
		t.Fatalf("validateProvisionSpec rejected the valid spec: %v", err)
	}
}

// TestCheckPanelCoherenceTable pins the cross-record match provision demands.
func TestCheckPanelCoherenceTable(t *testing.T) {
	binding := contractProvisionBinding()
	content := panelContent{owner: provisionTestOwner, binding: binding}
	goodReg := registerRecord{Binding: binding, Owner: provisionTestOwner}
	state := initialState(binding, provisionTestOwner, contractProvisionTime())
	if err := checkPanelCoherence(content, goodReg, state); err != nil {
		t.Fatalf("coherent panel rejected: %v", err)
	}
	otherBinding := binding
	otherBinding.PanePID++
	otherState := initialState(otherBinding, provisionTestOwner, contractProvisionTime())
	if err := checkPanelCoherence(content, goodReg, otherState); err == nil {
		t.Fatal("binding mismatch between state and request accepted")
	}
	otherReg := registerRecord{Binding: otherBinding, Owner: provisionTestOwner}
	if err := checkPanelCoherence(content, otherReg, state); err == nil {
		t.Fatal("binding mismatch between register and request accepted")
	}
	wrongOwnerReg := registerRecord{Binding: binding, Owner: provisionTestAltOwner}
	if err := checkPanelCoherence(content, wrongOwnerReg, state); err == nil {
		t.Fatal("register owner mismatch accepted")
	}
	wrongOwnerState := initialState(binding, provisionTestAltOwner, contractProvisionTime())
	if err := checkPanelCoherence(content, goodReg, wrongOwnerState); err == nil {
		t.Fatal("state owner mismatch accepted")
	}
	advancedReg := registerRecord{Binding: binding, Owner: provisionTestOwner, Epoch: 3, OwnerGen: 1}
	advancedState := state
	advancedState.Epoch = 3
	advancedState.OwnerGen = 1
	if err := checkPanelCoherence(content, advancedReg, advancedState); err != nil {
		t.Fatalf("post-transfer coherence rejected: %v", err)
	}
	skewedState := state
	skewedState.Epoch = 4
	if err := checkPanelCoherence(content, advancedReg, skewedState); err == nil {
		t.Fatal("epoch skew between register and state accepted")
	}
}
