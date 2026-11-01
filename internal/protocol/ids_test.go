package protocol

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// patternCase binds one pattern type to sample values.
type patternCase struct {
	pattern string
	valid   []string
	invalid []string
	check   func(string) bool
}

// Sample identifiers shared by the pattern tables.
const (
	sampleULID  = "01JB0000000000000000000000"
	sampleAgent = "agent-1"
	sampleBogus = "bogus"
)

func v1Patterns() []patternCase {
	zeros := "0000000000000000000000000000000000000000000000000000000000000000"
	return []patternCase{
		{pattern: AgentID("").JSONPattern(), valid: []string{sampleAgent, "a", "x.y_z-0"}, invalid: []string{"", "Agent-1", "-x", "a/b"}, check: func(s string) bool { return AgentID(s).Valid() }},
		{pattern: NodeID("").JSONPattern(), valid: []string{testNode, "n"}, invalid: []string{"", "Node"}, check: func(s string) bool { return NodeID(s).Valid() }},
		{pattern: ULID("").JSONPattern(), valid: []string{sampleULID}, invalid: []string{"", "01jb0000000000000000000000", "01JB000000000000000000000", "01IB0000000000000000000000"}, check: func(s string) bool { return ULID(s).Valid() }},
		{pattern: ChannelDecisionID("").JSONPattern(), valid: []string{"chd_" + sampleULID}, invalid: []string{"", sampleULID, "chd_x"}, check: func(s string) bool { return ChannelDecisionID(s).Valid() }},
		{pattern: LoopID("").JSONPattern(), valid: []string{"loop_" + sampleULID}, invalid: []string{"", "loop_x"}, check: func(s string) bool { return LoopID(s).Valid() }},
		{pattern: ThreadRef("").JSONPattern(), valid: []string{"thr_" + sampleULID}, invalid: []string{"", "thr_x"}, check: func(s string) bool { return ThreadRef(s).Valid() }},
		{pattern: ThreadID("").JSONPattern(), valid: []string{"thread-1", "T"}, invalid: []string{"", "-x"}, check: func(s string) bool { return ThreadID(s).Valid() }},
		{pattern: OpaqueID("").JSONPattern(), valid: []string{"sess-1", "a:b@c"}, invalid: []string{"", "-x", "a b"}, check: func(s string) bool { return OpaqueID(s).Valid() }},
		{pattern: SHA256Hex("").JSONPattern(), valid: []string{zeros}, invalid: []string{"", zeros[:63], zeros + "0", "g" + zeros[1:]}, check: func(s string) bool { return SHA256Hex(s).Valid() }},
		{pattern: Digest("").JSONPattern(), valid: []string{"sha256:" + zeros}, invalid: []string{"", zeros, "SHA256:" + zeros}, check: func(s string) bool { return Digest(s).Valid() }},
		{pattern: SigningKeyID("").JSONPattern(), valid: []string{"ed25519:" + sampleAgent + ":v1"}, invalid: []string{"", "ed25519:" + sampleAgent + ":v0", "x25519:" + sampleAgent + ":v1", "ed25519:agent-1:1"}, check: func(s string) bool { return SigningKeyID(s).Valid() }},
		{pattern: EncryptionKeyID("").JSONPattern(), valid: []string{"x25519:" + sampleAgent + ":v12"}, invalid: []string{"", "x25519:" + sampleAgent + ":v0"}, check: func(s string) bool { return EncryptionKeyID(s).Valid() }},
		{pattern: BlobRef("").JSONPattern(), valid: []string{"blob://capture/cap-1/0001"}, invalid: []string{"", "blob://", "blob://a b"}, check: func(s string) bool { return BlobRef(s).Valid() }},
		{pattern: TmuxPaneID("").JSONPattern(), valid: []string{"%1", "%123"}, invalid: []string{"", "1", "%"}, check: func(s string) bool { return TmuxPaneID(s).Valid() }},
		{pattern: UsageRatio("").JSONPattern(), valid: []string{"7/12", "13/12"}, invalid: []string{"", "7", "7/"}, check: func(s string) bool { return UsageRatio(s).Valid() }},
		{pattern: NoticeLine("").JSONPattern(), valid: []string{"[GIFFORD] 1 pending message"}, invalid: []string{"", "a\nb"}, check: func(s string) bool { return NoticeLine(s).Valid() }},
		{pattern: EventType("").JSONPattern(), valid: []string{"capture.chunk", "control.pause"}, invalid: []string{"", "Capture.chunk", "capture.Chunk", "capture.", "nope"}, check: func(s string) bool { return EventType(s).Valid() }},
		{pattern: ControlID("").JSONPattern(), valid: []string{"ctl_" + sampleULID}, invalid: []string{"", "ctl_x"}, check: func(s string) bool { return ControlID(s).Valid() }},
		{pattern: ApprovalID("").JSONPattern(), valid: []string{"apr_" + sampleULID}, invalid: []string{"", "apr_x"}, check: func(s string) bool { return ApprovalID(s).Valid() }},
		{pattern: ApprovalOption("").JSONPattern(), valid: []string{"approve_once", "approve_thread:10m", "set_link:allow", "reject", "extend:+10", "set_mode:step", "steps:3", "resume", "stop", "deliver_now", "hold"}, invalid: []string{"", "approve_thread:0m", "extend:10", "steps:0", sampleBogus}, check: func(s string) bool { return ApprovalOption(s).Valid() }},
	}
}

// TestPatterns checks that every Go checker agrees with its compiled
// JSONPattern over the sample tables.
func TestPatterns(t *testing.T) {
	t.Parallel()

	for _, pc := range v1Patterns() {
		re := regexp.MustCompile(pc.pattern)
		for _, s := range pc.valid {
			require.True(t, re.MatchString(s), "pattern rejects %q", s)
			require.True(t, pc.check(s), "Go checker rejects %q", s)
		}
		for _, s := range pc.invalid {
			require.False(t, re.MatchString(s), "pattern accepts %q", s)
			require.False(t, pc.check(s), "Go checker accepts %q", s)
		}
	}
}
