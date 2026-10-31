package analyst

import (
	"regexp"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

// Screen features of analyze_agent_state.v2. They read only the analyze
// input every caller already sends: the screen lines after redaction and
// masking, the output counts and the harness. The live Analyst and
// `gifford eval predict` both render the prompt through the gateway, so
// both compute them with this code. A feature is evidence for the model,
// never a verdict.
//
// The patterns are texts of each harness's own interface, in the version
// its profile measured. They belong to the prompt version: a changed
// pattern changes the prompt text, so it is a new version, never an edit.

// Values of a yes-or-no feature.
const (
	featureYes = "yes"
	featureNo  = "no"
)

// Values of the busy feature.
const (
	busyRunningTool = "running_tool"
	busyThinking    = "thinking"
)

// Values of the output feature.
const (
	outputStreaming = "streaming"
	outputQuiet     = "quiet"
)

// inputMarker is the row the live session writes in place of each row of
// the measured input box. Dataset windows never hold it.
const inputMarker = "[input]"

// analyzeFeatures are the features of one analyze input, as the v2 user
// message writes them.
type analyzeFeatures struct {
	ApprovalMenu, QuestionPending, ErrorVisible, Busy, Output string
}

// harnessScreen holds the patterns of one harness's screen.
type harnessScreen struct {
	// menu matches an approval or permission menu that waits for a choice.
	menu *regexp.Regexp
	// busy matches the harness's working indicator; tool, with it, a
	// command or tool running.
	busy, tool *regexp.Regexp
	// errorLine matches an error the agent does not get past.
	errorLine *regexp.Regexp
	// lastMessage returns the agent's last line of text, false when the
	// screen does not show where the message ends.
	lastMessage func(lines []string) (string, bool)
}

// harnessScreens are the harnesses with patterns of their own.
var harnessScreens = map[protocol.Harness]harnessScreen{
	protocol.HarnessMuseCode: {
		menu:        regexp.MustCompile(`Would you like to `),
		busy:        regexp.MustCompile(`esc to interrupt`),
		tool:        regexp.MustCompile(`Running command .*running \(`),
		errorLine:   regexp.MustCompile(`model failed: `),
		lastMessage: museLastMessage,
	},
	protocol.HarnessOpencode: {
		menu:        regexp.MustCompile(`Permission required`),
		busy:        regexp.MustCompile(`esc interrupt`),
		tool:        regexp.MustCompile(`(?m)^\s*┃?\s*[\x{2800}-\x{28FF}] \S`),
		errorLine:   regexp.MustCompile(`Cannot connect to API`),
		lastMessage: opencodeLastMessage,
	},
}

// The patterns of a harness without its own. They can only say yes: a
// screen they do not match may still show a menu or an error.
var (
	genericMenu  = regexp.MustCompile(`(?i)\[y/n\]|\(y/n\)`)
	genericError = regexp.MustCompile(`(?m)^\s*Error:|Traceback \(most recent call last\)`)
)

// features computes the features of one analyze input.
func features(in analyzeInput) analyzeFeatures {
	screen := strings.Join(in.ScreenLines, "\n")
	out := analyzeFeatures{Output: outputActivity(in.OutputBytes)}
	h, ok := harnessScreens[in.Harness]
	if !ok {
		out.ApprovalMenu = yesOrUnknown(genericMenu.MatchString(screen))
		out.ErrorVisible = yesOrUnknown(genericError.MatchString(screen))
		out.QuestionPending, out.Busy = unknownValue, unknownValue
		return out
	}
	out.ApprovalMenu = yesNo(h.menu.MatchString(screen))
	out.ErrorVisible = yesNo(h.errorLine.MatchString(screen))
	switch {
	case !h.busy.MatchString(screen):
		out.Busy = noneValue
	case h.tool.MatchString(screen):
		out.Busy = busyRunningTool
	default:
		out.Busy = busyThinking
	}
	out.QuestionPending = unknownValue
	if last, found := h.lastMessage(in.ScreenLines); found {
		out.QuestionPending = yesNo(strings.HasSuffix(last, "?"))
	}
	return out
}

// museLastMessage reads Muse Code's screen: its input box, or a menu in
// its place, sits between the last two rules, and the agent's message
// ends above them, past its "◆ Worked for" line.
func museLastMessage(lines []string) (string, bool) {
	var rules []int
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "────") {
			rules = append(rules, i)
		}
	}
	if len(rules) < 2 {
		return "", false
	}
	return lastTextAbove(lines, rules[len(rules)-2], "◆ Worked for")
}

// opencodeLastMessage reads OpenCode's screen: the agent's last finished
// message ends above its footer, "▣ Build · …".
func opencodeLastMessage(lines []string) (string, bool) {
	end := -1
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "▣ ") {
			end = i
		}
	}
	if end < 0 {
		return "", false
	}
	return lastTextAbove(lines, end, "")
}

// lastTextAbove returns the last line of text above row end, without the
// frame's bars. It skips the live input marker and, if skip is not empty,
// the lines that start with skip.
func lastTextAbove(lines []string, end int, skip string) (string, bool) {
	for i := end - 1; i >= 0; i-- {
		t := strings.TrimSpace(strings.Trim(strings.TrimSpace(lines[i]), "┃"))
		if t == "" || t == inputMarker || (skip != "" && strings.HasPrefix(t, skip)) {
			continue
		}
		return t, true
	}
	return "", false
}

// outputActivity reads the output counts: bytes in the last half second
// are streaming, none are quiet. A longer span would not do: an idle Muse
// Code screen still writes every few seconds.
func outputActivity(b outputBytes) string {
	if b.Last500ms > 0 {
		return outputStreaming
	}
	return outputQuiet
}

func yesNo(b bool) string {
	if b {
		return featureYes
	}
	return featureNo
}

func yesOrUnknown(b bool) string {
	if b {
		return featureYes
	}
	return unknownValue
}
