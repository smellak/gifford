package vt

import (
	"testing"

	ansi "github.com/charmbracelet/x/ansi"
	ansiparser "github.com/charmbracelet/x/ansi/parser"
	"github.com/stretchr/testify/require"
)

func TestBoundaryTrackerMatchesANSIParser(t *testing.T) {
	t.Parallel()

	states := []struct {
		name   string
		prefix []byte
		state  parseState
	}{
		{name: "ground", state: stateGround},
		{name: "escape", prefix: []byte{0x1b}, state: stateEscape},
		{name: "escape-intermediate", prefix: []byte{0x1b, ' '}, state: stateEscapeIntermediate},
		{name: "csi-entry", prefix: []byte{0x1b, '['}, state: stateCSIEntry},
		{name: "csi-param", prefix: []byte{0x1b, '[', '1'}, state: stateCSIParam},
		{name: "csi-intermediate", prefix: []byte{0x1b, '[', ' '}, state: stateCSIIntermediate},
		{name: "dcs-entry", prefix: []byte{0x1b, 'P'}, state: stateDCSEntry},
		{name: "dcs-param", prefix: []byte{0x1b, 'P', '1'}, state: stateDCSParam},
		{name: "dcs-intermediate", prefix: []byte{0x1b, 'P', ' '}, state: stateDCSIntermediate},
		{name: "dcs-data", prefix: []byte{0x1b, 'P', 'q'}, state: stateDCSData},
		{name: "osc-string", prefix: []byte{0x1b, ']'}, state: stateOSC},
		{name: "sos", prefix: []byte{0x1b, 'X'}, state: stateSOS},
		{name: "pm", prefix: []byte{0x1b, '^'}, state: statePM},
		{name: "apc", prefix: []byte{0x1b, '_'}, state: stateAPC},
		{name: "utf8-two-byte", prefix: []byte{0xc2}, state: stateUTF8One},
		{name: "utf8-three-byte", prefix: []byte{0xe0}, state: stateUTF8Two},
		{name: "utf8-four-byte", prefix: []byte{0xf0}, state: stateUTF8Three},
	}
	for _, test := range states {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parser := ansi.NewParser()
			for value := range 256 {
				b := byte(value)
				trackerState := stateGround
				parser.Reset()
				for _, prefixByte := range test.prefix {
					trackerState, _, _ = advance(trackerState, prefixByte)
					parser.Advance(prefixByte)
				}
				require.Equal(t, test.state, trackerState, "prefix % x", test.prefix)
				require.Equal(t, ansiState(test.state), parser.State(), "parser prefix % x", test.prefix)

				trackerState, _, _ = advance(trackerState, b)
				parser.Advance(b)
				require.Equal(t, parser.State(), ansiState(trackerState), "byte 0x%02x", b)
			}
		})
	}
}

func ansiState(state parseState) ansiparser.State {
	switch state {
	case stateGround:
		return ansiparser.GroundState
	case stateEscape:
		return ansiparser.EscapeState
	case stateEscapeIntermediate:
		return ansiparser.EscapeIntermediateState
	case stateCSIEntry:
		return ansiparser.CsiEntryState
	case stateCSIParam:
		return ansiparser.CsiParamState
	case stateCSIIntermediate:
		return ansiparser.CsiIntermediateState
	case stateDCSEntry:
		return ansiparser.DcsEntryState
	case stateDCSParam:
		return ansiparser.DcsParamState
	case stateDCSIntermediate:
		return ansiparser.DcsIntermediateState
	case stateDCSData:
		return ansiparser.DcsStringState
	case stateOSC:
		return ansiparser.OscStringState
	case stateSOS:
		return ansiparser.SosStringState
	case statePM:
		return ansiparser.PmStringState
	case stateAPC:
		return ansiparser.ApcStringState
	case stateUTF8One, stateUTF8Two, stateUTF8Three:
		return ansiparser.Utf8State
	default:
		panic("unknown boundary tracker state")
	}
}
