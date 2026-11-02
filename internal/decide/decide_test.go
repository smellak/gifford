package decide

import "testing"

// freeGuard is a guard snapshot that passes every guard.
func freeGuard() Guard {
	return Guard{
		DetectorCertain: true,
		Busy:            false,
		BoxBusy:         false,
		HumanActive:     false,
		RegistryValid:   true,
		IsCLI:           true,
	}
}

func TestDecideRows(t *testing.T) {
	good := freeGuard()
	cases := []struct {
		name string
		c    Control
		g    Guard
		free bool
		want Decision
	}{
		{"controls hold", ControlHold, good, true, Decision{Hold, ReasonControlsHold}},
		{"controls deny", ControlDeny, good, true, Decision{Drop, ReasonControlsDeny}},
		{"controls changed", ControlChanged, good, true, Decision{Drop, ReasonControlsChanged}},
		{"deny beats busy guards", ControlDeny, Guard{Busy: true}, true, Decision{Drop, ReasonControlsDeny}},
		{"detector unknown", ControlAllow, Guard{DetectorCertain: false}, true, Decision{Uncertain, ReasonDetectorUnknown}},
		{"unknown beats busy", ControlAllow, Guard{Busy: true}, true, Decision{Uncertain, ReasonDetectorUnknown}},
		{"agent busy", ControlAllow, Guard{DetectorCertain: true, Busy: true}, true, Decision{Hold, ReasonAgentBusy}},
		{"box busy", ControlAllow, func() Guard { g := freeGuard(); g.BoxBusy = true; return g }(), true, Decision{Hold, ReasonBoxBusy}},
		{"human active", ControlAllow, func() Guard { g := freeGuard(); g.HumanActive = true; return g }(), true, Decision{Hold, ReasonHumanActive}},
		{"registry invalid", ControlAllow, func() Guard { g := freeGuard(); g.RegistryValid = false; return g }(), true, Decision{Hold, ReasonRegistryInvalid}},
		{"not cli", ControlAllow, func() Guard { g := freeGuard(); g.IsCLI = false; return g }(), true, Decision{Hold, ReasonNotCLI}},
		{"state not free", ControlAllow, good, false, Decision{Hold, ReasonAgentBusy}},
		{"agent free notifies", ControlAllow, good, true, Decision{Notify, ReasonAgentFree}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Decide(tc.c, tc.g, tc.free); got != tc.want {
				t.Fatalf("Decide() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
