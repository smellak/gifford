package protocol

func v111Enums() []enumCase {
	return []enumCase{
		{
			zero: BridgeReminderCancelReason(""),
			check: func(s string) bool {
				return BridgeReminderCancelReason(s).Valid()
			},
			values: []string{
				string(BridgeReminderCancelExpectationResolved),
				string(BridgeReminderCancelModeSwitch),
				string(BridgeReminderCancelRollback),
			},
		},
		{
			zero: BridgeReminderTerminalState(""),
			check: func(s string) bool {
				return BridgeReminderTerminalState(s).Valid()
			},
			values: []string{
				string(BridgeReminderTerminalDelivered),
				string(BridgeReminderTerminalExpired),
				string(BridgeReminderTerminalCancelled),
				string(BridgeReminderTerminalUncertain),
			},
		},
		{
			zero: BridgeReminderDisposition(""),
			check: func(s string) bool {
				return BridgeReminderDisposition(s).Valid()
			},
			values: []string{
				string(BridgeReminderDispositionCreated),
				string(BridgeReminderDispositionDuplicate),
			},
		},
	}
}
