package protocol

func v12Enums() []enumCase {
	return []enumCase{
		{zero: NotificationClass(""), check: func(s string) bool { return NotificationClass(s).Valid() }, values: []string{string(NotificationClassMessage), string(NotificationClassApproval)}},
		{zero: NotificationReason(""), check: func(s string) bool { return NotificationReason(s).Valid() }, values: []string{string(NotificationReasonBaseline), string(NotificationReasonMessageAvailable), string(NotificationReasonDisplayOnly), string(NotificationReasonChannelHeld), string(NotificationReasonChannelApprovalRequired), string(NotificationReasonApprovalPending)}},
		{zero: NotificationState(""), check: func(s string) bool { return NotificationState(s).Valid() }, values: []string{string(NotificationStateBaselined), string(NotificationStatePending), string(NotificationStateRendered)}},
		{zero: NotificationAdapter(""), check: func(s string) bool { return NotificationAdapter(s).Valid() }, values: []string{string(NotificationAdapterNone), string(NotificationAdapterTMUX)}},
		{zero: NotificationFailureReason(""), check: func(s string) bool { return NotificationFailureReason(s).Valid() }, values: []string{string(NotificationFailureAdapterDisabled), string(NotificationFailureAdapterUnavailable), string(NotificationFailureUnsupportedFlags), string(NotificationFailureClientUnavailable), string(NotificationFailureAdapterTimeout), string(NotificationFailureInvalidTemplate), string(NotificationFailureAdapterFailed)}},
	}
}
