package protocol

func v14Enums() []enumCase {
	return []enumCase{
		{
			zero:  EmitEventStatus(""),
			check: func(s string) bool { return EmitEventStatus(s).Valid() },
			values: []string{
				string(EmitEventStatusStored),
				string(EmitEventStatusAlreadyStored),
				string(EmitEventStatusRejected),
			},
		},
		{
			zero:  EmitEventRejectReason(""),
			check: func(s string) bool { return EmitEventRejectReason(s).Valid() },
			values: []string{
				string(EmitEventRejectReasonNotAllowed),
				string(EmitEventRejectReasonConflict),
			},
		},
	}
}
