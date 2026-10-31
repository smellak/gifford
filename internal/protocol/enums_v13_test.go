package protocol

func v13Enums() []enumCase {
	return []enumCase{
		{
			zero:  StateObservationScreen(""),
			check: func(s string) bool { return StateObservationScreen(s).Valid() },
			values: []string{
				string(StateObservationScreenAvailable),
				string(StateObservationScreenWithheld),
			},
		},
	}
}
