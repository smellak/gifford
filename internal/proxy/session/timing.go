package session

import "time"

const (
	// OutputQuietPeriod is the calm required after the harness's last
	// output before a delivery may prepare or commit. Offline evaluation
	// replays this exact guard threshold when measuring false SAFE.
	OutputQuietPeriod = 300 * time.Millisecond

	// Keep the writer and its package tests phrased in the local guard's
	// terms while exposing one source of truth to the evaluator.
	outputQuietPeriod = OutputQuietPeriod
)
