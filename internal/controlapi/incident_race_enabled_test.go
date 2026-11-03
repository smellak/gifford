//go:build race

package controlapi

import "time"

const (
	incidentRaceEnabled = true
	incidentRaceBudget  = 10 * time.Second
)
