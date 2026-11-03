//go:build !race

package controlapi

import "time"

const (
	incidentRaceEnabled = false
	incidentRaceBudget  = time.Duration(0)
)
