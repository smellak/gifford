package eval

import (
	"math"
	"slices"

	"github.com/smellak/gifford/internal/dataset"
)

const (
	// DefaultTheta is θ when validation cannot set it.
	DefaultTheta = 0.97
	// MaxFalseSafe is the false SAFE rate θ allows on validation.
	MaxFalseSafe = 0.005
	// tMin and tMax bound the temperature search.
	tMin       = 0.05
	tMax       = 20.0
	gridPoints = 401
	refineIter = 100
	logFloor   = 1e-15
)

// scaled is temperature scaling: p_i ∝ exp(log p_i / T). An invalid line
// is UNKNOWN with certainty.
func scaled(p Prediction, t float64) [numLabels]float64 {
	var out [numLabels]float64
	if p.Invalid {
		out[idxG] = 1
		return out
	}
	maxLog := math.Inf(-1)
	var logs [numLabels]float64
	for i, v := range p.Probs {
		logs[i] = math.Inf(-1)
		if v > 0 {
			logs[i] = math.Log(v) / t
		}
		maxLog = max(maxLog, logs[i])
	}
	sum := 0.0
	for i := range logs {
		if !math.IsInf(logs[i], -1) {
			out[i] = math.Exp(logs[i] - maxLog)
			sum += out[i]
		}
	}
	for i := range out {
		out[i] /= sum
	}
	return out
}

// tieOrder breaks argmax ties: UNKNOWN first, then B–F, A last, so a tie
// never reads as idle.
func tieOrder() [numLabels]int { return [numLabels]int{idxG, 1, 2, 3, 4, 5, idxA} }

// argmax is the predicted label.
func argmax(d [numLabels]float64) int {
	order := tieOrder()
	best := order[0]
	for _, i := range order[1:] {
		if d[i] > d[best] {
			best = i
		}
	}
	return best
}

func logLoss(ps []Prediction, t float64) float64 {
	s := 0.0
	for _, p := range ps {
		s -= math.Log(max(scaled(p, t)[p.Truth], logFloor))
	}
	return s / float64(len(ps))
}

// FitTemperature picks T in [0.05, 20] minimising the validation
// log-loss: a fixed log-spaced grid, then golden-section refinement next
// to the best point. The valid validation lines only; none gives T = 1,
// and the second result false.
func FitTemperature(ps []Prediction) (float64, bool) {
	var val []Prediction
	for _, p := range ps {
		if p.Split == dataset.SplitValidation && !p.Invalid {
			val = append(val, p)
		}
	}
	if len(val) == 0 {
		return 1, false
	}
	lo, hi := math.Log(tMin), math.Log(tMax)
	step := (hi - lo) / (gridPoints - 1)
	best, bestLoss := 0, math.Inf(1)
	for i := range gridPoints {
		if l := logLoss(val, math.Exp(lo+float64(i)*step)); l < bestLoss {
			best, bestLoss = i, l
		}
	}
	a := lo + float64(max(best-1, 0))*step
	b := lo + float64(min(best+1, gridPoints-1))*step
	phi := (math.Sqrt(5) - 1) / 2
	c, d := b-phi*(b-a), a+phi*(b-a)
	fc, fd := logLoss(val, math.Exp(c)), logLoss(val, math.Exp(d))
	for range refineIter {
		if fc <= fd {
			b, d, fd = d, c, fc
			c = b - phi*(b-a)
			fc = logLoss(val, math.Exp(c))
		} else {
			a, c, fc = c, d, fd
			d = a + phi*(b-a)
			fd = logLoss(val, math.Exp(d))
		}
	}
	t := math.Exp((a + b) / 2)
	if logLoss(val, t) > bestLoss {
		t = math.Exp(lo + float64(best)*step)
	}
	return t, true
}

// FitTheta is the smallest threshold on calibrated p(A) that leaves the
// model-only false SAFE rate on validation at 0.5 % or less: of the
// validation windows whose truth is not A, at most that share may have
// p(A) ≥ θ. Without validation windows that are not A, θ is
// DefaultTheta. The second result says whether validation set it.
func FitTheta(ps []Prediction, t float64) (float64, bool) {
	var q []float64
	for _, p := range ps {
		if p.Split == dataset.SplitValidation && p.Truth != idxA {
			q = append(q, scaled(p, t)[idxA])
		}
	}
	if len(q) == 0 {
		return DefaultTheta, false
	}
	slices.SortFunc(q, func(a, b float64) int { return -cmpFloat(a, b) })
	// k windows may be SAFE: θ goes just above the (k+1)-th largest p(A).
	k := len(q) * 5 / 1000
	return math.Nextafter(q[k], math.Inf(1)), true
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
