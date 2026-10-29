package eval

import (
	"math"
	"slices"
)

// DefaultBins is the ECE bin count.
const DefaultBins = 15

// Rate is a count over a count; an empty denominator is no data.
type Rate struct {
	Num, Den int
}

// Value is the rate, or false without data.
func (r Rate) Value() (float64, bool) {
	if r.Den == 0 {
		return 0, false
	}
	return float64(r.Num) / float64(r.Den), true
}

// Bin is one row of the reliability table: windows whose confidence fell
// in [Lo, Hi), their mean confidence and their accuracy.
type Bin struct {
	Lo, Hi     float64
	N          int
	Confidence float64
	Accuracy   float64
}

// Metrics are one set of windows scored.
type Metrics struct {
	N        int
	Accuracy float64
	MacroF1  float64
	// MacroLabels are the labels macro-F1 averages over: those with
	// support in the truth, the same for every profile over the same
	// windows.
	MacroLabels []int
	// Recall per label; Support counts the true windows of each.
	Recall  [numLabels]float64
	Support [numLabels]int
	// Confusion[truth][predicted].
	Confusion [numLabels][numLabels]int
	// FalseIdle: predicted A, truth not A. FalseSafe: calibrated
	// p(A) ≥ θ, truth not A. Both over the windows whose truth is not A.
	FalseIdle, FalseSafe Rate
	ECERaw, ECECal       float64
	BrierRaw, BrierCal   float64
	// Unknown counts windows predicted UNKNOWN, invalid lines included.
	Unknown                Rate
	LatencyP50, LatencyP95 float64
	Reliability            []Bin
}

// Score computes the metrics of ps under temperature t, threshold theta
// and bins ECE bins. Predictions come from the calibrated distribution.
func Score(ps []Prediction, t, theta float64, bins int) Metrics {
	m := Metrics{N: len(ps)}
	if len(ps) == 0 {
		return m
	}
	correct := 0
	raw := make([][numLabels]float64, len(ps))
	cal := make([][numLabels]float64, len(ps))
	lat := make([]float64, 0, len(ps))
	for i, p := range ps {
		raw[i], cal[i] = scaled(p, 1), scaled(p, t)
		pred := argmax(cal[i])
		m.Confusion[p.Truth][pred]++
		m.Support[p.Truth]++
		if pred == p.Truth {
			correct++
		}
		if pred == idxG {
			m.Unknown.Num++
		}
		if p.Truth != idxA {
			m.FalseIdle.Den++
			m.FalseSafe.Den++
			if pred == idxA {
				m.FalseIdle.Num++
			}
			if cal[i][idxA] >= theta {
				m.FalseSafe.Num++
			}
		}
		lat = append(lat, p.LatencyMS)
	}
	m.Unknown.Den = len(ps)
	m.Accuracy = float64(correct) / float64(len(ps))

	f1s, classes := 0.0, 0
	for c := range numLabels {
		predicted := 0
		for t := range numLabels {
			predicted += m.Confusion[t][c]
		}
		tp := m.Confusion[c][c]
		if m.Support[c] > 0 {
			m.Recall[c] = float64(tp) / float64(m.Support[c])
			prec := 0.0
			if predicted > 0 {
				prec = float64(tp) / float64(predicted)
			}
			f1 := 0.0
			if prec+m.Recall[c] > 0 {
				f1 = 2 * prec * m.Recall[c] / (prec + m.Recall[c])
			}
			f1s += f1
			classes++
			m.MacroLabels = append(m.MacroLabels, c)
		}
	}
	m.MacroF1 = f1s / float64(classes)

	m.ECERaw, _ = ece(ps, raw, bins)
	m.ECECal, m.Reliability = ece(ps, cal, bins)
	m.BrierRaw, m.BrierCal = brier(ps, raw), brier(ps, cal)
	slices.Sort(lat)
	m.LatencyP50, m.LatencyP95 = percentile(lat, 0.50), percentile(lat, 0.95)
	return m
}

// ece is the expected calibration error over fixed equal-width bins of
// the top probability, with its reliability table.
func ece(ps []Prediction, ds [][numLabels]float64, bins int) (float64, []Bin) {
	table := make([]Bin, bins)
	confSum := make([]float64, bins)
	hits := make([]int, bins)
	for i, d := range ds {
		pred := argmax(d)
		conf := d[pred]
		b := min(int(conf*float64(bins)), bins-1)
		table[b].N++
		confSum[b] += conf
		if pred == ps[i].Truth {
			hits[b]++
		}
	}
	e := 0.0
	for b := range table {
		table[b].Lo = float64(b) / float64(bins)
		table[b].Hi = float64(b+1) / float64(bins)
		if table[b].N == 0 {
			continue
		}
		table[b].Confidence = confSum[b] / float64(table[b].N)
		table[b].Accuracy = float64(hits[b]) / float64(table[b].N)
		e += math.Abs(table[b].Accuracy-table[b].Confidence) * float64(table[b].N) / float64(len(ps))
	}
	return e, table
}

// brier is the multi-class Brier score: the mean squared distance to the
// one-hot truth, summed over labels.
func brier(ps []Prediction, ds [][numLabels]float64) float64 {
	s := 0.0
	for i, d := range ds {
		for c, v := range d {
			y := 0.0
			if c == ps[i].Truth {
				y = 1
			}
			s += (v - y) * (v - y)
		}
	}
	return s / float64(len(ps))
}

// percentile is nearest-rank over sorted values.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}
