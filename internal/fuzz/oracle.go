package fuzz

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// noticeOnly holds the runes a notice has and the human never types.
const noticeOnly = "ABCDEFGHIJKLMNOPQRSTUVWXYZ[]"

// lineVerdict is what the oracle finds in the lines the harness got.
type lineVerdict struct {
	// Lines counts the lines the harness got: submitted, cancelled,
	// cleared, or left when it ended.
	Lines int
	// Mixed counts the lines that are neither exactly a notice nor
	// exactly what the human meant to send there.
	Mixed int
	// LostKeys counts the keys of the human's lines, Enter or Ctrl-C
	// included, that never reached the harness as sent and in order.
	LostKeys int
	// Seen counts, per notice, the lines that were exactly it.
	Seen map[string]int
	// Detail says what each mixed line and each lost line was.
	Detail []string
}

// checkLines compares the lines the harness got with what the human
// meant to send and with the notices. A line that is exactly a notice
// is that notice; every other line must be the human's next one. The
// human's lines are matched in order, by a longest common subsequence,
// so one bad line costs only itself and the lines it stood for.
func checkLines(got, want []Record, notices []string) lineVerdict {
	v := lineVerdict{Lines: len(got), Seen: make(map[string]int, len(notices))}
	isNotice := make(map[string]bool, len(notices))
	for _, n := range notices {
		isNotice[n] = true
		v.Seen[n] = 0
	}
	var human []Record
	for _, r := range got {
		if r.Kind == RecordLine && isNotice[r.Text] {
			v.Seen[r.Text]++
			continue
		}
		human = append(human, r)
	}
	matchedGot, matchedWant := align(human, want)
	for i, r := range human {
		if !matchedGot[i] {
			v.Mixed++
			what := "which the human never sent there"
			if strings.ContainsAny(r.Text, noticeOnly) {
				what = "which holds part of a notice"
			}
			v.Detail = append(v.Detail, fmt.Sprintf("mixed line: the harness got %s, %s", r, what))
		}
	}
	for i, r := range want {
		if !matchedWant[i] {
			v.LostKeys += utf8.RuneCountInString(r.Text) + 1
			v.Detail = append(v.Detail, fmt.Sprintf("lost keys: the human sent %s, which never arrived as sent", r))
		}
	}
	return v
}

// align matches a and b along a longest common subsequence and reports
// which elements of each it matched.
func align(a, b []Record) (inA, inB []bool) {
	n, m := len(a), len(b)
	// lcs[i][j] is the length of the longest common subsequence of
	// a[i:] and b[j:].
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	inA, inB = make([]bool, n), make([]bool, m)
	for i, j := 0, 0; i < n && j < m; {
		switch {
		case a[i] == b[j]:
			inA[i], inB[j] = true, true
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			i++
		default:
			j++
		}
	}
	return inA, inB
}
