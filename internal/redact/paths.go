package redact

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// pathsRule masks configured sensitive paths and anything below them. The
// entries arrive deduped and longest first, so nested paths mask from the
// outside in.
type pathsRule struct {
	entries []string
}

func (p *pathsRule) find(lines []string) []finding {
	out, _ := p.findCovered(lines, nil)
	return out
}

// findCovered locates every literal occurrence with its §6.5 boundaries,
// letting covered runes through: absent, covered, or outside letters,
// digits and _ . ~ - / before, and absent, covered, slash or outside
// letters, digits and _ . ~ - after. Covered means masked by a finding of
// kinds 1-6 or by an enabled occurrence, its extension included. The
// enabled occurrences are the least fixed point over the literal ones on
// the original offsets: a worklist re-checks an occurrence only when a
// rune next to it becomes covered, and each occurrence changes at most
// once.
func (p *pathsRule) findCovered(lines []string, covered [][][2]int) ([]finding, int) {
	var out []finding
	checks := 0
	for li, line := range lines {
		var cov [][2]int
		if li < len(covered) {
			cov = covered[li]
		}
		found, n := p.coverLine(li, line, cov)
		out = append(out, found...)
		checks += n
	}
	return out, checks
}

// pathOcc is one literal occurrence with its bare extension. Enabled is
// not counted (g): an occurrence that overlaps an earlier finding still
// enables its neighbors even though it adds no count.
type pathOcc struct {
	start, end int
	enabled    bool
}

// coverLine counts the occurrences of one line to the fixed point,
// emitting one finding per enabled occurrence and reporting how many
// occurrence checks it made. The neighbors of a newly
// enabled occurrence come from indexes by start and by end instead of a
// sweep over every occurrence, and coverage is incremental: a union-find
// "next unmarked" structure marks every line byte at most once, and a
// Fenwick tree per index counts actionable neighbors (checked and failed,
// waiting for coverage) so ranges with nothing to requeue are skipped
// without a walk. Collection is O(E·L) for E entries on a line of L
// bytes; sorting is O(N log N) for N occurrences; every occurrence
// changes state O(1) times at O(log N) per change; every visited neighbor
// range either is skipped in O(log N) or requeues at least one
// occurrence. The enabled set is the same least fixed point as the old
// worklist: every coverage change touching an occurrence requeues it, and
// coverage only grows, so the result is schedule-independent.
func (p *pathsRule) coverLine(li int, line string, cov [][2]int) ([]finding, int) {
	var occs []pathOcc
	// bareEnds records every bare-run end in one pass, built lazily on
	// the first extension: extending an occurrence then costs O(1) no
	// matter how many occurrences share the run.
	var bareEnds []int
	bareEnd := func(end int) int {
		if bareEnds == nil {
			bareEnds = bareRunEnds(line)
		}
		if end < 0 {
			end = 0
		}
		if end > len(line) {
			end = len(line)
		}
		return bareEnds[end]
	}
	for _, e := range p.entries {
		rest := line
		off := 0
		for {
			i := strings.Index(rest, e)
			if i < 0 {
				break
			}
			start := off + i
			end := start + len(e)
			rest = rest[i+len(e):]
			off = end
			if end < len(line) && line[end] == '/' {
				end = bareEnd(end)
			}
			occs = append(occs, pathOcc{start: start, end: end})
		}
	}
	byStart := make([]int, len(occs))
	byEnd := make([]int, len(occs))
	posInStart := make([]int, len(occs))
	posInEnd := make([]int, len(occs))
	for i := range occs {
		byStart[i] = i
		byEnd[i] = i
	}
	sort.SliceStable(byStart, func(a, b int) bool { return occs[byStart[a]].start < occs[byStart[b]].start })
	sort.SliceStable(byEnd, func(a, b int) bool { return occs[byEnd[a]].end < occs[byEnd[b]].end })
	for pos, j := range byStart {
		posInStart[j] = pos
	}
	for pos, j := range byEnd {
		posInEnd[j] = pos
	}
	covered := newCoverage(len(line))
	for _, r := range cov {
		covered.mark(r[0], r[1])
	}
	// actionable holds the checked-and-failed occurrences: the only ones a
	// new cover can rescue. The trees count them in index order so empty
	// neighbor ranges skip their walk.
	actionable := make([]bool, len(occs))
	byStartWaiting := newFenwick(len(occs))
	byEndWaiting := newFenwick(len(occs))
	wait := func(j int, on bool) {
		if actionable[j] == on {
			return
		}
		actionable[j] = on
		d := 1
		if !on {
			d = -1
		}
		byStartWaiting.add(posInStart[j], d)
		byEndWaiting.add(posInEnd[j], d)
	}
	queued := make([]bool, len(occs))
	queue := make([]int, len(occs))
	for i := range occs {
		queue[i] = i
		queued[i] = true
	}
	requeue := func(members []int) {
		for _, j := range members {
			if actionable[j] {
				wait(j, false)
				queued[j] = true
				queue = append(queue, j)
			}
		}
	}
	var out []finding
	checks := 0
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		queued[i] = false
		if occs[i].enabled {
			continue
		}
		checks++
		if !beforeOK(line, occs[i].start, covered) || !afterOK(line, occs[i].end, covered) {
			wait(i, true)
			continue
		}
		occs[i].enabled = true
		start, end := occs[i].start, occs[i].end
		covered.mark(start, end)
		out = append(out, finding{kind: KindSensitivePath, parts: []span{{line: li, start: start, end: end}}})
		if lo, hi := startRange(byStart, occs, start, end); byStartWaiting.sum(lo, hi) > 0 {
			requeue(byStart[lo:hi])
		}
		if lo, hi := endRange(byEnd, occs, start, end); byEndWaiting.sum(lo, hi) > 0 {
			requeue(byEnd[lo:hi])
		}
	}
	return out, checks
}

// startRange returns the index span of occurrences starting just inside
// (start, end]: the byte before their start lies in the newly covered
// [start, end).
func startRange(byStart []int, occs []pathOcc, start, end int) (int, int) {
	lo := sort.Search(len(byStart), func(k int) bool { return occs[byStart[k]].start > start })
	hi := sort.Search(len(byStart), func(k int) bool { return occs[byStart[k]].start > end })
	return lo, hi
}

// endRange returns the index span of occurrences ending inside
// [start, end): their end rune lies in the newly covered range.
func endRange(byEnd []int, occs []pathOcc, start, end int) (int, int) {
	lo := sort.Search(len(byEnd), func(k int) bool { return occs[byEnd[k]].end >= start })
	hi := sort.Search(len(byEnd), func(k int) bool { return occs[byEnd[k]].end >= end })
	return lo, hi
}

// beforeOK checks the rune before an occurrence: absent, covered, or
// outside letters, digits and _ . ~ - /.
func beforeOK(line string, start int, covered *coverage) bool {
	if start == 0 {
		return true
	}
	if covered.at(start - 1) {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(line[:start])
	return !isPathRune(r, true)
}

// afterOK checks the rune after an occurrence: absent, covered, slash, or
// outside letters, digits and _ . ~ -.
func afterOK(line string, end int, covered *coverage) bool {
	if end >= len(line) {
		return true
	}
	if covered.at(end) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(line[end:])
	if r == '/' {
		return true
	}
	return !isPathRune(r, false)
}

// coverage is the incremental covered set of one line: a union-find
// "next unmarked" structure over the bytes, so marking a span touches
// only its still-unmarked bytes and every byte is marked at most once.
// Queries and marks are amortized near-constant time.
type coverage struct {
	next []int
}

// newCoverage returns the empty covered set of a line of n bytes.
func newCoverage(n int) *coverage {
	next := make([]int, n+1)
	for i := range next {
		next[i] = i
	}
	return &coverage{next: next}
}

// find returns the smallest unmarked byte at or past i.
func (c *coverage) find(i int) int {
	n := len(c.next) - 1
	if i > n {
		return n
	}
	root := i
	for c.next[root] != root {
		root = c.next[root]
	}
	for c.next[i] != root {
		c.next[i], i = root, c.next[i]
	}
	return root
}

// at reports whether byte pos is covered.
func (c *coverage) at(pos int) bool {
	return pos >= 0 && pos < len(c.next)-1 && c.find(pos) != pos
}

// markEach marks [start, end), clamped to the line, calling set once per
// newly marked byte.
func (c *coverage) markEach(start, end int, set func(b int)) {
	if start < 0 {
		start = 0
	}
	if end > len(c.next)-1 {
		end = len(c.next) - 1
	}
	for i := c.find(start); i < end; i = c.find(i) {
		c.next[i] = i + 1
		set(i)
	}
}

// mark covers [start, end), clamped to the line.
func (c *coverage) mark(start, end int) {
	if start < 0 {
		start = 0
	}
	if end > len(c.next)-1 {
		end = len(c.next) - 1
	}
	for i := c.find(start); i < end; i = c.find(i) {
		c.next[i] = i + 1
	}
}

// fenwick counts points in index order with logarithmic add and range
// sum, so coverLine learns whether a neighbor range holds anything to
// requeue without walking it.
type fenwick struct {
	bit []int
}

// newFenwick returns the all-zero counter of n positions.
func newFenwick(n int) *fenwick {
	return &fenwick{bit: make([]int, n+1)}
}

// add moves position pos by d.
func (f *fenwick) add(pos, d int) {
	for i := pos + 1; i < len(f.bit); i += i & -i {
		f.bit[i] += d
	}
}

// prefix returns the count below pos.
func (f *fenwick) prefix(pos int) int {
	total := 0
	for i := pos; i > 0; i -= i & -i {
		total += f.bit[i]
	}
	return total
}

// sum returns the count on [lo, hi).
func (f *fenwick) sum(lo, hi int) int {
	if hi < lo {
		lo, hi = hi, lo
	}
	return f.prefix(hi) - f.prefix(lo)
}

// isPathRune reports the runes binding a path to its neighbors. The
// before set adds the slash, so a subpath never matches inside its
// parent's prefix.
func isPathRune(r rune, before bool) bool {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return true
	}
	switch r {
	case '_', '.', '~', '-':
		return true
	case '/':
		return before
	default:
		return false
	}
}

// bareRunEnds records, for every byte of line, where the bare run
// starting there ends: one right-to-left pass, so each occurrence
// extends in O(1). A continuation byte behaves as extendBare reads it:
// RuneError counts as a bare rune.
func bareRunEnds(line string) []int {
	ends := make([]int, len(line)+1)
	ends[len(line)] = len(line)
	for i := len(line) - 1; i >= 0; i-- {
		r, size := utf8.DecodeRuneInString(line[i:])
		if !isBareRune(r) {
			ends[i] = i
			continue
		}
		next := i + size
		if next > len(line) {
			next = len(line)
		}
		ends[i] = ends[next]
	}
	return ends
}
