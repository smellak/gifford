//go:build linux

package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

type procInspector struct {
	root string
}

// NewProcessInspector constructs the Linux process-tree guard. An empty root
// selects the production /proc filesystem; tests may provide a private tree.
func NewProcessInspector(root string) ProcessInspector {
	if root == "" {
		root = "/proc"
	}
	return &procInspector{root: filepath.Clean(root)}
}

func (p *procInspector) Descendants(process protocol.ProcessIdentity) ([]ProcessToken, error) {
	if process.PID <= 0 {
		return nil, fmt.Errorf("process guard: invalid session pid")
	}
	start, err := p.readStartTime(process.PID)
	if err != nil {
		return nil, err
	}
	if start != process.ProcessStartTime {
		return nil, fmt.Errorf("process guard: session pid identity changed")
	}
	seen := map[int64]bool{process.PID: true}
	queue, err := p.children(process.PID)
	if err != nil {
		return nil, err
	}
	out := make([]ProcessToken, 0, len(queue))
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		if seen[pid] {
			return nil, fmt.Errorf("process guard: child cycle at pid %d", pid)
		}
		seen[pid] = true
		childStart, err := p.readStartTime(pid)
		if err != nil {
			return nil, err
		}
		out = append(out, ProcessToken{PID: pid, StartTime: childStart})
		children, err := p.children(pid)
		if err != nil {
			return nil, err
		}
		queue = append(queue, children...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PID == out[j].PID {
			return out[i].StartTime < out[j].StartTime
		}
		return out[i].PID < out[j].PID
	})
	return out, nil
}

func (p *procInspector) children(pid int64) ([]int64, error) {
	taskRoot := filepath.Join(p.root, strconv.FormatInt(pid, 10), "task")
	tasks, err := os.ReadDir(taskRoot)
	if err != nil {
		return nil, fmt.Errorf("process guard: list threads of pid %d: %w", pid, err)
	}
	seen := make(map[int64]bool)
	for _, task := range tasks {
		thread, err := strconv.ParseInt(task.Name(), 10, 64)
		if err != nil || thread <= 0 {
			continue
		}
		path := filepath.Join(taskRoot, task.Name(), "children")
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("process guard: read children of pid %d thread %d: %w", pid, thread, err)
		}
		for _, field := range strings.Fields(string(raw)) {
			child, err := strconv.ParseInt(field, 10, 64)
			if err != nil || child <= 0 {
				return nil, fmt.Errorf("process guard: parse child %q of pid %d thread %d", field, pid, thread)
			}
			seen[child] = true
		}
	}
	children := make([]int64, 0, len(seen))
	for child := range seen {
		children = append(children, child)
	}
	sort.Slice(children, func(i, j int) bool { return children[i] < children[j] })
	return children, nil
}

func (p *procInspector) readStartTime(pid int64) (int64, error) {
	path := filepath.Join(p.root, strconv.FormatInt(pid, 10), "stat")
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("process guard: read stat of pid %d: %w", pid, err)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return 0, fmt.Errorf("process guard: malformed stat of pid %d", pid)
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return 0, fmt.Errorf("process guard: short stat of pid %d", pid)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("process guard: parse start of pid %d: %w", pid, err)
	}
	return start, nil
}
