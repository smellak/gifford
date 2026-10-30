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

func (p *procInspector) Inspect(process protocol.ProcessIdentity) (ProcessTree, error) {
	if process.PID <= 0 {
		return ProcessTree{}, fmt.Errorf("process guard: invalid session pid")
	}
	bootID, err := p.readBootID()
	if err != nil {
		return ProcessTree{}, err
	}
	start, err := p.readStartTime(process.PID)
	if err != nil {
		return ProcessTree{}, err
	}
	if start != process.ProcessStartTime {
		return ProcessTree{}, fmt.Errorf("process guard: session pid identity changed")
	}
	seen := map[int64]bool{process.PID: true}
	queue, err := p.children(process.PID)
	if err != nil {
		return ProcessTree{}, err
	}
	direct := make([]ProcessToken, 0, len(queue))
	out := make([]ProcessToken, 0, len(queue))
	tokens := make(map[int64]ProcessToken, len(queue))
	for _, pid := range queue {
		childStart, readErr := p.readStartTime(pid)
		if readErr != nil {
			return ProcessTree{}, readErr
		}
		token := ProcessToken{PID: pid, StartTime: childStart}
		direct = append(direct, token)
		tokens[pid] = token
	}
	remaining := append([]int64(nil), queue...)
	for len(remaining) > 0 {
		pid := remaining[0]
		remaining = remaining[1:]
		if seen[pid] {
			return ProcessTree{}, fmt.Errorf("process guard: child cycle at pid %d", pid)
		}
		seen[pid] = true
		token, ok := tokens[pid]
		if !ok {
			childStart, err := p.readStartTime(pid)
			if err != nil {
				return ProcessTree{}, err
			}
			token = ProcessToken{PID: pid, StartTime: childStart}
			tokens[pid] = token
		}
		out = append(out, token)
		children, err := p.children(pid)
		if err != nil {
			return ProcessTree{}, err
		}
		remaining = append(remaining, children...)
	}
	sortTokens(direct)
	sortTokens(out)
	return ProcessTree{
		BootID: bootID, Root: ProcessToken{PID: process.PID, StartTime: start},
		DirectChildren: direct, Descendants: out,
	}, nil
}

func sortTokens(tokens []ProcessToken) {
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].PID == tokens[j].PID {
			return tokens[i].StartTime < tokens[j].StartTime
		}
		return tokens[i].PID < tokens[j].PID
	})
}

func (p *procInspector) readBootID() (string, error) {
	raw, err := os.ReadFile(filepath.Join(p.root, "sys", "kernel", "random", "boot_id"))
	if err != nil {
		return "", fmt.Errorf("process guard: read boot id: %w", err)
	}
	bootID := strings.TrimSpace(string(raw))
	if bootID == "" || len(bootID) > 128 || strings.IndexFunc(bootID, func(r rune) bool {
		return r < 0x21 || r > 0x7e
	}) >= 0 {
		return "", fmt.Errorf("process guard: malformed boot id")
	}
	return bootID, nil
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
