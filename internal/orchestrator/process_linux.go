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
	start, _, err := p.readStat(process.PID)
	if err != nil {
		return ProcessTree{}, err
	}
	if start != process.ProcessStartTime {
		return ProcessTree{}, fmt.Errorf("process guard: session pid identity changed")
	}
	root := ProcessToken{PID: process.PID, StartTime: start}
	children, err := p.children(process.PID)
	if err != nil {
		return ProcessTree{}, err
	}
	type pendingProcess struct {
		pid    int64
		parent ProcessToken
	}
	remaining := make([]pendingProcess, 0, len(children))
	for _, child := range children {
		remaining = append(remaining, pendingProcess{pid: child, parent: root})
	}
	seen := map[int64]bool{process.PID: true}
	out := make([]ProcessNode, 0, len(remaining))
	for len(remaining) > 0 {
		pending := remaining[0]
		remaining = remaining[1:]
		if seen[pending.pid] {
			return ProcessTree{}, fmt.Errorf("process guard: child cycle at pid %d", pending.pid)
		}
		seen[pending.pid] = true
		node, err := p.readNode(pending.pid, pending.parent)
		if err != nil {
			return ProcessTree{}, err
		}
		out = append(out, node)
		children, err := p.children(pending.pid)
		if err != nil {
			return ProcessTree{}, err
		}
		for _, child := range children {
			remaining = append(remaining, pendingProcess{pid: child, parent: node.Token})
		}
	}
	confirmedStart, _, err := p.readStat(process.PID)
	if err != nil {
		return ProcessTree{}, err
	}
	if confirmedStart != start {
		return ProcessTree{}, fmt.Errorf("process guard: session pid identity changed")
	}
	sortNodes(out)
	return ProcessTree{BootID: bootID, Root: root, Descendants: out}, nil
}

func sortNodes(nodes []ProcessNode) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Token.PID == nodes[j].Token.PID {
			return nodes[i].Token.StartTime < nodes[j].Token.StartTime
		}
		return nodes[i].Token.PID < nodes[j].Token.PID
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

func (p *procInspector) readNode(pid int64, parent ProcessToken) (ProcessNode, error) {
	start, parentPID, err := p.readStat(pid)
	if err != nil {
		return ProcessNode{}, err
	}
	if parentPID != parent.PID {
		return ProcessNode{}, fmt.Errorf(
			"process guard: pid %d parent changed from %d to %d", pid, parent.PID, parentPID,
		)
	}
	executable, err := p.readExecutable(pid)
	if err != nil {
		return ProcessNode{}, err
	}
	confirmedStart, confirmedParent, err := p.readStat(pid)
	if err != nil {
		return ProcessNode{}, err
	}
	confirmedExecutable, err := p.readExecutable(pid)
	if err != nil {
		return ProcessNode{}, err
	}
	if confirmedStart != start || confirmedParent != parentPID || confirmedExecutable != executable {
		return ProcessNode{}, fmt.Errorf("process guard: pid %d identity changed during observation", pid)
	}
	return ProcessNode{
		Token: ProcessToken{PID: pid, StartTime: start}, Parent: parent, Executable: executable,
	}, nil
}

func (p *procInspector) readExecutable(pid int64) (string, error) {
	path := filepath.Join(p.root, strconv.FormatInt(pid, 10), "exe")
	target, err := os.Readlink(path)
	if err != nil {
		return "", fmt.Errorf("process guard: read executable of pid %d: %w", pid, err)
	}
	if target == "" || !filepath.IsAbs(target) || filepath.Clean(target) != target ||
		strings.HasSuffix(target, " (deleted)") {
		return "", fmt.Errorf("process guard: malformed executable of pid %d", pid)
	}
	return target, nil
}

func (p *procInspector) readStat(pid int64) (startTime int64, parentPID int64, err error) {
	path := filepath.Join(p.root, strconv.FormatInt(pid, 10), "stat")
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, fmt.Errorf("process guard: read stat of pid %d: %w", pid, err)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return 0, 0, fmt.Errorf("process guard: malformed stat of pid %d", pid)
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return 0, 0, fmt.Errorf("process guard: short stat of pid %d", pid)
	}
	parent, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || parent <= 0 {
		return 0, 0, fmt.Errorf("process guard: parse parent of pid %d", pid)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("process guard: parse start of pid %d: %w", pid, err)
	}
	return start, parent, nil
}
