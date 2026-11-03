package tmuxinstance

import (
	"context"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// monitorChainNode is one persisted link of a monitored launcher chain:
// the node's exact identity plus the edge into it.
type monitorChainNode struct {
	pid   int64
	start int64
	comm  string
	uid   uint32
	gid   uint32
	euid  uint32
	egid  uint32
	ppid  int64
}

func monitorNode(pid int64, info procInfo) monitorChainNode {
	return monitorChainNode{
		pid: pid, start: info.start, comm: info.comm,
		uid: info.uid, gid: info.gid, euid: info.euid, egid: info.egid,
		ppid: info.ppid,
	}
}

// sudoChainLength is the closed sudo_monitor_shell_v1 shape: the pane
// runs sudo, whose only child is sudo, whose only child is a shell, whose
// only child is the registered CLI.
const sudoChainLength = 4

// resolveMonitorChain walks the profile's closed shape from the pane. Every
// descent takes exactly one child: more links, fewer links or another order
// refuse with the closed chain-shape code. The harness CLI's own children
// (hooks, emitters) are out of scope and never inspected.
func (r *Registry) resolveMonitorChain(reg Registration, panePID int64, pane procInfo) ([sudoChainLength]monitorChainNode, procInfo, error) {
	var nodes [sudoChainLength]monitorChainNode
	var empty procInfo
	nodes[0] = monitorNode(panePID, pane)

	second, secondInfo, err := r.singleMonitorChild(panePID, "pane")
	if err != nil {
		return nodes, empty, err
	}
	if secondInfo.comm != "sudo" {
		return nodes, empty, refuse(RefusalChainShape,
			"pane %s does not lead sudo, sudo, shell to %q", reg.PaneID, reg.HarnessComm)
	}
	nodes[1] = monitorNode(second, secondInfo)

	third, thirdInfo, err := r.singleMonitorChild(second, "sudo")
	if err != nil {
		return nodes, empty, err
	}
	if _, shell := shells[thirdInfo.comm]; !shell {
		return nodes, empty, refuse(RefusalChainShape,
			"pane %s does not lead sudo, sudo, shell to %q", reg.PaneID, reg.HarnessComm)
	}
	nodes[2] = monitorNode(third, thirdInfo)

	fourth, fourthInfo, err := r.singleMonitorChild(third, "shell")
	if err != nil {
		return nodes, empty, err
	}
	if fourthInfo.comm != reg.HarnessComm {
		return nodes, empty, refuse(RefusalChainShape,
			"pane %s does not lead sudo, sudo, shell to %q", reg.PaneID, reg.HarnessComm)
	}
	nodes[3] = monitorNode(fourth, fourthInfo)
	return nodes, fourthInfo, nil
}

// singleMonitorChild returns the only child of pid. Anything else is a
// shape violation, not a search: the profile never looks further.
func (r *Registry) singleMonitorChild(pid int64, what string) (int64, procInfo, error) {
	kids, err := r.sys.children(pid)
	if err != nil {
		if errors.Is(err, errProcessGone) {
			return 0, procInfo{}, refuseErr(RefusalChainShape,
				fmt.Sprintf("end of the %s chain", what), err)
		}
		return 0, procInfo{}, fmt.Errorf("list children of %d: %w", pid, err)
	}
	if len(kids) != 1 {
		return 0, procInfo{}, refuse(RefusalChainShape,
			"%s %d has %d children, not the single next link", what, pid, len(kids))
	}
	info, err := r.sys.process(kids[0])
	if err != nil {
		if errors.Is(err, errProcessGone) {
			return 0, procInfo{}, refuseErr(RefusalChainShape,
				fmt.Sprintf("end of the %s chain", what), err)
		}
		return 0, procInfo{}, fmt.Errorf("read child of %d: %w", pid, err)
	}
	return kids[0], info, nil
}

// insertMonitorChain persists the registered chain. The instance row
// already exists; a storage failure removes it again so a half-monitored
// instance never stays ACTIVE.
func (r *Registry) insertMonitorChain(ctx context.Context, id protocol.ULID, nodes [sudoChainLength]monitorChainNode) error {
	for position, node := range nodes {
		if _, err := r.db.ExecContext(ctx, `INSERT INTO tmux_monitor_chains (
			instance_id, position, pid, start, comm, uid, gid, euid, egid, ppid
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, position, node.pid, node.start, node.comm,
			node.uid, node.gid, node.euid, node.egid, node.ppid); err != nil {
			return fmt.Errorf("insert monitor chain link %d: %w", position, err)
		}
	}
	return nil
}

// selectMonitorChain loads the persisted chain, oldest position first.
func (r *Registry) selectMonitorChain(ctx context.Context, id protocol.ULID) ([]monitorChainNode, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT position, pid, start, comm,
		uid, gid, euid, egid, ppid FROM tmux_monitor_chains
		WHERE instance_id = ? ORDER BY position`, id)
	if err != nil {
		return nil, fmt.Errorf("select monitor chain: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var nodes []monitorChainNode
	for rows.Next() {
		var position int
		var node monitorChainNode
		if err := rows.Scan(&position, &node.pid, &node.start, &node.comm,
			&node.uid, &node.gid, &node.euid, &node.egid, &node.ppid); err != nil {
			return nil, fmt.Errorf("scan monitor chain: %w", err)
		}
		if position != len(nodes) {
			return nil, fmt.Errorf("monitor chain of %s is not dense: %w", id, ErrInvalidArgument)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read monitor chain: %w", err)
	}
	return nodes, nil
}

// checkMonitorChain revalidates every persisted node and edge. Any drift
// in pid reuse, comm, owners or parentage invalidates the instance: the
// writer must register the new chain before another Enter.
func (r *Registry) checkMonitorChain(ctx context.Context, inst Instance) (Reason, error) {
	nodes, err := r.selectMonitorChain(ctx, inst.ID)
	if err != nil {
		return "", err
	}
	if len(nodes) != sudoChainLength {
		return ReasonChainChanged, nil
	}
	for position, node := range nodes {
		proc, err := r.sys.process(node.pid)
		if errors.Is(err, errProcessGone) {
			return ReasonChainChanged, nil
		}
		if err != nil {
			return "", err
		}
		if proc.start != node.start || proc.comm != node.comm ||
			proc.uid != node.uid || proc.gid != node.gid ||
			proc.euid != node.euid || proc.egid != node.egid ||
			proc.ppid != node.ppid {
			return ReasonChainChanged, nil
		}
		if position == sudoChainLength-1 &&
			(proc.argv0 != inst.HarnessArgv0 || proc.argv1 != inst.HarnessArgv1) {
			return ReasonChainChanged, nil
		}
	}
	return "", nil
}
