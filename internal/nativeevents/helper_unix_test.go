//go:build unix

package nativeevents

import (
	"errors"
	"sync"

	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
)

type testInspector struct {
	tree orchestrator.ProcessTree
	err  error
}

type recordingEmitterLifecycle struct {
	activated chan EmitterType
	failed    chan EmitterType
}

func newRecordingEmitterLifecycle() *recordingEmitterLifecycle {
	return &recordingEmitterLifecycle{
		activated: make(chan EmitterType, 4), failed: make(chan EmitterType, 4),
	}
}

func (l *recordingEmitterLifecycle) NativeEmitterActivated(emitter EmitterType) error {
	l.activated <- emitter
	return nil
}

func (l *recordingEmitterLifecycle) NativeEmitterFailed(emitter EmitterType) {
	l.failed <- emitter
}

func (i testInspector) Inspect(protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	return i.tree, i.err
}

func (i testInspector) InspectPeer(
	_ protocol.ProcessIdentity,
	peer orchestrator.ProcessToken,
) (orchestrator.PeerLineage, error) {
	if i.err != nil {
		return orchestrator.PeerLineage{}, i.err
	}
	return testPeerLineage(i.tree, peer)
}

func testPeerLineage(
	tree orchestrator.ProcessTree,
	peer orchestrator.ProcessToken,
) (orchestrator.PeerLineage, error) {
	direct := make([]orchestrator.ProcessToken, 0, 1)
	parents := make(map[orchestrator.ProcessToken]orchestrator.ProcessToken, len(tree.Descendants))
	for _, node := range tree.Descendants {
		if _, duplicate := parents[node.Token]; duplicate {
			return orchestrator.PeerLineage{}, errors.New("duplicate process")
		}
		parents[node.Token] = node.Parent
		if node.Parent == tree.Root {
			direct = append(direct, node.Token)
		}
	}
	if len(direct) != 1 {
		return orchestrator.PeerLineage{}, errors.New("expected one harness")
	}
	harness := direct[0]
	seen := make(map[orchestrator.ProcessToken]struct{}, len(parents))
	for current := peer; current != harness; {
		if _, duplicate := seen[current]; duplicate {
			return orchestrator.PeerLineage{}, errors.New("process cycle")
		}
		seen[current] = struct{}{}
		parent, ok := parents[current]
		if !ok || parent == tree.Root {
			return orchestrator.PeerLineage{}, errors.New("peer outside harness")
		}
		current = parent
	}
	return orchestrator.PeerLineage{Root: tree.Root, Harness: harness, Peer: peer}, nil
}

type sequenceInspector struct {
	mu    sync.Mutex
	trees []orchestrator.ProcessTree
	next  int
}

func (i *sequenceInspector) Inspect(protocol.ProcessIdentity) (orchestrator.ProcessTree, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.next >= len(i.trees) {
		return orchestrator.ProcessTree{}, errors.New("unexpected process inspection")
	}
	tree := i.trees[i.next]
	i.next++
	return tree, nil
}

func (i *sequenceInspector) InspectPeer(
	_ protocol.ProcessIdentity,
	peer orchestrator.ProcessToken,
) (orchestrator.PeerLineage, error) {
	tree, err := i.Inspect(protocol.ProcessIdentity{})
	if err != nil {
		return orchestrator.PeerLineage{}, err
	}
	return testPeerLineage(tree, peer)
}
