//go:build linux

package adapter

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	museProcessTestRoleEnv    = "GIFFORD_TEST_MUSE_PROCESS_ROLE"
	museProcessTestRunEnv     = "GIFFORD_TEST_MUSE_PROCESS_RUN"
	museProcessTestLatencyEnv = "GIFFORD_TEST_MUSE_PROCESS_LATENCY"
	museProcessTestSocketEnv  = "GIFFORD_TEST_MUSE_PROCESS_SOCKET"
	museProcessTestGateEnv    = "GIFFORD_TEST_MUSE_PROCESS_GATE"
	museProcessTestReadyEnv   = "GIFFORD_TEST_MUSE_PROCESS_READY"
	museProcessTestIndexEnv   = "GIFFORD_TEST_MUSE_PROCESS_INDEX"
)

func TestNativeHelloIgnoresIncompleteUnrelatedFakeProc(t *testing.T) {
	fixture := newMuseLinuxFixture(t)
	require.NoError(t, fixture.nativeListener.Close())

	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID + 2_000_000, ProcessStartTime: 10,
		UID: process.UID, GID: process.GID,
	}
	procRoot := t.TempDir()
	writeMuseServerProcBoot(t, procRoot)
	harnessPID := root.PID + 1
	disappearedPID := root.PID + 2
	writeMuseServerProcNode(t, procRoot, root.PID, 1, root.ProcessStartTime, []int64{harnessPID})
	writeMuseServerProcNode(t, procRoot, harnessPID, root.PID, 20, []int64{process.PID, disappearedPID})
	writeMuseServerProcNode(t, procRoot, process.PID, harnessPID, process.ProcessStartTime, nil)
	// disappearedPID remains listed in the harness's children snapshot but has
	// no /proc directory, exactly the teardown race reported by procInspector.

	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-fake-proc", SessionID: "session-fake-proc", AgentID: fixture.agent,
		Harness: protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: fixture.runtime, Root: root,
		Inspector: orchestrator.NewPeerProcessInspector(procRoot), Queue: queue,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	require.Equal(t, fixture.nativePath, server.Path())

	hello, _ := museFixtureMessage(t)
	probe, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	require.NoError(t, nativeevents.WriteMessage(probe, hello))
	message, err := nativeevents.ReadMessage(probe)
	require.NoError(t, err)
	require.Equal(t, nativeevents.HelloResult{
		Operation: nativeevents.OperationHelloResult,
		Accepted:  true,
	}, message)
	require.NoError(t, probe.Close())
	require.NoDirExists(t, filepath.Join(fixture.runtime, museDiagnosticDirectory),
		"unrelated process churn must not produce a Muse marker")
	_, queued := queue.Head()
	require.False(t, queued, "the probe sent no event")
	require.Zero(t, queue.Dropped())
}

func writeMuseServerProcBoot(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "sys", "kernel", "random")
	require.NoError(t, os.MkdirAll(path, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "boot_id"), []byte("muse-process-test\n"), 0o600))
}

func writeMuseServerProcNode(
	t *testing.T,
	root string,
	pid, parent, start int64,
	children []int64,
) {
	t.Helper()
	directory := filepath.Join(root, strconv.FormatInt(pid, 10))
	task := filepath.Join(directory, "task", strconv.FormatInt(pid, 10))
	require.NoError(t, os.MkdirAll(task, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(directory, "stat"),
		museFixtureProcStat(int(pid), int(parent), uint64(start)),
		0o600,
	))
	require.NoError(t, os.Symlink("/bin/true", filepath.Join(directory, "exe")))
	fields := make([]string, 0, len(children))
	for _, child := range children {
		fields = append(fields, strconv.FormatInt(child, 10))
	}
	require.NoError(t, os.WriteFile(
		filepath.Join(task, "children"), []byte(strings.Join(fields, " ")+"\n"), 0o600,
	))
}

func TestNativeHelloIgnoresUnrelatedReparent(t *testing.T) {
	t.Parallel()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID + 2_500_000, ProcessStartTime: 10,
		UID: process.UID, GID: process.GID,
	}
	procRoot := t.TempDir()
	writeMuseServerProcBoot(t, procRoot)
	harnessPID := root.PID + 1
	reparentedPID := root.PID + 2
	writeMuseServerProcNode(t, procRoot, root.PID, 1, root.ProcessStartTime, []int64{harnessPID})
	writeMuseServerProcNode(t, procRoot, harnessPID, root.PID, 20, []int64{process.PID, reparentedPID})
	writeMuseServerProcNode(t, procRoot, process.PID, harnessPID, process.ProcessStartTime, nil)
	writeMuseServerProcNode(t, procRoot, reparentedPID, root.PID, 30, nil)

	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-reparent", SessionID: "session-reparent", AgentID: "agent-reparent",
		Harness: protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: t.TempDir(), Root: root,
		Inspector: orchestrator.NewPeerProcessInspector(procRoot), Queue: queue,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	hello, _ := museFixtureMessage(t)
	require.NoError(t, nativeevents.WriteMessage(conn, hello))
	message, err := nativeevents.ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, nativeevents.HelloResult{
		Operation: nativeevents.OperationHelloResult, Accepted: true,
	}, message)
	_, queued := queue.Head()
	require.False(t, queued)
}

func TestNativeHelloRejectsPeerReparent(t *testing.T) {
	t.Parallel()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID + 2_750_000, ProcessStartTime: 10,
		UID: process.UID, GID: process.GID,
	}
	procRoot := t.TempDir()
	writeMuseServerProcBoot(t, procRoot)
	harnessPID := root.PID + 1
	writeMuseServerProcNode(t, procRoot, root.PID, 1, root.ProcessStartTime, []int64{harnessPID})
	writeMuseServerProcNode(t, procRoot, harnessPID, root.PID, 20, nil)
	writeMuseServerProcNode(
		t, procRoot, process.PID, root.PID, process.ProcessStartTime, nil,
	)

	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-peer-reparent", SessionID: "session-peer-reparent",
		AgentID: "agent-peer-reparent", Harness: protocol.HarnessMuseCode,
		HarnessVersion: museHarnessVersion, AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: t.TempDir(), Root: root,
		Inspector: orchestrator.NewPeerProcessInspector(procRoot), Queue: queue,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	hello, _ := museFixtureMessage(t)
	require.NoError(t, nativeevents.WriteMessage(conn, hello))
	message, err := nativeevents.ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, nativeevents.HelloResult{
		Operation: nativeevents.OperationHelloResult,
		Reason:    nativeevents.HelloRejectPeerNotInTree,
	}, message)
}

func TestMuseAckLossStillRetainsStoredEvent(t *testing.T) {
	t.Parallel()
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID + 3_000_000, ProcessStartTime: 10,
		UID: process.UID, GID: process.GID,
	}
	rootToken := orchestrator.ProcessToken{PID: root.PID, StartTime: root.ProcessStartTime}
	harnessToken := orchestrator.ProcessToken{PID: root.PID + 1, StartTime: 20}
	peerToken := orchestrator.ProcessToken{PID: process.PID, StartTime: process.ProcessStartTime}
	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-ack-loss", SessionID: "session-ack-loss", AgentID: "agent-ack-loss",
		Harness: protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: t.TempDir(), Root: root,
		Inspector: museBurstInspector{tree: orchestrator.ProcessTree{
			Root: rootToken,
			Descendants: []orchestrator.ProcessNode{
				{Token: harnessToken, Parent: rootToken},
				{Token: peerToken, Parent: harnessToken},
			},
		}},
		Queue: queue,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	hello, event := museFixtureMessage(t)
	event.EmitterEventID = "muse-ack-loss"
	require.NoError(t, nativeevents.WriteMessage(conn, hello))
	message, err := nativeevents.ReadMessage(conn)
	require.NoError(t, err)
	require.Equal(t, nativeevents.HelloResult{
		Operation: nativeevents.OperationHelloResult, Accepted: true,
	}, message)
	require.NoError(t, nativeevents.WriteMessage(conn, event))
	require.Eventually(t, func() bool {
		_, ok := queue.Head()
		return ok
	}, time.Second, time.Millisecond)
	heap, ok := queue.Head()
	require.True(t, ok)
	require.NoError(t, conn.Close(), "the client disappears before receiving the ACK")
	queue.Settle(protocol.EmitEventResult{
		EventID: heap.EventID, Status: protocol.EmitEventStatusStored,
	})
	generation, stored := queue.StoredSnapshot()
	require.Equal(t, int64(1), generation)
	require.Equal(t, []protocol.StateObservationEvent{{
		EventID: heap.EventID, Type: heap.EventType, At: heap.WallTime,
	}}, stored)
}

func TestMuseRealProcessChurnClassifiesLosses(t *testing.T) {
	switch os.Getenv(museProcessTestRoleEnv) {
	case "harness":
		runMuseProcessHarness(t)
		return
	case "emitter":
		runMuseProcessEmitter(t)
		return
	}
	if os.Getenv(museProcessTestRunEnv) != "1" {
		t.Skip("set GIFFORD_TEST_MUSE_PROCESS_RUN=1 for real /proc churn")
	}

	const total = 39
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID, ProcessStartTime: process.ProcessStartTime,
		UID: process.UID, GID: process.GID,
	}
	queue, err := nativeevents.NewQueue(nativeevents.Lineage{
		NodeID: "node-real-churn", SessionID: "session-real-churn", AgentID: "agent-real-churn",
		Harness: protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	})
	require.NoError(t, err)
	runtimeDir, err := os.MkdirTemp("", "gm-real-") //nolint:usetesting // keep the Unix socket path below its platform limit
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(runtimeDir)) })
	server, err := nativeevents.Listen(t.Context(), nativeevents.Options{
		RuntimeDir: runtimeDir, Root: root, Queue: queue,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	drainCtx, cancelDrain := context.WithCancel(t.Context())
	t.Cleanup(cancelDrain)
	var stored atomic.Int64
	storedTypes := make(map[protocol.EventType]int)
	var storedMu sync.Mutex
	go func() {
		for {
			select {
			case <-drainCtx.Done():
				return
			case <-queue.HeadChanged():
			}
			for {
				head, ok := queue.Head()
				if !ok {
					break
				}
				storedMu.Lock()
				storedTypes[head.EventType]++
				storedMu.Unlock()
				queue.Settle(protocol.EmitEventResult{
					EventID: head.EventID, Status: protocol.EmitEventStatusStored,
				})
				stored.Add(1)
			}
		}
	}()

	gate := filepath.Join(t.TempDir(), "gate")
	harness := museProcessHelperCommand(t.Context(), "harness", map[string]string{
		museProcessTestSocketEnv:          server.Path(),
		museProcessTestGateEnv:            gate,
		"GIFFORD_TEST_MUSE_PROCESS_TOTAL": strconv.Itoa(total),
	})
	output, err := harness.CombinedOutput()
	require.NoErrorf(t, err, "harness helper: %s", output)

	results := museProcessResults(string(output))
	require.Lenf(t, results, total, "helper output:\n%s", output)
	attemptedTypes := make(map[protocol.EventType]int)
	for index := range total {
		event := museProcessEvent(index)
		require.NoError(t, event.Validate())
		attemptedTypes[event.Type]++
	}
	require.Len(t, attemptedTypes, 12, "every event-producing Muse hook must be attempted")
	acked := 0
	rejections := make(map[string]int)
	errorsByStage := make(map[string]int)
	for _, result := range results {
		switch {
		case result == "ack":
			acked++
		case strings.HasPrefix(result, "reject:"):
			rejections[strings.TrimPrefix(result, "reject:")]++
		case strings.HasPrefix(result, "error:"):
			stage := strings.SplitN(strings.TrimPrefix(result, "error:"), ":", 2)[0]
			errorsByStage[stage]++
		default:
			t.Fatalf("unknown helper result %q", result)
		}
	}
	require.Eventually(t, func() bool {
		return stored.Load() >= int64(acked)
	}, 2*time.Second, time.Millisecond)
	require.Equal(t, int64(acked), stored.Load(), "stored without ACK identifies an ACK-loss path")
	require.Empty(t, errorsByStage, "the stress run should classify losses as explicit hello rejects")
	require.Empty(t, rejections, "unrelated process churn must not reject legitimate hooks")
	require.Equal(t, total, acked)
	require.Equal(t, int64(total), stored.Load())
	require.Equal(t, attemptedTypes, storedTypes)
	t.Logf(
		"acked=%d stored=%d rejected=%v attempted_types=%v stored_types=%v",
		acked, stored.Load(), rejections, attemptedTypes, storedTypes,
	)
}

func TestMuseRealProcessInspectionLatency(t *testing.T) {
	if os.Getenv(museProcessTestRoleEnv) == "latency_harness" {
		runMuseProcessLatencyHarness(t)
		return
	}
	if os.Getenv(museProcessTestLatencyEnv) != "1" {
		t.Skip("set GIFFORD_TEST_MUSE_PROCESS_LATENCY=1 for real /proc latency")
	}

	// The observed workload contains 39 Muse hooks. Sixty-four live leaves plus
	// their harness is deliberately conservative for a Muse-sized hook/subagent/tool subtree.
	const (
		leaves     = 64
		iterations = 500
	)
	directory := t.TempDir()
	ready := filepath.Join(directory, "ready")
	stop := filepath.Join(directory, "stop")
	helper := exec.CommandContext(
		t.Context(),
		os.Args[0], "-test.run=^TestMuseRealProcessInspectionLatency$", "-test.count=1",
	)
	helper.Env = museProcessEnvironment(map[string]string{
		museProcessTestRoleEnv:            "latency_harness",
		museProcessTestLatencyEnv:         "1",
		museProcessTestReadyEnv:           ready,
		museProcessTestGateEnv:            stop,
		"GIFFORD_TEST_MUSE_PROCESS_TOTAL": strconv.Itoa(leaves),
	})
	var output bytes.Buffer
	helper.Stdout, helper.Stderr = &output, &output
	require.NoError(t, helper.Start())
	var (
		waitOnce sync.Once
		waitErr  error
	)
	waitHelper := func() {
		waitOnce.Do(func() {
			_ = os.WriteFile(stop, nil, 0o600)
			waitErr = helper.Wait()
		})
	}
	t.Cleanup(waitHelper)
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, 10*time.Second, time.Millisecond, "latency helper: %s", output.String())

	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	root := protocol.ProcessIdentity{
		PID: process.PID, ProcessStartTime: process.ProcessStartTime,
		UID: process.UID, GID: process.GID,
	}
	inspector := orchestrator.NewProcessInspector("")
	started := time.Now()
	tree, err := inspector.Inspect(root)
	cold := time.Since(started)
	require.NoError(t, err)
	require.Len(t, tree.Descendants, leaves+1)

	samples := make([]time.Duration, 0, iterations)
	for range iterations {
		started = time.Now()
		tree, err = inspector.Inspect(root)
		samples = append(samples, time.Since(started))
		require.NoError(t, err)
		require.Len(t, tree.Descendants, leaves+1)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(percent int) time.Duration {
		index := (len(samples)*percent + 99) / 100
		if index > 0 {
			index--
		}
		return samples[index]
	}
	t.Logf(
		"real /proc inspection descendants=%d iterations=%d cold=%s p50=%s p95=%s p99=%s max=%s",
		len(tree.Descendants), iterations, cold, percentile(50), percentile(95),
		percentile(99), samples[len(samples)-1],
	)

	waitHelper()
	require.NoErrorf(t, waitErr, "latency helper: %s", output.String())
}

func runMuseProcessLatencyHarness(t *testing.T) {
	t.Helper()
	total, err := strconv.Atoi(os.Getenv("GIFFORD_TEST_MUSE_PROCESS_TOTAL"))
	require.NoError(t, err)
	require.Positive(t, total)
	children := make([]*exec.Cmd, 0, total)
	defer func() {
		for _, child := range children {
			_ = child.Process.Kill()
		}
		for _, child := range children {
			_ = child.Wait()
		}
	}()
	for range total {
		child := exec.CommandContext(t.Context(), "sleep", "60")
		require.NoError(t, child.Start())
		children = append(children, child)
	}
	require.NoError(t, os.WriteFile(os.Getenv(museProcessTestReadyEnv), nil, 0o600))
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(os.Getenv(museProcessTestGateEnv))
		return statErr == nil
	}, 15*time.Second, time.Millisecond)
}

func runMuseProcessHarness(t *testing.T) {
	t.Helper()
	total, err := strconv.Atoi(os.Getenv("GIFFORD_TEST_MUSE_PROCESS_TOTAL"))
	require.NoError(t, err)
	require.Positive(t, total)
	gate := os.Getenv(museProcessTestGateEnv)
	require.NotEmpty(t, gate)

	type emitterProcess struct {
		cmd *exec.Cmd
		out bytes.Buffer
	}
	emitters := make([]*emitterProcess, total)
	for index := range total {
		ready := fmt.Sprintf("%s.ready.%d", gate, index)
		cmd := museProcessHelperCommand(t.Context(), "emitter", map[string]string{
			museProcessTestSocketEnv: os.Getenv(museProcessTestSocketEnv),
			museProcessTestGateEnv:   gate,
			museProcessTestReadyEnv:  ready,
			museProcessTestIndexEnv:  strconv.Itoa(index),
		})
		entry := &emitterProcess{cmd: cmd}
		cmd.Stdout, cmd.Stderr = &entry.out, &entry.out
		require.NoError(t, cmd.Start())
		emitters[index] = entry
	}
	require.Eventually(t, func() bool {
		for index := range total {
			if _, statErr := os.Stat(fmt.Sprintf("%s.ready.%d", gate, index)); statErr != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Millisecond)

	churnCtx, cancelChurn := context.WithCancel(context.Background())
	var churn sync.WaitGroup
	for range 1 {
		churn.Add(1)
		go func() {
			defer churn.Done()
			for churnCtx.Err() == nil {
				_ = exec.CommandContext(churnCtx, "sleep", "0.001").Run()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	require.NoError(t, os.WriteFile(gate, nil, 0o600))
	for _, emitter := range emitters {
		require.NoErrorf(t, emitter.cmd.Wait(), "emitter helper: %s", emitter.out.String())
	}
	cancelChurn()
	churn.Wait()
	for _, emitter := range emitters {
		results := museProcessResults(emitter.out.String())
		if len(results) != 1 {
			fmt.Printf("MUSE_RESULT=error:no_result:%q\n", emitter.out.String())
			continue
		}
		fmt.Printf("MUSE_RESULT=%s\n", results[0])
	}
}

func runMuseProcessEmitter(t *testing.T) {
	t.Helper()
	index, err := strconv.Atoi(os.Getenv(museProcessTestIndexEnv))
	if err != nil {
		fmt.Println("MUSE_RESULT=error:index")
		return
	}
	if err := os.WriteFile(os.Getenv(museProcessTestReadyEnv), nil, 0o600); err != nil {
		fmt.Printf("MUSE_RESULT=error:ready:%v\n", err)
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(os.Getenv(museProcessTestGateEnv)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			fmt.Println("MUSE_RESULT=error:gate_timeout")
			return
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv(museProcessTestSocketEnv))
	if err != nil {
		fmt.Printf("MUSE_RESULT=error:dial:%v\n", err)
		return
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	hello := nativeevents.Hello{
		Operation: nativeevents.OperationHello,
		Harness:   protocol.HarnessMuseCode, HarnessVersion: museHarnessVersion,
		EmitterType: nativeevents.EmitterTypeHook, EmitterVersion: museEmitterVersion,
		SchemaVersion: nativeevents.SchemaVersion,
	}
	if err := nativeevents.WriteMessage(conn, hello); err != nil {
		fmt.Printf("MUSE_RESULT=error:write_hello:%v\n", err)
		return
	}
	message, err := nativeevents.ReadMessage(conn)
	if err != nil {
		fmt.Printf("MUSE_RESULT=error:read_hello:%v\n", err)
		return
	}
	result, ok := message.(nativeevents.HelloResult)
	if !ok {
		fmt.Println("MUSE_RESULT=error:hello_type")
		return
	}
	if !result.Accepted {
		fmt.Printf("MUSE_RESULT=reject:%s\n", result.Reason)
		return
	}
	event := museProcessEvent(index)
	if err := nativeevents.WriteMessage(conn, event); err != nil {
		fmt.Printf("MUSE_RESULT=error:write_event:%v\n", err)
		return
	}
	message, err = nativeevents.ReadMessage(conn)
	if err != nil {
		fmt.Printf("MUSE_RESULT=error:read_ack:%v\n", err)
		return
	}
	ack, ok := message.(nativeevents.Ack)
	if !ok || ack.Sequence != event.Sequence || ack.EmitterEventID != event.EmitterEventID {
		fmt.Println("MUSE_RESULT=error:invalid_ack")
		return
	}
	fmt.Println("MUSE_RESULT=ack")
}

func museProcessEvent(index int) nativeevents.Event {
	event := nativeevents.Event{
		Operation: nativeevents.OperationEvent, Sequence: 1,
		EmitterEventID: protocol.OpaqueID(fmt.Sprintf("real-process-event-%d", index)),
		ObservedAt:     time.Now().UnixMilli(), SessionRef: museProtocolSessionRef,
	}
	switch index % 12 {
	case 0:
		event.Type = protocol.EventTypeSessionStarted
	case 1:
		event.Type = protocol.EventTypeSessionEnded
	case 2:
		event.Type, event.TurnRef = protocol.EventTypePromptSubmitted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
	case 3:
		event.Type, event.TurnRef = protocol.EventTypeTurnStarted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
	case 4:
		event.Type, event.TurnRef = protocol.EventTypeTurnStreaming, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
	case 5:
		event.Type, event.TurnRef = protocol.EventTypeToolStarted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
		event.ToolRef = protocol.OpaqueID(fmt.Sprintf("tool-%d", index))
	case 6:
		event.Type, event.TurnRef = protocol.EventTypeApprovalRequested, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
		event.RequestRef = protocol.OpaqueID(fmt.Sprintf("request-%d", index))
	case 7:
		event.Type, event.TurnRef = protocol.EventTypeToolCompleted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
		event.ToolRef = protocol.OpaqueID(fmt.Sprintf("tool-%d", index))
		event.Result = protocol.NativeEventResultOK
	case 8:
		event.Type, event.TurnRef = protocol.EventTypeToolFailed, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
		event.ToolRef = protocol.OpaqueID(fmt.Sprintf("tool-%d", index))
		event.Result = protocol.NativeEventResultFailed
	case 9:
		event.Type, event.TurnRef = protocol.EventTypeSubagentStarted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
	case 10:
		event.Type, event.TurnRef = protocol.EventTypeContextCompactionCompleted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
	case 11:
		event.Type, event.TurnRef = protocol.EventTypeTurnCompleted, protocol.OpaqueID(fmt.Sprintf("turn-%d", index))
		event.Result = protocol.NativeEventResultCancelled
	}
	return event
}

func museProcessHelperCommand(
	ctx context.Context,
	role string,
	values map[string]string,
) *exec.Cmd {
	cmd := exec.CommandContext(
		ctx, os.Args[0], "-test.run=^TestMuseRealProcessChurnClassifiesLosses$", "-test.count=1",
	)
	values[museProcessTestRoleEnv] = role
	values[museProcessTestRunEnv] = "1"
	cmd.Env = museProcessEnvironment(values)
	return cmd
}

func museProcessEnvironment(values map[string]string) []string {
	environment := os.Environ()
	for key, value := range values {
		prefix := key + "="
		filtered := environment[:0]
		for _, entry := range environment {
			if !strings.HasPrefix(entry, prefix) {
				filtered = append(filtered, entry)
			}
		}
		filtered = append(filtered, prefix+value)
		environment = filtered
	}
	return environment
}

func museProcessResults(output string) []string {
	var results []string
	for _, line := range strings.Split(output, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "MUSE_RESULT="); ok {
			results = append(results, value)
		}
	}
	return results
}
