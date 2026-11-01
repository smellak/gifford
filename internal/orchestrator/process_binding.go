package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	processBindingStateBound   = "BOUND"
	processBindingStateBlocked = "BLOCKED"
	untrustedProcessDetail     = "untrusted harness descendant present"
)

type processBinding struct {
	state               string
	agent               protocol.AgentID
	bootID              string
	parent              ProcessToken
	child               ProcessToken
	layoutVersion       int64
	childExecutable     string
	auxiliary           ProcessToken
	auxiliaryExecutable string
	hasAuxiliary        bool
}

type processLayout struct {
	harness   ProcessNode
	auxiliary *ProcessNode
}

// processGuard binds the one direct harness child on first use and then
// revalidates that exact parent, child, executable and system boot on every
// use. pinLayout is true before PREPARE: it durably fixes whether the measured
// auxiliary is absent or identifies its exact process. The pre-COMMIT call
// passes false and may only revalidate that fixed layout, never learn another.
func (e *Executor) processGuard(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	pinLayout bool,
) (bool, string, error) {
	if !record.sessionID.Valid() {
		return false, "session process identity is unavailable", nil
	}
	binding, found, err := loadProcessBinding(ctx, tx, record.sessionID)
	if err != nil {
		return false, "", err
	}
	if found && binding.state == processBindingStateBlocked {
		return false, "session process binding is permanently blocked", nil
	}
	if found && !pinLayout && (record.processBindingVersion < 1 ||
		binding.layoutVersion != record.processBindingVersion) {
		return false, "process binding changed after preparation", nil
	}

	tree, inspectErr := e.processes.Inspect(record.process)
	if inspectErr != nil {
		if isProcessObservationIncomplete(inspectErr) {
			return false, truncate(inspectErr.Error(), 200), nil
		}
		if err := e.blockProcessBinding(ctx, tx, record, "OBSERVATION_FAILED"); err != nil {
			return false, "", err
		}
		return false, truncate(inspectErr.Error(), 200), nil
	}
	parent := ProcessToken{PID: record.process.PID, StartTime: record.process.ProcessStartTime}
	if tree.Root != parent {
		if err := e.blockProcessBinding(ctx, tx, record, "PARENT_CHANGED"); err != nil {
			return false, "", err
		}
		return false, "session process identity changed", nil
	}
	tree = e.withoutRegisteredEndpoint(record, tree)
	auxiliaryName, err := e.allowedAuxiliaryExecutable(ctx, tx, record)
	if err != nil {
		return false, "", err
	}

	if !found {
		if !pinLayout {
			return false, "session process binding was not fixed before preparation", nil
		}
		layout, detail := validateProcessLayout(tree, nil, auxiliaryName)
		if detail != "" {
			return false, detail, nil
		}
		binding = bindingFromLayout(record, tree, parent, layout)
		if err := e.insertProcessBinding(ctx, tx, record.sessionID, binding); err != nil {
			return false, "", err
		}
		return true, "", nil
	}

	ok, detail, blockReason := validStoredProcessCore(binding, record, tree, parent)
	if !ok {
		if err := e.blockProcessBinding(ctx, tx, record, blockReason); err != nil {
			return false, "", err
		}
		return false, detail, nil
	}
	layout, detail := validateProcessLayout(tree, &binding, auxiliaryName)
	if detail != "" {
		return false, detail, nil
	}
	if binding.childExecutable == "" {
		if !pinLayout {
			return false, "bound harness executable was not fixed before preparation", nil
		}
		binding.childExecutable = layout.harness.Executable
	}
	if pinLayout {
		setBindingAuxiliary(&binding, layout.auxiliary)
		if err := e.pinProcessLayout(ctx, tx, record.sessionID, binding); err != nil {
			return false, "", err
		}
		return true, "", nil
	}
	if !bindingMatchesLayout(binding, layout) {
		return false, "process layout changed after preparation", nil
	}
	return true, "", nil
}

// withoutRegisteredEndpoint removes only the exact live endpoint node for
// this agent session. Its children retain their original parent edge, so an
// endpoint can never confer trust on a subtree.
func (e *Executor) withoutRegisteredEndpoint(
	record deliveryRecord,
	tree ProcessTree,
) ProcessTree {
	if e.endpoints == nil {
		return tree
	}
	pid, start, ok := e.endpoints.RegisteredEndpointProcess(record.AgentID, record.sessionID)
	if !ok || pid <= 0 || start < 0 {
		return tree
	}
	target := ProcessToken{PID: pid, StartTime: start}
	descendants, matches := withoutProcessNode(tree.Descendants, target)
	if matches != 1 {
		return tree
	}
	tree.Descendants = descendants
	return tree
}

func withoutProcessNode(nodes []ProcessNode, target ProcessToken) ([]ProcessNode, int) {
	filtered := make([]ProcessNode, 0, len(nodes))
	matches := 0
	for _, node := range nodes {
		if node.Token == target {
			matches++
			continue
		}
		filtered = append(filtered, node)
	}
	return filtered, matches
}

func validStoredProcessCore(
	binding processBinding,
	record deliveryRecord,
	tree ProcessTree,
	parent ProcessToken,
) (bool, string, string) {
	switch {
	case binding.agent != record.AgentID:
		return false, "session agent identity changed", "AGENT_CHANGED"
	case binding.bootID != tree.BootID:
		return false, "system boot identity changed", "BOOT_CHANGED"
	case binding.parent != parent:
		return false, "session process identity changed", "PARENT_CHANGED"
	}
	matches := 0
	var harness ProcessNode
	for _, node := range tree.Descendants {
		if node.Token == binding.child && node.Parent == parent {
			matches++
			harness = node
		}
	}
	if matches != 1 {
		return false, "bound harness process identity changed", "CHILD_CHANGED"
	}
	if binding.childExecutable != "" && harness.Executable != binding.childExecutable {
		return false, "bound harness executable changed", "CHILD_EXECUTABLE_CHANGED"
	}
	return true, "", ""
}

// validateProcessLayout is the single structural rule used by preview, the
// binding guard and pre-COMMIT revalidation. After endpoint removal the tree
// is exactly harness, or harness -> one profile-declared sibling executable.
func validateProcessLayout(
	tree ProcessTree,
	binding *processBinding,
	auxiliaryName string,
) (processLayout, string) {
	direct := make([]ProcessNode, 0, 2)
	for _, node := range tree.Descendants {
		if node.Parent == tree.Root {
			direct = append(direct, node)
		}
	}
	if len(direct) != 1 {
		if binding == nil {
			return processLayout{}, fmt.Sprintf("expected one direct harness child, found %d", len(direct))
		}
		return processLayout{}, untrustedProcessDetail
	}
	harness := direct[0]
	if binding != nil && harness.Token != binding.child {
		return processLayout{}, "bound harness process identity changed"
	}
	if !validExecutablePath(harness.Executable) {
		return processLayout{}, "harness executable identity is unavailable"
	}
	layout := processLayout{harness: harness}
	if len(tree.Descendants) == 1 {
		return layout, ""
	}
	if len(tree.Descendants) != 2 || auxiliaryName == "" {
		return processLayout{}, untrustedProcessDetail
	}
	var auxiliary *ProcessNode
	for index := range tree.Descendants {
		node := &tree.Descendants[index]
		if node.Token == harness.Token {
			continue
		}
		if auxiliary != nil {
			return processLayout{}, untrustedProcessDetail
		}
		auxiliary = node
	}
	if auxiliary == nil || auxiliary.Parent != harness.Token ||
		!validExecutablePath(auxiliary.Executable) {
		return processLayout{}, untrustedProcessDetail
	}
	expected := filepath.Join(filepath.Dir(harness.Executable), auxiliaryName)
	if auxiliary.Executable != expected {
		return processLayout{}, untrustedProcessDetail
	}
	layout.auxiliary = auxiliary
	return layout, ""
}

func validExecutablePath(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path &&
		!strings.HasSuffix(path, " (deleted)")
}

func (e *Executor) allowedAuxiliaryExecutable(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT manifest_json FROM sessions
		WHERE session_id = ? AND agent_id = ?`, record.sessionID, record.AgentID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load process profile: %w", err)
	}
	manifest, ok := validProcessManifest(raw, record.AgentID)
	if !ok {
		return "", nil
	}
	auxiliary, ok := e.processAuxiliaryFor(manifest)
	if !ok {
		return "", nil
	}
	return auxiliary, nil
}

func validProcessManifest(raw string, agent protocol.AgentID) (protocol.CapabilityManifest, bool) {
	var manifest protocol.CapabilityManifest
	if json.Unmarshal([]byte(raw), &manifest) != nil || manifest.Validate() != nil ||
		manifest.AgentID != agent {
		return protocol.CapabilityManifest{}, false
	}
	return manifest, true
}

func (e *Executor) processAuxiliaryFor(manifest protocol.CapabilityManifest) (string, bool) {
	version := profiles.ParseVersion(manifest.HarnessVersion)
	if version == "" {
		return "", false
	}
	profile, err := e.processProfiles.Profile(string(manifest.Harness), version)
	if err != nil || profile.Harness != string(manifest.Harness) {
		return "", false
	}
	return profile.AuxiliaryExecutable, true
}

func bindingFromLayout(
	record deliveryRecord,
	tree ProcessTree,
	parent ProcessToken,
	layout processLayout,
) processBinding {
	binding := processBinding{
		state: processBindingStateBound, agent: record.AgentID, bootID: tree.BootID,
		parent: parent, child: layout.harness.Token, layoutVersion: 1,
		childExecutable: layout.harness.Executable,
	}
	setBindingAuxiliary(&binding, layout.auxiliary)
	return binding
}

func setBindingAuxiliary(binding *processBinding, auxiliary *ProcessNode) {
	binding.hasAuxiliary = auxiliary != nil
	binding.auxiliary = ProcessToken{}
	binding.auxiliaryExecutable = ""
	if auxiliary != nil {
		binding.auxiliary = auxiliary.Token
		binding.auxiliaryExecutable = auxiliary.Executable
	}
}

func bindingMatchesLayout(binding processBinding, layout processLayout) bool {
	if binding.childExecutable != layout.harness.Executable ||
		binding.hasAuxiliary != (layout.auxiliary != nil) {
		return false
	}
	return layout.auxiliary == nil ||
		(binding.auxiliary == layout.auxiliary.Token &&
			binding.auxiliaryExecutable == layout.auxiliary.Executable)
}

func loadProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	session protocol.OpaqueID,
) (processBinding, bool, error) {
	var binding processBinding
	var bootID, childExecutable, auxiliaryExecutable, blockReason sql.NullString
	var parentPID, parentStart, childPID, childStart, layoutVersion sql.NullInt64
	var auxiliaryPID, auxiliaryStart sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT state, agent_id, boot_id,
		parent_pid, parent_start_time, child_pid, child_start_time,
		layout_version, child_executable, auxiliary_pid, auxiliary_start_time,
		auxiliary_executable, block_reason
		FROM orchestrator_process_bindings WHERE session_id = ?`, session).Scan(
		&binding.state, &binding.agent, &bootID,
		&parentPID, &parentStart, &childPID, &childStart,
		&layoutVersion, &childExecutable, &auxiliaryPID, &auxiliaryStart,
		&auxiliaryExecutable, &blockReason)
	if errors.Is(err, sql.ErrNoRows) {
		return processBinding{}, false, nil
	}
	if err != nil {
		return processBinding{}, false, fmt.Errorf("load process binding: %w", err)
	}
	binding.bootID = bootID.String
	binding.parent = ProcessToken{PID: parentPID.Int64, StartTime: parentStart.Int64}
	binding.child = ProcessToken{PID: childPID.Int64, StartTime: childStart.Int64}
	binding.layoutVersion = layoutVersion.Int64
	binding.childExecutable = childExecutable.String
	if layoutVersion.Valid && layoutVersion.Int64 < 1 {
		return processBinding{}, false, fmt.Errorf("load process binding: invalid layout version")
	}
	if childExecutable.Valid && !validExecutablePath(childExecutable.String) {
		return processBinding{}, false, fmt.Errorf("load process binding: invalid harness executable")
	}
	auxiliaryFields := 0
	if auxiliaryPID.Valid {
		auxiliaryFields++
	}
	if auxiliaryStart.Valid {
		auxiliaryFields++
	}
	if auxiliaryExecutable.Valid {
		auxiliaryFields++
	}
	if auxiliaryFields != 0 && auxiliaryFields != 3 {
		return processBinding{}, false, fmt.Errorf("load process binding: incomplete auxiliary identity")
	}
	if auxiliaryFields == 3 {
		binding.hasAuxiliary = true
		binding.auxiliary = ProcessToken{PID: auxiliaryPID.Int64, StartTime: auxiliaryStart.Int64}
		binding.auxiliaryExecutable = auxiliaryExecutable.String
		if binding.auxiliary.PID < 1 || binding.auxiliary.StartTime < 0 ||
			!validExecutablePath(binding.auxiliaryExecutable) {
			return processBinding{}, false, fmt.Errorf("load process binding: invalid auxiliary identity")
		}
	}
	return binding, true, nil
}

func (e *Executor) insertProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	session protocol.OpaqueID,
	binding processBinding,
) error {
	stamp := eventstore.FormatTime(e.clock.Now().UTC())
	auxiliaryPID, auxiliaryStart, auxiliaryExecutable := bindingAuxiliaryValues(binding)
	_, err := tx.ExecContext(ctx, `INSERT INTO orchestrator_process_bindings (
		session_id, agent_id, state, boot_id, parent_pid, parent_start_time,
		child_pid, child_start_time, layout_version, child_executable, auxiliary_pid,
		auxiliary_start_time, auxiliary_executable, block_reason, created_at, updated_at
	) VALUES (?, ?, 'BOUND', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
		session, binding.agent, binding.bootID,
		binding.parent.PID, binding.parent.StartTime,
		binding.child.PID, binding.child.StartTime, binding.layoutVersion, binding.childExecutable,
		auxiliaryPID, auxiliaryStart, auxiliaryExecutable, stamp, stamp)
	if err != nil {
		return fmt.Errorf("bind harness process: %w", err)
	}
	return nil
}

func (e *Executor) pinProcessLayout(
	ctx context.Context,
	tx *sql.Tx,
	session protocol.OpaqueID,
	binding processBinding,
) error {
	stamp := eventstore.FormatTime(e.clock.Now().UTC())
	auxiliaryPID, auxiliaryStart, auxiliaryExecutable := bindingAuxiliaryValues(binding)
	result, err := tx.ExecContext(ctx, `UPDATE orchestrator_process_bindings SET
		layout_version = COALESCE(layout_version, 0) + 1,
		child_executable = ?, auxiliary_pid = ?, auxiliary_start_time = ?,
		auxiliary_executable = ?, updated_at = ?
		WHERE session_id = ? AND state = 'BOUND'
		AND child_pid = ? AND child_start_time = ?`,
		binding.childExecutable, auxiliaryPID, auxiliaryStart, auxiliaryExecutable, stamp,
		session, binding.child.PID, binding.child.StartTime)
	if err != nil {
		return fmt.Errorf("pin process layout: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("pin process layout result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("pin process layout: binding changed")
	}
	return nil
}

func bindingAuxiliaryValues(binding processBinding) (any, any, any) {
	if !binding.hasAuxiliary {
		return nil, nil, nil
	}
	return binding.auxiliary.PID, binding.auxiliary.StartTime, binding.auxiliaryExecutable
}

func captureProcessBindingVersion(
	ctx context.Context,
	tx *sql.Tx,
	record *deliveryRecord,
) error {
	binding, found, err := loadProcessBinding(ctx, tx, record.sessionID)
	if err != nil {
		return err
	}
	if !found || binding.state != processBindingStateBound || binding.layoutVersion < 1 ||
		binding.childExecutable == "" {
		return fmt.Errorf("capture process binding: binding is unavailable")
	}
	record.processBindingVersion = binding.layoutVersion
	return nil
}

func (e *Executor) blockProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	reason string,
) error {
	stamp := eventstore.FormatTime(e.clock.Now().UTC())
	_, err := tx.ExecContext(ctx, `INSERT INTO orchestrator_process_bindings (
		session_id, agent_id, state, block_reason, created_at, updated_at
	) VALUES (?, ?, 'BLOCKED', ?, ?, ?)
	ON CONFLICT(session_id) DO UPDATE SET
		state = 'BLOCKED', block_reason = excluded.block_reason,
		updated_at = excluded.updated_at`,
		record.sessionID, record.AgentID, reason, stamp, stamp)
	if err != nil {
		return fmt.Errorf("block harness process binding: %w", err)
	}
	return nil
}
