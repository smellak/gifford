package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testPaneHost = "node-1"

func validPaneOperationTarget() PaneOperationTarget {
	return PaneOperationTarget{
		OwnerUID:  1000,
		Socket:    "/run/tmux/default",
		PaneID:    "%7",
		Host:      testPaneHost,
		BootID:    "12345678-1234-1234-1234-123456789abc",
		PanePID:   2345,
		PaneStart: 987654,
	}
}

func TestPaneOperationContractsValidateStrictly(t *testing.T) {
	t.Parallel()

	target := validPaneOperationTarget()
	require.NoError(t, target.Validate())
	require.NoError(t, (PaneOperationParams{Target: target}).Validate())
	require.NoError(t, (PaneAttemptParams{Target: target, ExpectedAttempt: "-"}).Validate())
	require.NoError(t, (PaneAttemptParams{
		Target: target, ExpectedAttempt: "0123456789abcdef0123456789abcdef",
	}).Validate())
	require.NoError(t, (PaneOperationResult{Changed: true}).Validate())

	invalidTargets := map[string]func(*PaneOperationTarget){
		"owner uid":  func(value *PaneOperationTarget) { value.OwnerUID = 0 },
		"socket":     func(value *PaneOperationTarget) { value.Socket = "/run/tmux/../default" },
		"pane id":    func(value *PaneOperationTarget) { value.PaneID = "%07" },
		"host":       func(value *PaneOperationTarget) { value.Host = "Node-1" },
		"boot id":    func(value *PaneOperationTarget) { value.BootID = "12345678-1234-1234-1234-123456789ABc" },
		"pane pid":   func(value *PaneOperationTarget) { value.PanePID = 0 },
		"pane start": func(value *PaneOperationTarget) { value.PaneStart = 0 },
	}
	for name, mutate := range invalidTargets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			invalid := target
			mutate(&invalid)
			require.Error(t, invalid.Validate())
		})
	}

	for _, attempt := range []PaneAttemptID{"", "ABCDEF0123456789abcdef0123456789", "0123"} {
		require.Error(t, (PaneAttemptParams{Target: target, ExpectedAttempt: attempt}).Validate())
	}
}

func TestPaneControlMethodSpecsAndVersionedSchema(t *testing.T) {
	t.Parallel()

	wantParams := map[ControlMethod]any{
		ControlMethodPaneProvision: &PaneOperationParams{},
		ControlMethodPaneTake:      &PaneOperationParams{},
		ControlMethodPaneReturn:    &PaneOperationParams{},
		ControlMethodPaneReconcile: &PaneAttemptParams{},
		ControlMethodPaneRepair:    &PaneOperationParams{},
	}
	for _, spec := range ControlMethodSpecs() {
		want, ok := wantParams[spec.Method]
		if !ok {
			continue
		}
		require.IsType(t, want, spec.NewParams())
		require.IsType(t, &PaneOperationResult{}, spec.NewResult())
		require.True(t, spec.Mutates)
		delete(wantParams, spec.Method)
	}
	require.Empty(t, wantParams)

	request := map[string]any{
		"jsonrpc": "2.0",
		"id":      float64(1),
		"method":  "pane.take",
		"params": map[string]any{
			"target": map[string]any{
				"owner_uid":  float64(1000),
				"socket":     "/run/tmux/default",
				"pane_id":    "%7",
				"host":       testPaneHost,
				"boot_id":    "12345678-1234-1234-1234-123456789abc",
				"pane_pid":   float64(2345),
				"pane_start": float64(987654),
			},
		},
	}
	var frozen, current Root
	for _, root := range Roots() {
		switch root.Name {
		case "rpc_request_v1_7":
			frozen = root
		case "rpc_request_v1_9":
			current = root
		}
	}
	require.NotNil(t, frozen.New)
	require.NotNil(t, current.New)
	require.Error(t, compileSchema(t, frozen).Validate(request))
	require.NoError(t, compileSchema(t, current).Validate(request))
}
