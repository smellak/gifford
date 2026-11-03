package controlapi

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestPaneOperationTelemetryLogsOnlyStructuredContentFreeFields(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	telemetry := NewPaneOperationTelemetry(slog.New(slog.NewTextHandler(&output, nil)))
	version := 3
	target := protocol.PaneOperationTarget{
		OwnerUID: 1000, Socket: "/run/tmux/private-value", PaneID: "%7", Host: paneServiceTestHost,
		BootID: paneServiceTestBootID, PanePID: 2345, PaneStart: 987654,
	}
	err := telemetry.Record(t.Context(), PaneOperationTelemetryEvent{
		Operation: PaneAdminReconcile,
		Actor:     PaneOperationActor{UID: 42, PID: 700},
		Target:    target,
		Attempt:   "0123456789abcdef0123456789abcdef",
		Result:    PaneLogFailed,
		Cause:     PaneCauseNotPublished,
		Evidence: &PaneOperationEvidence{
			WriterExited: PaneEvidenceYes, ChildExited: PaneEvidenceYes,
			BindingEqual: PaneEvidenceYes, ComposerEmpty: PaneEvidenceNo,
			LineVisible: PaneEvidenceYes,
			Record: &PaneRecordEvidence{
				Size: 1024, MagicValid: PaneEvidenceYes,
				ChecksumValid: PaneEvidenceYes, Version: &version,
			},
		},
	})
	require.NoError(t, err)
	logged := output.String()
	for _, field := range []string{
		"event=pane_admin_operation", "op=reconcile", "actor.uid=42", "actor.pid=700",
		"target.uid=1000", "target.socket_sha=", "target.pane=%7",
		"attempt=0123456789abcdef0123456789abcdef", "result=failed",
		"cause=pane_not_published",
		"evidence.writer_exited=yes", "evidence.child_exited=yes",
		"evidence.binding_equal=yes", "evidence.composer_empty=no",
		"evidence.line_visible=yes", "evidence.record.st_size=1024",
		"evidence.record.magic_valid=yes", "evidence.record.checksum_valid=yes",
		"evidence.record.version=3",
	} {
		require.Contains(t, logged, field)
	}
	require.NotContains(t, logged, target.Socket)
	require.NotContains(t, strings.ToLower(logged), "capture")
}

func TestPaneOperationTelemetryRejectsInvalidEventsWithoutLogging(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	telemetry := NewPaneOperationTelemetry(slog.New(slog.NewTextHandler(&output, nil)))
	err := telemetry.Record(t.Context(), PaneOperationTelemetryEvent{
		Operation: PaneAdminTake, Actor: PaneOperationActor{UID: 42},
		Attempt: "-", Result: PaneLogChanged,
	})
	require.ErrorIs(t, err, errPaneOperationTelemetry)
	require.Empty(t, output.String())
}
