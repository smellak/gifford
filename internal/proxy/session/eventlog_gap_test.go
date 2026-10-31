package session_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

func TestCaptureGapEventYieldsNoReplayWindows(t *testing.T) {
	for _, cause := range []protocol.CaptureGapCause{
		protocol.CaptureGapCauseQuota,
		protocol.CaptureGapCauseIngestDied,
	} {
		t.Run(string(cause), func(t *testing.T) {
			dataDir := t.TempDir()
			id := "gap-" + string(cause)
			log, err := session.CreateEventLog(dataDir, id, session.EventLogOptions{})
			require.NoError(t, err)
			at := time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC)
			emit := func(seq int64, typ protocol.EventType, payload any) {
				log.Emit(session.Event{
					Session: id, Type: typ, Payload: payload, Sequence: seq,
					WallTime: at.Add(time.Duration(seq) * time.Millisecond), MonotonicNS: seq * int64(time.Millisecond),
				})
			}
			emit(0, protocol.EventTypePTYOpened, session.PTYOpened{
				Cols: 80, Rows: 24, Harness: protocol.HarnessCodex,
			})
			emit(1, protocol.EventTypeCaptureGapDetected, protocol.CaptureGapDetected{
				CaptureSessionID: protocol.OpaqueID(id), Cause: cause,
			})
			emit(2, protocol.EventTypePTYClosed, session.PTYClosed{})
			require.NoError(t, log.Close())
			require.NoError(t, os.WriteFile(
				filepath.Join(session.RecordingsDir(dataDir), id+session.RecordingSuffix), nil, 0o600,
			))

			src, err := replay.Open(replay.Options{DataDir: dataDir, SessionID: id})
			require.ErrorIs(t, err, replay.ErrReplay)
			require.Nil(t, src, "a capture gap must yield no replay windows")
		})
	}
}
