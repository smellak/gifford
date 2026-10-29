package capture

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Sentinel errors of the capture package. Callers map them to exit codes;
// tests compare with errors.Is.
var (
	ErrUsage           = errors.New("invalid capture invocation")
	ErrPipeExists      = errors.New("pane already has a pipe")
	ErrAlreadyCaptured = errors.New("agent or pane already captured")
	ErrNotCaptured     = errors.New("no live capture for the agent")
	ErrPaneNotFound    = errors.New("pane not found")
	ErrTargetChanged   = errors.New("capture target changed")
	ErrTmuxCommand     = errors.New("tmux command failed")
	ErrUnsafePath      = errors.New("unsafe capture path")
	ErrAttach          = errors.New("capture attach failed")
	ErrAttachCommitted = errors.New("capture attach committed")
	ErrStopPending     = errors.New("stop requested but not yet honoured")
	ErrBusy            = errors.New("capture lock busy")
	ErrQuota           = errors.New("capture budget refused")
)

// AttachOptions configures Attach. Target is the tmux pane target, Socket
// the server socket (empty means discover it), Data the data directory
// (empty means the platform default), and Limits overrides selected
// bounds; zero fields take the defaults.
type AttachOptions struct {
	Agent   string
	Harness protocol.Harness
	Target  string
	Socket  string
	Replace bool
	Data    string
	Limits  Limits
}

// IngestOptions configures Ingest. In is the pane stream: fd 0 under
// tmux, an os.Pipe end in tests. StartSequence and Attempt bind the run
// to its committed boundary.
type IngestOptions struct {
	Data          string
	Agent         string
	Session       string
	StartSequence int64
	Attempt       string
	In            *os.File
}

// SuperviseOptions configures Supervise. Once runs a single pass.
type SuperviseOptions struct {
	Data string
	Once bool
}

// StatusOptions configures Status.
type StatusOptions struct {
	Data  string
	Agent string
}

// StopOptions configures Stop. Reason is manual or relaunched_with_run.
type StopOptions struct {
	Data   string
	Agent  string
	Reason string
	Detail string
}

// Limits bounds one capture session. Durations marshal as strings.
// Zero values mean unset and take the defaults through resolve.
type Limits struct {
	ChunkMaxBytes     int64    `json:"chunk_max_bytes"`
	FrameMaxBytes     int64    `json:"frame_max_bytes"`
	SegmentMaxBytes   int64    `json:"segment_max_bytes"`
	SegmentMaxAge     Duration `json:"segment_max_age"`
	ChunkMaxAge       Duration `json:"chunk_max_age"`
	FlushInterval     Duration `json:"flush_interval"`
	QueueMaxBytes     int64    `json:"queue_max_bytes"`
	ResizeInterval    Duration `json:"resize_interval"`
	ReserveBlockBytes int64    `json:"reserve_block_bytes"`
	SnapshotMaxBytes  int64    `json:"snapshot_max_bytes"`
	LogMaxBytes       int64    `json:"log_max_bytes"`
	AgentQuotaBytes   int64    `json:"agent_quota_bytes"`
	NodeQuotaBytes    int64    `json:"node_quota_bytes"`
}

// DefaultLimits returns the bounds attach writes.
func DefaultLimits() Limits {
	return Limits{
		ChunkMaxBytes:     16384,
		FrameMaxBytes:     4194304,
		SegmentMaxBytes:   67108864,
		SegmentMaxAge:     Duration(time.Hour),
		ChunkMaxAge:       Duration(50 * time.Millisecond),
		FlushInterval:     Duration(250 * time.Millisecond),
		QueueMaxBytes:     16777216,
		ResizeInterval:    Duration(time.Second),
		ReserveBlockBytes: 16777216,
		SnapshotMaxBytes:  67108864,
		LogMaxBytes:       1048576,
		AgentQuotaBytes:   4294967296,
		NodeQuotaBytes:    21474836480,
	}
}

// resolve fills unset (zero) fields with the defaults.
func (l Limits) resolve() Limits {
	d := DefaultLimits()
	out := l
	if out.ChunkMaxBytes == 0 {
		out.ChunkMaxBytes = d.ChunkMaxBytes
	}
	if out.FrameMaxBytes == 0 {
		out.FrameMaxBytes = d.FrameMaxBytes
	}
	if out.SegmentMaxBytes == 0 {
		out.SegmentMaxBytes = d.SegmentMaxBytes
	}
	if out.SegmentMaxAge == 0 {
		out.SegmentMaxAge = d.SegmentMaxAge
	}
	if out.ChunkMaxAge == 0 {
		out.ChunkMaxAge = d.ChunkMaxAge
	}
	if out.FlushInterval == 0 {
		out.FlushInterval = d.FlushInterval
	}
	if out.QueueMaxBytes == 0 {
		out.QueueMaxBytes = d.QueueMaxBytes
	}
	if out.ResizeInterval == 0 {
		out.ResizeInterval = d.ResizeInterval
	}
	if out.ReserveBlockBytes == 0 {
		out.ReserveBlockBytes = d.ReserveBlockBytes
	}
	if out.SnapshotMaxBytes == 0 {
		out.SnapshotMaxBytes = d.SnapshotMaxBytes
	}
	if out.LogMaxBytes == 0 {
		out.LogMaxBytes = d.LogMaxBytes
	}
	if out.AgentQuotaBytes == 0 {
		out.AgentQuotaBytes = d.AgentQuotaBytes
	}
	if out.NodeQuotaBytes == 0 {
		out.NodeQuotaBytes = d.NodeQuotaBytes
	}
	return out
}

// Validate checks every rule of the limits table. Callers wrap the error
// in the sentinel their path needs.
func (l Limits) Validate() error {
	bad := func(format string, args ...any) error {
		return errors.New("limits: " + fmt.Sprintf(format, args...))
	}
	if l.ChunkMaxBytes < 1 || l.ChunkMaxBytes > 1048576 {
		return bad("chunk_max_bytes %d outside 1-1048576", l.ChunkMaxBytes)
	}
	if l.ChunkMaxBytes > l.FrameMaxBytes {
		return bad("chunk_max_bytes %d above frame_max_bytes %d", l.ChunkMaxBytes, l.FrameMaxBytes)
	}
	if l.FrameMaxBytes > l.SegmentMaxBytes {
		return bad("frame_max_bytes %d above segment_max_bytes %d", l.FrameMaxBytes, l.SegmentMaxBytes)
	}
	if l.FrameMaxBytes*4096 < l.SegmentMaxBytes {
		return bad("frame_max_bytes %d below segment_max_bytes/4096", l.FrameMaxBytes)
	}
	if l.SegmentMaxBytes < 1 || l.SegmentMaxBytes > 1073741824 {
		return bad("segment_max_bytes %d outside 1-1073741824", l.SegmentMaxBytes)
	}
	checkAge := func(name string, v Duration, lo, hi time.Duration) error {
		if time.Duration(v) < lo || time.Duration(v) > hi {
			return bad("%s %s outside %s-%s", name, time.Duration(v), lo, hi)
		}
		return nil
	}
	if err := checkAge("segment_max_age", l.SegmentMaxAge, time.Second, 24*time.Hour); err != nil {
		return err
	}
	if err := checkAge("chunk_max_age", l.ChunkMaxAge, time.Millisecond, time.Second); err != nil {
		return err
	}
	if err := checkAge("flush_interval", l.FlushInterval, 10*time.Millisecond, 10*time.Second); err != nil {
		return err
	}
	if err := checkAge("resize_interval", l.ResizeInterval, 100*time.Millisecond, time.Hour); err != nil {
		return err
	}
	if l.QueueMaxBytes < 65536 || l.QueueMaxBytes > 268435456 {
		return bad("queue_max_bytes %d outside 65536-268435456", l.QueueMaxBytes)
	}
	if l.QueueMaxBytes < 2*l.ChunkMaxBytes {
		return bad("queue_max_bytes %d below 2x chunk_max_bytes", l.QueueMaxBytes)
	}
	if l.ReserveBlockBytes < 2*l.ChunkMaxBytes+4096 {
		return bad("reserve_block_bytes %d below 2x chunk_max_bytes + 4096", l.ReserveBlockBytes)
	}
	if l.ReserveBlockBytes > l.AgentQuotaBytes {
		return bad("reserve_block_bytes %d above agent_quota_bytes %d", l.ReserveBlockBytes, l.AgentQuotaBytes)
	}
	if l.SnapshotMaxBytes < 65536 || l.SnapshotMaxBytes > 268435456 {
		return bad("snapshot_max_bytes %d outside 65536-268435456", l.SnapshotMaxBytes)
	}
	if l.LogMaxBytes < 4096 || l.LogMaxBytes > 16777216 {
		return bad("log_max_bytes %d outside 4096-16777216", l.LogMaxBytes)
	}
	if l.AgentQuotaBytes < 131072+l.LogMaxBytes+l.SnapshotMaxBytes+l.ReserveBlockBytes {
		return bad("agent_quota_bytes %d below control, log, snapshot and reserve", l.AgentQuotaBytes)
	}
	if l.NodeQuotaBytes < l.AgentQuotaBytes+65536 {
		return bad("node_quota_bytes %d below agent_quota_bytes + 65536", l.NodeQuotaBytes)
	}
	return nil
}

// Duration is a time.Duration that marshals as a duration string.
type Duration time.Duration

// MarshalJSON renders the duration in string form.
func (d Duration) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(time.Duration(d).String())
	if err != nil {
		return nil, fmt.Errorf("duration: marshal: %w", err)
	}
	return b, nil
}

// UnmarshalJSON parses a duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}
