package protocol

// This file holds the bootstrap capture payloads of §5.2: one root per
// capture event. Sequence, wall_time and monotonic_ns live on the envelope,
// never in a payload.

// CaptureAttached is the payload of capture.attached.
type CaptureAttached struct {
	CaptureSessionID     OpaqueID   `json:"capture_session_id"`
	AgentID              AgentID    `json:"agent_id"`
	Harness              Harness    `json:"harness"`
	TmuxServer           string     `json:"tmux_server" jsonschema:"minLength=1,maxLength=128"`
	TmuxSession          string     `json:"tmux_session" jsonschema:"minLength=1,maxLength=128"`
	TmuxWindow           string     `json:"tmux_window" jsonschema:"minLength=1,maxLength=128"`
	TmuxPane             TmuxPaneID `json:"tmux_pane"`
	PaneCols             int64      `json:"pane_cols" jsonschema:"minimum=1,maximum=10000"`
	PaneRows             int64      `json:"pane_rows" jsonschema:"minimum=1,maximum=10000"`
	ReplacedExistingPipe bool       `json:"replaced_existing_pipe"`
}

// Validate checks the payload.
func (c CaptureAttached) Validate() error {
	return checkRoot(&c)
}

// CaptureInitialScreen is the payload of capture.initial_screen.
type CaptureInitialScreen struct {
	CaptureSessionID OpaqueID `json:"capture_session_id"`
	SnapshotBlobRef  BlobRef  `json:"snapshot_blob_ref"`
	ScreenHash       Digest   `json:"screen_hash"`
	Cols             int64    `json:"cols" jsonschema:"minimum=1,maximum=10000"`
	Rows             int64    `json:"rows" jsonschema:"minimum=1,maximum=10000"`
}

// Validate checks the payload.
func (c CaptureInitialScreen) Validate() error {
	return checkRoot(&c)
}

// CaptureChunk is the payload of capture.chunk: byte count, ANSI flag and
// the blob reference of the intact bytes.
type CaptureChunk struct {
	CaptureSessionID OpaqueID `json:"capture_session_id"`
	ByteCount        int64    `json:"byte_count" jsonschema:"minimum=1,maximum=1048576"`
	ANSIPresent      bool     `json:"ansi_present"`
	RawBlobRef       BlobRef  `json:"raw_blob_ref"`
}

// Validate checks the payload.
func (c CaptureChunk) Validate() error {
	return checkRoot(&c)
}

// CaptureResize is the payload of capture.resize.
type CaptureResize struct {
	CaptureSessionID OpaqueID `json:"capture_session_id"`
	Cols             int64    `json:"cols" jsonschema:"minimum=1,maximum=10000"`
	Rows             int64    `json:"rows" jsonschema:"minimum=1,maximum=10000"`
}

// Validate checks the payload.
func (c CaptureResize) Validate() error {
	return checkRoot(&c)
}

// CaptureRotated is the payload of capture.rotated.
type CaptureRotated struct {
	CaptureSessionID OpaqueID `json:"capture_session_id"`
	ClosedSegmentID  OpaqueID `json:"closed_segment_id"`
	NewSegmentID     OpaqueID `json:"new_segment_id"`
	Bytes            int64    `json:"bytes" jsonschema:"minimum=0"`
	Compressed       bool     `json:"compressed"`
	BytesCompressed  *int64   `json:"bytes_compressed,omitempty" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (c CaptureRotated) Validate() error {
	if err := checkRoot(&c); err != nil {
		return err
	}
	if c.ClosedSegmentID == c.NewSegmentID {
		return &ValidationError{Path: "new_segment_id", Reason: "must differ from closed_segment_id"}
	}
	if c.BytesCompressed != nil && !c.Compressed {
		return &ValidationError{Path: "bytes_compressed", Reason: "requires compressed"}
	}
	return nil
}

// CapturePaneRecreated is the payload of capture.pane_recreated.
type CapturePaneRecreated struct {
	CaptureSessionID OpaqueID   `json:"capture_session_id"`
	OldPane          TmuxPaneID `json:"old_pane"`
	NewPane          TmuxPaneID `json:"new_pane"`
}

// Validate checks the payload.
func (c CapturePaneRecreated) Validate() error {
	if err := checkRoot(&c); err != nil {
		return err
	}
	if c.OldPane == c.NewPane {
		return &ValidationError{Path: "new_pane", Reason: "must differ from old_pane"}
	}
	return nil
}

// CaptureReattached is the payload of capture.reattached.
type CaptureReattached struct {
	CaptureSessionID OpaqueID `json:"capture_session_id"`
	GapFromSequence  int64    `json:"gap_from_sequence" jsonschema:"minimum=0"`
	GapToSequence    int64    `json:"gap_to_sequence" jsonschema:"minimum=0"`
	GapMS            int64    `json:"gap_ms" jsonschema:"minimum=0"`
}

// Validate checks the payload.
func (c CaptureReattached) Validate() error {
	if err := checkRoot(&c); err != nil {
		return err
	}
	if c.GapToSequence < c.GapFromSequence {
		return &ValidationError{Path: "gap_to_sequence", Reason: "must cover gap_from_sequence"}
	}
	return nil
}

// CaptureGapDetected is the payload of capture.gap_detected.
type CaptureGapDetected struct {
	CaptureSessionID OpaqueID        `json:"capture_session_id"`
	Cause            CaptureGapCause `json:"cause"`
}

// Validate checks the payload.
func (c CaptureGapDetected) Validate() error {
	return checkRoot(&c)
}

// CaptureQuotaReached is the payload of capture.quota_reached.
type CaptureQuotaReached struct {
	CaptureSessionID OpaqueID   `json:"capture_session_id"`
	QuotaScope       QuotaScope `json:"quota_scope"`
	Bytes            int64      `json:"bytes" jsonschema:"minimum=0"`
	LimitBytes       int64      `json:"limit_bytes" jsonschema:"minimum=1"`
}

// Validate checks the payload.
func (c CaptureQuotaReached) Validate() error {
	return checkRoot(&c)
}

// CaptureStopped is the payload of capture.stopped.
type CaptureStopped struct {
	CaptureSessionID OpaqueID          `json:"capture_session_id"`
	Reason           CaptureStopReason `json:"reason"`
	Detail           string            `json:"detail,omitempty" jsonschema:"maxLength=280"`
}

// Validate checks the payload.
func (c CaptureStopped) Validate() error {
	return checkRoot(&c)
}

// registerCapturePayloads adds the capture event payloads to the registry.
func registerCapturePayloads(regs *[]payloadRegistration) {
	*regs = append(*regs,
		payloadRegistration{New: func() any { return &CaptureAttached{} }, Types: []EventType{EventTypeCaptureAttached}},
		payloadRegistration{New: func() any { return &CaptureInitialScreen{} }, Types: []EventType{EventTypeCaptureInitialScreen}},
		payloadRegistration{New: func() any { return &CaptureChunk{} }, Types: []EventType{EventTypeCaptureChunk}},
		payloadRegistration{New: func() any { return &CaptureResize{} }, Types: []EventType{EventTypeCaptureResize}},
		payloadRegistration{New: func() any { return &CaptureRotated{} }, Types: []EventType{EventTypeCaptureRotated}},
		payloadRegistration{New: func() any { return &CapturePaneRecreated{} }, Types: []EventType{EventTypeCapturePaneRecreated}},
		payloadRegistration{New: func() any { return &CaptureReattached{} }, Types: []EventType{EventTypeCaptureReattached}},
		payloadRegistration{New: func() any { return &CaptureGapDetected{} }, Types: []EventType{EventTypeCaptureGapDetected}},
		payloadRegistration{New: func() any { return &CaptureQuotaReached{} }, Types: []EventType{EventTypeCaptureQuotaReached}},
		payloadRegistration{New: func() any { return &CaptureStopped{} }, Types: []EventType{EventTypeCaptureStopped}},
	)
}
