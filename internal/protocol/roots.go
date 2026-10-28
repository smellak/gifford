package protocol

// This file holds the contract constants and the root registries. Roots()
// lists every versioned document with a JSON Schema and fixtures; later
// slices append their roots here without touching the v1 entries.

const (
	// SchemaVersion is the schema_version of every versioned document.
	SchemaVersion = 1
	// SchemaDialect is the JSON Schema dialect of schemas/v1/.
	SchemaDialect = "https://json-schema.org/draft/2020-12/schema"
	// SchemaIDBase is the $id prefix of schemas/v1/, built from the real
	// module path in go.mod.
	SchemaIDBase = "https://github.com/smellak/gifford/schemas/v1/"
	// WireProtocol is the protocol name carried in every wire envelope.
	WireProtocol = "gifford-message"
	// WireVersion is the wire envelope version.
	WireVersion = 1
	// WireContentType is the content type of every wire envelope.
	WireContentType = "application/gifford-message+json"
	// JSONRPCVersion is the JSON-RPC version of the Control API.
	JSONRPCVersion = "2.0"
	// MilestoneV1 marks roots frozen at contracts-v1.
	MilestoneV1 = "contracts-v1"
	// MilestoneV11 marks roots added at contracts-v1.1.
	MilestoneV11 = "contracts-v1.1"
)

// Root describes one versioned document: its schema file, its fixture
// directory and a constructor for its zero value.
type Root struct {
	// Name is the schema and fixture name (snake_case of the Go type).
	Name string
	// Since is the milestone that introduced the root.
	Since string
	// New returns a pointer to a zero value of the root type.
	New func() any
}

// Roots returns every registered root, v1 first. The slice is fresh.
func Roots() []Root {
	return []Root{
		{Name: "event_envelope", Since: MilestoneV1, New: func() any { return &EventEnvelope{} }},
		{Name: "capture_attached", Since: MilestoneV1, New: func() any { return &CaptureAttached{} }},
		{Name: "capture_initial_screen", Since: MilestoneV1, New: func() any { return &CaptureInitialScreen{} }},
		{Name: "capture_chunk", Since: MilestoneV1, New: func() any { return &CaptureChunk{} }},
		{Name: "capture_resize", Since: MilestoneV1, New: func() any { return &CaptureResize{} }},
		{Name: "capture_rotated", Since: MilestoneV1, New: func() any { return &CaptureRotated{} }},
		{Name: "capture_pane_recreated", Since: MilestoneV1, New: func() any { return &CapturePaneRecreated{} }},
		{Name: "capture_reattached", Since: MilestoneV1, New: func() any { return &CaptureReattached{} }},
		{Name: "capture_gap_detected", Since: MilestoneV1, New: func() any { return &CaptureGapDetected{} }},
		{Name: "capture_quota_reached", Since: MilestoneV1, New: func() any { return &CaptureQuotaReached{} }},
		{Name: "capture_stopped", Since: MilestoneV1, New: func() any { return &CaptureStopped{} }},
		{Name: "capability_manifest", Since: MilestoneV1, New: func() any { return &CapabilityManifest{} }},
	}
}
