// Package protocol is the frozen contract of the Gifford wire: the event
// envelope, capture payloads, capability manifest, state snapshot, logical
// message and wire envelope, orchestration contracts, Model Gateway
// contracts and the Core ↔ Proxy agent-socket frames, with their JSON
// Schemas in schemas/v1/ and fixtures in testdata/.
//
// Milestones: contracts-v1 freezes the base types (SchemaVersion 1);
// contracts-v1.1 only adds Channel Control Plane types without renaming,
// removing or changing anything shipped in v1; contracts-v1.2 adds the
// metadata-only notification.list contracts without changing prior roots.
// Consumers that accept v1.2 Control API methods use
// rpc_request_v1_2.schema.json; rpc_request.schema.json remains the frozen
// v1.1 request envelope. contracts-v1.4 adds the live delivery.list result
// with acknowledgement metadata and rpc_request_v1_4.schema.json, without
// changing the frozen message.list result. contracts-v1.5 adds the local model
// lifecycle requests, durable selection, and complete call provenance through
// new versioned roots while retaining every earlier schema byte-for-byte.
//
// Invariants: after the freeze, any change is breaking: commit with !,
// bump the schema version, generate into schemas/vN/, update fixtures
// and notify every package owner. Unknown versions fail with
// ErrUnsupportedVersion.
//
// What it does not do: it holds no behaviour beyond validation; it never
// performs I/O, generates IDs, keeps clocks or persists anything.
// DecisionRecord is the inert, replayable record of a gate decision; the
// deciding channel.Decision lives in internal/channel, and no delivery
// function accepts a record.
package protocol

//go:generate go test -run=TestGenerateSchemas -count=1 . -args -update
