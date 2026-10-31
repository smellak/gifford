package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// testDB is a fresh, migrated node database.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store.DB()
}

// sqlFixture is a gateway whose records go to db, under random call ids
// so that several fixtures can share one database.
func sqlFixture(t *testing.T, db *sql.DB, url string, tr Transport) fixture {
	t.Helper()
	rec, err := NewSQLRecorder(db)
	require.NoError(t, err)
	return newFixture(t, url, tr, func(d *Deps) { d.Recorder, d.IDs = rec, RandomCallIDs{} })
}

// storedCall reads one glimmer_calls row back as a record.
func storedCall(t *testing.T, db *sql.DB, id protocol.OpaqueID) protocol.GlimmerCallRecord {
	t.Helper()
	var rec protocol.GlimmerCallRecord
	var agent, adapterName, adapterRevision, adapterSHA, datasetVersion, datasetSHA, trainingRun,
		trainingCode, quantization, runtimeVersion, gpu, requestID, probabilities sql.NullString
	var created string
	err := db.QueryRowContext(t.Context(), `SELECT call_id, contract, agent_id, inference_profile,
		base_model_repo, base_model_revision, adapter_name, adapter_revision, adapter_sha256,
		dataset_version, dataset_sha256, training_run_id, training_code_revision, quantization,
		runtime, runtime_version, gpu, request_id, latency_ms, redaction_policy_version,
		redactions_count, input_sha256, output_sha256, probabilities_json, status, created_at
		FROM glimmer_calls WHERE call_id = ?`, string(id)).Scan(
		&rec.CallID, &rec.Contract, &agent, &rec.InferenceProfile,
		&rec.BaseModelRepo, &rec.BaseModelRevision, &adapterName, &adapterRevision, &adapterSHA,
		&datasetVersion, &datasetSHA, &trainingRun, &trainingCode, &quantization,
		&rec.Runtime, &runtimeVersion, &gpu, &requestID, &rec.LatencyMS, &rec.RedactionPolicyVersion,
		&rec.RedactionsCount, &rec.InputSHA256, &rec.OutputSHA256, &probabilities, &rec.Status, &created)
	require.NoError(t, err)
	rec.AgentID = protocol.AgentID(agent.String)
	rec.AdapterName, rec.AdapterRevision = adapterName.String, adapterRevision.String
	rec.AdapterSHA256 = protocol.SHA256Hex(adapterSHA.String)
	rec.DatasetVersion, rec.DatasetSHA256 = datasetVersion.String, protocol.SHA256Hex(datasetSHA.String)
	rec.TrainingRunID, rec.TrainingCodeRevision = protocol.OpaqueID(trainingRun.String), trainingCode.String
	rec.Quantization, rec.RuntimeVersion, rec.GPU = quantization.String, runtimeVersion.String, gpu.String
	rec.RequestID = protocol.OpaqueID(requestID.String)
	if probabilities.Valid {
		require.NoError(t, json.Unmarshal([]byte(probabilities.String), &rec.Probabilities))
	}
	rec.CreatedAt, err = eventstore.ParseTime(created)
	require.NoError(t, err, "created_at is in the event store's time format")
	return rec
}

// requireStored checks that the row of rec holds exactly rec, and NULL in
// each optional column rec leaves empty.
func requireStored(t *testing.T, db *sql.DB, rec protocol.GlimmerCallRecord) {
	t.Helper()
	got := storedCall(t, db, rec.CallID)
	require.True(t, rec.CreatedAt.Equal(got.CreatedAt))
	got.CreatedAt = rec.CreatedAt
	require.Equal(t, rec, got)
	var nulls int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT
		(agent_id IS NULL) + (adapter_name IS NULL) + (adapter_revision IS NULL) +
		(adapter_sha256 IS NULL) + (dataset_version IS NULL) + (dataset_sha256 IS NULL) +
		(training_run_id IS NULL) + (training_code_revision IS NULL) + (quantization IS NULL) +
		(runtime_version IS NULL) + (gpu IS NULL) + (request_id IS NULL) + (probabilities_json IS NULL)
		FROM glimmer_calls WHERE call_id = ?`, string(rec.CallID)).Scan(&nulls))
	empty := 0
	for _, v := range []string{
		string(rec.AgentID), rec.AdapterName, rec.AdapterRevision,
		string(rec.AdapterSHA256), rec.DatasetVersion, string(rec.DatasetSHA256), string(rec.TrainingRunID),
		rec.TrainingCodeRevision, rec.Quantization, rec.RuntimeVersion, rec.GPU, string(rec.RequestID),
	} {
		if v == "" {
			empty++
		}
	}
	if len(rec.Probabilities) == 0 {
		empty++
	}
	require.Equal(t, empty, nulls, "an empty optional field is NULL")
}

// allStoredText is every glimmer_calls row, every column, as text.
func allStoredText(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT * FROM glimmer_calls`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var b strings.Builder
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		for _, v := range vals {
			b.WriteString(v.String)
			b.WriteByte('\n')
		}
	}
	require.NoError(t, rows.Err())
	return b.String()
}

func TestSQLRecorderStoresEveryCall(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	secret := testSecret(t)
	byName := make(map[string]cassette)
	for _, c := range loadCassettes(t) {
		byName[c.Name] = c
	}
	for _, name := range []string{"analyze_ok", "choose_ok", "triage_ok", "triage_low_mass", "server_error"} {
		c := byName[name]
		var seen atomic.Int64
		srv := replayServer(t, c, &seen)
		f := sqlFixture(t, db, srv.URL, nil)
		res, _ := f.gw.Infer(context.Background(), c.inferRequest(t, secret))
		require.Equal(t, c.Want.Status, res.CallRecord.Status, name)
		requireStored(t, db, res.CallRecord)
		require.NotContains(t, allStoredText(t, db), srv.URL)
	}
	text := allStoredText(t, db)
	for _, s := range []string{
		secret, testToken, "please review the parser change",
		"export GITHUB_TOKEN", "tests: 9 passed", "the agent is at its prompt",
	} {
		require.NotContains(t, text, s, "no payload, prompt or answer is stored")
	}
}

// model.status reads the newest row by created_at, then call_id.
func TestSQLRecorderOrdersCallsByTime(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	f := sqlFixture(t, db, "http://127.0.0.1:1", &captureTransport{answer: []byte(okTriageAnswer)})
	first, err := f.gw.Infer(context.Background(), triageRequest)
	require.NoError(t, err)
	second, err := f.gw.Infer(context.Background(), triageRequest)
	require.NoError(t, err)
	var newest string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT call_id FROM glimmer_calls ORDER BY created_at DESC, call_id DESC LIMIT 1`).Scan(&newest))
	require.Equal(t, string(second.CallRecord.CallID), newest)
	require.NotEqual(t, first.CallRecord.CallID, second.CallRecord.CallID)
}

func TestSQLRecorderRefusesAnIncompleteRecord(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	rec, err := NewSQLRecorder(db)
	require.NoError(t, err)
	require.Error(t, rec.RecordCall(t.Context(), protocol.GlimmerCallRecord{CallID: "call-1"}))
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM glimmer_calls`).Scan(&n))
	require.Zero(t, n)

	f := sqlFixture(t, db, "http://127.0.0.1:1", &captureTransport{answer: []byte(okTriageAnswer)})
	f.gw.deps.IDs = sameIDs{}
	_, err = f.gw.Infer(context.Background(), triageRequest)
	require.NoError(t, err)
	res, err := f.gw.Infer(context.Background(), triageRequest)
	require.ErrorIs(t, err, ErrRecordFailed, "a second row with the same call id is refused")
	require.Equal(t, protocol.InferResult{}, res, "and its result is withheld")

	_, err = NewSQLRecorder(nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// sameIDs gives every call the same id.
type sameIDs struct{}

func (sameIDs) NewCallID() protocol.OpaqueID { return "call-same" }

func TestRandomCallIDs(t *testing.T) {
	t.Parallel()
	seen := make(map[protocol.OpaqueID]bool)
	for range 64 {
		id := RandomCallIDs{}.NewCallID()
		require.True(t, id.Valid(), id)
		require.True(t, strings.HasPrefix(string(id), "call-"))
		require.Len(t, string(id), len("call-")+32)
		require.False(t, seen[id], "call ids do not repeat")
		seen[id] = true
	}
}
