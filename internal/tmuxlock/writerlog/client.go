package writerlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	eventBudget   = 500 * time.Millisecond
	maxRequestLen = 4096
	maxACKLen     = 1024
	profile       = "gifford"
)

var (
	// ErrUnavailable means a configured mediator could not durably acknowledge
	// an event. In particular, an unavailable acquire must prevent any effect.
	ErrUnavailable = errors.New("writer log unavailable")
	// ErrDeadline means the event did not complete within its bounded budget.
	ErrDeadline = errors.New("writer log deadline")
	// ErrProtocol means the mediator returned a non-canonical or unexpected ACK.
	ErrProtocol = errors.New("writer log protocol violation")
	// ErrRejected means the mediator returned a well-formed negative ACK.
	ErrRejected = errors.New("writer log rejected")
	// ErrInvalid means the caller supplied an invalid path, descriptor, or ID.
	ErrInvalid = errors.New("invalid writer log argument")
	// ErrState means interval methods were called out of order.
	ErrState = errors.New("invalid writer log state")
	// ErrUncertain means an earlier physical effect may have happened even though
	// its journal event was not acknowledged. Such events must never be retried.
	ErrUncertain = errors.New("writer log outcome uncertain")
)

// Outcome is the closed result vocabulary for one physical write.
type Outcome string

const (
	// OutcomeOK means the physical write completed successfully.
	OutcomeOK Outcome = "ok"
	// OutcomeFailed means the physical write failed with no claim of completion.
	OutcomeFailed Outcome = "failed"
	// OutcomeUncertain means the caller cannot determine whether it completed.
	OutcomeUncertain Outcome = "uncertain"
)

// Client sends events to one explicitly configured local mediator. An empty
// socket path disables the client; there is deliberately no default path.
type Client struct {
	socketPath string
}

// New validates socketPath and returns a client. An empty path returns a
// disabled client whose intervals preserve local ordering without doing I/O.
func New(socketPath string) (*Client, error) {
	if socketPath == "" {
		return &Client{}, nil
	}
	if !validSocketPath(socketPath) {
		return nil, ErrInvalid
	}
	return &Client{socketPath: socketPath}, nil
}

// Enabled reports whether the client has an explicit mediator configuration.
func (client *Client) Enabled() bool {
	return client != nil && client.socketPath != ""
}

// Open records acquire and returns the interval associated with attempt. The
// caller must already retain an exclusive lock on lockFile and keep the same
// open file alive through Release.
func (client *Client) Open(
	ctx context.Context,
	lockFile *os.File,
	attempt string,
) (*Interval, error) {
	if client == nil || ctx == nil || lockFile == nil || !validHex32(attempt) {
		return nil, ErrInvalid
	}
	if _, err := lockFile.Stat(); err != nil {
		return nil, fmt.Errorf("%w: lock descriptor", ErrInvalid)
	}
	interval := &Interval{
		client:   client,
		ctx:      ctx,
		lockFile: lockFile,
		attempt:  attempt,
	}
	if !client.Enabled() {
		return interval, nil
	}
	if err := interval.emit(eventAcquire, "", "", false, false); err != nil {
		return nil, err
	}
	return interval, nil
}

// Interval is one acquired writer-log interval. Its methods are serialized so
// one interval cannot emit overlapping writes even if misused concurrently.
type Interval struct {
	mu sync.Mutex

	client   *Client
	ctx      context.Context
	lockFile *os.File
	attempt  string

	writeID        string
	effectPossible bool
	released       bool
	failed         error
}

// KeyBegin records the opening boundary for one physical write.
func (interval *Interval) KeyBegin(writeID string) error {
	if interval == nil {
		return ErrState
	}
	interval.mu.Lock()
	defer interval.mu.Unlock()
	if err := interval.ready(); err != nil {
		return err
	}
	if interval.writeID != "" || !validID(writeID) {
		return ErrState
	}
	if err := interval.emit(
		eventKeyBegin, writeID, "", false, interval.effectPossible,
	); err != nil {
		interval.failed = err
		return err
	}
	interval.writeID = writeID
	// After the opening boundary is acknowledged, the caller may begin the
	// physical write before the next method call. Later failures are therefore
	// conservatively uncertain.
	interval.effectPossible = true
	return nil
}

// KeyEnd records the terminal boundary and outcome for the current write. It
// receives a fresh event budget even if the operation context has expired.
func (interval *Interval) KeyEnd(writeID string, outcome Outcome) error {
	if interval == nil {
		return ErrState
	}
	interval.mu.Lock()
	defer interval.mu.Unlock()
	if err := interval.ready(); err != nil {
		return err
	}
	if interval.writeID == "" || interval.writeID != writeID || !validOutcome(outcome) {
		return ErrState
	}
	if err := interval.emit(
		eventKeyEnd, writeID, string(outcome), true, interval.effectPossible,
	); err != nil {
		interval.failed = err
		return err
	}
	interval.writeID = ""
	return nil
}

// Release records the interval's final event. It must be called while the
// caller still retains the lock and receives its own fresh event budget.
func (interval *Interval) Release() error {
	if interval == nil {
		return ErrState
	}
	interval.mu.Lock()
	defer interval.mu.Unlock()
	if err := interval.ready(); err != nil {
		return err
	}
	if interval.writeID != "" {
		return ErrState
	}
	// Mark the interval terminal before I/O so a lost ACK cannot be retried.
	interval.released = true
	if err := interval.emit(
		eventRelease, "", "", true, interval.effectPossible,
	); err != nil {
		interval.failed = err
		return err
	}
	return nil
}

func (interval *Interval) ready() error {
	if interval.failed != nil {
		return interval.failed
	}
	if interval.released {
		return ErrState
	}
	return nil
}

type event string

const (
	eventAcquire  event = "acquire"
	eventKeyBegin event = "key_begin"
	eventKeyEnd   event = "key_end"
	eventRelease  event = "release"
)

// wireRequest fields intentionally appear in lexical order. encoding/json
// preserves struct field order, yielding the canonical representation expected
// by the mediator without a map or a second normalization pass.
type wireRequest struct {
	Event       event   `json:"event"`
	OperationID string  `json:"operation_id"`
	Outcome     *string `json:"outcome"`
	Profile     string  `json:"profile"`
	Version     int     `json:"v"`
	WriteID     *string `json:"write_id"`
}

func (interval *Interval) emit(
	event event,
	writeID string,
	outcome string,
	closing bool,
	uncertain bool,
) error {
	if !interval.client.Enabled() {
		return nil
	}
	request := wireRequest{
		Event:       event,
		OperationID: interval.attempt,
		Profile:     profile,
		Version:     1,
	}
	if writeID != "" {
		request.WriteID = &writeID
	}
	if outcome != "" {
		request.Outcome = &outcome
	}
	body, err := json.Marshal(request)
	if err != nil || len(body) == 0 || len(body) > maxRequestLen {
		return withUncertainty(ErrProtocol, uncertain)
	}

	parent := interval.ctx
	if closing {
		// Closing events must remain available after the work deadline. Do not
		// inherit cancellation or a spent deadline from the operation context.
		parent = context.Background()
	}
	eventContext, cancel := context.WithTimeout(parent, eventBudget)
	defer cancel()
	ack, err := exchange(eventContext, interval.client.socketPath, interval.lockFile, body)
	if err != nil {
		kind := ErrUnavailable
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, os.ErrDeadlineExceeded) {
			kind = ErrDeadline
		}
		return withUncertainty(kind, uncertain)
	}
	err = decodeACK(ack)
	if err != nil {
		return withUncertainty(err, uncertain)
	}
	return nil
}

func decodeACK(raw []byte) error {
	if len(raw) == 0 || len(raw) > maxACKLen {
		return ErrProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return ErrProtocol
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrProtocol
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ErrProtocol
	}
	version, ok := exactInteger(value["v"])
	if !ok || version != 1 {
		return ErrProtocol
	}
	okValue, ok := value["ok"].(bool)
	if !ok {
		return ErrProtocol
	}
	if !okValue {
		if len(value) != 3 {
			return ErrProtocol
		}
		reason, ok := value["reason"].(string)
		if !ok || !validID(reason) {
			return ErrProtocol
		}
		return fmt.Errorf("%w: %s", ErrRejected, reason)
	}
	if len(value) != 3 {
		return ErrProtocol
	}
	sequence, ok := exactInteger(value["seq"])
	if !ok || sequence < 1 {
		return ErrProtocol
	}
	return nil
}

func exactInteger(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	integer, err := number.Int64()
	return integer, err == nil
}

func withUncertainty(err error, uncertain bool) error {
	if !uncertain || errors.Is(err, ErrUncertain) {
		return err
	}
	return &uncertainError{cause: err}
}

type uncertainError struct {
	cause error
}

func (err *uncertainError) Error() string {
	return ErrUncertain.Error() + ": " + err.cause.Error()
}

func (err *uncertainError) Unwrap() []error {
	return []error{ErrUncertain, err.cause}
}

func validSocketPath(value string) bool {
	if !path.IsAbs(value) || strings.HasPrefix(value, "//") || path.Clean(value) != value {
		return false
	}
	for _, character := range value {
		if character < 32 || character == 127 {
			return false
		}
	}
	return true
}

func validHex32(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func validID(value string) bool {
	if len(value) == 0 || len(value) > 160 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("_.:-", character) {
			continue
		}
		return false
	}
	return true
}

func validOutcome(outcome Outcome) bool {
	return outcome == OutcomeOK || outcome == OutcomeFailed || outcome == OutcomeUncertain
}
