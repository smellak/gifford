package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/smellak/gifford/internal/protocol"
)

const maxDeliveryBody = 1 << 20

// DeliveryReceiver owns receiver-side validation and durable storage after
// the transport has authenticated the peer and decoded the request.
type DeliveryReceiver interface {
	ReceiveDelivery(context.Context, protocol.NodeID, DeliveryRequest) ([]byte, error)
}

// DeliveryFailure classifies a safe HTTP refusal from a DeliveryReceiver.
type DeliveryFailure uint8

const (
	// DeliveryBadRequest means the authenticated peer sent invalid data.
	DeliveryBadRequest DeliveryFailure = iota + 1
	// DeliveryForbidden means trust, policy, or channel control refused it.
	DeliveryForbidden
	// DeliveryConflict means replay or immutable routing state conflicted.
	DeliveryConflict
)

// DeliveryError is a receiver refusal safe for the federation HTTP boundary.
// Receipt is set only for a signed CONTROL_DENIED response.
type DeliveryError struct {
	kind    DeliveryFailure
	code    protocol.ErrorCode
	receipt []byte
	cause   error
}

// NewDeliveryError constructs a typed refusal without exposing its cause.
func NewDeliveryError(
	kind DeliveryFailure,
	code protocol.ErrorCode,
	receipt []byte,
	cause error,
) *DeliveryError {
	return &DeliveryError{
		kind: kind, code: code, receipt: append([]byte(nil), receipt...), cause: cause,
	}
}

func (e *DeliveryError) Error() string {
	if e == nil || e.cause == nil {
		return "delivery rejected"
	}
	return "delivery rejected: " + e.cause.Error()
}

// Unwrap retains the internal cause for local tests and diagnostics.
func (e *DeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

type deliveryErrorResponse struct {
	ErrorCode protocol.ErrorCode `json:"error_code"`
}

func (s *Server) deliveryHandler(response http.ResponseWriter, request *http.Request) {
	if s.delivery == nil {
		notImplementedHandler(response, request)
		return
	}
	peer, ok := VerifiedNodeID(request.Context())
	if !ok {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	defer func() { _ = request.Body.Close() }()
	body := http.MaxBytesReader(response, request.Body, maxDeliveryBody)
	raw, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(response, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	delivery, err := decodeDeliveryFrame(raw)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	receipt, err := s.delivery.ReceiveDelivery(request.Context(), peer, delivery)
	if err != nil {
		s.writeDeliveryError(response, err)
		return
	}
	if len(receipt) == 0 {
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writeDeliveryJSON(response, http.StatusOK, receipt)
}

// decodeDeliveryFrame keeps transport decoding strict while leaving the
// receiver to apply stateful delivery checks in protocol order. In
// particular, replay identity is resolved before request coherence checks.
func decodeDeliveryFrame(raw []byte) (DeliveryRequest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request DeliveryRequest
	if err := decoder.Decode(&request); err != nil {
		return DeliveryRequest{}, fmt.Errorf("decode delivery request: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return DeliveryRequest{}, fmt.Errorf("decode delivery request: %w", err)
	}
	if _, _, err := request.DecodeEnvelope(); err != nil {
		return DeliveryRequest{}, err
	}
	return request, nil
}

func (s *Server) writeDeliveryError(response http.ResponseWriter, err error) {
	var rejected *DeliveryError
	if !errors.As(err, &rejected) {
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	status := http.StatusInternalServerError
	switch rejected.kind {
	case DeliveryBadRequest:
		status = http.StatusBadRequest
	case DeliveryForbidden:
		status = http.StatusForbidden
	case DeliveryConflict:
		status = http.StatusConflict
	default:
		http.Error(response, http.StatusText(status), status)
		return
	}
	if len(rejected.receipt) != 0 {
		writeDeliveryJSON(response, status, rejected.receipt)
		return
	}
	if !rejected.code.Valid() {
		http.Error(response, http.StatusText(status), status)
		return
	}
	raw, marshalErr := json.Marshal(deliveryErrorResponse{ErrorCode: rejected.code})
	if marshalErr != nil {
		http.Error(response, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	writeDeliveryJSON(response, status, raw)
}

func writeDeliveryJSON(response http.ResponseWriter, status int, raw []byte) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_, _ = response.Write(raw)
}
