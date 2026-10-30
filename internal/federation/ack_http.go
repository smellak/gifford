package federation

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/smellak/gifford/internal/protocol"
)

const maxAckBody = 64 << 10

// AckReceiver owns acknowledgement binding, replay protection and durable
// storage after the transport authenticates the peer.
type AckReceiver interface {
	ReceiveFederatedAck(context.Context, protocol.NodeID, AckRequest) ([]byte, error)
}

func (s *Server) ackHandler(response http.ResponseWriter, request *http.Request) {
	if s.ack == nil {
		notImplementedHandler(response, request)
		return
	}
	peer, ok := VerifiedNodeID(request.Context())
	if !ok {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	defer func() { _ = request.Body.Close() }()
	body := http.MaxBytesReader(response, request.Body, maxAckBody)
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
	ack, err := DecodeAckRequest(raw)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	receipt, err := s.ack.ReceiveFederatedAck(request.Context(), peer, ack)
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
