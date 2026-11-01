package federation

import (
	"errors"
	"io"
	"net/http"
)

const maxArtifactAckBody = 64 << 10

func (s *Server) artifactAckHandler(response http.ResponseWriter, request *http.Request) {
	if s.artifactAck == nil {
		notImplementedHandler(response, request)
		return
	}
	peer, ok := VerifiedNodeID(request.Context())
	if !ok {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	defer func() { _ = request.Body.Close() }()
	body := http.MaxBytesReader(response, request.Body, maxArtifactAckBody)
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
	ack, err := DecodeArtifactAckRequest(raw)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	receipt, err := s.artifactAck.ReceiveFederatedArtifactAck(request.Context(), peer, ack)
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
