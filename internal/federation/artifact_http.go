package federation

import (
	"errors"
	"io"
	"net/http"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	maxArtifactManifestBody = 4 << 20
	maxArtifactChunkBody    = 2 << 20
	maxArtifactCompleteBody = 64 << 10
)

func (s *Server) artifactManifestHandler(response http.ResponseWriter, request *http.Request) {
	if s.artifact == nil {
		notImplementedHandler(response, request)
		return
	}
	raw, peer, ok := readArtifactFrame(response, request, maxArtifactManifestBody)
	if !ok {
		return
	}
	manifest, err := DecodeArtifactManifestRequest(raw)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	receipt, err := s.artifact.ReceiveArtifactManifest(request.Context(), peer, manifest)
	writeArtifactResult(s, response, receipt, err)
}

func (s *Server) artifactChunkHandler(response http.ResponseWriter, request *http.Request) {
	if s.artifact == nil {
		notImplementedHandler(response, request)
		return
	}
	raw, peer, ok := readArtifactFrame(response, request, maxArtifactChunkBody)
	if !ok {
		return
	}
	chunk, err := DecodeArtifactChunkRequest(raw)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	receipt, err := s.artifact.ReceiveArtifactChunk(request.Context(), peer, chunk)
	writeArtifactResult(s, response, receipt, err)
}

func (s *Server) artifactCompleteHandler(response http.ResponseWriter, request *http.Request) {
	if s.artifact == nil {
		notImplementedHandler(response, request)
		return
	}
	raw, peer, ok := readArtifactFrame(response, request, maxArtifactCompleteBody)
	if !ok {
		return
	}
	complete, err := DecodeArtifactCompleteRequest(raw)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	receipt, err := s.artifact.CompleteArtifact(request.Context(), peer, complete)
	writeArtifactResult(s, response, receipt, err)
}

func readArtifactFrame(
	response http.ResponseWriter,
	request *http.Request,
	limit int64,
) ([]byte, protocol.NodeID, bool) {
	peer, ok := VerifiedNodeID(request.Context())
	if !ok {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return nil, "", false
	}
	defer func() { _ = request.Body.Close() }()
	body := http.MaxBytesReader(response, request.Body, limit)
	raw, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(response, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return nil, "", false
		}
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return nil, "", false
	}
	return raw, peer, true
}

func writeArtifactResult(s *Server, response http.ResponseWriter, receipt []byte, err error) {
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
