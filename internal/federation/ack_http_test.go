package federation

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

type ackReceiverFunc func(context.Context, protocol.NodeID, AckRequest) ([]byte, error)

func (f ackReceiverFunc) ReceiveFederatedAck(
	ctx context.Context,
	peer protocol.NodeID,
	request AckRequest,
) ([]byte, error) {
	return f(ctx, peer, request)
}

func TestAckHandlerStrictBodyAndAuthenticatedBinding(t *testing.T) {
	t.Parallel()
	exact, err := testAckRequest().CanonicalBytes()
	require.NoError(t, err)
	wantReceipt := []byte(`{"signed":"receipt"}`)
	called := false
	server := &Server{ack: ackReceiverFunc(func(
		_ context.Context,
		peer protocol.NodeID,
		request AckRequest,
	) ([]byte, error) {
		called = true
		require.Equal(t, deliveryTestOriginNode, peer)
		require.Equal(t, testAckRequest(), request)
		return wantReceipt, nil
	})}

	request := httptest.NewRequestWithContext(
		t.Context(), http.MethodPost, "/v1/ack", bytes.NewReader(exact),
	)
	request = request.WithContext(context.WithValue(
		request.Context(), verifiedNodeKey{}, deliveryTestOriginNode,
	))
	response := httptest.NewRecorder()
	server.ackHandler(response, request)
	require.True(t, called)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.Equal(t, wantReceipt, response.Body.Bytes())

	unauthenticated := httptest.NewRecorder()
	server.ackHandler(unauthenticated,
		httptest.NewRequestWithContext(
			t.Context(), http.MethodPost, "/v1/ack", bytes.NewReader(exact),
		))
	require.Equal(t, http.StatusForbidden, unauthenticated.Code)

	malformedRequest := httptest.NewRequestWithContext(
		t.Context(), http.MethodPost, "/v1/ack",
		bytes.NewReader(append(exact, []byte(" {}")...)),
	)
	malformedRequest = malformedRequest.WithContext(context.WithValue(
		malformedRequest.Context(), verifiedNodeKey{}, deliveryTestOriginNode,
	))
	malformed := httptest.NewRecorder()
	server.ackHandler(malformed, malformedRequest)
	require.Equal(t, http.StatusBadRequest, malformed.Code)

	largeRequest := httptest.NewRequestWithContext(
		t.Context(), http.MethodPost, "/v1/ack",
		io.LimitReader(bytes.NewReader(bytes.Repeat([]byte{'x'}, maxAckBody+1)), maxAckBody+1),
	)
	largeRequest = largeRequest.WithContext(context.WithValue(
		largeRequest.Context(), verifiedNodeKey{}, deliveryTestOriginNode,
	))
	large := httptest.NewRecorder()
	server.ackHandler(large, largeRequest)
	require.Equal(t, http.StatusRequestEntityTooLarge, large.Code)
}

func TestAckHandlerMapsReceiverFailuresAndNilReceiver(t *testing.T) {
	t.Parallel()
	exact, err := testAckRequest().CanonicalBytes()
	require.NoError(t, err)
	for _, test := range []struct {
		name string
		err  error
		want int
	}{
		{"bad request", NewDeliveryError(DeliveryBadRequest, "", nil, io.ErrUnexpectedEOF), http.StatusBadRequest},
		{"forbidden", NewDeliveryError(DeliveryForbidden, "", nil, io.ErrUnexpectedEOF), http.StatusForbidden},
		{"conflict", NewDeliveryError(DeliveryConflict, "", nil, io.ErrUnexpectedEOF), http.StatusConflict},
		{"internal", io.ErrUnexpectedEOF, http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := &Server{ack: ackReceiverFunc(func(
				context.Context, protocol.NodeID, AckRequest,
			) ([]byte, error) {
				return nil, test.err
			})}
			request := httptest.NewRequestWithContext(
				t.Context(), http.MethodPost, "/v1/ack", bytes.NewReader(exact),
			)
			request = request.WithContext(context.WithValue(
				request.Context(), verifiedNodeKey{}, deliveryTestOriginNode,
			))
			response := httptest.NewRecorder()
			server.ackHandler(response, request)
			require.Equal(t, test.want, response.Code)
		})
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(
		t.Context(), http.MethodPost, "/v1/ack", bytes.NewReader(exact),
	)
	request = request.WithContext(context.WithValue(
		request.Context(), verifiedNodeKey{}, deliveryTestOriginNode,
	))
	(&Server{}).ackHandler(response, request)
	require.Equal(t, http.StatusNotImplemented, response.Code)
}
