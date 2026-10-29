package hub

import (
	"errors"

	"github.com/smellak/gifford/internal/protocol"
)

// errInvalidArgument means a caller-supplied argument breaks its rules.
var errInvalidArgument = errors.New("invalid argument")

// callError is a refusal safe to expose on the local JSON-RPC boundary.
// cause stays local; rpcError contains only the fixed public shape.
type callError struct {
	code    int
	message string
	domain  protocol.ErrorCode
	cause   error
}

func (e *callError) Error() string { return e.message + ": " + e.cause.Error() }

func (e *callError) Unwrap() error { return e.cause }

func (e *callError) rpcError() *rpcError {
	answer := &rpcError{Code: e.code, Message: e.message}
	if e.domain != "" {
		answer.Data = &rpcErrorData{ErrorCode: e.domain}
	}
	return answer
}

func invalidParams(err error) error {
	return &callError{code: codeInvalidRequest, message: "Invalid params", cause: err}
}

func domainError(code protocol.ErrorCode, err error) error {
	return &callError{code: codeDomainError, message: "Request rejected", domain: code, cause: err}
}
