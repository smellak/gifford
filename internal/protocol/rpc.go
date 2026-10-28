package protocol

// This file holds the JSON-RPC envelope of §6.8: the request, the
// response, the typed error and its data. Method params and results live
// in params.go; the specs in control_method.go decode params strictly.

// rpcErrorCodes are the JSON-RPC codes, with -32000 as the typed domain
// error carrying an RPCErrorData.
const (
	rpcCodeParseError     = -32700
	rpcCodeInvalidRequest = -32600
	rpcCodeMethodNotFound = -32601
	rpcCodeInvalidParams  = -32602
	rpcCodeInternalError  = -32603
	rpcCodeDomainError    = -32000
)

// RPCRequest is one Control API call.
type RPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int64         `json:"id" jsonschema:"minimum=1"`
	Method  ControlMethod `json:"method"`
	Params  Payload       `json:"params"`
}

// Validate checks the envelope and strictly decodes the params into the
// method's params type.
func (r RPCRequest) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.JSONRPC != JSONRPCVersion {
		return &ValidationError{Path: "jsonrpc", Reason: "must be " + JSONRPCVersion}
	}
	for _, spec := range ControlMethodSpecs() {
		if spec.Method != r.Method {
			continue
		}
		params := spec.NewParams()
		if err := decodeStrict(r.Params, params); err != nil {
			return &ValidationError{Path: "params", Reason: "bad params"}
		}
		if v, ok := params.(nestedValidator); ok {
			if err := v.Validate(); err != nil {
				return prefixPath(err, "params")
			}
		}
		return nil
	}
	return &ValidationError{Path: "method", Reason: "unknown method"}
}

// RPCResponse is one Control API answer: exactly one of result, error.
type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int64     `json:"id" jsonschema:"minimum=1"`
	Result  Payload   `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

// Validate checks the envelope and the exactly-one rule.
func (r RPCResponse) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.JSONRPC != JSONRPCVersion {
		return &ValidationError{Path: "jsonrpc", Reason: "must be " + JSONRPCVersion}
	}
	if (r.Result == nil) == (r.Error == nil) {
		return &ValidationError{Path: "result", Reason: "exactly one of result, error"}
	}
	return nil
}

// RPCError is one typed Control API error.
type RPCError struct {
	Code    int64         `json:"code" jsonschema:"enum=-32700,enum=-32600,enum=-32601,enum=-32602,enum=-32603,enum=-32000"`
	Message string        `json:"message" jsonschema:"minLength=1,maxLength=512"`
	Data    *RPCErrorData `json:"data,omitempty"`
}

// Validate checks the error and the data rule.
func (e RPCError) Validate() error {
	if err := checkRoot(&e); err != nil {
		return err
	}
	if (e.Data != nil) != (e.Code == rpcCodeDomainError) {
		return &ValidationError{Path: "data", Reason: "present iff code is -32000"}
	}
	return nil
}

// RPCErrorData carries the domain detail of a -32000 error.
type RPCErrorData struct {
	ErrorCode       ErrorCode       `json:"error_code"`
	CurrentVersion  *int64          `json:"current_version,omitempty" jsonschema:"minimum=0"`
	CurrentVersions ControlVersions `json:"current_versions,omitempty"`
	Detail          string          `json:"detail,omitempty" jsonschema:"maxLength=280"`
}

// Validate checks the error data.
func (d RPCErrorData) Validate() error {
	return checkRoot(&d)
}
