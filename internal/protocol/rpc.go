package protocol

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
)

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

// RPCRequestV12 is the additive Control API request envelope published for
// contracts-v1.2. Consumers that may send notification.list validate requests
// against rpc_request_v1_2.schema.json; rpc_request.schema.json stays frozen at
// the contracts-v1.1 method set.
type RPCRequestV12 RPCRequest

// Validate applies the current Control API request rules.
func (r RPCRequestV12) Validate() error {
	return RPCRequest(r).Validate()
}

// RPCRequestV14 is the additive Control API request envelope published for
// contracts-v1.4. It adds delivery.list while keeping the v1.1 and v1.2
// request schemas frozen.
type RPCRequestV14 RPCRequest

// Validate applies the current Control API request rules.
func (r RPCRequestV14) Validate() error {
	return RPCRequest(r).Validate()
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
		if err := checkParamsPresence(r.Params, params); err != nil {
			return err
		}
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

// checkParamsPresence verifies raw params against the shape of the
// method's params type before decoding: every required field (json tag
// without omitempty/omitzero) is present and no field is an explicit
// null. It recurses into *struct fields and struct elements of slices
// and maps, never into Payload or Value. A violation is a
// ValidationError whose path starts with params.
func checkParamsPresence(raw Payload, into any) error {
	t := reflect.TypeOf(into)
	if t == nil {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err == nil && obj != nil {
		return checkPresenceObject(obj, t, "params")
	}
	return nil // not an object: decodeStrict reports it
}

// checkPresenceObject checks one JSON object against a struct type,
// reading the same json tags as the walker.
func checkPresenceObject(obj map[string]json.RawMessage, t reflect.Type, path string) error {
	names := make(map[string]struct{}, t.NumField())
	for i := range t.NumField() {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name, _ := jsonFieldName(sf)
		if name == "-" {
			continue
		}
		names[name] = struct{}{}
	}
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := names[key]; !ok {
			return &ValidationError{Path: path + "." + key, Reason: reasonUnknownField}
		}
	}
	for i := range t.NumField() {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name, optional := jsonFieldName(sf)
		if name == "-" {
			continue
		}
		fieldPath := path + "." + name
		raw, present := obj[name]
		if !present {
			if !optional {
				return &ValidationError{Path: fieldPath, Reason: "required"}
			}
			continue
		}
		if isJSONNull(raw) {
			return &ValidationError{Path: fieldPath, Reason: reasonNullInvalid}
		}
		if err := checkPresenceValue(raw, sf.Type, fieldPath); err != nil {
			return err
		}
	}
	return nil
}

// checkPresenceValue recurses into nested shapes: *struct fields and
// struct elements of slices, arrays and maps.
func checkPresenceValue(raw json.RawMessage, ft reflect.Type, path string) error {
	if ft == reflect.TypeOf(Payload{}) || ft == reflect.TypeOf(Value{}) {
		return nil
	}
	switch ft.Kind() {
	default:
		return nil
	case reflect.Pointer:
		if ft.Elem().Kind() == reflect.Struct {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(raw, &obj); err == nil && obj != nil {
				return checkPresenceObject(obj, ft.Elem(), path)
			}
		}
	case reflect.Slice, reflect.Array:
		elem := ft.Elem()
		for elem.Kind() == reflect.Pointer {
			elem = elem.Elem()
		}
		if elem == reflect.TypeOf(Payload{}) || elem == reflect.TypeOf(Value{}) {
			return nil
		}
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err == nil && items != nil {
			for i, item := range items {
				itemPath := path + "[" + strconv.Itoa(i) + "]"
				if isJSONNull(item) {
					return &ValidationError{Path: itemPath, Reason: reasonNullInvalid}
				}
				if err := checkPresenceItem(item, elem, itemPath); err != nil {
					return err
				}
			}
		}
		return nil // not an array: decodeStrict reports it
	case reflect.Map:
		elem := ft.Elem()
		for elem.Kind() == reflect.Pointer {
			elem = elem.Elem()
		}
		if elem.Kind() != reflect.Struct {
			return checkPresenceScalarMap(raw, path)
		}
		var items map[string]json.RawMessage
		if err := json.Unmarshal(raw, &items); err == nil && items != nil {
			for key, item := range items {
				itemPath := path + "." + key
				if isJSONNull(item) {
					return &ValidationError{Path: itemPath, Reason: reasonNullInvalid}
				}
				var obj map[string]json.RawMessage
				if err := json.Unmarshal(item, &obj); err == nil && obj != nil {
					if err := checkPresenceObject(obj, elem, itemPath); err != nil {
						return err
					}
				}
			}
		}
		return nil // not an object: decodeStrict reports it
	}
	return nil
}

// isJSONNull reports whether raw is a JSON null literal.
func isJSONNull(raw json.RawMessage) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}

// checkPresenceScalarMap rejects null entries inside scalar maps such as
// ControlVersions: a null CAS version is not a zero. Values are never
// interpreted here.
func checkPresenceScalarMap(raw json.RawMessage, path string) error {
	var items map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err == nil && items != nil {
		for key, item := range items {
			if isJSONNull(item) {
				return &ValidationError{Path: path + "." + key, Reason: reasonNullInvalid}
			}
		}
	}
	return nil // not an object: decodeStrict reports it
}

// checkPresenceItem checks one slice element against its type, descending
// into struct elements with the same object check.
func checkPresenceItem(item json.RawMessage, elem reflect.Type, itemPath string) error {
	if elem.Kind() != reflect.Struct {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(item, &obj); err == nil && obj != nil {
		return checkPresenceObject(obj, elem, itemPath)
	}
	return nil // not an object: decodeStrict reports it
}

// reasonNullInvalid is the reason when a params field is an explicit null.
const reasonNullInvalid = "null is not valid"

// reasonUnknownField is the reason when a params object carries a key that
// is not exactly the json name of one of its fields.
const reasonUnknownField = "unknown field"
