package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// errUnsanitisable is any reason a payload cannot be sanitised. It never
// leaves the package: the caller reports redaction_failed.
var errUnsanitisable = errors.New("payload cannot be sanitised")

// maxSanitiseDepth bounds how deep a payload may nest.
const maxSanitiseDepth = 16

// fieldKind is how one payload field is sanitised.
type fieldKind int

const (
	// kindScalar is a number, a boolean or null: no content.
	kindScalar fieldKind = iota
	// kindToken is an enum value or id. It must come out of the text
	// redactor unchanged, or the payload is refused.
	kindToken
	// kindText is free text, redacted by RedactText.
	kindText
	// kindScreenRows is a list of screen rows, redacted together by
	// RedactScreen so a secret split across rows is still found.
	kindScreenRows
	// kindScreenText is a screen as one string of newline-separated rows.
	kindScreenText
	kindObject
	kindList
)

// field is the sanitising schema of a payload value.
type field struct {
	kind   fieldKind
	fields map[string]*field // kindObject
	elem   *field            // kindList
}

func scalar() *field { return &field{kind: kindScalar} }

func token() *field { return &field{kind: kindToken} }

func object(fields map[string]*field) *field { return &field{kind: kindObject, fields: fields} }

func listOf(elem *field) *field { return &field{kind: kindList, elem: elem} }

// payloadSchema is the sanitising schema of a contract's payload. Every
// text field of every contract is classified here; a field that is not
// is refused, so a new payload field is a deliberate change to this
// table.
func payloadSchema(c protocol.ModelContract) (*field, bool) {
	switch c {
	case protocol.ModelContractAnalyzeAgentState:
		// The model input of dataset.AnalyzeInput.
		return object(map[string]*field{
			"harness":                    token(),
			"seconds_since_last_output":  scalar(),
			"output_bytes":               object(map[string]*field{"last_500ms": scalar(), "last_2s": scalar(), "last_10s": scalar()}),
			"new_child_processes":        listOf(token()),
			"previous_state":             token(),
			"previous_state_age_seconds": scalar(),
			"screen_lines":               {kind: kindScreenRows},
		}), true
	case protocol.ModelContractTriageMessage:
		return object(map[string]*field{"text": {kind: kindText}}), true
	case protocol.ModelContractChooseOrchestrationAction:
		return schemaOf(reflect.TypeFor[protocol.OrchestrationRequest](), map[string]*field{
			"screen": {kind: kindScreenText},
		})
	default:
		return nil, false
	}
}

// schemaOf derives a schema from a protocol type. Named string types are
// enums or ids, so tokens. A plain string field is free content and must
// be classified in overrides, by its JSON name; otherwise the type has no
// schema.
func schemaOf(t reflect.Type, overrides map[string]*field) (*field, bool) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return scalar(), true
	case reflect.String:
		if t.PkgPath() == "" {
			return nil, false
		}
		return token(), true
	case reflect.Slice, reflect.Array:
		elem, ok := schemaOf(t.Elem(), nil)
		return listOf(elem), ok
	case reflect.Struct:
		fields := make(map[string]*field, t.NumField())
		for i := range t.NumField() {
			sf := t.Field(i)
			name, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
			if name == "" || name == "-" {
				return nil, false
			}
			if f, ok := overrides[name]; ok {
				fields[name] = f
				continue
			}
			f, ok := schemaOf(sf.Type, nil)
			if !ok {
				return nil, false
			}
			fields[name] = f
		}
		return object(fields), true
	default:
		return nil, false
	}
}

// Redactor masks secrets in free text and on screens. *redact.Redactor
// satisfies it.
type Redactor interface {
	RedactText(s string) (redact.TextResult, error)
	RedactScreen(s redact.Screen) (redact.ScreenResult, error)
}

// sanitiser redacts one payload, field by field, keeping the policy
// version and the redaction count.
type sanitiser struct {
	red     Redactor
	version string
	count   int64
}

// sanitisePayload decodes payload, redacts every value by its schema and
// re-encodes it. JSON escapes are decoded first, so an escaped secret is
// redacted as the receiver will read it.
func sanitisePayload(red Redactor, c protocol.ModelContract, payload []byte) (out []byte, version string, count int64, err error) {
	schema, ok := payloadSchema(c)
	if !ok {
		return nil, "", 0, errUnsanitisable
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var v any
	if dec.Decode(&v) != nil {
		return nil, "", 0, errUnsanitisable
	}
	if !atEOF(dec) {
		return nil, "", 0, errUnsanitisable
	}
	if _, isObj := v.(map[string]any); !isObj {
		return nil, "", 0, errUnsanitisable
	}
	s := &sanitiser{red: red}
	// The empty probe names the policy version even for a payload with
	// no text.
	if err := s.text("", nil); err != nil {
		return nil, "", 0, err
	}
	clean, err := s.walk(v, schema, 0)
	if err != nil {
		return nil, "", 0, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if enc.Encode(clean) != nil {
		return nil, "", 0, errUnsanitisable
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), s.version, s.count, nil
}

// atEOF reports whether nothing but white space follows the value.
func atEOF(dec *json.Decoder) bool {
	var extra any
	return errors.Is(dec.Decode(&extra), io.EOF)
}

func (s *sanitiser) walk(v any, f *field, depth int) (any, error) {
	if depth > maxSanitiseDepth {
		return nil, errUnsanitisable
	}
	if v == nil {
		return nil, nil
	}
	switch f.kind {
	case kindScalar:
		switch v.(type) {
		case json.Number, bool:
			return v, nil
		}
	case kindToken:
		if str, ok := v.(string); ok {
			var out string
			if err := s.text(str, &out); err != nil || out != str {
				return nil, errUnsanitisable
			}
			return str, nil
		}
	case kindText:
		if str, ok := v.(string); ok {
			var out string
			if err := s.text(str, &out); err != nil {
				return nil, err
			}
			return out, nil
		}
	case kindScreenRows:
		if rows, ok := stringList(v); ok {
			return s.screen(rows)
		}
	case kindScreenText:
		if str, ok := v.(string); ok {
			rows, err := s.screen(strings.Split(str, "\n"))
			if err != nil {
				return nil, err
			}
			return strings.Join(rows, "\n"), nil
		}
	case kindObject:
		if obj, ok := v.(map[string]any); ok {
			out := make(map[string]any, len(obj))
			for k, val := range obj {
				sub, known := f.fields[k]
				if !known {
					return nil, errUnsanitisable
				}
				clean, err := s.walk(val, sub, depth+1)
				if err != nil {
					return nil, err
				}
				out[k] = clean
			}
			return out, nil
		}
	case kindList:
		if list, ok := v.([]any); ok {
			out := make([]any, len(list))
			for i, val := range list {
				clean, err := s.walk(val, f.elem, depth+1)
				if err != nil {
					return nil, err
				}
				out[i] = clean
			}
			return out, nil
		}
	}
	return nil, errUnsanitisable
}

func stringList(v any) ([]string, bool) {
	list, ok := v.([]any)
	if !ok {
		return nil, false
	}
	rows := make([]string, len(list))
	for i, val := range list {
		if rows[i], ok = val.(string); !ok {
			return nil, false
		}
	}
	return rows, true
}

// text redacts one string, storing the result in out when out is set.
func (s *sanitiser) text(str string, out *string) error {
	res, err := s.red.RedactText(str)
	if err != nil || !s.keep(res.PolicyVersion, res.Counts.Total) {
		return errUnsanitisable
	}
	if out != nil {
		*out = res.Text
	}
	return nil
}

// screen redacts rows as one screen. The payload carries no pane width,
// so rows are joined by the continuation and prefix rules, not by soft
// wrap.
func (s *sanitiser) screen(rows []string) ([]string, error) {
	res, err := s.red.RedactScreen(redact.Screen{Rows: rows})
	if err != nil || len(res.Rows) != len(rows) || !s.keep(res.PolicyVersion, res.Counts.Total) {
		return nil, errUnsanitisable
	}
	return res.Rows, nil
}

// keep records one redaction; every one must name the same version.
func (s *sanitiser) keep(version string, n int64) bool {
	if version == "" || (s.version != "" && version != s.version) || n < 0 {
		return false
	}
	s.version = version
	s.count += n
	return true
}
