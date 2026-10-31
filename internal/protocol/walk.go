package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// This file implements the single structural validator every Validate()
// starts with. It reads the same struct tags the schema generator reads:
// the "json" tag (required unless "omitempty"/"omitzero") and the
// "jsonschema" tag (minimum, maximum, minLength, maxLength, minItems,
// maxItems, uniqueItems, enum). Named types are checked through Valid(),
// nested values through Validate(). Cross-field rules live in each type's
// own Validate method, after the structural walk.

// validater is implemented by every named scalar type in this package.
type validater interface {
	Valid() bool
}

// nestedValidator is implemented by every nested and root type.
type nestedValidator interface {
	Validate() error
}

// reasonRequired is the reason for absent required values.
const reasonRequired = "required"

// checkStruct validates the exported fields of the struct v in field order
// and returns the first violation. Path is the JSON path prefix of v
// (empty at the root).
func checkStruct(v reflect.Value, path string) error {
	t := v.Type()
	for i := range t.NumField() {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		if err := checkField(v.Field(i), sf, path); err != nil {
			return err
		}
	}
	return nil
}

// checkRoot runs the structural walk over the struct pointed to by root.
func checkRoot(root any) error {
	return checkStruct(reflect.ValueOf(root).Elem(), "")
}

func checkField(fv reflect.Value, sf reflect.StructField, path string) error {
	name, optional := jsonFieldName(sf)
	if name == "-" {
		return nil
	}
	fieldPath := name
	if path != "" {
		fieldPath = path + "." + name
	}
	rules := parseRules(sf.Tag.Get("jsonschema"))

	// Pointers: nil is an error only when required and non-nullable;
	// nullable fields remain required on the wire but may explicitly be null.
	for fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			if !optional && !rules.nullable {
				return &ValidationError{Path: fieldPath, Reason: reasonRequired}
			}
			return nil
		}
		fv = fv.Elem()
	}

	switch fv.Interface().(type) {
	case time.Time:
		t := fv.Interface().(time.Time)
		if t.IsZero() {
			if !optional {
				return &ValidationError{Path: fieldPath, Reason: reasonRequired}
			}
			return nil
		}
		return nil
	case Payload:
		return checkRawJSON(fv.Bytes(), fieldPath, optional, true)
	case Value:
		return checkRawJSON(fv.Bytes(), fieldPath, optional, false)
	case ControlVersions:
		return checkControlVersions(fv, fieldPath, optional)
	}

	switch fv.Kind() {
	case reflect.String:
		return checkString(fv, fieldPath, optional, rules)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return checkInt(fv, fieldPath, rules)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return checkUnsignedInt(fv, fieldPath, rules)
	case reflect.Float32, reflect.Float64:
		return checkFloat(fv, fieldPath, rules)
	case reflect.Bool:
		return nil
	case reflect.Slice, reflect.Array:
		return checkSlice(fv, fieldPath, optional, rules)
	case reflect.Map:
		return checkMap(fv, fieldPath, optional)
	case reflect.Struct:
		if nv, ok := fv.Interface().(nestedValidator); ok {
			if err := nv.Validate(); err != nil {
				return prefixPath(err, fieldPath)
			}
			return nil
		}
		return checkStruct(fv, fieldPath)
	default:
		return &ValidationError{Path: fieldPath, Reason: "unsupported kind " + fv.Kind().String()}
	}
}

// prefixPath rewrites a nested *ValidationError to live under fieldPath.
func prefixPath(err error, fieldPath string) error {
	var ve *ValidationError
	if errors.As(err, &ve) {
		ve.Path = fieldPath + "." + ve.Path
		return ve
	}
	return fmt.Errorf("%s: %w", fieldPath, err)
}

func jsonFieldName(sf reflect.StructField) (string, bool) {
	tag := sf.Tag.Get("json")
	if tag == "" {
		return sf.Name, false
	}
	parts := strings.Split(tag, ",")
	optional := false
	for _, opt := range parts[1:] {
		if opt == "omitempty" || opt == "omitzero" {
			optional = true
		}
	}
	return parts[0], optional
}

type fieldRules struct {
	minimum     *int64
	maximum     *int64
	minLength   *int64
	maxLength   *int64
	minItems    *int64
	maxItems    *int64
	uniqueItems bool
	nullable    bool
	enumInts    []int64
}

func parseRules(tag string) fieldRules {
	var r fieldRules
	if tag == "" {
		return r
	}
	for _, part := range strings.Split(tag, ",") {
		name, val, _ := strings.Cut(part, "=")
		switch name {
		case "minimum":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				r.minimum = &n
			}
		case "maximum":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				r.maximum = &n
			}
		case "minLength":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n >= 0 {
				r.minLength = &n
			}
		case "maxLength":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n >= 0 {
				r.maxLength = &n
			}
		case "minItems":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n >= 0 {
				r.minItems = &n
			}
		case "maxItems":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil && n >= 0 {
				r.maxItems = &n
			}
		case "uniqueItems":
			r.uniqueItems = true
		case "nullable":
			r.nullable = true
		case "enum":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				r.enumInts = append(r.enumInts, n)
			}
		}
	}
	return r
}

func checkString(fv reflect.Value, path string, optional bool, rules fieldRules) error {
	s := fv.String()
	if s == "" {
		if !optional {
			return &ValidationError{Path: path, Reason: reasonRequired}
		}
		return nil
	}
	if v, ok := fv.Interface().(validater); ok && !v.Valid() {
		return &ValidationError{Path: path, Reason: "invalid value"}
	}
	n := int64(utf8.RuneCountInString(s))
	if rules.minLength != nil && n < *rules.minLength {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("shorter than %d", *rules.minLength)}
	}
	if rules.maxLength != nil && n > *rules.maxLength {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("longer than %d", *rules.maxLength)}
	}
	return nil
}

func checkInt(fv reflect.Value, path string, rules fieldRules) error {
	n := fv.Int()
	if len(rules.enumInts) > 0 {
		ok := false
		for _, allowed := range rules.enumInts {
			if n == allowed {
				ok = true
				break
			}
		}
		if !ok {
			return &ValidationError{Path: path, Reason: fmt.Sprintf("must be one of %v", rules.enumInts)}
		}
	}
	if rules.minimum != nil && n < *rules.minimum {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("smaller than %d", *rules.minimum)}
	}
	if rules.maximum != nil && n > *rules.maximum {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("larger than %d", *rules.maximum)}
	}
	return nil
}

func checkUnsignedInt(fv reflect.Value, path string, rules fieldRules) error {
	n := fv.Uint()
	if len(rules.enumInts) > 0 {
		ok := false
		for _, allowed := range rules.enumInts {
			if allowed >= 0 && n == uint64(allowed) {
				ok = true
				break
			}
		}
		if !ok {
			return &ValidationError{Path: path, Reason: fmt.Sprintf("must be one of %v", rules.enumInts)}
		}
	}
	if rules.minimum != nil && *rules.minimum >= 0 && n < uint64(*rules.minimum) {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("smaller than %d", *rules.minimum)}
	}
	if rules.maximum != nil && *rules.maximum >= 0 && n > uint64(*rules.maximum) {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("larger than %d", *rules.maximum)}
	}
	return nil
}

func checkFloat(fv reflect.Value, path string, rules fieldRules) error {
	f := fv.Float()
	if rules.minimum != nil && f < float64(*rules.minimum) {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("smaller than %d", *rules.minimum)}
	}
	if rules.maximum != nil && f > float64(*rules.maximum) {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("larger than %d", *rules.maximum)}
	}
	return nil
}

func checkSlice(fv reflect.Value, path string, optional bool, rules fieldRules) error {
	if fv.Kind() == reflect.Slice && fv.IsNil() {
		if !optional {
			return &ValidationError{Path: path, Reason: reasonRequired}
		}
		return nil
	}
	n := int64(fv.Len())
	if rules.minItems != nil && n < *rules.minItems {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("fewer than %d items", *rules.minItems)}
	}
	if rules.maxItems != nil && n > *rules.maxItems {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("more than %d items", *rules.maxItems)}
	}
	if rules.uniqueItems {
		seen := make(map[any]struct{}, fv.Len())
		for i := range fv.Len() {
			key := fv.Index(i).Interface()
			if _, dup := seen[key]; dup {
				return &ValidationError{Path: path, Reason: "duplicate items"}
			}
			seen[key] = struct{}{}
		}
	}
	for i := range fv.Len() {
		elemPath := fmt.Sprintf("%s[%d]", path, i)
		if err := checkElement(fv.Index(i), elemPath); err != nil {
			return err
		}
	}
	return nil
}

// checkElement validates one slice element or map value.
func checkElement(ev reflect.Value, path string) error {
	for ev.Kind() == reflect.Pointer {
		if ev.IsNil() {
			return &ValidationError{Path: path, Reason: reasonRequired}
		}
		ev = ev.Elem()
	}
	if ev.Kind() == reflect.Struct {
		if nv, ok := ev.Interface().(nestedValidator); ok {
			if err := nv.Validate(); err != nil {
				return prefixPath(err, path)
			}
			return nil
		}
		return checkStruct(ev, path)
	}
	if v, ok := ev.Interface().(validater); ok && !v.Valid() {
		return &ValidationError{Path: path, Reason: "invalid value"}
	}
	return nil
}

func checkMap(fv reflect.Value, path string, optional bool) error {
	if fv.IsNil() {
		if !optional {
			return &ValidationError{Path: path, Reason: reasonRequired}
		}
		return nil
	}
	iter := fv.MapRange()
	for iter.Next() {
		key := iter.Key()
		elemPath := fmt.Sprintf("%s[%v]", path, key.Interface())
		if err := checkElement(iter.Value(), elemPath); err != nil {
			return err
		}
	}
	return nil
}

// checkRawJSON validates raw JSON bytes: present unless optional and empty,
// and of the required top-level shape (object when wantObject).
func checkRawJSON(b []byte, path string, optional, wantObject bool) error {
	if len(bytes.TrimSpace(b)) == 0 {
		if !optional {
			return &ValidationError{Path: path, Reason: reasonRequired}
		}
		return nil
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return &ValidationError{Path: path, Reason: fmt.Sprintf("invalid JSON: %v", err)}
	}
	if wantObject {
		if _, ok := v.(map[string]any); !ok {
			return &ValidationError{Path: path, Reason: "must be a JSON object"}
		}
	}
	return nil
}

// checkControlVersions validates the version vector: non-nil when required,
// at most 64 entries, keys of 1–200 runes without control characters,
// values of at least zero.
func checkControlVersions(fv reflect.Value, path string, optional bool) error {
	if fv.IsNil() {
		if !optional {
			return &ValidationError{Path: path, Reason: reasonRequired}
		}
		return nil
	}
	m := fv.Interface().(ControlVersions)
	if len(m) > 64 {
		return &ValidationError{Path: path, Reason: "more than 64 entries"}
	}
	for k, v := range m {
		n := int64(utf8.RuneCountInString(k))
		if n < 1 || n > 200 || strings.ContainsRune(k, '\x00') || strings.IndexFunc(k, isControlRune) >= 0 {
			return &ValidationError{Path: path, Reason: fmt.Sprintf("invalid key %q", k)}
		}
		if v < 0 {
			return &ValidationError{Path: path, Reason: fmt.Sprintf("negative value for key %q", k)}
		}
	}
	return nil
}

func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7f
}
