package redact

// Kind names one class of secret the redactor masks. Kinds are listed in
// priority order: on bytes claimed by two findings the earlier kind wins,
// the later one only widens the mask without counting.
type Kind string

// Kind values in priority order.
const (
	KindPrivateKey       Kind = "private_key"
	KindConnectionString Kind = "connection_string"
	KindAuthorization    Kind = "authorization"
	KindAPIKey           Kind = "api_key"
	KindJWT              Kind = "jwt"
	KindSecretAssignment Kind = "secret_assignment"
	KindSensitivePath    Kind = "sensitive_path"
)

// EnumValues returns the kinds in priority order. The slice is fresh: the
// caller may reorder it without affecting later calls.
func (Kind) EnumValues() []string {
	return []string{
		string(KindPrivateKey),
		string(KindConnectionString),
		string(KindAuthorization),
		string(KindAPIKey),
		string(KindJWT),
		string(KindSecretAssignment),
		string(KindSensitivePath),
	}
}

// Valid reports whether k is a known kind. Matching is exact: empty and
// differently cased names are not kinds.
func (k Kind) Valid() bool {
	switch k {
	case KindPrivateKey,
		KindConnectionString,
		KindAuthorization,
		KindAPIKey,
		KindJWT,
		KindSecretAssignment,
		KindSensitivePath:
		return true
	default:
		return false
	}
}

// priority is the kind's index in EnumValues order. Unknown kinds sort last;
// the engine rejects them before sorting, so this never decides output.
func (k Kind) priority() int {
	switch k {
	case KindPrivateKey:
		return 0
	case KindConnectionString:
		return 1
	case KindAuthorization:
		return 2
	case KindAPIKey:
		return 3
	case KindJWT:
		return 4
	case KindSecretAssignment:
		return 5
	case KindSensitivePath:
		return 6
	default:
		return 7
	}
}
