package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/protocol"
)

// ErrInvalidConfig is wrapped by every configuration failure. Its errors
// name a field and a rule, never a value: a value may be the endpoint or
// its credential.
var ErrInvalidConfig = errors.New("invalid gateway config")

// maxConfigBytes bounds the configuration document.
const maxConfigBytes = 64 << 10

// maxTimeout bounds a call's deadline.
const maxTimeout = 5 * time.Minute

// redactedEndpoint is how an Endpoint prints, encodes and logs.
const redactedEndpoint = "[endpoint redacted]"

// Endpoint is where the Glimmer endpoint lives and how to authenticate
// to it. Both are read from the environment at run time. It never prints,
// encodes or logs its values: every formatting path writes a fixed text.
type Endpoint struct {
	url   *url.URL
	token string
}

// Format writes a fixed text for every verb, %+v and %#v included.
func (Endpoint) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redactedEndpoint) }

// String returns a fixed text.
func (Endpoint) String() string { return redactedEndpoint }

// GoString returns a fixed text.
func (Endpoint) GoString() string { return redactedEndpoint }

// MarshalJSON encodes a fixed text.
func (Endpoint) MarshalJSON() ([]byte, error) { return []byte(`"` + redactedEndpoint + `"`), nil }

// MarshalText encodes a fixed text.
func (Endpoint) MarshalText() ([]byte, error) { return []byte(redactedEndpoint), nil }

// LogValue logs a fixed text.
func (Endpoint) LogValue() slog.Value { return slog.StringValue(redactedEndpoint) }

// Profile is how one contract is served: its GatewayProfile and, for
// analyst-lora, the adapter digest every call record carries.
type Profile struct {
	protocol.GatewayProfile
	AdapterSHA256 protocol.SHA256Hex
}

// Config is the validated gateway configuration.
type Config struct {
	Endpoint          Endpoint
	Timeout           time.Duration
	BaseModelRepo     string
	BaseModelRevision string
	Runtime           string
	Quantization      string
	// Profiles holds one profile for every model contract.
	Profiles map[protocol.ModelContract]Profile
}

// rawConfig is the YAML shape. The endpoint is named by environment
// variables, so the document never holds the URL or the credential.
type rawConfig struct {
	Endpoint struct {
		URLEnv   string `yaml:"url_env"`
		TokenEnv string `yaml:"token_env"`
	} `yaml:"endpoint"`
	TimeoutMS int64 `yaml:"timeout_ms"`
	BaseModel struct {
		Repo     string `yaml:"repo"`
		Revision string `yaml:"revision"`
	} `yaml:"base_model"`
	Runtime      string       `yaml:"runtime"`
	Quantization string       `yaml:"quantization"`
	Profiles     []rawProfile `yaml:"profiles"`
}

type rawProfile struct {
	Contract         protocol.ModelContract    `yaml:"contract"`
	InferenceProfile protocol.InferenceProfile `yaml:"inference_profile"`
	AdapterName      string                    `yaml:"adapter_name"`
	AdapterRevision  string                    `yaml:"adapter_revision"`
	AdapterSHA256    protocol.SHA256Hex        `yaml:"adapter_sha256"`
}

func configErr(field, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidConfig, field, reason)
}

// ParseConfig reads the gateway configuration and resolves its endpoint
// through getenv. It refuses unknown keys, a missing or duplicate
// contract profile, a profile its GatewayProfile.Validate refuses, an
// adapter digest on base or missing on analyst-lora, an endpoint that is
// not https (plain http only to a loopback address), and an empty
// credential.
func ParseConfig(r io.Reader, getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, configErr("endpoint", "no environment")
	}
	b, err := io.ReadAll(io.LimitReader(r, maxConfigBytes+1))
	if err != nil {
		return Config{}, configErr("document", "unreadable")
	}
	if len(b) > maxConfigBytes {
		return Config{}, configErr("document", "too large")
	}
	var raw rawConfig
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	// The decoder's error quotes the document, so it is not wrapped.
	if err := dec.Decode(&raw); err != nil {
		return Config{}, configErr("document", "not a valid gateway config")
	}
	ep, err := resolveEndpoint(raw.Endpoint.URLEnv, raw.Endpoint.TokenEnv, getenv)
	if err != nil {
		return Config{}, err
	}
	if raw.TimeoutMS < 1 || time.Duration(raw.TimeoutMS)*time.Millisecond > maxTimeout {
		return Config{}, configErr("timeout_ms", "out of range")
	}
	cfg := Config{
		Endpoint:          ep,
		Timeout:           time.Duration(raw.TimeoutMS) * time.Millisecond,
		BaseModelRepo:     raw.BaseModel.Repo,
		BaseModelRevision: raw.BaseModel.Revision,
		Runtime:           raw.Runtime,
		Quantization:      raw.Quantization,
		Profiles:          make(map[protocol.ModelContract]Profile, len(raw.Profiles)),
	}
	for _, f := range []struct {
		name  string
		value string
		max   int
		opt   bool
	}{
		{"base_model.repo", cfg.BaseModelRepo, 200, false},
		{"base_model.revision", cfg.BaseModelRevision, 100, false},
		{"runtime", cfg.Runtime, 50, false},
		{"quantization", cfg.Quantization, 50, true},
	} {
		if (f.value == "" && !f.opt) || len(f.value) > f.max {
			return Config{}, configErr(f.name, "length out of range")
		}
	}
	for _, rp := range raw.Profiles {
		p := Profile{
			GatewayProfile: protocol.GatewayProfile{
				Contract:         rp.Contract,
				InferenceProfile: rp.InferenceProfile,
				AdapterName:      rp.AdapterName,
				AdapterRevision:  rp.AdapterRevision,
			},
			AdapterSHA256: rp.AdapterSHA256,
		}
		if err := p.Validate(); err != nil {
			return Config{}, configErr("profiles", "invalid profile")
		}
		lora := p.InferenceProfile == protocol.InferenceProfileAnalystLoRA
		if lora != (p.AdapterSHA256 != "") || (lora && !p.AdapterSHA256.Valid()) {
			return Config{}, configErr("profiles.adapter_sha256", "required on analyst-lora only")
		}
		if _, dup := cfg.Profiles[p.Contract]; dup {
			return Config{}, configErr("profiles", "contract configured twice")
		}
		cfg.Profiles[p.Contract] = p
	}
	for _, c := range protocol.ModelContract("").EnumValues() {
		if _, ok := cfg.Profiles[protocol.ModelContract(c)]; !ok {
			return Config{}, configErr("profiles", "every contract needs a profile")
		}
	}
	return cfg, nil
}

// resolveEndpoint reads the endpoint from the named variables.
func resolveEndpoint(urlEnv, tokenEnv string, getenv func(string) string) (Endpoint, error) {
	if urlEnv == "" || tokenEnv == "" {
		return Endpoint{}, configErr("endpoint", "url_env and token_env are required")
	}
	u, err := url.Parse(getenv(urlEnv))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return Endpoint{}, configErr("endpoint.url_env", "not an endpoint URL")
	}
	switch u.Scheme {
	case "https":
	case "http":
		if ip := net.ParseIP(u.Hostname()); ip == nil || !ip.IsLoopback() {
			return Endpoint{}, configErr("endpoint.url_env", "plain http only to a loopback address")
		}
	default:
		return Endpoint{}, configErr("endpoint.url_env", "not an endpoint URL")
	}
	token := getenv(tokenEnv)
	if token == "" {
		return Endpoint{}, configErr("endpoint.token_env", "credential is empty")
	}
	for i := range len(token) {
		if token[i] <= ' ' || token[i] > '~' {
			return Endpoint{}, configErr("endpoint.token_env", "credential is not visible ASCII")
		}
	}
	return Endpoint{url: u, token: token}, nil
}
