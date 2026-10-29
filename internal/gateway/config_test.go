package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const testHTTPS = "https://glimmer.example.invalid/v1/infer"

// Config fields named by refusals.
const (
	fieldURLEnv    = "endpoint.url_env"
	fieldTokenEnv  = "endpoint.token_env"
	fieldProfiles  = "profiles"
	fieldAdapterSH = "profiles.adapter_sha256"
	fieldDocument  = "document"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t, testHTTPS)
	require.Len(t, cfg.Profiles, 3)
	lora := cfg.Profiles[protocol.ModelContractAnalyzeAgentState]
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, lora.InferenceProfile)
	require.Equal(t, protocol.SHA256Hex(testAdapter), lora.AdapterSHA256)
	require.Equal(t, protocol.InferenceProfileBase, cfg.Profiles[protocol.ModelContractTriageMessage].InferenceProfile)
	require.Equal(t, testHTTPS, cfg.Endpoint.url.String())
	require.Equal(t, testToken, cfg.Endpoint.token)
}

func TestParseConfigRefuses(t *testing.T) {
	t.Parallel()
	const loraLine = "    inference_profile: analyst-lora\n"
	cases := []struct {
		name  string
		edit  func(string) string
		env   map[string]string
		field string
	}{
		{"unknown key", func(s string) string { return s + "sentinel_key: 1\n" }, nil, fieldDocument},
		{"not yaml", func(string) string { return "{sentinel" }, nil, fieldDocument},
		{"no url env", func(s string) string { return strings.Replace(s, "  url_env: "+testURLEnv+"\n", "", 1) }, nil, "endpoint"},
		{"url unset", nil, map[string]string{testTokenEnv: testToken}, fieldURLEnv},
		{"token unset", nil, map[string]string{testURLEnv: testHTTPS}, fieldTokenEnv},
		{"token with a space", nil, map[string]string{testURLEnv: testHTTPS, testTokenEnv: "sentinel token"}, fieldTokenEnv},
		{"token with a newline", nil, map[string]string{testURLEnv: testHTTPS, testTokenEnv: "sentinel\r\nX-Evil: 1"}, fieldTokenEnv},
		{"plain http off loopback", nil, map[string]string{testURLEnv: "http://glimmer.example.invalid/infer", testTokenEnv: testToken}, fieldURLEnv},
		{"credentials in the url", nil, map[string]string{testURLEnv: "https://sentinel:pw@glimmer.example.invalid/", testTokenEnv: testToken}, fieldURLEnv},
		{"query in the url", nil, map[string]string{testURLEnv: "https://glimmer.example.invalid/?key=sentinel", testTokenEnv: testToken}, fieldURLEnv},
		{"other scheme", nil, map[string]string{testURLEnv: "ftp://glimmer.example.invalid/", testTokenEnv: testToken}, fieldURLEnv},
		{"a timeout is not configurable", func(s string) string { return s + "timeout_ms: 2000\n" }, nil, fieldDocument},
		{"no base model", func(s string) string { return strings.Replace(s, "  repo: example.invalid/glimmer\n", "", 1) }, nil, "base_model.repo"},
		{"no runtime", func(s string) string { return strings.Replace(s, "runtime: vllm\n", "", 1) }, nil, "runtime"},
		{"missing contract", func(s string) string {
			return strings.Replace(s, "  - contract: TRIAGE_MESSAGE\n    inference_profile: base\n", "", 1)
		}, nil, fieldProfiles},
		{"contract twice", func(s string) string {
			return s + "  - contract: TRIAGE_MESSAGE\n    inference_profile: base\n"
		}, nil, fieldProfiles},
		{"unknown contract", func(s string) string {
			return s + "  - contract: CLASSIFY\n    inference_profile: base\n"
		}, nil, fieldProfiles},
		{"lora without adapter", func(s string) string {
			return strings.Replace(s, "    adapter_name: analyst\n", "", 1)
		}, nil, fieldProfiles},
		{"lora on triage", func(s string) string {
			return strings.Replace(s, "  - contract: TRIAGE_MESSAGE\n    inference_profile: base\n",
				"  - contract: TRIAGE_MESSAGE\n"+loraLine+"    adapter_name: a\n    adapter_revision: r\n    adapter_sha256: "+testAdapter+"\n", 1)
		}, nil, fieldProfiles},
		{"lora without digest", func(s string) string {
			return strings.Replace(s, "    adapter_sha256: "+testAdapter+"\n", "", 1)
		}, nil, fieldAdapterSH},
		{"bad digest", func(s string) string { return strings.Replace(s, testAdapter, "AB", 1) }, nil, fieldAdapterSH},
		{"digest on base", func(s string) string {
			return strings.Replace(s, "  - contract: TRIAGE_MESSAGE\n    inference_profile: base\n",
				"  - contract: TRIAGE_MESSAGE\n    inference_profile: base\n    adapter_sha256: "+testAdapter+"\n", 1)
		}, nil, fieldAdapterSH},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := testConfigYAML
			if tc.edit != nil {
				doc = tc.edit(doc)
			}
			env := tc.env
			if env == nil {
				env = map[string]string{testURLEnv: testHTTPS, testTokenEnv: testToken}
			}
			_, err := ParseConfig(strings.NewReader(doc), envOf(env))
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), ": "+tc.field+": ")
			require.NotContains(t, err.Error(), "sentinel", "no value in the error")
			require.NotContains(t, err.Error(), testToken)
			require.NotContains(t, err.Error(), "example.invalid/v1")
		})
	}
	_, err := ParseConfig(strings.NewReader(testConfigYAML), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = ParseConfig(strings.NewReader(strings.Repeat("#", maxConfigBytes+1)), envOf(nil))
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestEndpointNeverPrints(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t, testHTTPS)
	out := make([]string, 0, 20)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
		out = append(out, fmt.Sprintf(verb, cfg), fmt.Sprintf(verb, cfg.Endpoint), fmt.Sprintf(verb, &cfg))
	}
	enc, err := json.Marshal(cfg)
	require.NoError(t, err)
	out = append(out, string(enc))
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("Config", "config", cfg, "endpoint", cfg.Endpoint)
	out = append(out, logged.String())
	for _, s := range out {
		require.NotContains(t, s, testToken)
		require.NotContains(t, s, "glimmer.example.invalid")
	}
}
