package redacttest

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/smellak/gifford/internal/redact"
)

// errUnknownValue means Value was asked for a name the fixture does not
// define. The error names the request, never a secret. It stays private:
// §9.4 promises that text, not a variable.
var errUnknownValue = errors.New("unknown redacttest value")

// Alphabets for draw. Secret text is assembled from these at run time, so
// no committed line holds a secret shape.
const (
	alphabetBase32    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	alphabetAlnum     = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	alphabetBase64    = alphabetAlnum + "+/"
	alphabetBase64URL = alphabetAlnum + "-_"
	alphabetHex       = "0123456789abcdef"
	alphabetDigits    = "0123456789"
	alphabetLower     = "abcdefghijklmnopqrstuvwxyz"
)

// draw picks n runes from alphabet with splitmix64 at seed: one fixed seed
// per value, so every run sees the same bytes. Lengths matter, seeds do
// not. No random-number generator is involved anywhere on this path.
func draw(seed uint64, alphabet string, n int) string {
	s := seed
	var sb strings.Builder
	sb.Grow(n)
	for range n {
		s += 0x9e3779b97f4a7c15
		z := s
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		sb.WriteByte(alphabet[z%uint64(len(alphabet))])
	}
	return sb.String()
}

// marker builds a PEM marker line from parts, so no source line holds a
// whole marker. Tests needing one build theirs the same way.
func marker(word, label string) string {
	return strings.Repeat("-", 5) + word + " " + label + strings.Repeat("-", 5)
}

// Value assembles the named synthetic secret at run time. Multi-line
// values join with newline; pem_pkcs8_inline joins with the two characters
// backslash-n instead, matching the JSON-escaped form on one line.
func Value(name string) (string, error) {
	switch name {
	case "aws_access_key_id":
		return "AKIA" + draw(1, alphabetBase32, 16), nil
	case "aws_secret_access_key":
		return draw(2, alphabetBase64, 40), nil
	case "github_pat":
		return "ghp_" + draw(3, alphabetAlnum, 36), nil
	case "github_fine_grained":
		return "github_pat_" + draw(4, alphabetAlnum, 22) + "_" + draw(104, alphabetAlnum, 59), nil
	case "gitlab_pat":
		return "glpat-" + draw(5, alphabetAlnum, 20), nil
	case "slack_bot":
		return "xoxb-" + draw(6, alphabetDigits, 12) + "-" +
			draw(106, alphabetDigits, 13) + "-" + draw(206, alphabetAlnum, 24), nil
	case "stripe_live":
		return "sk_live_" + draw(7, alphabetAlnum, 24), nil
	case "openai_project":
		return "sk-proj-" + draw(8, alphabetBase64URL, 156), nil
	case "anthropic":
		return "sk-ant-api03-" + draw(9, alphabetBase64URL, 93) + "AA", nil
	case "google_api":
		return "AIza" + draw(10, alphabetBase64URL, 35), nil
	case "huggingface":
		return "hf_" + draw(11, alphabetAlnum, 34), nil
	case "npm":
		return "npm_" + draw(12, alphabetAlnum, 36), nil
	case "pypi":
		return "pypi-AgEIcHlwaS5vcmc" + draw(13, alphabetBase64URL, 60), nil
	case "sendgrid":
		return "SG." + draw(14, alphabetBase64URL, 22) + "." + draw(114, alphabetBase64URL, 43), nil
	case "jwt":
		head := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
		body := base64.RawURLEncoding.EncodeToString(
			[]byte(`{"sub":"user-42","iat":1798000000,"scope":"repo"}`),
		)
		return head + "." + body + "." + draw(15, alphabetBase64URL, 43), nil
	case "db_userinfo":
		return "app_rw:" + draw(16, alphabetAlnum, 20), nil
	case "x_access_userinfo":
		return "x-access-token:ghp_" + draw(3, alphabetAlnum, 36), nil
	case "basic_credentials":
		return base64.StdEncoding.EncodeToString([]byte("deploy:" + draw(18, alphabetAlnum, 16))), nil
	case "hex64":
		return draw(19, alphabetHex, 64), nil
	case "cookie_session":
		return "session=" + draw(20, alphabetBase64URL, 32), nil
	case "digest_params":
		return `username="ci", nonce="` + draw(21, alphabetHex, 16) +
			`", response="` + draw(121, alphabetHex, 32) + `"`, nil
	case "digest_params_json":
		return `username=\"ci\", nonce=\"` + draw(21, alphabetHex, 16) +
			`\", response=\"` + draw(121, alphabetHex, 32) + `\"`, nil
	case "password_spaces":
		return draw(23, alphabetLower, 7) + " " + draw(123, alphabetLower, 6) +
			" " + draw(223, alphabetAlnum, 9), nil
	case "pem_openssh":
		lines := []string{marker("BEGIN", "OPENSSH PRIVATE KEY")}
		for i, n := range []int{70, 70, 70, 70, 34} {
			lines = append(lines, draw(uint64(24+i), alphabetBase64, n))
		}
		return strings.Join(append(lines, marker("END", "OPENSSH PRIVATE KEY")), "\n"), nil
	case "pem_pkcs8_inline":
		sep := `\n`
		return marker("BEGIN", "PRIVATE KEY") + sep + draw(25, alphabetBase64, 64) + sep +
			draw(25, alphabetBase64, 56) + sep + marker("END", "PRIVATE KEY") + sep, nil
	case "sensitive_contract":
		return "/srv/clients/northwind/contracts/2026-q3.md", nil
	case "acme_env":
		return "~/work/clients/acme/deploy.env", nil
	case "sensitive_notes":
		return "~/work/clients/acme/notes/rotation.txt", nil
	case "git_sha":
		return draw(29, alphabetHex, 40), nil
	case "uuid":
		return draw(30, alphabetHex, 8) + "-" + draw(130, alphabetHex, 4) + "-" +
			draw(230, alphabetHex, 4) + "-" + draw(330, alphabetHex, 4) + "-" +
			draw(430, alphabetHex, 12), nil
	case "sha256_hex":
		return draw(31, alphabetHex, 64), nil
	case "sha256_b64":
		return draw(32, alphabetBase64, 43) + "=", nil
	case "cert_body":
		return draw(33, alphabetBase64, 64) + "\n" +
			draw(133, alphabetBase64, 64) + "\n" + draw(233, alphabetBase64, 20) + "==", nil
	default:
		return "", fmt.Errorf("redacttest: unknown value %q: %w", name, errUnknownValue)
	}
}

// Options returns the sensitive paths the fixture screens assume: the
// contract subtree and the acme home subtree.
func Options() redact.Options {
	return redact.Options{SensitivePaths: []string{"/srv/clients/northwind", "~/work/clients/acme"}}
}
