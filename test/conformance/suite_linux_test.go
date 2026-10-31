//go:build conformance && linux

package conformance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/profiles"
)

const (
	harnessEnv = "GIFFORD_CONFORMANCE_HARNESS"
	commandEnv = "GIFFORD_CONFORMANCE_COMMAND"
	profileEnv = "GIFFORD_CONFORMANCE_PROFILE"
	cwdEnv     = "GIFFORD_CONFORMANCE_CWD"
	reportEnv  = "GIFFORD_CONFORMANCE_REPORT"

	syntheticModeEnv   = "GIFFORD_CONFORMANCE_SYNTHETIC_MODE"
	syntheticMutantEnv = "GIFFORD_CONFORMANCE_SYNTHETIC_MUTANT"
	syntheticChildEnv  = "GIFFORD_CONFORMANCE_SYNTHETIC_CHILD"
	senderModeEnv      = "GIFFORD_CONFORMANCE_SENDER_MODE"
	senderBinaryEnv    = "GIFFORD_CONFORMANCE_GIFFORD_BINARY"

	syntheticHarness = "synthetic"
	genericHarness   = "generic"
	unknownVersion   = "unknown"

	mutantProfileWarning = "profile_warning"
	mutantChild          = "extra_child"
	mutantIgnoreSubmit   = "ignore_submit"
	mutantNoEcho         = "no_echo"
)

var allowedHarnesses = map[string]bool{
	"claude-code":    true,
	"codex":          true,
	"muse-code":      true,
	"opencode":       true,
	"antigravity":    true,
	syntheticHarness: true,
}

type originalVariable struct {
	value string
	set   bool
}

type suiteConfig struct {
	harness        string
	profileHarness string
	command        []string
	profile        profiles.Profile
	profilePath    string
	cwd            string
	repoRoot       string
	root           string
	gifford        string
	version        string
	report         *reportWriter
	reportInRoot   bool
	originalXDG    map[string]originalVariable
}

var activeSuite *suiteConfig

func TestMain(m *testing.M) {
	if code, handled := runHelperMode(); handled {
		os.Exit(code)
	}

	config, err := prepareSuite()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "conformance setup failed: %v\n", err)
		if config != nil {
			config.recordSetupFailure(err)
			_ = config.report.Close()
			_, _ = fmt.Fprintf(os.Stderr, "private root preserved: %s\nreport: %s\n",
				config.root, config.report.path)
		}
		os.Exit(2)
	}
	activeSuite = config
	_, _ = fmt.Fprintf(os.Stderr, "conformance report: %s\n", config.report.path)

	code := m.Run()
	if err := config.report.Close(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "close conformance report: %v\n", err)
		code = 1
	}
	if code == 0 && !config.reportInRoot {
		if err := os.RemoveAll(config.root); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "remove conformance root: %v\n", err)
			code = 1
		}
	} else if code != 0 {
		_, _ = fmt.Fprintf(os.Stderr, "private root preserved: %s\n", config.root)
	}
	os.Exit(code)
}

func runHelperMode() (int, bool) {
	switch {
	case os.Getenv(syntheticChildEnv) == "1":
		return runSyntheticChild(), true
	case os.Getenv(senderModeEnv) == "1":
		if hasVersionArgument() {
			_, _ = fmt.Fprintln(os.Stdout, "gifford-conformance-sender 1.0.0")
			return 0, true
		}
		return runSenderHarness(), true
	case os.Getenv(syntheticModeEnv) == "1":
		if hasVersionArgument() {
			if os.Getenv(syntheticMutantEnv) == mutantProfileWarning {
				_, _ = fmt.Fprintln(os.Stdout, "gifford-conformance-synthetic development")
			} else {
				_, _ = fmt.Fprintln(os.Stdout, "gifford-conformance-synthetic 1.0.0")
			}
			return 0, true
		}
		return runSyntheticHarness(os.Getenv(syntheticMutantEnv)), true
	default:
		return 0, false
	}
}

func hasVersionArgument() bool {
	for _, argument := range os.Args[1:] {
		if argument == "--version" {
			return true
		}
	}
	return false
}

func prepareSuite() (*suiteConfig, error) {
	// Unix sockets cap their path length, so every nested run starts under a
	// deliberately short root rather than testing.TempDir or a descriptive name.
	root, err := os.MkdirTemp("", "gc-*")
	if err != nil {
		return nil, fmt.Errorf("create private root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure private root: %w", err)
	}

	reportPath := strings.TrimSpace(os.Getenv(reportEnv))
	reportInRoot := reportPath == ""
	if reportInRoot {
		reportPath = filepath.Join(root, "conformance-report.txt")
	} else {
		reportPath, err = filepath.Abs(reportPath)
		if err != nil {
			return nil, fmt.Errorf("resolve report path: %w", err)
		}
	}
	report, err := newReportWriter(reportPath)
	if err != nil {
		return nil, err
	}
	config := &suiteConfig{root: root, report: report, reportInRoot: reportInRoot}

	config.harness = strings.TrimSpace(os.Getenv(harnessEnv))
	if !allowedHarnesses[config.harness] {
		return config, fmt.Errorf("%s must be one of claude-code, codex, muse-code, opencode, antigravity or synthetic", harnessEnv)
	}
	config.profileHarness = config.harness
	if config.harness == syntheticHarness {
		config.profileHarness = genericHarness
	}

	config.repoRoot, err = conformanceRepoRoot()
	if err != nil {
		return config, err
	}
	config.command, err = conformanceCommand(config.harness)
	if err != nil {
		return config, err
	}
	config.profilePath, config.profile, err = conformanceProfile(config)
	if err != nil {
		return config, err
	}
	config.cwd, err = conformanceCWD(config)
	if err != nil {
		return config, err
	}
	config.originalXDG = captureOriginalXDG()

	buildDir := filepath.Join(root, "build")
	if err := secureMkdirAll(buildDir); err != nil {
		return config, err
	}
	config.gifford = filepath.Join(buildDir, "gifford")
	if err := buildConformanceGifford(config, buildDir); err != nil {
		return config, err
	}
	config.version = probeConfiguredVersion(config)
	return config, nil
}

func conformanceRepoRoot() (string, error) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("locate conformance source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("locate repository root: %w", err)
	}
	return root, nil
}

func conformanceCommand(harness string) ([]string, error) {
	raw := strings.TrimSpace(os.Getenv(commandEnv))
	if raw == "" && harness == syntheticHarness {
		executable, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("locate synthetic harness: %w", err)
		}
		return []string{executable}, nil
	}
	if raw == "" {
		return nil, fmt.Errorf("%s must contain the harness argv as JSON", commandEnv)
	}
	var command []string
	if err := json.Unmarshal([]byte(raw), &command); err != nil {
		return nil, fmt.Errorf("decode %s JSON: %w", commandEnv, err)
	}
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return nil, fmt.Errorf("%s must contain a non-empty executable", commandEnv)
	}
	for _, argument := range command {
		if strings.IndexByte(argument, 0) >= 0 {
			return nil, fmt.Errorf("%s contains a NUL byte", commandEnv)
		}
	}
	resolved, err := exec.LookPath(command[0])
	if err != nil {
		return nil, errors.New("resolve harness executable: executable not found")
	}
	command[0], err = filepath.Abs(resolved)
	if err != nil {
		return nil, fmt.Errorf("resolve harness executable path: %w", err)
	}
	return command, nil
}

func conformanceProfile(config *suiteConfig) (string, profiles.Profile, error) {
	path := strings.TrimSpace(os.Getenv(profileEnv))
	if path == "" && config.harness == syntheticHarness {
		path = filepath.Join(config.repoRoot, "test", "conformance", "testdata", "synthetic-profile.yaml")
	}
	if path == "" {
		return "", profiles.Profile{}, fmt.Errorf("%s must name a measured YAML profile", profileEnv)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", profiles.Profile{}, fmt.Errorf("resolve measured profile: %w", err)
	}
	raw, err := os.ReadFile(absolute)
	if err != nil {
		return "", profiles.Profile{}, fmt.Errorf("read measured profile: %w", err)
	}
	profile, err := profiles.ParseFile(config.profileHarness, raw)
	if err != nil {
		return "", profiles.Profile{}, fmt.Errorf("parse measured profile: %w", err)
	}
	if !profile.Measured {
		return "", profiles.Profile{}, errors.New("conformance profile must declare measured: true")
	}
	if profile.Harness != config.profileHarness {
		return "", profiles.Profile{}, fmt.Errorf("conformance profile harness %q does not match %q", profile.Harness, config.profileHarness)
	}
	return absolute, profile, nil
}

func conformanceCWD(config *suiteConfig) (string, error) {
	path := strings.TrimSpace(os.Getenv(cwdEnv))
	if path == "" && config.harness == syntheticHarness {
		path = filepath.Join(config.root, "synthetic-cwd")
		if err := secureMkdirAll(path); err != nil {
			return "", err
		}
	}
	if path == "" {
		var err error
		path, err = os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve harness cwd: %w", err)
		}
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve harness cwd: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("inspect harness cwd: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("harness cwd is not a directory")
	}
	return absolute, nil
}

func captureOriginalXDG() map[string]originalVariable {
	result := make(map[string]originalVariable, 3)
	for _, name := range []string{"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_RUNTIME_DIR"} {
		value, ok := os.LookupEnv(name)
		result[name] = originalVariable{value: value, set: ok}
	}
	return result
}

func buildConformanceGifford(config *suiteConfig, buildDir string) error {
	realProfile := filepath.Join(config.repoRoot, "profiles", config.profileHarness+".yaml")
	overlayRaw, err := json.Marshal(struct {
		Replace map[string]string `json:"Replace"`
	}{Replace: map[string]string{realProfile: config.profilePath}})
	if err != nil {
		return fmt.Errorf("encode profile overlay: %w", err)
	}
	overlay := filepath.Join(buildDir, "overlay.json")
	if err := os.WriteFile(overlay, overlayRaw, 0o600); err != nil {
		return fmt.Errorf("write profile overlay: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-overlay", overlay,
		"-o", config.gifford, "./cmd/gifford")
	cmd.Dir = config.repoRoot
	cmd.Env = os.Environ()
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build gifford with measured profile: %w: %s", err, boundedText(output.String(), 4000))
	}
	return nil
}

func probeConfiguredVersion(config *suiteConfig) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, config.command[0], "--version")
	cmd.Dir = config.cwd
	cmd.Env = os.Environ()
	if config.harness == syntheticHarness {
		cmd.Env = setEnv(cmd.Env, syntheticModeEnv, "1")
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = ioDiscard{}
	if err := cmd.Run(); err != nil {
		return unknownVersion
	}
	line, _ := bufio.NewReader(bytes.NewReader(output.Bytes())).ReadString('\n')
	version := profiles.ParseVersion(strings.TrimSpace(line))
	if version == "" {
		return unknownVersion
	}
	return version
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func setEnv(environment []string, name, value string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if key != name {
			result = append(result, entry)
		}
	}
	return append(result, name+"="+value)
}

func secureMkdirAll(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create private directory: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("secure private directory: %w", err)
	}
	return nil
}

type reportWriter struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func newReportWriter(path string) (*reportWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open conformance report: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure conformance report: %w", err)
	}
	return &reportWriter{file: file, path: path}, nil
}

func (w *reportWriter) Record(harness, version, scenario, status, evidence, root string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	evidence = boundedText(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(evidence), 1200)
	line := fmt.Sprintf("harness=%s version=%s scenario=%s %s evidence=%s",
		harness, version, scenario, status, strconv.Quote(evidence))
	if status == "FAIL" {
		line += " root=" + strconv.Quote(root)
	}
	if _, err := fmt.Fprintln(w.file, line); err != nil {
		return fmt.Errorf("write conformance report: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync conformance report: %w", err)
	}
	return nil
}

func (w *reportWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (config *suiteConfig) recordSetupFailure(setupErr error) {
	if config.report == nil {
		return
	}
	harness := config.harness
	if harness == "" {
		harness = "unknown"
	}
	_ = config.report.Record(harness, unknownVersion, "SETUP", "FAIL", setupErr.Error(), config.root)
}

func boundedText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + "..."
}
