// Package config loads and validates config.yaml.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/notifications"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

// SupportedSchemaVersion is the only value Config.SchemaVersion may have.
const SupportedSchemaVersion = 1

// ErrUnsupportedSchemaVersion means config.yaml declares a schema_version
// this binary does not understand.
var ErrUnsupportedSchemaVersion = errors.New("unsupported config schema_version")

// ErrInvalidNodeID means config.yaml declares a node_id outside
// protocol.NodeID. A missing node_id is not a load error: commands that
// need it refuse with their own message.
var ErrInvalidNodeID = errors.New("invalid config node_id")

// ErrInvalidOperatorAgent means the configured incident owner is not an agent id.
var ErrInvalidOperatorAgent = errors.New("invalid config operator_agent")

// ErrInvalidOrganizationID means config.yaml declares an organization_id
// outside the same canonical character set and length as node_id.
var ErrInvalidOrganizationID = errors.New("invalid config organization_id")

// ErrInvalidFederation means the federation listener or trust bundle
// configuration cannot be used safely.
var ErrInvalidFederation = errors.New("invalid config federation")

// ErrInvalidNotifications means the optional tmux notification adapter is
// present but cannot be invoked safely and deterministically.
var ErrInvalidNotifications = errors.New("invalid config notifications")

// ErrInvalidChannel means the channel configuration cannot select a known
// embedded loop budget profile.
var ErrInvalidChannel = errors.New("invalid config channel")

// ErrInvalidBridge means the bridge panel line is not a semicolon-free
// notice line. Errors name the field but never include deployment text.
var ErrInvalidBridge = errors.New("invalid config bridge")

// ErrInvalidNodeSocket means node_socket cannot be served safely: its path
// is not a clean absolute socket path, or its bridge uid is missing, root or
// not a plain decimal uid.
var ErrInvalidNodeSocket = errors.New("invalid config node_socket")

// ErrInvalidTmuxAttach means the node's attached-pane safety window is not a
// positive duration string.
var ErrInvalidTmuxAttach = errors.New("invalid config tmux_attach")

// DefaultTmuxHumanActivityWindow is the fail-closed recent-client window used
// when the node configuration does not override it.
const DefaultTmuxHumanActivityWindow = 15 * time.Second

// ErrInvalidTmuxInstances means tmux_instances.statements is not a clean
// absolute path.
var ErrInvalidTmuxInstances = errors.New("invalid config tmux_instances")

// Config is the parsed shape of config.yaml. It carries only what this
// subphase needs. Every later subphase that adds a component with its own
// configuration adds its own top-level field to this struct in its own PR.
type Config struct {
	SchemaVersion  int              `yaml:"schema_version"`
	NodeID         protocol.NodeID  `yaml:"node_id"`
	OperatorAgent  protocol.AgentID `yaml:"operator_agent,omitempty"`
	OrganizationID string           `yaml:"organization_id"`
	Federation     Federation       `yaml:"federation"`
	Notifications  Notifications    `yaml:"notifications"`
	Channel        Channel          `yaml:"channel"`
	Bridge         Bridge           `yaml:"bridge"`
	NodeSocket     *NodeSocket      `yaml:"node_socket"`
	TmuxAttach     TmuxAttach       `yaml:"tmux_attach"`
	TmuxInstances  TmuxInstances    `yaml:"tmux_instances"`
	ML             ML               `yaml:"ml"`
	StateAnalysis  *StateAnalysis   `yaml:"state_analysis"`
}

// Channel configures the local channel policy defaults.
type Channel struct {
	BudgetProfile protocol.BudgetProfile `yaml:"budget_profile"`
}

// Bridge carries the deployment's bridge panel line. Empty means the
// neutral default; anything set must be one semicolon-free notice line.
type Bridge struct {
	NoticeLine string `yaml:"notice_line"`
	// ResponseWatchProducer is the pinned service producer the hub stamps
	// on bridge response reminders. The bridge caller never supplies it:
	// it derives from the bridge uid and this configuration. Empty means
	// the node refuses reminder PUTs.
	ResponseWatchProducer protocol.AgentID `yaml:"response_watch_producer"`
}

// NodeSocket opens the socket that every local user may connect to. The
// node classifies each peer by SO_PEERCRED, and BridgeUID is the only uid
// that reaches the bridge methods. It is a number, never a user name: a
// name would be resolved again at each start, and a recreated user would
// silently become the bridge. Nil keeps the node reachable only through its
// private runtime directory.
type NodeSocket struct {
	Path      string
	BridgeUID uint32
}

// TmuxAttach configures delivery to node-registered, already-running panes.
type TmuxAttach struct {
	HumanActivityWindow time.Duration `yaml:"human_activity_window"`
	// HelperManifestSHA256 pins the privileged helper's manifest
	// revision. Empty keeps the raw tmux path; set routes every
	// command through the helper and refuses servers without a bank.
	HelperManifestSHA256 string `yaml:"helper_manifest_sha256"`
}

// TmuxInstances configures the registry of agents that already run in a
// tmux pane.
type TmuxInstances struct {
	// Statements is the administrator's instances file. Empty means
	// instances.yaml in the config dir.
	Statements string `yaml:"statements"`
}

// StatementsPath returns the instances file the node reconciles: on a
// system node, /etc/gifford/instances.yaml unless configured.
func (t TmuxInstances) StatementsPath(configDir string) string {
	if t.Statements != "" {
		return t.Statements
	}
	return filepath.Join(configDir, "instances.yaml")
}

// StateAnalysis enables the node's ANALYZE_AGENT_STATE route and pins the
// exact profile and calibration evidence written beside every state snapshot.
// Nil leaves model analysis disabled.
type StateAnalysis struct {
	GatewayConfig       string
	ProfileRevision     string
	ProfileSHA256       protocol.SHA256Hex
	CalibrationRevision string
	CalibrationSHA256   protocol.SHA256Hex
	Temperature         float64
	IdleThreshold       float64
}

// ErrInvalidStateAnalysis means the optional state-analysis deployment is not
// fully pinned. Errors name fields but never include endpoint credentials.
var ErrInvalidStateAnalysis = errors.New("invalid config state_analysis")

// Federation configures the optional node-to-node HTTPS listener. An empty
// Listen disables it.
type Federation struct {
	Listen      string `yaml:"listen"`
	TrustBundle string `yaml:"trust_bundle"`
	Advertise   string `yaml:"advertise"`
}

// Notifications configures the optional human-notification adapter. A nil
// TMUX value means notifications remain durable but no external action runs.
type Notifications struct {
	TMUX *NotificationTMUX `yaml:"tmux"`
}

// NotificationTMUX names exactly one socket and one client. The service never
// discovers or falls back to another client.
type NotificationTMUX struct {
	Socket   string        `yaml:"socket"`
	Client   string        `yaml:"client"`
	Duration time.Duration `yaml:"duration"`
	Timeout  time.Duration `yaml:"timeout"`
	Template string        `yaml:"template"`
}

// ML configures the dataset and evaluation tools.
type ML struct {
	// Readiness are the dataset preparation gate's thresholds.
	Readiness dataset.ReadinessThresholds `yaml:"readiness"`
	// ScreenLines is ml.screen_lines, N of the screen projection: how
	// many of the screen's last rows the model sees, 1 to
	// screenproj.MaxLines. It has no default; 0 is absent.
	ScreenLines int `yaml:"screen_lines"`
	// Mask is ml.mask: "none" for no domain mask, variant B, or the
	// absolute path of the domain mask list. It has no default; "" is
	// absent.
	Mask string `yaml:"mask"`
	// PermissionDialogSources is the exact harness/version allowlist for the
	// permission-dialog observation policy. Empty keeps the feature off.
	PermissionDialogSources []session.PermissionDialogSource `yaml:"permission_dialog_sources"`
}

// ErrInvalidReadiness means ml.readiness holds a value that is not a
// non-negative integer in range.
var ErrInvalidReadiness = errors.New("invalid config ml.readiness")

// ErrInvalidProjection means ml.screen_lines or ml.mask is not valid, or
// is absent where the screen projection is needed: it has no default, so
// the dataset build and the Analyst refuse to run without both.
var ErrInvalidProjection = errors.New("invalid config ml screen projection")

// ErrInvalidPermissionDialogSources means the opt-in list is not an exact,
// duplicate-free set of measured harness/version pairs.
var ErrInvalidPermissionDialogSources = errors.New("invalid config ml.permission_dialog_sources")

// DefaultConfig is what Load returns when config.yaml does not exist yet.
func DefaultConfig() Config {
	return Config{
		SchemaVersion: SupportedSchemaVersion,
		Channel:       Channel{BudgetProfile: protocol.BudgetProfileDefault},
		TmuxAttach: TmuxAttach{
			HumanActivityWindow: DefaultTmuxHumanActivityWindow,
		},
		ML: ML{Readiness: dataset.DefaultReadinessThresholds()},
	}
}

// DefaultPath returns the path of config.yaml inside a resolved config dir.
func DefaultPath(configDir string) string {
	return filepath.Join(configDir, "config.yaml")
}

// rawConfig is what Load decodes config.yaml into. schema_version is kept
// as a yaml.Node so that an absent key (the zero Node) can be told apart
// from a key that is present with any value, 0 and null included.
type rawConfig struct {
	SchemaVersion  yaml.Node         `yaml:"schema_version"`
	NodeID         *string           `yaml:"node_id"`
	OperatorAgent  yaml.Node         `yaml:"operator_agent"`
	OrganizationID *string           `yaml:"organization_id"`
	Federation     *rawFederation    `yaml:"federation"`
	Notifications  *rawNotifications `yaml:"notifications"`
	Channel        yaml.Node         `yaml:"channel"`
	Bridge         *rawBridge        `yaml:"bridge"`
	NodeSocket     *rawNodeSocket    `yaml:"node_socket"`
	TmuxAttach     *rawTmuxAttach    `yaml:"tmux_attach"`
	TmuxInstances  *rawTmuxInstances `yaml:"tmux_instances"`
	ML             *rawML            `yaml:"ml"`
	StateAnalysis  *rawStateAnalysis `yaml:"state_analysis"`
}

type rawChannel struct {
	BudgetProfile yaml.Node `yaml:"budget_profile"`
}

type rawBridge struct {
	NoticeLine            yaml.Node `yaml:"notice_line"`
	ResponseWatchProducer yaml.Node `yaml:"response_watch_producer"`
}

func (r *rawBridge) apply(dst *Bridge) error {
	if err := r.applyProducer(dst); err != nil {
		return err
	}
	n := &r.NoticeLine
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr {
		return fmt.Errorf("bridge.notice_line must be a string: %w", ErrInvalidBridge)
	}
	line := protocol.NoticeLine(n.Value)
	if !line.Valid() || strings.Contains(n.Value, ";") {
		return fmt.Errorf("bridge.notice_line: %w", ErrInvalidBridge)
	}
	dst.NoticeLine = n.Value
	return nil
}

func (r *rawBridge) applyProducer(dst *Bridge) error {
	n := &r.ResponseWatchProducer
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr {
		return fmt.Errorf("bridge.response_watch_producer must be a string: %w", ErrInvalidBridge)
	}
	producer := protocol.AgentID(n.Value)
	if !producer.Valid() {
		return fmt.Errorf("bridge.response_watch_producer: %w", ErrInvalidBridge)
	}
	dst.ResponseWatchProducer = producer
	return nil
}

type rawNodeSocket struct {
	Path      yaml.Node `yaml:"path"`
	BridgeUID yaml.Node `yaml:"bridge_uid"`
}

// maxSocketPath leaves room for the terminating NUL of sockaddr_un.sun_path.
const maxSocketPath = 107

func (r *rawNodeSocket) parse() (*NodeSocket, error) {
	path := &r.Path
	if path.Kind != yaml.ScalarNode || path.ShortTag() != tagStr || !filepath.IsAbs(path.Value) ||
		filepath.Clean(path.Value) != path.Value || len(path.Value) > maxSocketPath {
		return nil, fmt.Errorf("node_socket.path must be a clean absolute path of at most %d bytes: %w",
			maxSocketPath, ErrInvalidNodeSocket)
	}
	uid := &r.BridgeUID
	if uid.Kind != yaml.ScalarNode || uid.ShortTag() != tagInt {
		return nil, fmt.Errorf("node_socket.bridge_uid must be a number: %w", ErrInvalidNodeSocket)
	}
	value, err := strconv.ParseUint(uid.Value, 10, 32)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != uid.Value {
		return nil, fmt.Errorf("node_socket.bridge_uid must be a decimal uid other than 0: %w", ErrInvalidNodeSocket)
	}
	return &NodeSocket{Path: path.Value, BridgeUID: uint32(value)}, nil
}

type rawTmuxAttach struct {
	HumanActivityWindow  yaml.Node `yaml:"human_activity_window"`
	HelperManifestSHA256 yaml.Node `yaml:"helper_manifest_sha256"`
}

func (r *rawTmuxAttach) apply(dst *TmuxAttach) error {
	if err := r.applyWindow(dst); err != nil {
		return err
	}
	return r.applyHelper(dst)
}

func (r *rawTmuxAttach) applyWindow(dst *TmuxAttach) error {
	n := &r.HumanActivityWindow
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr {
		return fmt.Errorf("tmux_attach.human_activity_window must be a duration string: %w",
			ErrInvalidTmuxAttach)
	}
	window, err := time.ParseDuration(n.Value)
	if err != nil || window <= 0 {
		return fmt.Errorf("tmux_attach.human_activity_window: %w", ErrInvalidTmuxAttach)
	}
	dst.HumanActivityWindow = window
	return nil
}

func (r *rawTmuxAttach) applyHelper(dst *TmuxAttach) error {
	n := &r.HelperManifestSHA256
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr {
		return fmt.Errorf("tmux_attach.helper_manifest_sha256 must be a string: %w",
			ErrInvalidTmuxAttach)
	}
	if len(n.Value) != 64 || !isLowerHex(n.Value) {
		return fmt.Errorf("tmux_attach.helper_manifest_sha256 must be 64 lowercase hex: %w",
			ErrInvalidTmuxAttach)
	}
	dst.HelperManifestSHA256 = n.Value
	return nil
}

func isLowerHex(value string) bool {
	for index := range len(value) {
		char := value[index]
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

type rawTmuxInstances struct {
	Statements yaml.Node `yaml:"statements"`
}

func (r *rawTmuxInstances) apply(dst *TmuxInstances) error {
	n := &r.Statements
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr || !filepath.IsAbs(n.Value) ||
		filepath.Clean(n.Value) != n.Value {
		return fmt.Errorf("tmux_instances.statements must be a clean absolute path: %w", ErrInvalidTmuxInstances)
	}
	dst.Statements = n.Value
	return nil
}

func (r *rawChannel) apply(dst *Channel) error {
	n := &r.BudgetProfile
	if n.Kind == 0 {
		return nil
	}
	profile := protocol.BudgetProfile(n.Value)
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr ||
		profile == protocol.BudgetProfileCustom || !profile.Valid() {
		return fmt.Errorf("channel.budget_profile: %w", ErrInvalidChannel)
	}
	dst.BudgetProfile = profile
	return nil
}

func applyChannelNode(n *yaml.Node, dst *Channel) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("channel must be a mapping: %w", ErrInvalidChannel)
	}
	for i := 0; i < len(n.Content); i += 2 {
		key := n.Content[i]
		if key.Kind != yaml.ScalarNode || key.Value != "budget_profile" {
			return fmt.Errorf("channel field %q: %w", key.Value, ErrInvalidChannel)
		}
	}
	var raw rawChannel
	if err := n.Decode(&raw); err != nil {
		return fmt.Errorf("channel: %w: %w", err, ErrInvalidChannel)
	}
	return raw.apply(dst)
}

type rawStateAnalysis struct {
	GatewayConfig       yaml.Node `yaml:"gateway_config"`
	ProfileRevision     yaml.Node `yaml:"profile_revision"`
	ProfileSHA256       yaml.Node `yaml:"profile_sha256"`
	CalibrationRevision yaml.Node `yaml:"calibration_revision"`
	CalibrationSHA256   yaml.Node `yaml:"calibration_sha256"`
	Temperature         yaml.Node `yaml:"temperature"`
	IdleThreshold       yaml.Node `yaml:"idle_threshold"`
}

func (r *rawStateAnalysis) parse() (*StateAnalysis, error) {
	var out StateAnalysis
	stringsToRead := []struct {
		name string
		node *yaml.Node
		dst  *string
		max  int
	}{
		{"gateway_config", &r.GatewayConfig, &out.GatewayConfig, 4096},
		{"profile_revision", &r.ProfileRevision, &out.ProfileRevision, 100},
		{"calibration_revision", &r.CalibrationRevision, &out.CalibrationRevision, 100},
	}
	for _, field := range stringsToRead {
		if field.node.Kind != yaml.ScalarNode || field.node.ShortTag() != tagStr ||
			field.node.Value == "" || len(field.node.Value) > field.max {
			return nil, fmt.Errorf("state_analysis.%s: %w", field.name, ErrInvalidStateAnalysis)
		}
		*field.dst = field.node.Value
	}
	if !filepath.IsAbs(out.GatewayConfig) {
		return nil, fmt.Errorf("state_analysis.gateway_config: %w", ErrInvalidStateAnalysis)
	}
	for _, field := range []struct {
		name string
		node *yaml.Node
		dst  *protocol.SHA256Hex
	}{
		{"profile_sha256", &r.ProfileSHA256, &out.ProfileSHA256},
		{"calibration_sha256", &r.CalibrationSHA256, &out.CalibrationSHA256},
	} {
		if field.node.Kind != yaml.ScalarNode || field.node.ShortTag() != tagStr {
			return nil, fmt.Errorf("state_analysis.%s: %w", field.name, ErrInvalidStateAnalysis)
		}
		*field.dst = protocol.SHA256Hex(field.node.Value)
		if !field.dst.Valid() {
			return nil, fmt.Errorf("state_analysis.%s: %w", field.name, ErrInvalidStateAnalysis)
		}
	}
	for _, field := range []struct {
		name string
		node *yaml.Node
		dst  *float64
	}{
		{"temperature", &r.Temperature, &out.Temperature},
		{"idle_threshold", &r.IdleThreshold, &out.IdleThreshold},
	} {
		if field.node.Kind != yaml.ScalarNode ||
			(field.node.ShortTag() != "!!float" && field.node.ShortTag() != tagInt) ||
			field.node.Decode(field.dst) != nil || math.IsNaN(*field.dst) || math.IsInf(*field.dst, 0) {
			return nil, fmt.Errorf("state_analysis.%s: %w", field.name, ErrInvalidStateAnalysis)
		}
	}
	if out.Temperature <= 0 || out.Temperature > 100 || out.IdleThreshold <= 0 || out.IdleThreshold > 1 {
		return nil, fmt.Errorf("state_analysis calibration range: %w", ErrInvalidStateAnalysis)
	}
	return &out, nil
}

type rawNotifications struct {
	TMUX *rawNotificationTMUX `yaml:"tmux"`
}

type rawNotificationTMUX struct {
	Socket   yaml.Node `yaml:"socket"`
	Client   yaml.Node `yaml:"client"`
	Duration yaml.Node `yaml:"duration"`
	Timeout  yaml.Node `yaml:"timeout"`
	Template yaml.Node `yaml:"template"`
}

type rawFederation struct {
	Listen      yaml.Node `yaml:"listen"`
	TrustBundle yaml.Node `yaml:"trust_bundle"`
	Advertise   yaml.Node `yaml:"advertise"`
}

func (raw *rawFederation) apply(dst *Federation) error {
	if err := decodeFederationString("listen", &raw.Listen, &dst.Listen); err != nil {
		return err
	}
	if err := decodeFederationString("trust_bundle", &raw.TrustBundle, &dst.TrustBundle); err != nil {
		return err
	}
	if err := decodeFederationString("advertise", &raw.Advertise, &dst.Advertise); err != nil {
		return err
	}
	if dst.Advertise != "" && !validFederationEndpoint(dst.Advertise) {
		return fmt.Errorf("federation.advertise %q: %w", dst.Advertise, ErrInvalidFederation)
	}
	if dst.Listen == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(dst.Listen)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 0 || portNumber > 65535 ||
		strings.ContainsAny(host, "/?# ") {
		return fmt.Errorf("federation.listen %q: %w", dst.Listen, ErrInvalidFederation)
	}
	if dst.TrustBundle == "" {
		return fmt.Errorf("federation.trust_bundle is required with federation.listen: %w", ErrInvalidFederation)
	}
	if dst.Advertise == "" {
		return fmt.Errorf("federation.advertise is required with federation.listen: %w", ErrInvalidFederation)
	}
	return nil
}

func validFederationEndpoint(raw string) bool {
	parsed, err := url.ParseRequestURI(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" &&
		parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == ""
}

func decodeFederationString(name string, node *yaml.Node, dst *string) error {
	if node.Kind == 0 {
		return nil
	}
	if node.Kind != yaml.ScalarNode || node.ShortTag() != tagStr {
		return fmt.Errorf("federation.%s must be a string: %w", name, ErrInvalidFederation)
	}
	*dst = node.Value
	return nil
}

func (raw *rawNotifications) apply(dst *Notifications) error {
	if raw.TMUX == nil {
		return nil
	}
	configured := NotificationTMUX{
		Duration: notifications.DefaultDuration,
		Timeout:  notifications.DefaultTimeout,
		Template: notifications.DefaultTemplate,
	}
	for _, field := range []struct {
		name string
		node *yaml.Node
		dst  *string
	}{
		{"socket", &raw.TMUX.Socket, &configured.Socket},
		{"client", &raw.TMUX.Client, &configured.Client},
		{"template", &raw.TMUX.Template, &configured.Template},
	} {
		if field.node.Kind == 0 {
			continue
		}
		if field.node.Kind != yaml.ScalarNode || field.node.ShortTag() != tagStr {
			return fmt.Errorf("notifications.tmux.%s must be a string: %w",
				field.name, ErrInvalidNotifications)
		}
		*field.dst = field.node.Value
	}
	for _, field := range []struct {
		name string
		node *yaml.Node
		dst  *time.Duration
	}{
		{"duration", &raw.TMUX.Duration, &configured.Duration},
		{"timeout", &raw.TMUX.Timeout, &configured.Timeout},
	} {
		if field.node.Kind == 0 {
			continue
		}
		if field.node.Kind != yaml.ScalarNode || field.node.ShortTag() != tagStr {
			return fmt.Errorf("notifications.tmux.%s must be a duration string: %w",
				field.name, ErrInvalidNotifications)
		}
		parsed, err := time.ParseDuration(field.node.Value)
		if err != nil {
			return fmt.Errorf("notifications.tmux.%s: %w", field.name, ErrInvalidNotifications)
		}
		*field.dst = parsed
	}
	if _, err := notifications.NewTMUXAdapter(notifications.TMUXOptions{
		Socket: configured.Socket, Client: configured.Client,
		Duration: configured.Duration, Timeout: configured.Timeout,
		Template: configured.Template,
	}); err != nil {
		return fmt.Errorf("notifications.tmux: %w", ErrInvalidNotifications)
	}
	dst.TMUX = &configured
	return nil
}

type rawML struct {
	Readiness               *rawReadiness `yaml:"readiness"`
	ScreenLines             yaml.Node     `yaml:"screen_lines"`
	Mask                    yaml.Node     `yaml:"mask"`
	PermissionDialogSources yaml.Node     `yaml:"permission_dialog_sources"`
}

// rawReadiness keeps each threshold as a node: only a plain integer is
// accepted, so a string, a float or null is refused, not coerced.
type rawReadiness struct {
	TrainMin                  yaml.Node `yaml:"train_min"`
	ValidationMin             yaml.Node `yaml:"validation_min"`
	TestMin                   yaml.Node `yaml:"test_min"`
	LabelTrainMin             yaml.Node `yaml:"label_train_min"`
	LabelTestMin              yaml.Node `yaml:"label_test_min"`
	RareLabelTrainMin         yaml.Node `yaml:"rare_label_train_min"`
	RareLabelTestMin          yaml.Node `yaml:"rare_label_test_min"`
	RareLabelHarnessesMin     yaml.Node `yaml:"rare_label_harnesses_min"`
	RareLabelTestHarnessesMin yaml.Node `yaml:"rare_label_test_harnesses_min"`
	UnknownTrainMin           yaml.Node `yaml:"unknown_train_min"`
	LabelShareMinPercent      yaml.Node `yaml:"label_share_min_percent"`
	HarnessesMin              yaml.Node `yaml:"harnesses_min"`
	HarnessTrainMin           yaml.Node `yaml:"harness_train_min"`
	SyntheticMaxPercent       yaml.Node `yaml:"synthetic_max_percent"`
}

// apply overrides t with every threshold present.
func (r *rawReadiness) apply(t *dataset.ReadinessThresholds) error {
	for _, f := range []struct {
		name string
		node *yaml.Node
		dst  *int64
	}{
		{"train_min", &r.TrainMin, &t.TrainMin},
		{"validation_min", &r.ValidationMin, &t.ValidationMin},
		{"test_min", &r.TestMin, &t.TestMin},
		{"label_train_min", &r.LabelTrainMin, &t.LabelTrainMin},
		{"label_test_min", &r.LabelTestMin, &t.LabelTestMin},
		{"rare_label_train_min", &r.RareLabelTrainMin, &t.RareLabelTrainMin},
		{"rare_label_test_min", &r.RareLabelTestMin, &t.RareLabelTestMin},
		{"rare_label_harnesses_min", &r.RareLabelHarnessesMin, &t.RareLabelHarnessesMin},
		{"rare_label_test_harnesses_min", &r.RareLabelTestHarnessesMin, &t.RareLabelTestHarnessesMin},
		{"unknown_train_min", &r.UnknownTrainMin, &t.UnknownTrainMin},
		{"label_share_min_percent", &r.LabelShareMinPercent, &t.LabelShareMinPercent},
		{"harnesses_min", &r.HarnessesMin, &t.HarnessesMin},
		{"harness_train_min", &r.HarnessTrainMin, &t.HarnessTrainMin},
		{"synthetic_max_percent", &r.SyntheticMaxPercent, &t.SyntheticMaxPercent},
	} {
		if f.node.Kind == 0 {
			continue
		}
		var v int64
		if f.node.Kind != yaml.ScalarNode || f.node.ShortTag() != "!!int" || f.node.Decode(&v) != nil || v < 0 {
			return fmt.Errorf("ml.readiness.%s: %w", f.name, ErrInvalidReadiness)
		}
		*f.dst = v
	}
	if err := t.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidReadiness, err)
	}
	return nil
}

// Load reads and validates config.yaml at path. A missing file is not an
// error: Load returns DefaultConfig(). An unknown top-level key, invalid
// YAML, or a schema_version that is present and is not the integer 1 are
// errors. Only an absent schema_version gets the default.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return DefaultConfig(), nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}

	var raw rawConfig
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg := DefaultConfig()
	if n := &raw.SchemaVersion; n.Kind != 0 {
		var v int
		if n.Kind != yaml.ScalarNode || n.ShortTag() != "!!int" || n.Decode(&v) != nil || v != SupportedSchemaVersion {
			return Config{}, fmt.Errorf("%s: schema_version %q: %w", path, n.Value, ErrUnsupportedSchemaVersion)
		}
		cfg.SchemaVersion = v
	}
	if raw.NodeID != nil {
		id := protocol.NodeID(*raw.NodeID)
		if !id.Valid() {
			return Config{}, fmt.Errorf("%s: node_id %q: %w", path, *raw.NodeID, ErrInvalidNodeID)
		}
		cfg.NodeID = id
	}
	if n := &raw.OperatorAgent; n.Kind != 0 {
		if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr || !protocol.AgentID(n.Value).Valid() {
			return Config{}, fmt.Errorf("%s: %w", path, ErrInvalidOperatorAgent)
		}
		cfg.OperatorAgent = protocol.AgentID(n.Value)
	}
	if raw.OrganizationID != nil {
		if !protocol.NodeID(*raw.OrganizationID).Valid() {
			return Config{}, fmt.Errorf("%s: organization_id %q: %w",
				path, *raw.OrganizationID, ErrInvalidOrganizationID)
		}
		cfg.OrganizationID = *raw.OrganizationID
	}
	if raw.Federation != nil {
		if err := raw.Federation.apply(&cfg.Federation); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.Notifications != nil {
		if err := raw.Notifications.apply(&cfg.Notifications); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.Channel.Kind != 0 {
		if err := applyChannelNode(&raw.Channel, &cfg.Channel); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.Bridge != nil {
		if err := raw.Bridge.apply(&cfg.Bridge); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.NodeSocket != nil {
		cfg.NodeSocket, err = raw.NodeSocket.parse()
		if err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.TmuxAttach != nil {
		if err := raw.TmuxAttach.apply(&cfg.TmuxAttach); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.TmuxInstances != nil {
		if err := raw.TmuxInstances.apply(&cfg.TmuxInstances); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.ML != nil && raw.ML.Readiness != nil {
		if err := raw.ML.Readiness.apply(&cfg.ML.Readiness); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.ML != nil {
		if err := raw.ML.applyProjection(&cfg.ML); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
		if err := raw.ML.applyPermissionDialogSources(&cfg.ML); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	if raw.StateAnalysis != nil {
		cfg.StateAnalysis, err = raw.StateAnalysis.parse()
		if err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	return cfg, nil
}

// YAML tags of the projection's keys.
const (
	tagInt = "!!int"
	tagStr = "!!str"
)

// applyProjection sets ml.screen_lines and ml.mask when present: a plain
// integer from 1 to screenproj.MaxLines, and "none" or an absolute path.
// Any other value is refused, not coerced.
func (r *rawML) applyProjection(dst *ML) error {
	if n := &r.ScreenLines; n.Kind != 0 {
		var v int
		if n.Kind != yaml.ScalarNode || n.ShortTag() != tagInt || n.Decode(&v) != nil || !validScreenLines(v) {
			return errScreenLines("must be")
		}
		dst.ScreenLines = v
	}
	if n := &r.Mask; n.Kind != 0 {
		if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr || !validMask(n.Value) {
			return errMask("must be")
		}
		dst.Mask = n.Value
	}
	return nil
}

func (r *rawML) applyPermissionDialogSources(dst *ML) error {
	node := &r.PermissionDialogSources
	if node.Kind == 0 {
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return ErrInvalidPermissionDialogSources
	}
	sources := make([]session.PermissionDialogSource, 0, len(node.Content))
	seen := make(map[string]struct{}, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.MappingNode {
			return ErrInvalidPermissionDialogSources
		}
		values := map[string]string{}
		for i := 0; i < len(item.Content); i += 2 {
			key, value := item.Content[i], item.Content[i+1]
			if key.Kind != yaml.ScalarNode || value.Kind != yaml.ScalarNode || value.ShortTag() != tagStr ||
				(key.Value != "harness" && key.Value != "harness_version") {
				return ErrInvalidPermissionDialogSources
			}
			if _, duplicate := values[key.Value]; duplicate {
				return ErrInvalidPermissionDialogSources
			}
			values[key.Value] = value.Value
		}
		harness := protocol.Harness(values["harness"])
		version := values["harness_version"]
		if len(values) != 2 || !session.ValidHarness(harness) || !session.ValidHarnessVersion(version) ||
			!session.SupportsPermissionDialog(harness, version) {
			return ErrInvalidPermissionDialogSources
		}
		key := string(harness) + "\x00" + version
		if _, duplicate := seen[key]; duplicate {
			return ErrInvalidPermissionDialogSources
		}
		seen[key] = struct{}{}
		sources = append(sources, session.PermissionDialogSource{Harness: harness, HarnessVersion: version})
	}
	dst.PermissionDialogSources = sources
	return nil
}

func validScreenLines(n int) bool { return n >= 1 && n <= screenproj.MaxLines }

func validMask(v string) bool { return v == screenproj.DomainMaskNone || filepath.IsAbs(v) }

func errScreenLines(rule string) error {
	return fmt.Errorf("ml.screen_lines %s an integer from 1 to %d: %w", rule, screenproj.MaxLines, ErrInvalidProjection)
}

func errMask(rule string) error {
	return fmt.Errorf("ml.mask %s %q or an absolute path: %w", rule, screenproj.DomainMaskNone, ErrInvalidProjection)
}

// ProjectionPolicies are the screen projection's policies from ml, the
// same for the dataset build and the Analyst: N from ml.screen_lines,
// the v2 redactor, and the domain mask list ml.mask names, read from
// its file, or none. Both keys are required. The list's content never
// reaches an error.
func (m ML) ProjectionPolicies() (screenproj.Policies, error) {
	if !validScreenLines(m.ScreenLines) {
		return screenproj.Policies{}, errScreenLines("is required,")
	}
	if !validMask(m.Mask) {
		return screenproj.Policies{}, errMask("is required,")
	}
	mask := screenproj.NoMask()
	if m.Mask != screenproj.DomainMaskNone {
		var err error
		if mask, err = readMask(m.Mask); err != nil {
			return screenproj.Policies{}, err
		}
	}
	red, err := redact.New(redact.Options{})
	if err != nil {
		return screenproj.Policies{}, fmt.Errorf("%w: %w", ErrInvalidProjection, err)
	}
	return screenproj.Policies{Lines: m.ScreenLines, Redactor: red, Mask: mask}, nil
}

// readMask reads the domain mask list at path.
func readMask(path string) (screenproj.Mask, error) {
	f, err := os.Open(path)
	if err != nil {
		return screenproj.Mask{}, fmt.Errorf("ml.mask: %w", err)
	}
	defer func() { _ = f.Close() }()
	names, err := screenproj.ParseMaskList(f)
	if err != nil {
		return screenproj.Mask{}, fmt.Errorf("ml.mask: %w: %w", ErrInvalidProjection, err)
	}
	mask, err := screenproj.NewMask(names)
	if err != nil {
		return screenproj.Mask{}, fmt.Errorf("ml.mask: %w: %w", ErrInvalidProjection, err)
	}
	return mask, nil
}
