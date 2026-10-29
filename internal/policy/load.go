package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/protocol"
)

const versionPrefix = "policy-v1:"

// ErrInvalidPolicy means no rules were compiled because at least one policy
// file failed strict validation.
var ErrInvalidPolicy = errors.New("invalid federation policy")

type strictString string

func (value *strictString) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.ShortTag() != "!!str" {
		return fmt.Errorf("must be a string")
	}
	*value = strictString(node.Value)
	return nil
}

type rawFile struct {
	SchemaVersion yaml.Node  `yaml:"schema_version"`
	Rules         *[]rawRule `yaml:"rules"`
}

type rawRule struct {
	ID     strictString `yaml:"id"`
	Effect strictString `yaml:"effect"`
	Peer   strictString `yaml:"peer"`
	Action strictString `yaml:"action"`
	Agent  strictString `yaml:"agent"`
}

// Load reads *.yaml in lexical filename order and compiles all files or none.
// A missing directory is the valid empty policy, which denies every action.
func Load(dir string) (*Policy, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return compile(nil)
	}
	if err != nil {
		return nil, fmt.Errorf("read policy directory: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	rules := make([]Rule, 0)
	seen := make(map[string]string)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("policy file %s is not regular: %w", entry.Name(), ErrInvalidPolicy)
		}
		fileRules, err := loadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, rule := range fileRules {
			if first, exists := seen[rule.ID]; exists {
				return nil, fmt.Errorf("policy rule %q appears in %s and %s: %w",
					rule.ID, first, entry.Name(), ErrInvalidPolicy)
			}
			seen[rule.ID] = entry.Name()
			rules = append(rules, rule)
		}
	}
	return compile(rules)
}

func loadFile(filename string) ([]Rule, error) {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read policy file %s: %w", filepath.Base(filename), err)
	}
	var document rawFile
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&document); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse policy file %s: %w: %w", filepath.Base(filename), ErrInvalidPolicy, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple YAML documents")
		}
		return nil, fmt.Errorf("parse policy file %s: %w: %w", filepath.Base(filename), ErrInvalidPolicy, err)
	}
	var schemaVersion int
	if document.SchemaVersion.Kind != yaml.ScalarNode ||
		document.SchemaVersion.ShortTag() != "!!int" ||
		document.SchemaVersion.Decode(&schemaVersion) != nil || schemaVersion != 1 {
		return nil, fmt.Errorf("policy file %s requires integer schema_version 1: %w",
			filepath.Base(filename), ErrInvalidPolicy)
	}
	if document.Rules == nil {
		return nil, fmt.Errorf("policy file %s requires rules: %w", filepath.Base(filename), ErrInvalidPolicy)
	}
	rules := make([]Rule, 0, len(*document.Rules))
	for index, rawRule := range *document.Rules {
		rule, err := validateRule(rawRule)
		if err != nil {
			return nil, fmt.Errorf("policy file %s rule %d: %w", filepath.Base(filename), index+1, err)
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func validateRule(raw rawRule) (Rule, error) {
	rule := Rule{
		ID: string(raw.ID), Effect: Effect(raw.Effect), Peer: string(raw.Peer),
		Action: Action(raw.Action), Agent: string(raw.Agent),
	}
	if rule.ID == "" || strings.TrimSpace(rule.ID) != rule.ID {
		return Rule{}, fmt.Errorf("rule id is empty or has surrounding whitespace: %w", ErrInvalidPolicy)
	}
	if rule.Effect != EffectAllow && rule.Effect != EffectDeny {
		return Rule{}, fmt.Errorf("rule %q has effect %q: %w", rule.ID, rule.Effect, ErrInvalidPolicy)
	}
	if !validPeer(rule.Peer) {
		return Rule{}, fmt.Errorf("rule %q has peer %q: %w", rule.ID, rule.Peer, ErrInvalidPolicy)
	}
	if !validAction(rule.Action) {
		return Rule{}, fmt.Errorf("rule %q has action %q: %w", rule.ID, rule.Action, ErrInvalidPolicy)
	}
	if rule.Agent == "" {
		return Rule{}, fmt.Errorf("rule %q has an empty agent pattern: %w", rule.ID, ErrInvalidPolicy)
	}
	if _, err := path.Match(rule.Agent, "agent"); err != nil {
		return Rule{}, fmt.Errorf("rule %q has agent pattern %q: %w: %w",
			rule.ID, rule.Agent, ErrInvalidPolicy, err)
	}
	return rule, nil
}

func validPeer(peer string) bool {
	if peer == "*" {
		return true
	}
	if organization, ok := strings.CutPrefix(peer, "org:"); ok {
		return protocol.NodeID(organization).Valid()
	}
	return protocol.NodeID(peer).Valid()
}

func compile(rules []Rule) (*Policy, error) {
	canonical := make([]Rule, len(rules))
	copy(canonical, rules)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("encode canonical policy: %w", err)
	}
	digest := sha256.Sum256(encoded)
	digestHex := hex.EncodeToString(digest[:])
	return &Policy{
		Version: versionPrefix + digestHex[:12], Digest: digestHex,
		CompiledJSON: string(encoded), rules: canonical,
	}, nil
}
