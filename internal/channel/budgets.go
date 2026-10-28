package channel

import (
	"bytes"
	"embed"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/protocol"
	yaml "go.yaml.in/yaml/v3"
)

// budgetFiles embeds the loop budget profiles; exactly these three files
// must exist. The file system is read-only by construction.
//
//go:embed budgets/*.yaml
var budgetFiles embed.FS

// Budgets holds the embedded loop budget profiles; it is read-only.
type Budgets struct {
	profiles map[protocol.BudgetProfile]protocol.LoopBudget
}

// budgetYAML decodes one profile file; every dimension is a pointer so a
// missing key is visible.
type budgetYAML struct {
	MaxTurns               *int64 `yaml:"max_turns"`
	MaxMessages            *int64 `yaml:"max_messages"`
	MaxDurationMS          *int64 `yaml:"max_duration_ms"`
	MaxMessagesPerMinute   *int64 `yaml:"max_messages_per_minute"`
	MaxHops                *int64 `yaml:"max_hops"`
	MaxConsecutiveSamePair *int64 `yaml:"max_consecutive_same_pair"`
	MaxRetries             *int64 `yaml:"max_retries"`
	MaxArtifactCount       *int64 `yaml:"max_artifact_count"`
	MaxArtifactBytes       *int64 `yaml:"max_artifact_bytes"`
}

// LoadBudgets decodes and validates budgets/*.yaml.
func LoadBudgets() (Budgets, error) {
	names := [...]string{"demo", "default", "strict"}
	profiles := make(map[protocol.BudgetProfile]protocol.LoopBudget, len(names))
	for _, name := range names {
		raw, err := budgetFiles.ReadFile("budgets/" + name + ".yaml")
		if err != nil {
			return Budgets{}, fmt.Errorf("%w: budgets/%s.yaml: %v", ErrInvalidBudget, name, err)
		}
		budget, err := parseBudgetFile(name, raw)
		if err != nil {
			return Budgets{}, err
		}
		profiles[protocol.BudgetProfile(name)] = budget
	}
	return Budgets{profiles: profiles}, nil
}

// parseBudgetFile decodes one profile, names its file and key on any
// violation and copies the values into a validated LoopBudget.
func parseBudgetFile(name string, raw []byte) (protocol.LoopBudget, error) {
	fail := func(key string, err error) (protocol.LoopBudget, error) {
		return protocol.LoopBudget{}, fmt.Errorf("%w: budgets/%s.yaml: %s: %v", ErrInvalidBudget, name, key, err)
	}
	var decoded budgetYAML
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&decoded); err != nil {
		return fail("document", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fail("document", fmt.Errorf("expected exactly one document"))
	}
	values := []struct {
		key   string
		value *int64
	}{
		{"max_turns", decoded.MaxTurns},
		{"max_messages", decoded.MaxMessages},
		{"max_duration_ms", decoded.MaxDurationMS},
		{"max_messages_per_minute", decoded.MaxMessagesPerMinute},
		{"max_hops", decoded.MaxHops},
		{"max_consecutive_same_pair", decoded.MaxConsecutiveSamePair},
		{"max_retries", decoded.MaxRetries},
		{"max_artifact_count", decoded.MaxArtifactCount},
		{"max_artifact_bytes", decoded.MaxArtifactBytes},
	}
	numbers := make(map[string]int64, len(values))
	for _, field := range values {
		if field.value == nil {
			return fail(field.key, fmt.Errorf("missing key"))
		}
		if *field.value <= 0 {
			return fail(field.key, fmt.Errorf("value must be positive"))
		}
		numbers[field.key] = *field.value
	}
	budget := protocol.LoopBudget{
		MaxTurns:               numbers["max_turns"],
		MaxMessages:            numbers["max_messages"],
		MaxDurationMS:          numbers["max_duration_ms"],
		MaxMessagesPerMinute:   numbers["max_messages_per_minute"],
		MaxHops:                numbers["max_hops"],
		MaxConsecutiveSamePair: numbers["max_consecutive_same_pair"],
		MaxRetries:             numbers["max_retries"],
		MaxArtifactCount:       numbers["max_artifact_count"],
		MaxArtifactBytes:       numbers["max_artifact_bytes"],
	}
	if err := budget.Validate(); err != nil {
		return fail("budget", err)
	}
	return budget, nil
}

// Profile returns a copy of the demo, default or strict profile. Custom (an
// explicit budget) is not a file: it and any other value return
// ErrUnknownBudgetProfile.
func (b Budgets) Profile(p protocol.BudgetProfile) (protocol.LoopBudget, error) {
	budget, ok := b.profiles[p]
	if !ok {
		return protocol.LoopBudget{}, fmt.Errorf("%w: %s", ErrUnknownBudgetProfile, p)
	}
	return budget, nil
}
