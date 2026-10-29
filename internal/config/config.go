// Package config loads and validates config.yaml.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
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

// Config is the parsed shape of config.yaml. It carries only what this
// subphase needs. Every later subphase that adds a component with its own
// configuration adds its own top-level field to this struct in its own PR.
type Config struct {
	SchemaVersion int             `yaml:"schema_version"`
	NodeID        protocol.NodeID `yaml:"node_id"`
	ML            ML              `yaml:"ml"`
}

// ML configures the dataset and evaluation tools.
type ML struct {
	// Readiness are the dataset preparation gate's thresholds.
	Readiness dataset.ReadinessThresholds `yaml:"readiness"`
}

// ErrInvalidReadiness means ml.readiness holds a value that is not a
// non-negative integer in range.
var ErrInvalidReadiness = errors.New("invalid config ml.readiness")

// DefaultConfig is what Load returns when config.yaml does not exist yet.
func DefaultConfig() Config {
	return Config{SchemaVersion: SupportedSchemaVersion, ML: ML{Readiness: dataset.DefaultReadinessThresholds()}}
}

// DefaultPath returns the path of config.yaml inside a resolved config dir.
func DefaultPath(configDir string) string {
	return filepath.Join(configDir, "config.yaml")
}

// rawConfig is what Load decodes config.yaml into. schema_version is kept
// as a yaml.Node so that an absent key (the zero Node) can be told apart
// from a key that is present with any value, 0 and null included.
type rawConfig struct {
	SchemaVersion yaml.Node `yaml:"schema_version"`
	NodeID        *string   `yaml:"node_id"`
	ML            *rawML    `yaml:"ml"`
}

type rawML struct {
	Readiness *rawReadiness `yaml:"readiness"`
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
	if raw.ML != nil && raw.ML.Readiness != nil {
		if err := raw.ML.Readiness.apply(&cfg.ML.Readiness); err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}
	}
	return cfg, nil
}
