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
}

// DefaultConfig is what Load returns when config.yaml does not exist yet.
func DefaultConfig() Config {
	return Config{SchemaVersion: SupportedSchemaVersion}
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
	return cfg, nil
}
