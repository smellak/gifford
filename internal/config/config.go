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
)

// SupportedSchemaVersion is the only value Config.SchemaVersion may have.
const SupportedSchemaVersion = 1

// ErrUnsupportedSchemaVersion means config.yaml declares a schema_version
// this binary does not understand.
var ErrUnsupportedSchemaVersion = errors.New("unsupported config schema_version")

// Config is the parsed shape of config.yaml. It carries only what this
// subphase needs. Every later subphase that adds a component with its own
// configuration adds its own top-level field to this struct in its own PR.
type Config struct {
	SchemaVersion int `yaml:"schema_version"`
}

// DefaultConfig is what Load returns when config.yaml does not exist yet.
func DefaultConfig() Config {
	return Config{SchemaVersion: SupportedSchemaVersion}
}

// DefaultPath returns the path of config.yaml inside a resolved config dir.
func DefaultPath(configDir string) string {
	return filepath.Join(configDir, "config.yaml")
}

// Load reads and validates config.yaml at path. A missing file is not an
// error: Load returns DefaultConfig(). An unknown top-level key, invalid
// YAML, or an unsupported schema_version are errors.
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return DefaultConfig(), nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.SchemaVersion == 0 {
		cfg.SchemaVersion = SupportedSchemaVersion
	}
	if cfg.SchemaVersion != SupportedSchemaVersion {
		return Config{}, fmt.Errorf("%s: schema_version %d: %w", path, cfg.SchemaVersion, ErrUnsupportedSchemaVersion)
	}
	return cfg, nil
}
