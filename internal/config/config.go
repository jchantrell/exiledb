package config

import (
	"fmt"
	"strings"
)

type Config struct {
	Patch      string
	Database   string
	Tables     []string
	Files      []string
	Languages  []string
	LogLevel   string
	LogFormat  string
	GgpkPath   string
	SchemaPath string
}

func Validate(cfg *Config) error {
	if err := validateTableNames(cfg.Tables); err != nil {
		return fmt.Errorf("invalid table configuration: %w", err)
	}

	if err := validateLanguages(cfg.Languages); err != nil {
		return fmt.Errorf("invalid language configuration: %w", err)
	}

	if len(cfg.Languages) == 0 {
		cfg.Languages = []string{"English"}
	}

	// The game's virtual filesystem is case-insensitive; canonicalize file
	// paths to lowercase so index lookups, sprite matching, and output names
	// all agree.
	for i, f := range cfg.Files {
		cfg.Files[i] = strings.ToLower(f)
	}

	return nil
}
