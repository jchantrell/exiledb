package config

import "fmt"

type Config struct {
	Patch     string
	Database  string
	Tables    []string
	Files     []string
	Languages []string
	LogLevel  string
	LogFormat string
	GgpkPath  string
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

	return nil
}
