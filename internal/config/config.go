package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Patch     string   `mapstructure:"patch"`
	Database  string   `mapstructure:"database"`
	Tables    []string `mapstructure:"tables"`
	Languages []string `mapstructure:"languages"`
	LogLevel  string   `mapstructure:"log_level"`
	LogFormat string   `mapstructure:"log_format"`
}

// Load initializes and loads configuration from file
func Load(cfgFile string) (*Config, error) {
	// Set defaults
	viper.SetDefault("patch", "3.26.0.11")
	viper.SetDefault("database", "exile.db")
	viper.SetDefault("languages", []string{"English"})
	viper.SetDefault("log_level", "info")
	viper.SetDefault("log_format", "text")

	// Config file handling
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}

		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigName("exiledb")
		viper.SetConfigType("yaml")
	}

	// Read config file (optional)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate table names if provided
	if err := validateTableNames(cfg.Tables); err != nil {
		return nil, fmt.Errorf("invalid table configuration: %w", err)
	}

	// Validate languages if provided
	if err := validateLanguages(cfg.Languages); err != nil {
		return nil, fmt.Errorf("invalid language configuration: %w", err)
	}

	// Ensure English is always included if languages is empty
	if len(cfg.Languages) == 0 {
		cfg.Languages = []string{"English"}
	}

	return &cfg, nil
}
