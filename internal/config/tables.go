package config

import "fmt"

func validateTableNames(tables []string) error {
	for _, table := range tables {
		if table == "" {
			return fmt.Errorf("table name cannot be empty")
		}

		for _, char := range table {
			if !((char >= 'a' && char <= 'z') ||
				(char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') ||
				char == '_') {
				return fmt.Errorf("invalid table name '%s': contains invalid character '%c', only alphanumeric characters and underscores are allowed", table, char)
			}
		}
	}
	return nil
}
