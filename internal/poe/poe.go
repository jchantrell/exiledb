// Package poe holds Path of Exile domain knowledge shared across layers:
// game-version semantics, dat file path conventions, and the schema-name to
// snake_case convention used for table and column naming.
package poe

import (
	"fmt"
	"strconv"
	"strings"
)

// DatExtension is the dat file extension used by current game clients.
const DatExtension = ".datc64"

// ParseGameVersion parses a game version string and returns the major version number
func ParseGameVersion(version string) (int, error) {
	if version == "" {
		return 0, fmt.Errorf("version string cannot be empty")
	}

	parts := strings.Split(version, ".")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid version format: %s", version)
	}

	majorVersion, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid major version number: %s", parts[0])
	}

	if majorVersion < 3 || majorVersion > 4 {
		return 0, fmt.Errorf("unsupported game version: %d (must be 3.x or 4.x)", majorVersion)
	}

	return majorVersion, nil
}

// IsPoE2 reports whether the patch version belongs to Path of Exile 2 (4.x+).
func IsPoE2(version string) bool {
	major, err := ParseGameVersion(version)
	if err != nil {
		return false
	}
	return major >= 4
}

// DatPath returns the correct dat file path for a table based on game version.
// PoE2 (4.x+) stores tables under data/balance/, PoE1 under data/.
func DatPath(patch, tableName, extension string) string {
	lower := strings.ToLower(tableName)
	if IsPoE2(patch) {
		return fmt.Sprintf("data/balance/%s%s", lower, extension)
	}
	return fmt.Sprintf("data/%s%s", lower, extension)
}

// DatLangPath returns the correct language-specific dat file path based on game version.
func DatLangPath(patch, language, tableName, extension string) string {
	lower := strings.ToLower(tableName)
	langLower := strings.ToLower(language)
	if IsPoE2(patch) {
		return fmt.Sprintf("data/balance/%s/%s%s", langLower, lower, extension)
	}
	return fmt.Sprintf("data/%s/%s%s", langLower, lower, extension)
}

// ToSnakeCase converts a community-schema name (CamelCase) to the snake_case
// form used for SQL tables and columns.
func ToSnakeCase(s string) string {
	if s == "" {
		return s
	}

	var result strings.Builder
	result.Grow(len(s) + 10) // Pre-allocate some extra space for underscores

	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		if r >= 'A' && r <= 'Z' {
			result.WriteRune(r - 'A' + 'a')
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}
