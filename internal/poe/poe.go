// Package poe holds Path of Exile domain knowledge shared across layers:
// game-version semantics, dat file path conventions, and the schema-name to
// snake_case convention used for table and column naming.
package poe

import (
	"fmt"
	"strconv"
	"strings"
)

const DatExtension = ".datc64"

// DatExtensions are the parseable 64-bit dat-table extensions across PoE's
// history, in preference order: the current .datc64 and the pre-2023 .dat64
// (both read by the same 64-bit parser). The 32-bit .dat and the localized
// companion files (.datcl64/.datl64) are excluded — the 32-bit reader was
// removed and the companions aren't tables.
var DatExtensions = []string{DatExtension, ".dat64"}

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

func IsPoE2(version string) bool {
	major, err := ParseGameVersion(version)
	if err != nil {
		return false
	}
	return major >= 4
}

func DatPath(patch, tableName, extension string) string {
	lower := strings.ToLower(tableName)
	if IsPoE2(patch) {
		return fmt.Sprintf("data/balance/%s%s", lower, extension)
	}
	return fmt.Sprintf("data/%s%s", lower, extension)
}

func DatLangPath(patch, language, tableName, extension string) string {
	lower := strings.ToLower(tableName)
	langLower := strings.ToLower(language)
	if IsPoE2(patch) {
		return fmt.Sprintf("data/balance/%s/%s%s", langLower, lower, extension)
	}
	return fmt.Sprintf("data/%s/%s%s", langLower, lower, extension)
}

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
