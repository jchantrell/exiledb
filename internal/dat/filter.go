package dat

import (
	"fmt"
	"path/filepath"
	"strings"
)

// GetTableNameFromPath extracts the table name from a DAT file path.
// It removes the directory prefix and file extension to return just the table name.
//
// Example:
//   GetTableNameFromPath("data/baseitemtypes.datc64") returns "baseitemtypes"
//   GetTableNameFromPath("Data/Simplified Chinese/baseitemtypes.datc64") returns "baseitemtypes"
func GetTableNameFromPath(path string) string {
	if path == "" {
		return ""
	}

	// Remove directory prefix
	name := filepath.Base(path)

	// Remove .datc64 extension (case-insensitive)
	if strings.HasSuffix(strings.ToLower(name), ".datc64") {
		name = name[:len(name)-7] // Remove last 7 characters (.datc64)
	}

	return name
}

// FilterDATFiles filters a list of file paths to find DAT files matching the specified criteria.
// It filters by file extension (.datc64), language, and table names based on the configuration.
//
// Parameters:
//   - allPaths: List of all file paths to filter
//   - languages: List of languages to include (empty means English only)
//   - tables: List of table names to include (empty when allTables is true)
//   - allTables: If true, include all tables regardless of the tables parameter
//
// Returns:
//   A filtered list of DAT file paths that match all criteria
func FilterDATFiles(allPaths []string, languages []string, tables []string, allTables bool) []string {
	datFiles := make([]string, 0) // Initialize as empty slice, not nil slice

	for _, path := range allPaths {
		if !IsDATFile(path) {
			continue
		}

		// Filter by language if specified
		if len(languages) > 0 && !MatchesLanguage(path, languages) {
			continue
		}

		// Filter by tables if specified
		if !allTables && len(tables) > 0 && !MatchesTable(path, tables) {
			continue
		}

		datFiles = append(datFiles, path)
	}

	return datFiles
}

// IsDATFile checks if a file path represents a .datc64 DAT file.
// This function performs a case-insensitive check for the .datc64 extension.
//
// Example:
//   IsDATFile("data/baseitemtypes.datc64") returns true
//   IsDATFile("data/baseitemtypes.txt") returns false
//   IsDATFile("data/BaseItemTypes.DATC64") returns true (case-insensitive)
func IsDATFile(path string) bool {
	lowerPath := strings.ToLower(path)
	return strings.HasSuffix(lowerPath, ".datc64")
}

// MatchesLanguage checks if a DAT file path matches any of the specified languages.
// Language-specific files have paths like "Data/Simplified Chinese/..." while
// default English files are directly in "Data/".
//
// Language matching logic:
//   - Files directly in "Data/" are considered English (default)
//   - Files in "Data/{Language}/" are considered to be in that specific language
//   - If the languages list contains "English", default files in "Data/" will match
//   - If languages list is empty, no files match
//
// Parameters:
//   - path: The file path to check
//   - languages: List of language names to match against
//
// Returns:
//   true if the file matches any of the specified languages, false otherwise
//
// Example:
//   MatchesLanguage("Data/baseitemtypes.datc64", []string{"English"}) returns true
//   MatchesLanguage("Data/Simplified Chinese/baseitemtypes.datc64", []string{"Simplified Chinese"}) returns true
//   MatchesLanguage("Data/baseitemtypes.datc64", []string{"French"}) returns false
func MatchesLanguage(path string, languages []string) bool {
	// If no languages specified, nothing matches
	if len(languages) == 0 {
		return false
	}

	// Normalize path to check for "Data/" prefix
	normalizedPath := strings.Replace(path, "\\", "/", -1) // Handle Windows paths

	// Check if it's a file directly in Data/ (English default) - case insensitive
	lowerPath := strings.ToLower(normalizedPath)
	if strings.HasPrefix(lowerPath, "data/") {
		pathAfterData := normalizedPath[5:] // Remove "Data/" prefix
		// If there's no subdirectory (no more slashes), it's a default English file
		if !strings.Contains(pathAfterData, "/") {
			// Check if English is in the language list (case-insensitive)
			for _, lang := range languages {
				if strings.EqualFold(lang, "English") {
					return true
				}
			}
			return false
		}
	}

	// Check for language-specific paths
	for _, lang := range languages {
		langPath := fmt.Sprintf("Data/%s/", lang)
		if strings.HasPrefix(normalizedPath, langPath) {
			return true
		}
		// Also check lowercase "data" prefix
		langPathLower := fmt.Sprintf("data/%s/", lang)
		if strings.HasPrefix(lowerPath, langPathLower) {
			return true
		}
	}

	return false
}

// MatchesTable checks if a DAT file path matches any of the specified table names.
// The comparison is case-insensitive to handle variations in table name casing.
//
// Parameters:
//   - path: The file path to check
//   - tables: List of table names to match against
//
// Returns:
//   true if the file's table name matches any of the specified tables, false otherwise
//
// Example:
//   MatchesTable("data/baseitemtypes.datc64", []string{"BaseItemTypes"}) returns true
//   MatchesTable("data/baseitemtypes.datc64", []string{"Skills"}) returns false
//   MatchesTable("data/BaseItemTypes.datc64", []string{"baseitemtypes"}) returns true (case-insensitive)
func MatchesTable(path string, tables []string) bool {
	baseName := GetTableNameFromPath(path)
	for _, table := range tables {
		if strings.EqualFold(baseName, table) {
			return true
		}
	}
	return false
}