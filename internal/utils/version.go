package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// VersionInfo represents parsed version components
type VersionInfo struct {
	Major int
	Minor int
	Patch int
	Build int
}

// ParseGameVersion parses a game version string and returns the major version number
func ParseGameVersion(version string) (int, error) {
	if version == "" {
		return 0, fmt.Errorf("version string cannot be empty")
	}

	// Split version by dots and take the first part
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

// ParseVersionInfo parses a full version string (e.g., "3.21.2.1") into components
func ParseVersionInfo(version string) (*VersionInfo, error) {
	if version == "" {
		return nil, fmt.Errorf("version string cannot be empty")
	}

	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid version format: %s (expected at least major.minor)", version)
	}

	info := &VersionInfo{}
	var err error

	// Parse major version
	info.Major, err = strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid major version: %s", parts[0])
	}

	// Parse minor version
	info.Minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	// Parse patch version (optional)
	if len(parts) > 2 && parts[2] != "" {
		info.Patch, err = strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid patch version: %s", parts[2])
		}
	}

	// Parse build version (optional)
	if len(parts) > 3 && parts[3] != "" {
		info.Build, err = strconv.Atoi(parts[3])
		if err != nil {
			return nil, fmt.Errorf("invalid build version: %s", parts[3])
		}
	}

	return info, nil
}

// CompareVersions compares two version strings
// Returns -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
func CompareVersions(v1, v2 string) (int, error) {
	info1, err := ParseVersionInfo(v1)
	if err != nil {
		return 0, fmt.Errorf("error parsing version %s: %w", v1, err)
	}

	info2, err := ParseVersionInfo(v2)
	if err != nil {
		return 0, fmt.Errorf("error parsing version %s: %w", v2, err)
	}

	// Compare major
	if info1.Major < info2.Major {
		return -1, nil
	}
	if info1.Major > info2.Major {
		return 1, nil
	}

	// Compare minor
	if info1.Minor < info2.Minor {
		return -1, nil
	}
	if info1.Minor > info2.Minor {
		return 1, nil
	}

	// Compare patch
	if info1.Patch < info2.Patch {
		return -1, nil
	}
	if info1.Patch > info2.Patch {
		return 1, nil
	}

	// Compare build
	if info1.Build < info2.Build {
		return -1, nil
	}
	if info1.Build > info2.Build {
		return 1, nil
	}

	return 0, nil
}

// IsModernPoE checks if the version uses modern hash algorithm (≥3.21.2)
func IsModernPoE(version string) (bool, error) {
	cmp, err := CompareVersions(version, "3.21.2")
	if err != nil {
		return false, err
	}
	return cmp >= 0, nil
}
