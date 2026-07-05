package version

import "runtime/debug"

// Version is overridden at build time via
// -ldflags "-X github.com/jchantrell/exiledb/internal/version.Version=vX.Y.Z".
var Version = "dev"

func Get() string {
	if Version != "dev" {
		return Version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}
