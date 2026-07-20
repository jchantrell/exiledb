// Command backfill reconstructs historical data releases from Steam depots
// (the CDN only serves the current patch). It is repo tooling, not part of the
// exiledb CLI.
//
//	backfill pull     --game poe1   # content depot -> release artifacts
//	backfill versions --game poe1   # program depot -> client_version
//
// It drives DepotDownloader for depot access and calls exiledb's own internal
// packages for everything else, so cache paths, dat parsing and index handling
// come from one implementation rather than being restated in shell.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "pull":
		err = runPull(ctx, os.Args[2:])
	case "versions":
		err = runVersions(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: backfill <command> [flags]

commands:
  pull       pull each catalog patch's content depot and write release artifacts
  versions   resolve each release's client version from the program depot

run "backfill <command> -h" for flags
`)
}
