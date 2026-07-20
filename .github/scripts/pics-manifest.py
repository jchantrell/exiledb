#!/usr/bin/env python3
"""Print the current public-branch manifest ID for a Steam app/depot via
anonymous PICS (no license needed — this is public product info).

Prints nothing to stdout on failure so callers can degrade gracefully, but
reports the reason on stderr: a gate that silently stops gating looks exactly
like a gate that passed.

Needs the client extra: pip install 'steam[client]'.

Usage: pics-manifest.py <app_id> <depot_id>
"""
import sys
import warnings

warnings.filterwarnings("ignore")


def main() -> int:
    try:
        from steam.client import SteamClient
    except ImportError as e:
        print(f"pics-manifest: {e} (install with: pip install 'steam[client]')", file=sys.stderr)
        return 1

    app, depot = int(sys.argv[1]), sys.argv[2]
    try:
        client = SteamClient()
        if client.anonymous_login() != 1:
            print("pics-manifest: anonymous Steam login failed", file=sys.stderr)
            return 1
        info = client.get_product_info(apps=[app], timeout=30)
        print(info["apps"][app]["depots"][depot]["manifests"]["public"]["gid"])
        return 0
    except Exception as e:
        print(f"pics-manifest: app {app} depot {depot}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
