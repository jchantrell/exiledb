#!/usr/bin/env python3
"""Print the current public-branch manifest ID for a Steam app/depot via
anonymous PICS (no license needed — this is public product info). Prints
nothing on any failure so callers can degrade gracefully.

Usage: pics-manifest.py <app_id> <depot_id>
"""
import sys
import warnings

warnings.filterwarnings("ignore")

try:
    from steam.client import SteamClient

    app = int(sys.argv[1])
    depot = sys.argv[2]
    c = SteamClient()
    if c.anonymous_login() == 1:
        info = c.get_product_info(apps=[app], timeout=30)
        print(info["apps"][app]["depots"][depot]["manifests"]["public"]["gid"])
except Exception:
    pass
