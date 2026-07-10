# backfill

Local, one-shot tooling to reconstruct historical data releases from Steam
depots (not the CDN, which only serves the current patch). Ongoing releases stay
in CI (`.github/workflows/data.yaml`); this is only for the historical tail.

## Phases

- **Phase 0 — schema corpus** (`build-corpus.sh`): generate `schema.min.json` for
  every `dat-schema` commit → `data/corpus/<date>_<hash>.json` + `corpus-index.tsv`.
  Only needed for content-tier (named/typed) releases; the structural release
  format (`manifest.txt` + `dat-stats.jsonl` + `versions.json` + file diffs) is
  schema-free.

- **Phase 1 — backfill** (`backfill.sh`): for each catalog entry, oldest→newest:
  Steam-pull the content index + dat bundles, run `exiledb manifest`/`--stats`,
  diff files vs the previous release (`dat-diff.sh`), prune. Writes artifacts to
  `data/out/<epoch>/`. Releases are keyed by the content manifest's patch epoch
  (Unix seconds) — historical CDN/exe versions are unrecoverable, so the manifest
  date is the stable label (no program depot, no client version tag). **Does not
  publish** — run `gh release` yourself once the artifacts look right. dat-level
  diffs aren't persisted — they're derivable from any two `dat-stats.jsonl`.

## The catalog is the gate

One catalog per game, same format — TSV `epoch <TAB> date <TAB> content-manifest`,
header skipped:

- `poe1-content.tsv` — app 238960, content depot 238961
- `poe2-content.tsv` — app 2694490, content depot 2694491

Steam has no history API and manifest IDs aren't enumerable, so the full list
lives on SteamDB: export the content depot's manifest history and parse it into
this format.

## Run

    ./build-corpus.sh                 # Phase 0 (optional; content tier only)
    # PoE1 (defaults):
    DDL=/path/to/DepotDownloader ACCOUNT=<steam-user> ./backfill.sh
    # PoE2:
    DDL=... ACCOUNT=... APP=2694490 CONTENT_DEPOT=2694491 MAJOR=4 \
      CATALOG=poe2-content.tsv ./backfill.sh

Optional `THROTTLE=<seconds>` spaces out patches to stay under Steam's login
rate limit. `ACCOUNT` must own the game's license and have a cached session
(`-remember-password`). `data/` is git-ignored.
