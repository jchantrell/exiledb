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
  derive the version from the program-depot exe build tag, Steam-pull the content
  index + dat bundles, run `exiledb manifest`/`--stats`, diff files vs the
  previous release (`dat-diff.sh`), prune. Writes artifacts to
  `data/out/<version>/`. **Does not publish** — run `gh release` yourself once
  the artifacts look right. dat-level diffs aren't persisted — they're derivable
  from any two `dat-stats.jsonl`.

## The catalog is the gate

`catalog.tsv` lists `(content_manifest, program_manifest, date)`. Steam has no
history API and manifest IDs aren't enumerable — the full list lives on SteamDB.
The seeded rows are the partial set enumerable from Wayback + PICS. For true
full history, export the depot's manifest history from SteamDB into `catalog.tsv`.

## Run

    ./build-corpus.sh                 # Phase 0 (optional; content tier only)
    DDL=/path/to/DepotDownloader ACCOUNT=<steam-user> ./backfill.sh

`ACCOUNT` must own PoE2's free license and have a cached session
(`-remember-password`). `data/` is git-ignored.
