# backfill

Repo tooling to reconstruct historical data releases from Steam depots (not the
CDN, which only serves the current patch). Ongoing releases stay in CI
(`.github/workflows/data.yaml`); this is only for the historical tail. It is not
part of the `exiledb` CLI.

Everything except the schema corpus lives in one Go command
(`cmd/backfill`), which calls exiledb's own `internal/` packages directly —
cache paths, index handling and dat parsing come from one implementation rather
than being restated around the binary. DepotDownloader is the only external
process.

## Phases

- **Phase 0 — schema corpus** (`build-corpus.sh`): generate `schema.min.json` for
  every `dat-schema` commit → `data/corpus/<date>_<hash>.json` + `corpus-index.tsv`.
  Shell because it just drives `git` and `npm` over every commit. Only needed for
  content-tier (named/typed) releases; the structural release format
  (`manifest.txt` + `dat-stats.jsonl` + `versions.json` + file diffs) is schema-free.

- **Phase 1 — pull** (`backfill pull`): for each catalog entry, oldest→newest:
  Steam-pull the content index + the bundles holding dat tables, write
  `manifest.txt` / `dat-stats.jsonl` / `versions.json`, diff files against the
  previous release, prune the bundles. Writes to `data/out/<game>/<epoch>/`, each
  game in its own tree. Releases are keyed by the content manifest's patch
  **epoch** (Unix seconds): historical CDN versions are unrecoverable, so the
  patch date is the stable label. dat-level diffs aren't persisted — they're
  derivable from any two `dat-stats.jsonl`.

- **Phase 2 — versions** (`backfill versions`): pair each release with the
  program manifest live at its patch, read that build's `PathOfExileSteam.exe`
  for its build tag, and add `client_version` and `program_manifest` to
  `versions.json`. The exe is the only place a historical client version
  survives, and it is the one label CI can also derive for future patches — so
  it names releases in both eras. The epoch stays the release key.

**Neither publishes** — run `gh release` yourself once the artifacts look right.

## The catalogs are the gate

Steam has no history API and manifest IDs aren't enumerable, so the lists come
from SteamDB: export a depot's manifest history and parse it into
`epoch <TAB> date <TAB> manifest` (header skipped).

| file | depot |
| --- | --- |
| `poe1-content.tsv` | app 238960, content 238961 |
| `poe1-program.tsv` | app 238960, program 238962 |
| `poe2-content.tsv` | app 2694490, content 2694491 |
| `poe2-program.tsv` | app 2694490, program 2694492 |

## Run

    ./build-corpus.sh                                    # Phase 0 (optional; content tier only)

    go run ./cmd/backfill pull     -game poe1 -throttle 8s
    go run ./cmd/backfill versions -game poe1 -throttle 8s

`-ddl` and `-account` default to `$DDL` and `$ACCOUNT`. The account must own the
game's license and have a cached session (`-remember-password`); that session is
keyed to the DepotDownloader path, so keep it stable.

Both commands are resumable: `pull` skips releases already built, `versions`
skips builds already in `data/versions-<game>.tsv`. Steam rate-limits login
frequency, so both stop cleanly on `RateLimitExceeded` — cool down and rerun.
`-throttle` spaces out pulls to stay under it. `data/` is git-ignored.
