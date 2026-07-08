#!/usr/bin/env bash
# Phase 1: local historical backfill (content depot only). For each catalog
# entry (oldest->newest):
#   steam-pull content index + dat bundles -> cache
#   exiledb manifest + --stats -> artifacts
#   diff vs previous release (shared dat-diff.sh)
#   prune bundles
# Releases are keyed by patch epoch (Unix seconds). Historical CDN/exe versions
# are unrecoverable, so the manifest date is the stable label — no program
# depot, no client version tag.
# Produces release artifacts under data/out/<epoch>/. Does NOT publish to
# GitHub — run `gh release` separately once the artifacts look right.
#
# Catalog format (TSV, header skipped): epoch <TAB> date <TAB> content-manifest
# Required env: DDL=<DepotDownloader path>  ACCOUNT=<steam user (session cached)>
# Optional env: APP CONTENT_DEPOT MAJOR CATALOG
set -uo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$DIR/.." && pwd)
: "${DDL:?set DDL=/path/to/DepotDownloader}"
: "${ACCOUNT:?set ACCOUNT=<steam login (remember-password cached)>}"
APP=${APP:-238960}; CONTENT=${CONTENT_DEPOT:-238961}; MAJOR=${MAJOR:-3}
CATALOG=${CATALOG:-"$DIR/poe1-content.tsv"}
OUT="$DIR/data/out"
W="$DIR/data/work"
CACHE="$HOME/.exiledb/cache"
mkdir -p "$OUT" "$W"

go build -C "$REPO" -o "$DIR/data/exiledb" ./cmd/exiledb
EXE="$DIR/data/exiledb"

pull() { "$DDL" -app "$APP" -depot "$CONTENT" -manifest "$1" -filelist "$2" -dir "$3" \
           -username "$ACCOUNT" -remember-password </dev/null >/dev/null 2>&1; }

prev=""
while IFS=$'\t' read -r epoch date manifest; do
  [ -z "${epoch:-}" ] && continue
  case "$epoch" in \#*|epoch) continue;; esac

  ver="$MAJOR.$epoch"   # exiledb patch label: parts[0] selects the game layout, whole string keys the cache
  if [ -d "$OUT/$epoch" ]; then echo "skip $epoch ($date, already built)"; prev="$epoch"; continue; fi

  printf 'regex:^Bundles2/_\\.index\\.bin$\n' > "$W/fl_index.txt"
  pull "$manifest" "$W/fl_index.txt" "$W/idx"
  [ -f "$W/idx/Bundles2/_.index.bin" ] || { echo "SKIP $epoch ($date): index pull failed ($manifest)"; continue; }
  mkdir -p "$CACHE/$ver"; cp "$W/idx/Bundles2/_.index.bin" "$CACHE/$ver/_.index.bin"
  go run -C "$REPO" ./backfill/cmd/datbundles "$ver" > "$W/fl_data.txt"
  pull "$manifest" "$W/fl_data.txt" "$W/bundles"
  find "$W/bundles/Bundles2" -name '*.bundle.bin' 2>/dev/null | while read -r f; do
    rel=${f#"$W"/bundles/Bundles2/}; name=${rel%.bundle.bin}; san=${name//\//_}
    cp "$f" "$CACHE/$ver/$san"
  done

  mkdir -p "$OUT/$epoch"
  "$EXE" manifest --patch "$ver" > "$OUT/$epoch/manifest.txt" 2>/dev/null
  "$EXE" manifest --patch "$ver" --stats > "$OUT/$epoch/dat-stats.jsonl" 2>/dev/null
  printf '{"manifest":"%s","epoch":%s,"date":"%s"}\n' \
    "$manifest" "$epoch" "$date" > "$OUT/$epoch/versions.json"

  if [ -n "$prev" ] && [ -f "$OUT/$prev/dat-stats.jsonl" ]; then
    summary=$("$DIR/dat-diff.sh" "$OUT/$prev/manifest.txt" "$OUT/$epoch/manifest.txt" \
      "$OUT/$prev/dat-stats.jsonl" "$OUT/$epoch/dat-stats.jsonl" "$OUT/$epoch")
    echo "$date ($epoch): $summary"
  else
    echo "$date ($epoch): baseline ($(wc -l <"$OUT/$epoch/dat-stats.jsonl") dats, $(wc -l <"$OUT/$epoch/manifest.txt") files)"
  fi

  rm -rf "$CACHE/$ver" "$W/idx" "$W/bundles"
  prev="$epoch"
done < <(LC_ALL=C sort -t$'\t' -k1,1n "$CATALOG")

echo "backfill complete. artifacts in $OUT/"
