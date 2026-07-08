#!/usr/bin/env bash
# Phase 1: local historical backfill. For each catalog entry (oldest->newest):
#   version <- program-depot exe build tag
#   steam-pull content index + dat bundles -> cache
#   exiledb manifest + --stats -> artifacts
#   diff vs previous release (shared dat-diff.sh)
#   prune bundles
# Produces release artifacts under data/out/<version>/. Does NOT publish to
# GitHub — run `gh release` separately once the artifacts look right.
#
# Required env: DDL=<DepotDownloader path>  ACCOUNT=<steam user (session cached)>
# Optional env: APP CONTENT_DEPOT PROGRAM_DEPOT
set -uo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$DIR/.." && pwd)
: "${DDL:?set DDL=/path/to/DepotDownloader}"
: "${ACCOUNT:?set ACCOUNT=<steam login (remember-password cached)>}"
APP=${APP:-2694490}; CONTENT=${CONTENT_DEPOT:-2694491}; PROGRAM=${PROGRAM_DEPOT:-2694492}
CATALOG="$DIR/catalog.tsv"
OUT="$DIR/data/out"
W="$DIR/data/work"
CACHE="$HOME/.exiledb/cache"
mkdir -p "$OUT" "$W"

go build -C "$REPO" -o "$DIR/data/exiledb" ./cmd/exiledb
EXE="$DIR/data/exiledb"

pull() { "$DDL" -app "$APP" -depot "$1" -manifest "$2" -filelist "$3" -dir "$4" \
           -username "$ACCOUNT" -remember-password </dev/null >/dev/null 2>&1; }

prev=""
while IFS=$'\t' read -r content program date; do
  [ -z "${content:-}" ] && continue
  case "$content" in \#*) continue;; esac

  printf 'PathOfExileSteam.exe\n' > "$W/fl_exe.txt"
  pull "$PROGRAM" "$program" "$W/fl_exe.txt" "$W/exe"
  ver=$(strings -n 5 "$W/exe/PathOfExileSteam.exe" 2>/dev/null \
        | grep -oE 'tags/[0-9]+\.[0-9]+\.[0-9]+[a-z]?' | sed 's|tags/||' | sort -V | tail -1)
  [ -z "$ver" ] && { echo "SKIP $content ($date): no version tag in exe"; continue; }
  if [ -d "$OUT/$ver" ]; then echo "skip $ver (already built)"; prev="$ver"; continue; fi

  printf 'regex:^Bundles2/_\\.index\\.bin$\n' > "$W/fl_index.txt"
  pull "$CONTENT" "$content" "$W/fl_index.txt" "$W/idx"
  mkdir -p "$CACHE/$ver"; cp "$W/idx/Bundles2/_.index.bin" "$CACHE/$ver/_.index.bin"
  go run -C "$REPO" ./backfill/cmd/datbundles "$ver" > "$W/fl_data.txt"
  pull "$CONTENT" "$content" "$W/fl_data.txt" "$W/bundles"
  find "$W/bundles/Bundles2" -name '*.bundle.bin' 2>/dev/null | while read -r f; do
    rel=${f#"$W"/bundles/Bundles2/}; name=${rel%.bundle.bin}; san=${name//\//_}
    cp "$f" "$CACHE/$ver/$san"
  done

  mkdir -p "$OUT/$ver"
  "$EXE" manifest --patch "$ver" > "$OUT/$ver/manifest.txt" 2>/dev/null
  "$EXE" manifest --patch "$ver" --stats > "$OUT/$ver/dat-stats.jsonl" 2>/dev/null
  # cdn_version is unrecoverable for historical patches; manifest is the key.
  printf '{"cdn_version":null,"manifest":"%s","exe_tag":"%s","date":"%s"}\n' \
    "$content" "$ver" "$date" > "$OUT/$ver/versions.json"

  if [ -n "$prev" ] && [ -f "$OUT/$prev/dat-stats.jsonl" ]; then
    summary=$("$DIR/dat-diff.sh" "$OUT/$prev/manifest.txt" "$OUT/$ver/manifest.txt" \
      "$OUT/$prev/dat-stats.jsonl" "$OUT/$ver/dat-stats.jsonl" "$OUT/$ver")
    echo "$prev -> $ver: $summary"
  else
    echo "$ver: baseline ($(wc -l <"$OUT/$ver/dat-stats.jsonl") dats, $(wc -l <"$OUT/$ver/manifest.txt") files)"
  fi

  rm -rf "$CACHE/$ver" "$W/idx" "$W/bundles" "$W/exe"
  prev="$ver"
done < <(LC_ALL=C sort -t$'\t' -k3,3 "$CATALOG")

echo "backfill complete. artifacts in $OUT/"
