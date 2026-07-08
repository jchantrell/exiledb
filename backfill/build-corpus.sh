#!/usr/bin/env bash
# Phase 0: generate schema.min.json for every dat-schema commit.
# Output: data/corpus/<date>_<shorthash>.json and data/corpus-index.tsv (date<TAB>commit<TAB>path).
# Resumable: commits already in the index are skipped.
#
# Each commit ships its own built dist/cli.js, so we just force-checkout and run
# it. A HEAD build is kept as a fallback for the rare commit that gitignored dist.
set -uo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
REPO="$DIR/dat-schema"
CORPUS="$DIR/data/corpus"
INDEX="$DIR/data/corpus-index.tsv"
FALLBACK="$DIR/data/.head-cli.js"
LIMIT="${1:-0}"   # optional: only the newest N commits (0 = all)
mkdir -p "$CORPUS"
touch "$INDEX"

if [ ! -d "$REPO/.git" ]; then
  git clone -q https://github.com/poe-tool-dev/dat-schema.git "$REPO"
fi
cd "$REPO"
git checkout -qf main && git pull -q 2>/dev/null || true
[ -d node_modules ] || npm ci --silent
npm run generate >/dev/null 2>&1 || true            # HEAD build = fallback generator
cp -f dist/cli.js "$FALLBACK" 2>/dev/null || true

clean() { rm -f schema.min.json schema-poe1.min.json schema-poe2.min.json schema.json 2>/dev/null; }

commits=$(git rev-list --reverse main)
[ "$LIMIT" -gt 0 ] && commits=$(git rev-list main | head -n "$LIMIT" | tac)

ok=0; fail=0; skip=0
for c in $commits; do
  grep -q "	$c	" "$INDEX" && { skip=$((skip+1)); continue; }
  git checkout -qf "$c" 2>/dev/null || { fail=$((fail+1)); continue; }
  clean
  [ -f dist/cli.js ] || cp -f "$FALLBACK" dist/cli.js 2>/dev/null
  node dist/cli.js >/dev/null 2>&1 || true
  if [ -s schema.min.json ]; then
    date=$(git show -s --format=%cI "$c")
    out="$CORPUS/${date%%T*}_${c:0:12}.json"
    cp schema.min.json "$out"
    printf '%s\t%s\t%s\n' "$date" "$c" "$out" >> "$INDEX"
    ok=$((ok+1))
  else
    fail=$((fail+1))
  fi
done
git checkout -qf main 2>/dev/null
clean
LC_ALL=C sort -o "$INDEX" "$INDEX"
echo "corpus done: ok=$ok fail=$fail skip=$skip  total_index=$(wc -l <"$INDEX")"
