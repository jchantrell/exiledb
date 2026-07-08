#!/usr/bin/env bash
# Emits the file-level diff assets (added/removed manifest paths) and prints a
# console summary of dat changes. dat-diff.txt is intentionally NOT persisted:
# it is fully derivable from any two dat-stats.jsonl, so consumers compute it on
# demand for whatever pair they want.
# Usage: dat-diff.sh <prev_manifest> <cur_manifest> <prev_stats> <cur_stats> <out_dir>
set -uo pipefail
PM=$1; CM=$2; PS=$3; CS=$4; OUT=$5
mkdir -p "$OUT"
TAB=$'\t'

LC_ALL=C sort "$PM" > "$OUT/pf.txt"; LC_ALL=C sort "$CM" > "$OUT/cf.txt"
LC_ALL=C comm -13 "$OUT/pf.txt" "$OUT/cf.txt" > "$OUT/added-files.txt"
LC_ALL=C comm -23 "$OUT/pf.txt" "$OUT/cf.txt" > "$OUT/removed-files.txt"
rm -f "$OUT/pf.txt" "$OUT/cf.txt"

flatten() { jq -r '[.path,.sha256,.row_count,.row_width,.var_size]|@tsv' "$1" | LC_ALL=C sort -t"$TAB" -k1,1; }
dp=$(mktemp); dc=$(mktemp)
flatten "$PS" > "$dp"; flatten "$CS" > "$dc"
add=$(LC_ALL=C join -t"$TAB" -v2 -o 2.1 "$dp" "$dc" | grep -c . || true)
rem=$(LC_ALL=C join -t"$TAB" -v1 -o 1.1 "$dp" "$dc" | grep -c . || true)
cls=$(LC_ALL=C join -t"$TAB" -o 1.2,2.2,1.3,2.3,1.4,2.4,1.5,2.5 "$dp" "$dc" \
  | awk -F"$TAB" '$1!=$2{print ($5!=$6)?"schema":($3!=$4)?"rows":($7!=$8)?"data":"value"}')
rm -f "$dp" "$dc"
n() { printf '%s' "$cls" | grep -c "^$1\$" || true; }

printf 'files +%s/-%s  dats: +%s -%s ~%s (schema=%s rows=%s data=%s value=%s)\n' \
  "$(grep -c . "$OUT/added-files.txt" || true)" "$(grep -c . "$OUT/removed-files.txt" || true)" \
  "$add" "$rem" "$(printf '%s' "$cls" | grep -c . || true)" \
  "$(n schema)" "$(n rows)" "$(n data)" "$(n value)"
