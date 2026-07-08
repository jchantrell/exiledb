#!/usr/bin/env bash
# Shared dat-diff/file-diff logic (mirrors .github/workflows/data.yaml).
# Usage: dat-diff.sh <prev_manifest.txt> <cur_manifest.txt> <prev_stats.jsonl> <cur_stats.jsonl> <out_dir>
# Emits <out>/added-files.txt, removed-files.txt, dat-diff.txt and prints a summary line.
set -euo pipefail
PM=$1; CM=$2; PS=$3; CS=$4; OUT=$5
mkdir -p "$OUT"
TAB=$'\t'

LC_ALL=C sort "$PM" > "$OUT/pf.txt"; LC_ALL=C sort "$CM" > "$OUT/cf.txt"
LC_ALL=C comm -13 "$OUT/pf.txt" "$OUT/cf.txt" > "$OUT/added-files.txt"
LC_ALL=C comm -23 "$OUT/pf.txt" "$OUT/cf.txt" > "$OUT/removed-files.txt"

flatten() { jq -r '[.path,.sha256,.row_count,.row_width,.var_size]|@tsv' "$1" | LC_ALL=C sort -t"$TAB" -k1,1; }
flatten "$PS" > "$OUT/dp.tsv"; flatten "$CS" > "$OUT/dc.tsv"
{
  LC_ALL=C join -t"$TAB" -v2 -o 2.1 "$OUT/dp.tsv" "$OUT/dc.tsv" | sed 's/^/+ /'
  LC_ALL=C join -t"$TAB" -v1 -o 1.1 "$OUT/dp.tsv" "$OUT/dc.tsv" | sed 's/^/- /'
  LC_ALL=C join -t"$TAB" -o 1.1,1.2,2.2,1.3,2.3,1.4,2.4,1.5,2.5 "$OUT/dp.tsv" "$OUT/dc.tsv" \
    | awk -F"$TAB" '$2!=$3{cls=($6!=$7)?"schema":($4!=$5)?"rows":($8!=$9)?"data":"value"; printf "~ %s [%s]\n",$1,cls}'
} > "$OUT/dat-diff.txt"
rm -f "$OUT/pf.txt" "$OUT/cf.txt" "$OUT/dp.tsv" "$OUT/dc.tsv"

printf 'files +%s/-%s  dats: +%s -%s ~%s (schema=%s rows=%s data=%s value=%s)\n' \
  "$(wc -l <"$OUT/added-files.txt")" "$(wc -l <"$OUT/removed-files.txt")" \
  "$(grep -c '^+' "$OUT/dat-diff.txt" || true)" "$(grep -c '^-' "$OUT/dat-diff.txt" || true)" \
  "$(grep -c '^~' "$OUT/dat-diff.txt" || true)" "$(grep -c '\[schema\]' "$OUT/dat-diff.txt" || true)" \
  "$(grep -c '\[rows\]' "$OUT/dat-diff.txt" || true)" "$(grep -c '\[data\]' "$OUT/dat-diff.txt" || true)" \
  "$(grep -c '\[value\]' "$OUT/dat-diff.txt" || true)"
