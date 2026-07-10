#!/usr/bin/env bash
# Enrich each release's versions.json with the client version, read from the
# program-depot exe (the only place a historical version string survives).
#
#   pass 1 (resolve): for each unique program manifest in <game>-pairing.tsv,
#     pull PathOfExileSteam.exe, extract its build tag, cache it. Resumable —
#     cached manifests are skipped, so a rate-limit just means "cool down, rerun".
#   pass 2 (write): inject client_version + program_manifest into every
#     out/<game>/<content_epoch>/versions.json that has a resolved version.
#
# The epoch stays the release key; client_version is meaningful metadata only.
# Required env: DDL ACCOUNT.  Optional: GAME(poe1|poe2) APP PROGRAM_DEPOT THROTTLE
set -uo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
: "${DDL:?set DDL=/path/to/DepotDownloader}"
: "${ACCOUNT:?set ACCOUNT=<steam login>}"
GAME=${GAME:-poe1}
case "$GAME" in
  poe1) APP=${APP:-238960};  PROG=${PROGRAM_DEPOT:-238962};;
  poe2) APP=${APP:-2694490}; PROG=${PROGRAM_DEPOT:-2694492};;
  *) echo "unknown GAME=$GAME"; exit 2;;
esac
PAIRING=${PAIRING:-"$DIR/$GAME-pairing.tsv"}
OUT="$DIR/data/out/$GAME"
CACHE="$DIR/data/versions-$GAME.tsv"   # program_manifest <TAB> version (resumable)
W="$DIR/data/work"
mkdir -p "$W"; touch "$CACHE"

RATELIMIT=0
resolve() {   # program_manifest -> appends "<manifest>\t<version>" to CACHE
  local man=$1
  rm -rf "$W/exe"; printf 'regex:PathOfExileSteam\\.exe$\n' > "$W/fl.txt"
  "$DDL" -app "$APP" -depot "$PROG" -manifest "$man" -filelist "$W/fl.txt" -dir "$W/exe" \
    -username "$ACCOUNT" -remember-password </dev/null >"$W/ddl.log" 2>&1
  grep -q 'RateLimitExceeded' "$W/ddl.log" && { RATELIMIT=1; return; }
  local f v; f=$(find "$W/exe" -iname 'PathOfExileSteam.exe' | head -1)
  [ -z "$f" ] && { echo "  $man -> NO EXE"; return; }
  v=$(strings -n 6 "$f" | grep -oE 'tags/[0-9][0-9.]*[a-z]?' | sed 's|tags/||' | sort -V | tail -1)
  rm -rf "$W/exe"
  [ -z "$v" ] && { echo "  $man -> NO TAG"; return; }
  printf '%s\t%s\n' "$man" "$v" >> "$CACHE"
  echo "  $man -> $v"
}

echo "pass 1: resolving unique program manifests for $GAME"
for pm in $(awk -F'\t' 'NR>1{print $3}' "$PAIRING" | sort -u); do
  grep -qP "^$pm\t" "$CACHE" && continue
  resolve "$pm"
  [ "$RATELIMIT" = 1 ] && { echo "RATE-LIMITED — cool down, then rerun to resume"; break; }
  [ "${THROTTLE:-0}" != 0 ] && sleep "$THROTTLE"
done

echo "pass 2: writing client_version into versions.json"
wrote=0
while IFS=$'\t' read -r ce cm pm; do
  [ "$ce" = content_epoch ] && continue
  vj="$OUT/$ce/versions.json"; [ -f "$vj" ] || continue
  v=$(grep -P "^$pm\t" "$CACHE" | head -1 | cut -f2); [ -z "$v" ] && continue
  tmp=$(mktemp)
  jq --arg v "$v" --arg pm "$pm" '. + {client_version:$v, program_manifest:$pm}' "$vj" > "$tmp" && mv "$tmp" "$vj" && wrote=$((wrote+1))
done < "$PAIRING"
echo "done: resolved=$(($(wc -l <"$CACHE"))) versions cached, wrote client_version into $wrote releases"
