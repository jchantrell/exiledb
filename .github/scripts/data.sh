#!/usr/bin/env bash
set -euo pipefail

GAME="$1"
TAG="data-${GAME}-${VERSION}"
OUT="out"

case "$GAME" in
  poe1) TITLE_PREFIX="PoE1" ;;
  poe2) TITLE_PREFIX="PoE2" ;;
  *) echo "unknown game: $GAME" >&2; exit 1 ;;
esac

ASSETS=("$OUT/manifest.txt.gz" "$OUT/dat-stats.jsonl")
for f in versions.json added-files.txt removed-files.txt; do
  if [ -s "$OUT/$f" ]; then
    ASSETS+=("$OUT/$f")
  fi
done

if gh release view "$TAG" --repo "$GITHUB_REPOSITORY" &>/dev/null; then
  gh release upload "$TAG" "${ASSETS[@]}" --clobber --repo "$GITHUB_REPOSITORY"
  if [ -f "$OUT/added-files.txt" ]; then
    gh release edit "$TAG" --notes-file "$OUT/notes.md" --repo "$GITHUB_REPOSITORY"
  fi
else
  gh release create "$TAG" "${ASSETS[@]}" --latest=false \
    --title "${TITLE_PREFIX} Data ${VERSION}" \
    --notes-file "$OUT/notes.md" \
    --repo "$GITHUB_REPOSITORY"
fi
