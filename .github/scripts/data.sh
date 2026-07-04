#!/usr/bin/env bash
set -euo pipefail

GAME="$1"
TAG="data-${GAME}-${VERSION}"
POINTER_TAG="data-${GAME}-latest"
OUT="out"

case "$GAME" in
  poe1) TITLE_PREFIX="PoE1" ;;
  poe2) TITLE_PREFIX="PoE2" ;;
  *) echo "unknown game: $GAME" >&2; exit 1 ;;
esac

ASSETS=("$OUT/manifest.txt")
if [ -f "$OUT/added-files.txt" ]; then
  ASSETS+=("$OUT/added-files.txt" "$OUT/removed-files.txt")
fi

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

CURRENT=$(curl -sfL "https://github.com/${GITHUB_REPOSITORY}/releases/download/${POINTER_TAG}/latest.json" | jq -r '.version') || CURRENT=""
if [ -n "$CURRENT" ] && [ "$CURRENT" != "$VERSION" ]; then
  HIGHEST=$(printf '%s\n%s\n' "$CURRENT" "$VERSION" | sort -V | tail -1)
  if [ "$HIGHEST" != "$VERSION" ]; then
    echo "pointer $CURRENT is newer than $VERSION, leaving it"
    exit 0
  fi
fi

echo "{\"version\":\"${VERSION}\"}" > "$OUT/latest.json"
if gh release view "$POINTER_TAG" --repo "$GITHUB_REPOSITORY" &>/dev/null; then
  gh release upload "$POINTER_TAG" "$OUT/latest.json" --clobber --repo "$GITHUB_REPOSITORY"
else
  gh release create "$POINTER_TAG" "$OUT/latest.json" --latest=false \
    --title "${TITLE_PREFIX} Latest Data Pointer" \
    --notes "Points to the newest ${TITLE_PREFIX} data release." \
    --repo "$GITHUB_REPOSITORY"
fi
