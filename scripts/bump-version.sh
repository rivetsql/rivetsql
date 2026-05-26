#!/usr/bin/env bash
# Usage: ./scripts/bump-version.sh 0.2.0
#
# Updates the version in every pyproject.toml and rivet/VERSION
# so there is a single source of truth.
#
# Portable across GNU sed (Linux/CI) and BSD sed (macOS) by avoiding
# the in-place flag and rewriting files via a Python helper.
set -euo pipefail

NEW_VERSION="${1:?Usage: $0 <new-version>}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OLD_VERSION="$(cat "$REPO_ROOT/rivet/VERSION" | tr -d '[:space:]')"

if [[ "$NEW_VERSION" == "$OLD_VERSION" ]]; then
  echo "Already at version $NEW_VERSION"
  exit 0
fi

echo "Bumping $OLD_VERSION → $NEW_VERSION"

# Extract major.minor for range bounds
MAJOR="${NEW_VERSION%%.*}"
REST="${NEW_VERSION#*.}"
MINOR="${REST%%.*}"
NEXT_MINOR=$((MINOR + 1))
RANGE_LOW="${MAJOR}.${MINOR}.0"
RANGE_HIGH="${MAJOR}.${NEXT_MINOR}.0"
OLD_MAJOR="${OLD_VERSION%%.*}"
OLD_REST="${OLD_VERSION#*.}"
OLD_MINOR="${OLD_REST%%.*}"
OLD_RANGE_LOW="${OLD_MAJOR}.${OLD_MINOR}.0"
OLD_NEXT_MINOR=$((OLD_MINOR + 1))
OLD_RANGE_HIGH="${OLD_MAJOR}.${OLD_NEXT_MINOR}.0"

# Portable in-place replace (works on Linux + macOS, no BSD/GNU sed flag drift).
replace_in_file() {
  local pattern="$1" replacement="$2" file="$3"
  python3 - "$pattern" "$replacement" "$file" <<'PY'
import sys
pattern, replacement, path = sys.argv[1:4]
with open(path, encoding="utf-8") as fh:
    text = fh.read()
new = text.replace(pattern, replacement)
if new != text:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(new)
PY
}

# 1. Update VERSION file
printf '%s\n' "$NEW_VERSION" > "$REPO_ROOT/rivet/VERSION"

# 2. Update version = "..." in all pyproject.toml files
for f in \
  "$REPO_ROOT/rivet/pyproject.toml" \
  "$REPO_ROOT"/rivet/src/rivet_*/pyproject.toml; do
  replace_in_file "version = \"$OLD_VERSION\"" "version = \"$NEW_VERSION\"" "$f"
done

# 3. Update range pins in core optional-dependencies (only if minor changed)
if [[ "$MINOR" != "$OLD_MINOR" || "$MAJOR" != "$OLD_MAJOR" ]]; then
  replace_in_file ">=${OLD_RANGE_LOW},<${OLD_RANGE_HIGH}" ">=${RANGE_LOW},<${RANGE_HIGH}" \
    "$REPO_ROOT/rivet/pyproject.toml"
  for f in "$REPO_ROOT"/rivet/src/rivet_*/pyproject.toml; do
    replace_in_file ">=${OLD_RANGE_LOW},<${OLD_RANGE_HIGH}" ">=${RANGE_LOW},<${RANGE_HIGH}" "$f"
  done
fi

echo "Updated files:"
grep -n "version = \"$NEW_VERSION\"" "$REPO_ROOT"/rivet/pyproject.toml "$REPO_ROOT"/rivet/src/rivet_*/pyproject.toml
echo ""
echo "Done. Don't forget to update CHANGELOG.md"
