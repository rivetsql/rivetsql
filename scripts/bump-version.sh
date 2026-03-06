#!/usr/bin/env bash
# Usage: ./scripts/bump-version.sh 0.2.0
#
# Updates the version in every pyproject.toml and rivet/VERSION
# so there is a single source of truth.
set -euo pipefail

NEW_VERSION="${1:?Usage: $0 <new-version>}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OLD_VERSION="$(cat "$REPO_ROOT/rivet/VERSION" | tr -d '[:space:]')"

if [[ "$NEW_VERSION" == "$OLD_VERSION" ]]; then
  echo "Already at version $NEW_VERSION"
  exit 0
fi

echo "Bumping $OLD_VERSION → $NEW_VERSION"

# 1. Update VERSION file
printf '%s\n' "$NEW_VERSION" > "$REPO_ROOT/rivet/VERSION"

# 2. Update version = "..." in all pyproject.toml files
for f in \
  "$REPO_ROOT/rivet/pyproject.toml" \
  "$REPO_ROOT"/rivet/src/rivet_*/pyproject.toml; do
  sed -i '' "s/^version = \"$OLD_VERSION\"/version = \"$NEW_VERSION\"/" "$f"
done

# 3. Update == pins in core optional-dependencies
sed -i '' "s/==$OLD_VERSION/==$NEW_VERSION/g" "$REPO_ROOT/rivet/pyproject.toml"

# 4. Update rivetsql== pin in each plugin's dependencies
for f in "$REPO_ROOT"/rivet/src/rivet_*/pyproject.toml; do
  sed -i '' "s/\"rivetsql==$OLD_VERSION\"/\"rivetsql==$NEW_VERSION\"/" "$f"
done

echo "Updated files:"
grep -rn "version = \"$NEW_VERSION\"" "$REPO_ROOT"/rivet/pyproject.toml "$REPO_ROOT"/rivet/src/rivet_*/pyproject.toml
echo ""
echo "Done. Don't forget to update CHANGELOG.md"
