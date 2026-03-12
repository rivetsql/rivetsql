#!/usr/bin/env bash
# Install rivet core (editable) + all plugins (local source) for development.
#
# Usage: ./scripts/dev-install.sh
#        ./scripts/dev-install.sh --plugins-only   # skip core, reinstall plugins
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RIVET="$REPO_ROOT/rivet"

if [[ "${1:-}" != "--plugins-only" ]]; then
  echo "▸ Installing rivet core (editable)…"
  pip install -e "$RIVET[dev,test]" --quiet
fi

for plugin in aws databricks duckdb polars postgres pyspark rest; do
  echo "▸ Installing rivet-${plugin} (local)…"
  pip install "$RIVET/src/rivet_${plugin}" --quiet --force-reinstall --no-deps
done

echo ""
echo "✓ All packages installed from local source."
pip show rivetsql rivetsql-aws rivetsql-databricks rivetsql-duckdb rivetsql-polars rivetsql-postgres rivetsql-pyspark rivetsql-rest 2>/dev/null \
  | grep -E '^(Name|Version|Location):'
