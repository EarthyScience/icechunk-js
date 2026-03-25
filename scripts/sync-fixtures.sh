#!/bin/bash
# Sync test fixture data from the main icechunk repo.
# Uses a temporary sparse clone so no local checkout is needed.
#
# Usage:
#   ./scripts/sync-fixtures.sh              # from main branch
#   ./scripts/sync-fixtures.sh some-branch  # from a specific branch

set -euo pipefail

REPO="https://github.com/earth-mover/icechunk.git"
BRANCH="${1:-main}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DEST="$PROJECT_DIR/tests/data"

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "Cloning icechunk repo (sparse, branch=$BRANCH)..."
git clone --depth 1 --no-checkout --branch "$BRANCH" "$REPO" "$TMPDIR"

cd "$TMPDIR"
git sparse-checkout init --no-cone
git sparse-checkout set icechunk-python/tests/data/
git checkout
cd "$PROJECT_DIR"

mkdir -p "$DEST"
rsync -av --delete "$TMPDIR/icechunk-python/tests/data/" "$DEST/"

echo "Fixtures synced from $REPO @ $BRANCH"
