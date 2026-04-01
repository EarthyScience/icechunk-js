#!/bin/bash
# Ensure the pinned flatc version is available at .bin/flatc.
# Downloads it if missing or wrong version. Prints the path on stdout.
#
# Usage:
#   FLATC="$(./scripts/ensure-flatc.sh)"
#   "$FLATC" -T -o ./src/format/flatbuffers --gen-all ./flatbuffers/all.fbs

set -euo pipefail

FLATC_VERSION="25.12.19"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$PROJECT_DIR/.bin"
FLATC="$BIN_DIR/flatc"

# Check if we already have the right version
if [ -x "$FLATC" ]; then
  current=$("$FLATC" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "")
  if [ "$current" = "$FLATC_VERSION" ]; then
    echo "$FLATC"
    exit 0
  fi
  echo "flatc in .bin/ is v$current, need v$FLATC_VERSION" >&2
fi

# Download
case "$(uname -s)" in
  Linux)  ASSET="Linux.flatc.binary.clang++-18.zip" ;;
  Darwin) ASSET="Mac.flatc.binary.zip" ;;
  *)
    echo "error: cannot download flatc for $(uname -s)" >&2
    echo "Install flatc v$FLATC_VERSION to .bin/flatc manually." >&2
    exit 1
    ;;
esac

mkdir -p "$BIN_DIR"

URL="https://github.com/google/flatbuffers/releases/download/v${FLATC_VERSION}/${ASSET}"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Downloading flatc v${FLATC_VERSION}..." >&2
curl -sL "$URL" -o "$TMPDIR/flatc.zip"
unzip -o -q "$TMPDIR/flatc.zip" -d "$BIN_DIR/"
chmod +x "$FLATC"

echo "$FLATC"
