#!/usr/bin/env bash
# Download and extract a KuaiRand dataset version from Zenodo.
#
# Usage: ./scripts/download_data.sh [pure|1k|27k]   (default: pure)
#
# Files land in data/raw/KuaiRand-<Version>/. Downloads resume if interrupted,
# and every archive is checked against its published MD5 before extraction.
# Source: https://kuairand.com/  (license: CC BY-SA 4.0)

set -euo pipefail

VERSION="${1:-pure}"
BASE_URL="https://zenodo.org/records/10439422/files"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${DATA_DIR:-$REPO_ROOT/data/raw}"

case "$VERSION" in
  pure) NAME="KuaiRand-Pure"; MD5="0820331067a3784d9691136f772b35a7"; NEED_GB=1  ;;
  1k)   NAME="KuaiRand-1K";   MD5="6b0b9c8222d67fcd4c676218edca3f1f"; NEED_GB=10 ;;
  27k)  NAME="KuaiRand-27K";  MD5="3e3c799a24e2d23a4d2c757fbf9adf59"; NEED_GB=100 ;;
  *) echo "Unknown version '$VERSION'. Use one of: pure, 1k, 27k" >&2; exit 1 ;;
esac

ARCHIVE="$DATA_DIR/$NAME.tar.gz"
TARGET="$DATA_DIR/$NAME"

md5_of() {
  if command -v md5sum >/dev/null 2>&1; then md5sum "$1" | awk '{print $1}'
  else md5 -q "$1"; fi
}

mkdir -p "$DATA_DIR"

if [[ -d "$TARGET/data" ]]; then
  echo "$NAME already extracted at $TARGET"
  exit 0
fi

# Archive + extracted copy need roughly NEED_GB of free space.
free_gb=$(df -Pk "$DATA_DIR" | awk 'NR==2 {print int($4 / 1024 / 1024)}')
if (( free_gb < NEED_GB )); then
  echo "Need about ${NEED_GB} GB free for $NAME, only ${free_gb} GB available in $DATA_DIR." >&2
  echo "Set DATA_DIR to a larger disk, or use a smaller version." >&2
  exit 1
fi

echo "Downloading $NAME to $ARCHIVE"
curl -L --fail --retry 5 --retry-delay 5 -C - -o "$ARCHIVE" "$BASE_URL/$NAME.tar.gz"

echo "Verifying checksum"
actual=$(md5_of "$ARCHIVE")
if [[ "$actual" != "$MD5" ]]; then
  echo "MD5 mismatch for $ARCHIVE: expected $MD5, got $actual" >&2
  echo "Delete the archive and re-run to download it again." >&2
  exit 1
fi

echo "Extracting"
tar -xzf "$ARCHIVE" -C "$DATA_DIR"

echo "Done. Files:"
find "$TARGET" -type f -exec ls -lh {} \; | awk '{print $5, $NF}'
