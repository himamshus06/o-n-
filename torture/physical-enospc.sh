#!/usr/bin/env bash
set -Eeuo pipefail

IMAGE_SIZE_MB="${IMAGE_SIZE_MB:-128}"
ROOT="$(mktemp -d /tmp/kvdb-physical-enospc.XXXXXX)"
IMAGE="$ROOT/filesystem.img"
MOUNT="$ROOT/mount"
REPORT="${1:-artifacts/physical-enospc.json}"
EXECUTABLE="${KVDB_EXECUTABLE:-$PWD/kvdb}"

cleanup() {
  set +e
  if mountpoint -q "$MOUNT"; then
    sudo umount "$MOUNT"
  fi
  rm -rf -- "$ROOT"
}
trap cleanup EXIT INT TERM

if [[ -e "$MOUNT" && "$(find "$MOUNT" -mindepth 1 -print -quit)" ]]; then
  echo "refusing non-empty mount directory" >&2
  exit 2
fi
if [[ ! -x "$EXECUTABLE" ]]; then
  echo "missing executable: $EXECUTABLE" >&2
  exit 2
fi
if (( IMAGE_SIZE_MB < 64 || IMAGE_SIZE_MB > 512 )); then
  echo "IMAGE_SIZE_MB must be between 64 and 512" >&2
  exit 2
fi
if ! sudo -n true; then
  echo "passwordless sudo is required; refusing to configure mounts" >&2
  exit 2
fi

mkdir "$MOUNT"
fallocate -l "${IMAGE_SIZE_MB}M" "$IMAGE"
mkfs.ext4 -q -F "$IMAGE"
sudo mount -o loop "$IMAGE" "$MOUNT"
sudo chown "$(id -u):$(id -g)" "$MOUNT"
if [[ "$(find "$MOUNT" -mindepth 1 -print -quit)" ]]; then
  echo "mounted filesystem is unexpectedly non-empty" >&2
  exit 2
fi

mkdir -p "$(dirname "$REPORT")"
PYTHONPATH="$PWD/torture" python3 "$PWD/torture/physical_enospc.py" \
  --executable "$EXECUTABLE" \
  --database "$MOUNT/database.kv" \
  --report "$REPORT"
