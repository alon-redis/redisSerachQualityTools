#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
INDEX_NAME="${INDEX_NAME:-idx_hybrid}"
DOC_PREFIX="${DOC_PREFIX:-doc:}"
DOC_COUNT="${DOC_COUNT:-20000}"
RECREATE_INDEX="${RECREATE_INDEX:-0}"
REDIS_CLI_BIN="${REDIS_CLI_BIN:-redis-cli}"

if ! command -v "$REDIS_CLI_BIN" >/dev/null 2>&1; then
  echo "Error: redis-cli binary not found: $REDIS_CLI_BIN" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is required but not found in PATH" >&2
  exit 1
fi

if [[ "$RECREATE_INDEX" == "1" ]]; then
  echo "Dropping index '$INDEX_NAME' (if it exists) ..."
  "$REDIS_CLI_BIN" -h "$HOST" -p "$PORT" FT.DROPINDEX "$INDEX_NAME" DD >/dev/null 2>&1 || true
fi

echo "Creating index '$INDEX_NAME' ..."
"$REDIS_CLI_BIN" -h "$HOST" -p "$PORT" FT.CREATE "$INDEX_NAME" ON HASH PREFIX 1 "$DOC_PREFIX" SCHEMA \
  description TEXT \
  embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 >/dev/null

echo "Loading $DOC_COUNT hash documents via --pipe ..."
HOST="$HOST" PORT="$PORT" INDEX_NAME="$INDEX_NAME" DOC_PREFIX="$DOC_PREFIX" DOC_COUNT="$DOC_COUNT" \
REDIS_CLI_BIN="$REDIS_CLI_BIN" python3 - <<'PY' | "$REDIS_CLI_BIN" -h "$HOST" -p "$PORT" --pipe
import os
import random
import struct
import sys

doc_prefix = os.environ["DOC_PREFIX"]
doc_count = int(os.environ["DOC_COUNT"])

rng = random.Random(1337)
labels = ("running", "shoes", "trail", "sport", "road", "marathon")

for i in range(1, doc_count + 1):
    description = f"{labels[i % len(labels)]} {labels[(i + 2) % len(labels)]} model-{i % 200}"
    embedding = struct.pack("2f", rng.random(), rng.random())

    cmd = [
        b"HSET",
        f"{doc_prefix}{i}".encode(),
        b"description",
        description.encode(),
        b"embedding",
        embedding,
    ]

    sys.stdout.buffer.write(f"*{len(cmd)}\r\n".encode())
    for part in cmd:
        sys.stdout.buffer.write(f"${len(part)}\r\n".encode())
        sys.stdout.buffer.write(part + b"\r\n")
PY

echo "Done."
echo "Index: $INDEX_NAME | Documents loaded: $DOC_COUNT | Host: $HOST:$PORT"
