#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-6379}"
INDEX_NAME="${INDEX_NAME:-idx_hybrid}"
WORKERS="${WORKERS:-24}"
ITER="${ITER:-800}"
REDIS_CLI_BIN="${REDIS_CLI_BIN:-redis-cli}"
RESP_MODE="${RESP_MODE:-both}"

if ! command -v "$REDIS_CLI_BIN" >/dev/null 2>&1; then
  echo "Error: redis-cli binary not found: $REDIS_CLI_BIN" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is required but not found in PATH" >&2
  exit 1
fi

if [[ "$RESP_MODE" != "2" && "$RESP_MODE" != "3" && "$RESP_MODE" != "both" ]]; then
  echo "Error: RESP_MODE must be one of: 2, 3, both" >&2
  exit 1
fi

run_mode_stress() {
  local mode="$1"
  local resp_flag

  if [[ "$mode" == "2" ]]; then
    resp_flag="-2"
  else
    resp_flag="-3"
  fi

  echo "Starting stress run for RESP$mode (workers=$WORKERS, iter=$ITER) ..."
  for _worker in $(seq 1 "$WORKERS"); do
    (
      for _iter in $(seq 1 "$ITER"); do
        r=$((RANDOM % 3))

        if [[ "$r" -eq 0 ]]; then
          python3 - <<'PY' | "$REDIS_CLI_BIN" -h "$HOST" -p "$PORT" "$resp_flag" -x \
            FT.HYBRID "$INDEX_NAME" \
            SEARCH "@description:(running|shoes|trail|sport*)" \
            VSIM @embedding '$BLOB' KNN 4 K 300 EF_RUNTIME 600 \
            COMBINE RRF 2 K 60 WINDOW 500 \
            TIMEOUT 3 PARAMS 2 BLOB DIALECT 4 >/dev/null
import random
import struct
import sys
sys.stdout.buffer.write(struct.pack("2f", random.random(), random.random()))
PY
        elif [[ "$r" -eq 1 ]]; then
          python3 - <<'PY' | "$REDIS_CLI_BIN" -h "$HOST" -p "$PORT" "$resp_flag" -x \
            FT.HYBRID "$INDEX_NAME" \
            SEARCH "@description:zzzz_no_match_zzzz" \
            VSIM @embedding '$BLOB' KNN 4 K 300 EF_RUNTIME 600 \
            COMBINE RRF 2 K 60 WINDOW 500 \
            TIMEOUT 3 PARAMS 2 BLOB DIALECT 4 >/dev/null
import random
import struct
import sys
sys.stdout.buffer.write(struct.pack("2f", random.random(), random.random()))
PY
        else
          python3 - <<'PY' | "$REDIS_CLI_BIN" -h "$HOST" -p "$PORT" "$resp_flag" -x \
            FT.PROFILE "$INDEX_NAME" HYBRID QUERY \
            SEARCH "@description:(running|shoes|trail|sport*)" \
            VSIM @embedding '$BLOB' KNN 4 K 300 EF_RUNTIME 600 \
            COMBINE RRF 2 K 60 WINDOW 500 \
            TIMEOUT 3 PARAMS 2 BLOB DIALECT 4 >/dev/null
import random
import struct
import sys
sys.stdout.buffer.write(struct.pack("2f", random.random(), random.random()))
PY
        fi
      done
    ) &
  done
  wait

  echo "RESP$mode run complete."
}

if [[ "$RESP_MODE" == "2" || "$RESP_MODE" == "both" ]]; then
  run_mode_stress "2"
fi

if [[ "$RESP_MODE" == "3" || "$RESP_MODE" == "both" ]]; then
  run_mode_stress "3"
fi

echo "All requested stress runs completed."
