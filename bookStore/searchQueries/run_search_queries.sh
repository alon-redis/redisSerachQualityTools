# Redis query runner

`run_search_queries.sh` reads Redis Search commands from a text file and sends them with `redis-cli`.

It:
- sends `PING` before each command
- asks for your approval if `PONG` is not received
- asks for your approval if a command exits with code `1`
- sleeps 3 seconds after each command

## Usage

```bash
chmod +x run_search_queries.sh
./run_search_queries.sh cursorSearchQueryBatch.txt
```


#!/usr/bin/env bash
set -u

HOST="redis-10000.aws-alon-5160.env0.qa.redislabs.com"
PORT="10000"
INPUT_FILE="${1:-cursorSearchQueryBatch.txt}"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

if [[ ! -r /dev/tty ]]; then
  echo "Interactive prompts require /dev/tty, but it is not available." >&2
  exit 1
fi

wait_for_permission() {
  local prompt="$1"
  local answer

  while true; do
    read -r -p "$prompt [y/N]: " answer < /dev/tty
    case "$answer" in
      y|Y|yes|YES)
        return 0
        ;;
      n|N|no|NO|"")
        echo "Execution stopped by user."
        exit 1
        ;;
      *)
        echo "Please answer y or n."
        ;;
    esac
  done
}

check_ping() {
  local pong
  pong="$(redis-cli -h "$HOST" -p "$PORT" --raw PING 2>/dev/null || true)"

  if [[ "$pong" == "PONG" ]]; then
    return 0
  fi

  echo "PING response was not PONG. Received: ${pong:-<no response>}"
  wait_for_permission "PING failed. Continue anyway?"
  return 1
}

check_exit_code() {
  local rc="$1"

  if [[ "$rc" -eq 1 ]]; then
    wait_for_permission "Command returned exit code 1. Continue?"
  fi
}

normalize_stream() {
  sed -E 's#(DIALECT [0-9]+)(redis-cli -h )#\1\n\2#g' "$INPUT_FILE" |
  while IFS= read -r raw_line; do
    line="$(printf '%s' "$raw_line" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"
    [[ -z "$line" ]] && continue

    line="$(printf '%s' "$line" | sed -E 's#^redis-cli[[:space:]]+-h[[:space:]]+[^[:space:]]+[[:space:]]+-p[[:space:]]+[0-9]+[[:space:]]+##')"
    [[ -z "$line" ]] && continue

    printf '%s\n' "$line"
  done
}

while IFS= read -r cmd; do
  [[ -z "$cmd" ]] && continue

  check_ping

  echo "Running: $cmd"
  bash -lc "redis-cli -h '$HOST' -p '$PORT' --raw $cmd"
  rc=$?
  echo "Exit code: $rc"

  check_exit_code "$rc"

  sleep 3
done < <(normalize_stream)
