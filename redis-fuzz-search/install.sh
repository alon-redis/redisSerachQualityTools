#!/usr/bin/env bash
# install.sh — bootstrap redis-fuzz-search on a fresh Ubuntu 22.04 (Jammy) VM.
#
# What it does:
#   1. Verifies Ubuntu Jammy
#   2. Installs system build dependencies via apt
#   3. Installs Rust (stable) for the current user
#   4. Clones upstream enzosaracen/redis-fuzz with pinned submodules
#   5. Overlays the RediSearch-focused modifications from this repo
#   6. Applies the two known build patches (boost-qvm sed + serverLog noop)
#   7. Builds the `fuzz` and `replay` binaries
#   8. Runs a smoke test against the seeded `idx` index
#
# Usage (on a fresh VM, as the `ubuntu` user):
#   curl -fsSL https://raw.githubusercontent.com/alon-redis/redisSerachQualityTools/main/redis-fuzz-search/install.sh | bash
# or, after cloning this repo:
#   bash redis-fuzz-search/install.sh
#
# Environment knobs (export before running):
#   PREFIX            Install directory (default: $HOME/redis-fuzz)
#   QA_REPO_URL       URL of this repo (default: https://github.com/alon-redis/redisSerachQualityTools.git)
#   QA_REPO_BRANCH    Branch of this repo to overlay from (default: main)
#   UPSTREAM_URL      Upstream redis-fuzz URL (default: https://github.com/enzosaracen/redis-fuzz.git)
#   SKIP_APT=1        Skip the apt install step (assume deps are present)
#   SKIP_RUST=1       Skip the rustup install step (assume cargo is on PATH)
#   SKIP_BUILD=1      Stop after the overlay+patch (don't run make)
#   SKIP_SMOKE=1      Skip the post-build replay smoke test
#   JOBS              Parallelism for `make` (default: nproc)
#
# Exit codes:
#   0  success
#   1  fatal error (logged)

set -euo pipefail

# ---------- helpers ----------------------------------------------------------

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m[%s] WARN:\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '\033[1;31m[%s] FATAL:\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

# ---------- config -----------------------------------------------------------

PREFIX="${PREFIX:-$HOME/redis-fuzz}"
QA_REPO_URL="${QA_REPO_URL:-https://github.com/alon-redis/redisSerachQualityTools.git}"
QA_REPO_BRANCH="${QA_REPO_BRANCH:-main}"
UPSTREAM_URL="${UPSTREAM_URL:-https://github.com/enzosaracen/redis-fuzz.git}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 2)}"

# ---------- 1. OS check ------------------------------------------------------

log "Verifying Ubuntu Jammy..."
if ! grep -q "^VERSION_CODENAME=jammy" /etc/os-release 2>/dev/null; then
    warn "Not running Ubuntu 22.04 (Jammy). Continuing, but apt package names may differ."
fi

if [[ $EUID -eq 0 ]]; then
    warn "Running as root. Rust will be installed system-wide; this is unusual."
    SUDO=""
else
    SUDO="sudo"
fi

# ---------- 2. apt packages --------------------------------------------------

if [[ "${SKIP_APT:-0}" != "1" ]]; then
    log "Installing system packages (apt)..."
    export DEBIAN_FRONTEND=noninteractive
    $SUDO apt-get update -y
    $SUDO apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        lld \
        llvm \
        cmake \
        pkg-config \
        git \
        python3 \
        python3-pip \
        python3-venv \
        libssl-dev \
        libsystemd-dev \
        zlib1g-dev \
        libtool \
        autoconf \
        automake \
        curl \
        ca-certificates \
        unzip \
        file
else
    log "SKIP_APT=1, skipping apt install."
fi

# ---------- 3. Rust ----------------------------------------------------------

if [[ "${SKIP_RUST:-0}" != "1" ]]; then
    if command -v cargo >/dev/null 2>&1; then
        log "Rust already installed: $(cargo --version)"
    else
        log "Installing Rust (stable)..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
            | sh -s -- -y --default-toolchain stable --profile minimal
    fi
    # shellcheck disable=SC1091
    [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"
    rustup component add rust-src >/dev/null 2>&1 || true
else
    log "SKIP_RUST=1, skipping rustup install."
fi
command -v cargo >/dev/null 2>&1 || die "cargo not on PATH; run: source \$HOME/.cargo/env"

# ---------- 4. Clone upstream redis-fuzz with submodules ---------------------

mkdir -p "$(dirname "$PREFIX")"
if [[ -d "$PREFIX/.git" ]]; then
    log "Upstream clone already present at $PREFIX, fetching latest..."
    git -C "$PREFIX" fetch --recurse-submodules || true
else
    log "Cloning upstream redis-fuzz into $PREFIX (with submodules — this takes a while)..."
    git clone --recurse-submodules --jobs "$JOBS" "$UPSTREAM_URL" "$PREFIX"
fi
cd "$PREFIX"

# Sanity-check submodule populate
for sm in LibAFL src/redis src/redisearch; do
    if [[ -z "$(ls -A "$PREFIX/$sm" 2>/dev/null || true)" ]]; then
        warn "Submodule $sm is empty; re-running submodule init..."
        git -C "$PREFIX" submodule update --init --recursive --jobs "$JOBS" "$sm"
    fi
done

# ---------- 5. Overlay search-focused modifications --------------------------

log "Cloning overlay repo to /tmp/qa-tools..."
rm -rf /tmp/qa-tools
git clone --depth 1 --branch "$QA_REPO_BRANCH" "$QA_REPO_URL" /tmp/qa-tools

OVERLAY="/tmp/qa-tools/redis-fuzz-search"
[[ -d "$OVERLAY" ]] || die "Overlay path not found in cloned repo: $OVERLAY"

log "Overlaying modified files..."
for f in \
    defconfig.json \
    README.md \
    src/Makefile \
    src/harness.c \
    src/harness.h \
    src/lib.rs \
    src/replay.c \
    src/smith.rs
do
    if [[ -f "$OVERLAY/$f" ]]; then
        cp -v "$OVERLAY/$f" "$PREFIX/$f"
    else
        warn "Overlay file missing: $OVERLAY/$f"
    fi
done

# ---------- 6. Patches -------------------------------------------------------

log "Applying serverLog() noop patch to redis/src/server.h..."
if ! grep -q "#define serverLog(level, ...) 1" "$PREFIX/src/redis/src/server.h"; then
    cat >> "$PREFIX/src/redis/src/server.h" <<'EOF'

/* redis-fuzz-search: suppress server logging during fuzzing for ~3x speedup */
#undef serverLog
#define serverLog(level, ...) 1
EOF
else
    log "  already patched"
fi

# The boost-qvm sed must be applied AFTER RediSearch's CMake fetches the
# boost source. We try it now (no-op if path missing), and again after the
# first build attempt if the file appears.
apply_boost_patch() {
    local hits
    hits=$(find "$PREFIX/src/redisearch" -name quat_traits.hpp 2>/dev/null || true)
    if [[ -n "$hits" ]]; then
        log "Applying boost-qvm template-keyword patch to:"
        echo "$hits" | sed 's/^/    /'
        echo "$hits" | xargs sed -i 's/::template write_element_idx/::write_element_idx/g'
        return 0
    fi
    return 1
}
apply_boost_patch || log "  boost-src not yet fetched; will retry after first build attempt"

# ---------- 7. Build ---------------------------------------------------------

if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    log "SKIP_BUILD=1, stopping before make."
    log "To build manually: cd $PREFIX && make -j$JOBS"
    exit 0
fi

cd "$PREFIX"
log "Building (this can take 15-40 minutes on t3.xlarge)..."
if ! make -j"$JOBS" 2>&1 | tee /tmp/redis-fuzz-build.log; then
    warn "First build failed. Checking for boost-qvm issue..."
    if apply_boost_patch; then
        log "Re-running make after applying boost patch..."
        make -j"$JOBS" 2>&1 | tee -a /tmp/redis-fuzz-build.log \
            || die "Build still failing. Inspect /tmp/redis-fuzz-build.log"
    else
        die "Build failed and boost path not present. See /tmp/redis-fuzz-build.log"
    fi
fi

# ---------- 8. Smoke test ----------------------------------------------------

[[ -x "$PREFIX/fuzz" ]]   || die "fuzz binary not built"
[[ -x "$PREFIX/replay" ]] || die "replay binary not built"
[[ -f "$PREFIX/src/redisearch.so" ]] || die "redisearch.so not built/staged"

if [[ "${SKIP_SMOKE:-0}" != "1" ]]; then
    log "Running smoke test against the seeded idx index..."
    cd "$PREFIX"
    printf 'FT.INFO idx\nFT.SEARCH idx *\nFT.AGGREGATE idx * GROUPBY 1 @t REDUCE COUNT 0 AS n\n' \
        > /tmp/probe.cmds
    if ASAN_OPTIONS="detect_odr_violation=0:detect_leaks=0" \
       timeout 30 ./replay /tmp/probe.cmds > /tmp/probe.out 2>&1; then
        log "Smoke test OK. Replay output head:"
        head -40 /tmp/probe.out | sed 's/^/    /'
    else
        rc=$?
        warn "Smoke test exited non-zero ($rc). Replay output:"
        head -60 /tmp/probe.out | sed 's/^/    /' >&2
        warn "This is sometimes acceptable (replay always exits non-zero on EOF); inspect manually."
    fi
fi

# ---------- done -------------------------------------------------------------

cat <<EOF

============================================================
  redis-fuzz-search installed at: $PREFIX
============================================================

Run the fuzzer (N parallel workers):
    cd $PREFIX
    mkdir -p crashes corpus
    ./run 4

Replay/triage a crashing input:
    cd $PREFIX
    ./triage.py            # process ./crashes/* into ./repro/<hash>/
    ./triage.py dedup      # group repros by top stack frame

Tuning:
    edit $PREFIX/defconfig.json      (weights, blacklist, mutation probs)
    edit $PREFIX/src/smith.rs        (search_arg_override / gen_*_for biases)

Build log: /tmp/redis-fuzz-build.log
EOF
