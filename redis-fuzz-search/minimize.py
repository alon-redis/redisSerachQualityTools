#!/usr/bin/env python3

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Sequence

_SANITIZER_BANNERS = (
    re.compile(r"ERROR: (?:Address|UndefinedBehavior|Thread|Memory|Leak)Sanitizer"),
    re.compile(r"runtime error: "),
)

def _did_crash(stderr: str) -> bool:
    return any(p.search(stderr) for p in _SANITIZER_BANNERS)

def _run_replay(binary: Path, cmd_lines: Sequence[str], timeout: int = 60) -> bool:
    bin_path = str(binary.expanduser().resolve())
    with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
        tmp.writelines(cmd_lines)
        tmp_path = Path(tmp.name)
    try:
        env = os.environ.copy()
        env["ASAN_OPTIONS"] = "detect_odr_violation=0:detect_leaks=0"
        proc = subprocess.run(
            [bin_path, str(tmp_path)],
            text=True,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        proc = None
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass

    if proc is None:
        return False
    return _did_crash(proc.stderr)

def _chunk_sizes(n: int) -> List[int]:
    sizes: List[int] = []
    k = n // 2
    while k:
        sizes.append(k)
        k //= 2
    sizes.append(1)
    deduped: List[int] = []
    for s in sizes:
        if s not in deduped:
            deduped.append(s)
    return deduped


def minimise(binary: Path, lines: List[str]) -> List[str]:
    if not _run_replay(binary, lines):
        raise RuntimeError("Initial command list does not cause a crash")

    chunk_sizes = _chunk_sizes(len(lines))

    for chunk in chunk_sizes:
        i = 0
        while i < len(lines):
            candidate = lines[:i] + lines[i + chunk :]
            if not candidate:
                i += chunk
                continue
            if _run_replay(binary, candidate):
                lines = candidate
            else:
                i += chunk
    return lines

_DEF_OUT_SUFFIX = ".min"

def _parse_args(argv: List[str] | None = None):
    p = argparse.ArgumentParser(description="Greedy command‑list minimiser")
    p.add_argument("cmdlist", type=Path, help="Path to the original command list file")
    p.add_argument("-o", "--output", type=Path, help="Where to write the minimized list")
    p.add_argument("--binary", type=Path, default=Path("./replay"), help="Path to the replay binary")
    p.add_argument(
        "--timeout", type=int, default=60, help="Seconds to wait for replay before considering it a hang",
    )
    return p.parse_args(argv)

def main(argv: List[str] | None = None):
    args = _parse_args(argv)

    if not args.cmdlist.is_file():
        sys.exit(f"No cmdlist file: {args.cmdlist}")
    if not os.access(args.binary, os.X_OK):
        sys.exit(f"Replay binary not found or not executable: {args.binary}")

    with args.cmdlist.open() as f:
        original_lines = f.readlines()
    minimized = minimise(args.binary, original_lines)

    out_path = args.output or args.cmdlist.with_suffix(args.cmdlist.suffix + _DEF_OUT_SUFFIX)
    with out_path.open("w") as f:
        f.writelines(minimized)
    print(
        f"Minimization complete: {len(original_lines)} -> {len(minimized)} lines (-{len(original_lines)-len(minimized)})",
        file=sys.stderr,
    )
    print(out_path)

if __name__ == "__main__":
    main()
