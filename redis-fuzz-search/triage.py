#!/usr/bin/env python3
import argparse, logging, os, re, shutil, subprocess, sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

ASAN_PATTERNS = [
    r"==\d+==ERROR:\s*AddressSanitizer",
    r"\bAddressSanitizer:",
    r"\bASAN:\b",
    r"\bLeakSanitizer\b",
    r"\bUndefinedBehaviorSanitizer\b",
]
ASAN_RE = re.compile("|".join(ASAN_PATTERNS), re.IGNORECASE)
BT0_PREFIX_RE = re.compile(r"^\s*#0\b")
PATH_LINE_RE = re.compile(r"^\s*(?:/|\./|\.\./).+")

def has_sanitizer_report(text: str) -> bool:
    return bool(ASAN_RE.search(text))

def run(cmd, cwd=None, timeout=None, log=None):
    if log:
        log.info("Running: %s", " ".join(map(str, cmd)))
        log.debug("  cwd: %s", str(cwd or Path.cwd()))
        if timeout:
            log.debug("  timeout: %ss", timeout)
    try:
        env = os.environ.copy()
        env["ASAN_OPTIONS"] = "detect_odr_violation=0:detect_leaks=0"
        proc = subprocess.run(
            list(map(str, cmd)),
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
            text=True,
            env=env,
        )
        if log:
            log.debug("Exit code %s", proc.returncode)
        return proc.returncode, proc.stdout
    except subprocess.TimeoutExpired as e:
        if log:
            log.warning("Command timed out: %s", " ".join(map(str, cmd)))
        return 124, (e.stdout or "") + "\n[TIMEOUT]\n"
    except FileNotFoundError:
        if log:
            log.error("Command not found: %s", cmd[0])
        return 127, f"[ERROR] Command not found: {cmd[0]}\n"
    except Exception as e:
        if log:
            log.exception("Unexpected error running command")
        return 1, f"[ERROR] {e}\n"

def ensure_exec(path: Path):
    try:
        mode = path.stat().st_mode
        if not os.access(path, os.X_OK):
            path.chmod(mode | 0o111)
    except Exception:
        pass

def resolve_tool(p: Path, project_root: Path) -> Path:
    if p.is_absolute():
        return p
    cand = (project_root / p).resolve()
    if cand.exists():
        return cand
    cand2 = (Path.cwd() / p).resolve()
    if cand2.exists():
        return cand2
    found = shutil.which(str(p))
    return Path(found) if found else cand

def configure_logging(logfile: Path, verbose: bool):
    logfile.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("triage")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(levelname)s %(message)s")
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.debug("Log started at %s", datetime.now().isoformat())
    return logger

def extract_minimized_path(stdout: str, outdir: Path) -> Optional[Path]:
    lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if PATH_LINE_RE.match(ln):
            p = Path(ln)
            if not p.is_absolute():
                p = (Path.cwd() / p).resolve()
            return p
    default = outdir / "cmdlog.min"
    return default if default.exists() else None

def run_minimize(minimize_script: Path, cmdlog_path: Path, outdir: Path, log, timeout: int) -> Tuple[int, Path]:
    rc, out = run([minimize_script, cmdlog_path], timeout=timeout, log=log)
    produced = extract_minimized_path(out, outdir)
    target = outdir / "cmdlog.min"
    if produced and produced.exists():
        if produced.resolve() != target.resolve():
            try:
                shutil.copyfile(produced, target)
                log.info("Copied minimized file %s -> %s", produced, target)
            except Exception as e:
                log.warning("Failed to copy minimized file %s -> %s: %s", produced, target, e)
        else:
            log.info("Minimized file already at %s", target)
    else:
        target.write_text(out)
        log.info("No minimized file found; wrote minimize stdout to %s", target)
    return rc, target

def process_crash(crash_path: Path, args, log) -> bool:
    hashname = crash_path.name
    outdir = args.repro_dir / hashname
    outdir.mkdir(parents=True, exist_ok=True)

    cmdlog_path = outdir / "cmdlog"
    report_path = outdir / "report"

    if args.resume and report_path.exists():
        log.info("[%s] Skipping (report exists).", hashname)
        detected = has_sanitizer_report(report_path.read_text(errors="ignore"))
        if not detected:
            try:
                shutil.rmtree(outdir)
                log.info("[%s] Removed non-repro dir on resume: %s", hashname, outdir)
            except Exception as e:
                log.warning("[%s] Failed to delete %s: %s", hashname, outdir, e)
        return detected

    ensure_exec(args.dumpcrash_abs)
    rc, dump_out = run([args.dumpcrash_abs, crash_path],
                       timeout=args.dump_timeout, log=log)
    cmdlog_path.write_text(dump_out)
    log.info("[%s] dumpcrash exit=%d, wrote %d bytes to cmdlog",
             hashname, rc, len(dump_out))

    ensure_exec(args.replay_abs)
    rc, replay_out = run([args.replay_abs, cmdlog_path],
                         timeout=args.replay_timeout, log=log)
    report_path.write_text(replay_out)
    log.info("[%s] replay exit=%d, report bytes=%d",
             hashname, rc, len(replay_out))

    detected = has_sanitizer_report(replay_out)
    if not detected:
        try:
            shutil.rmtree(outdir)
            log.info("[%s] No sanitizer report detected; deleted %s", hashname, outdir)
        except Exception as e:
            log.warning("[%s] Failed to delete %s: %s", hashname, outdir, e)
        return False

    if args.minimize_abs.exists():
        log.info("[%s] Sanitizer report detected; running minimize (%s).",
                 hashname, args.minimize_abs)
        rc, min_path = run_minimize(args.minimize_abs, cmdlog_path, outdir, log, args.minimize_timeout)
        log.info("[%s] minimize exit=%d, minimized path=%s", hashname, rc, min_path)
    else:
        log.warning("[%s] Minimize tool not found: %s (skipping)", hashname, args.minimize_abs)

    return True

def normalise_bt(line: str) -> str:
    parts = line.strip().split()
    if len(parts) > 1:
        parts.pop(1)
    return " ".join(parts)

def first_bt_from_report(report_path: Path) -> Optional[str]:
    try:
        with report_path.open("r", errors="ignore") as fh:
            for raw in fh:
                if BT0_PREFIX_RE.match(raw):
                    return normalise_bt(raw)
    except (OSError, UnicodeDecodeError):
        return None
    return None

def count_lines(path: Path) -> int:
    try:
        with path.open("rb") as fh:
            return sum(chunk.count(b"\n") for chunk in iter(lambda: fh.read(1 << 16), b""))
    except OSError:
        return 0

def dedup_scan(repro_dir: Path) -> Tuple[List[str], Dict[str, List[Tuple[Path, int]]]]:
    mapping: Dict[str, List[Tuple[Path, int]]] = {}
    for hashdir in sorted(p for p in repro_dir.iterdir() if p.is_dir()):
        report = hashdir / "report"
        if not report.exists():
            continue
        try:
            text = report.read_text(errors="ignore")
        except OSError:
            continue
        if not has_sanitizer_report(text):
            continue
        sig = first_bt_from_report(report)
        if not sig:
            continue
        cmdlog = hashdir / "cmdlog"
        cmd_lines = count_lines(cmdlog) if cmdlog.exists() else 0
        mapping.setdefault(sig, []).append((hashdir, cmd_lines))
    patterns = sorted(mapping.keys())
    return patterns, mapping

def print_dedup_summary(patterns: List[str], mapping: Dict[str, List[Tuple[Path, int]]]):
    print(f"Found {len(patterns)} unique crash pattern(s):")
    for p in patterns:
        print(f" - {p}")
    print()
    for p in patterns:
        print(f"Pattern: {p}")
        for hashdir, cmd_count in sorted(mapping[p], key=lambda t: t[1]):
            print(f"  {hashdir} ({cmd_count} cmds)")
        print()

def parse_args_process(argv: List[str]):
    p = argparse.ArgumentParser(description="Process crashes: dumpcrash -> replay -> (optional) minimize.")
    p.add_argument("--project-root", default=".", type=Path)
    p.add_argument("--crashes-dir", default="./crashes", type=Path)
    p.add_argument("--repro-dir", default="./repro", type=Path)
    p.add_argument("--dumpcrash", default="./target/debug/dumpcrash", type=Path)
    p.add_argument("--replay", default="./replay", type=Path)
    p.add_argument("--minimize", default="./minimize.py", type=Path)
    p.add_argument("--logfile", default=None, type=Path)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--dump-timeout", type=int, default=60)
    p.add_argument("--replay-timeout", type=int, default=120)
    p.add_argument("--minimize-timeout", type=int, default=600)
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)

def parse_args_dedup(argv: List[str]):
    p = argparse.ArgumentParser(description="Deduplicate reproduced crashes by first #0 frame in reports.")
    p.add_argument("--project-root", default=".", type=Path)
    p.add_argument("--repro-dir", default="./repro", type=Path)
    p.add_argument("--logfile", default=None, type=Path)
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "dedup":
        args = parse_args_dedup(sys.argv[2:])
        project_root = args.project_root.resolve()
        args.repro_dir = (project_root / args.repro_dir).resolve() if not args.repro_dir.is_absolute() else args.repro_dir
        logfile = args.logfile or (args.repro_dir / "_dedup.log")
        logger = configure_logging(logfile, args.verbose)
        logger.info("dedup mode")
        logger.info("project-root: %s", project_root)
        logger.info("repro-dir   : %s", args.repro_dir)

        patterns, mapping = dedup_scan(args.repro_dir)
        print_dedup_summary(patterns, mapping)
        return 0

    args = parse_args_process(sys.argv[1:])
    project_root = args.project_root.resolve()
    args.crashes_dir = (project_root / args.crashes_dir).resolve() if not args.crashes_dir.is_absolute() else args.crashes_dir
    args.repro_dir = (project_root / args.repro_dir).resolve()   if not args.repro_dir.is_absolute()   else args.repro_dir
    args.dumpcrash_abs = resolve_tool(args.dumpcrash, project_root)
    args.replay_abs = resolve_tool(args.replay, project_root)
    args.minimize_abs = resolve_tool(args.minimize, project_root)

    args.repro_dir.mkdir(parents=True, exist_ok=True)
    logfile = args.logfile or (args.repro_dir / "_process.log")
    logger = configure_logging(logfile, args.verbose)

    logger.info("process mode")
    logger.info("project-root: %s", project_root)
    logger.info("crashes-dir: %s", args.crashes_dir)
    logger.info("repro-dir: %s", args.repro_dir)
    logger.info("dumpcrash: %s", args.dumpcrash_abs)
    logger.info("replay: %s", args.replay_abs)
    logger.info("minimize: %s", args.minimize_abs)
    missing = []
    for name, tool in [("dumpcrash", args.dumpcrash_abs), ("replay", args.replay_abs)]:
        if not tool.exists():
            missing.append(name)
    if missing:
        logger.error("Missing required tool(s): %s", ", ".join(missing))
        return 2

    if not args.crashes_dir.exists():
        logger.error("Crashes dir does not exist: %s", args.crashes_dir)
        return 2

    crash_files = sorted(p for p in args.crashes_dir.iterdir() if p.is_file() and not p.name.startswith("."))
    if not crash_files:
        logger.warning("No files found in %s", args.crashes_dir)

    processed = 0
    reproduced = 0

    for crash in crash_files:
        logger.info("== Processing %s ==", crash.name)
        try:
            did_repro = process_crash(crash, args, logger)
            processed += 1
            if did_repro:
                reproduced += 1
        except Exception:
            logger.exception("Failed processing %s", crash)

    summary = f"processed {processed} crash files and {reproduced} reprod"
    logger.info(summary)
    print(summary)
    return 0

if __name__ == "__main__":
    sys.exit(main())
