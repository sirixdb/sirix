#!/usr/bin/env python3
"""Record one night of the nightly benchmark tier (spec/CONV-conventions.md, CONV-303 and CONV-304).

Runs the curated set of `benchmarks.json` from the JMH jar of `:sirix-benchmarks`, in fresh
processes, and appends the figures to the results ledger as a single-arm `head` run of tier
`nightly`:

    cpu_ns_per_op        process CPU time per operation (CpuTimeProfiler)       judged
    alloc_bytes_per_op   allocation per operation, escape analysis off, no GC   judged
    ns_per_op            wall-clock time per operation, same process as the CPU  informational on a
                         figure                                                  hosted runner

CONV-304 wants at least 20 accepted fresh processes per benchmark for a night to count. The driver
therefore works process by process: every benchmark of the set once, then the allocation legs of the
set, then the next process. A time budget that ends the run early leaves every series with the same
number of processes, never one series complete and another empty.

    nightly_run.py --jar JAR --java JAVA --ledger FILE --night YYYY-MM-DD --out DIR
                   [--commit SHA] [--runner kind=hosted,provider=github,label=ubuntu-24.04,image=TEXT]
                   [--manifest FILE] [--processes 20] [--budget-seconds N]

Exit status: 0 every series has its processes, 4 at least one series is short of them (the rows that
were measured are recorded all the same), 5 the night is already in the ledger, 2 invalid input.
Standard library only.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import bench_ledger  # noqa: E402
from bench_ledger import LedgerError  # noqa: E402

MANIFEST_SCHEMA = 1
KIT = "jmh-nightly"
INCOMPLETE = 4
NIGHT_RECORDED = 5
DEFAULT_MANIFEST = Path(__file__).resolve().with_name("benchmarks.json")
CPU_PROFILER = "io.sirix.benchmark.CpuTimeProfiler"

# What the project's own Gradle `jmh` task gives its forked JVMs (bundles/sirix-benchmarks/build.gradle).
COMMON_JVM_ARGS = (
    "--add-modules=jdk.incubator.vector",
    "--enable-preview",
    "--enable-native-access=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
)
# CONV-302 allocation leg: escape analysis off, the Graal JIT off (HotSpot C2), a collector that never
# collects. A JVM without JVMCI ignores the unknown flag; it is recorded so the importer can check it.
ALLOCATION_JVM_ARGS = (
    "-XX:+IgnoreUnrecognizedVMOptions",
    "-XX:+UnlockExperimentalVMOptions",
    "-XX:+UseEpsilonGC",
    "-XX:-DoEscapeAnalysis",
    "-XX:-UseJVMCICompiler",
)

_BENCHMARK = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_VALUE = re.compile(r"^[A-Za-z0-9_.+-]+$")
_HEAP = re.compile(r"^[1-9][0-9]*[mMgG]$")
_NIGHT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class RunError(ValueError):
    """The manifest or the arguments cannot start a nightly run."""


class NightRecorded(RunError):
    """The ledger already holds accepted measurements for this night."""


@dataclass(frozen=True)
class Entry:
    """One series of the set: a benchmark method, its parameters and its allocation batch."""

    benchmark: str  # Class.method, the name the ledger records
    params: tuple[tuple[str, str], ...]
    batch: int  # method calls per operation of the single-shot allocation leg
    allocation: bool = True  # False for a benchmark whose allocation is below what the profiler can resolve

    @property
    def series(self) -> int:
        """The ledger series one process of this entry contributes: wall-clock, CPU time and, if measured, allocation."""
        return 3 if self.allocation else 2

    @property
    def include(self) -> str:
        """The JMH include regex that selects exactly this benchmark method."""
        return r"\." + self.benchmark.replace(".", r"\.") + "$"


@dataclass(frozen=True)
class Manifest:
    warmup_iterations: int
    measurement_iterations: int
    iteration_seconds: int
    allocation_warmup_iterations: int
    allocation_iterations: int
    allocation_heap: str
    timed_jvm_args: tuple[str, ...]
    entries: tuple[Entry, ...]

    @property
    def names(self) -> list[str]:
        return sorted({entry.benchmark for entry in self.entries})


def _positive(document: Mapping[str, Any], name: str) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RunError(f"manifest: '{name}' must be a positive integer, got {value!r}")
    return value


def parse_manifest(document: Any) -> Manifest:
    """A manifest as an object, refused whole when anything in it is unsafe to pass to a command line."""
    if not isinstance(document, dict) or document.get("schema") != MANIFEST_SCHEMA:
        raise RunError(f"manifest: an object with schema {MANIFEST_SCHEMA} is required")
    heap = document.get("allocation_heap")
    if not isinstance(heap, str) or not _HEAP.match(heap):
        raise RunError(f"manifest: 'allocation_heap' must be a size such as 3g, got {heap!r}")
    jvm_args = document.get("timed_jvm_args", [])
    if not isinstance(jvm_args, list) or not all(isinstance(arg, str) and arg.startswith("-X") for arg in jvm_args):
        raise RunError("manifest: 'timed_jvm_args' must be a list of -X options")
    raw_entries = document.get("benchmarks")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise RunError("manifest: 'benchmarks' must be a non-empty list")
    entries: list[Entry] = []
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    for raw in raw_entries:
        name = raw.get("benchmark") if isinstance(raw, dict) else None
        if not isinstance(name, str) or not _BENCHMARK.match(name):
            raise RunError(f"manifest: a benchmark entry names Class.method, got {raw!r}")
        params = raw.get("params", {})
        if not isinstance(params, dict) or not all(
            isinstance(name, str) and _VALUE.match(name) and isinstance(value, str) and _VALUE.match(value)
            for name, value in params.items()
        ):
            raise RunError(f"manifest: {raw['benchmark']}: 'params' maps names to single plain values")
        batch = _positive(raw, "batch")
        allocation = raw.get("allocation", True)
        if not isinstance(allocation, bool):
            raise RunError(f"manifest: {raw['benchmark']}: 'allocation' is true or false, got {allocation!r}")
        identity = (raw["benchmark"], tuple(sorted(params.items())))
        if identity in seen:
            raise RunError(f"manifest: {raw['benchmark']} {dict(params)} is listed twice")
        seen.add(identity)
        entries.append(Entry(raw["benchmark"], identity[1], batch, allocation))
    return Manifest(
        warmup_iterations=_positive(document, "warmup_iterations"),
        measurement_iterations=_positive(document, "measurement_iterations"),
        iteration_seconds=_positive(document, "iteration_seconds"),
        allocation_warmup_iterations=_positive(document, "allocation_warmup_iterations"),
        allocation_iterations=_positive(document, "allocation_iterations"),
        allocation_heap=heap,
        timed_jvm_args=tuple(jvm_args),
        entries=tuple(entries),
    )


def load_manifest(path: Path) -> Manifest:
    try:
        return parse_manifest(json.loads(path.read_text(encoding="utf-8")))
    except OSError as error:
        raise RunError(f"manifest {path}: {error.strerror or type(error).__name__}") from error
    except json.JSONDecodeError as error:
        raise RunError(f"manifest {path}: not JSON: {error.msg}") from error


def _jmh_command(java: str, jar: Path, entry: Entry, output: Path, tail: Sequence[str], jvm_args: Sequence[str]) -> list[str]:
    command = [java, *COMMON_JVM_ARGS, "-jar", str(jar), entry.include, "-f", "1"]
    for name, value in entry.params:
        command += ["-p", f"{name}={value}"]
    command += list(tail)
    command += ["-rf", "json", "-rff", str(output), "-jvmArgs", " ".join([*COMMON_JVM_ARGS, *jvm_args])]
    return command


def timed_command(manifest: Manifest, entry: Entry, java: str, jar: Path, output: Path) -> list[str]:
    """One fresh process of a time leg: wall-clock average time in ns with the CPU profiler attached."""
    seconds = f"{manifest.iteration_seconds}s"
    tail = [
        "-bm", "avgt", "-tu", "ns",
        "-wi", str(manifest.warmup_iterations), "-w", seconds,
        "-i", str(manifest.measurement_iterations), "-r", seconds,
        "-prof", CPU_PROFILER,
    ]  # fmt: skip
    return _jmh_command(java, jar, entry, output, tail, manifest.timed_jvm_args)


def allocation_command(manifest: Manifest, entry: Entry, java: str, jar: Path, output: Path) -> list[str]:
    """One fresh process of an allocation leg (CONV-302): single-shot batches, the GC profiler, no collection."""
    tail = [
        "-bm", "ss", "-bs", str(entry.batch),
        "-wi", str(manifest.allocation_warmup_iterations),
        "-i", str(manifest.allocation_iterations),
        "-prof", "gc",
    ]  # fmt: skip
    jvm_args = [f"-Xmx{manifest.allocation_heap}", *ALLOCATION_JVM_ARGS]
    return _jmh_command(java, jar, entry, output, tail, jvm_args)


Execute = Callable[[Sequence[str], Path], int]


def execute_process(command: Sequence[str], log: Path) -> int:
    """Run a command with its output in a log file; the return code, or 124 when it cannot start."""
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("wb") as sink:
        try:
            return subprocess.run(list(command), stdout=sink, stderr=subprocess.STDOUT, check=False).returncode
        except OSError as error:
            sink.write(f"cannot start {command[0]}: {error}\n".encode())
            return 124


def night_recorded(ledger: Path, night: str) -> bool:
    """True when the ledger already holds a nightly run for the night that has accepted measurements."""
    if not ledger.exists():
        return False
    rows = bench_ledger.read_ledger(ledger)
    runs = {row["run"] for row in rows if row["kind"] == "run" and row["tier"] == "nightly" and row["night"] == night}
    return any(row["kind"] == "measurement" and row["run"] in runs and row["accepted"] for row in rows)


@dataclass
class Summary:
    night: str
    commit: str
    run: str
    processes_wanted: int
    processes_completed: int
    failures: list[str]
    seconds: float
    series_short: list[str]
    stopped_for_budget: bool

    @property
    def complete(self) -> bool:
        return not self.series_short


def _series_counts(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, tuple[tuple[str, str], ...], str], int]:
    counts: dict[tuple[str, tuple[tuple[str, str], ...], str], int] = {}
    for row in rows:
        if row["accepted"]:
            key = (row["benchmark"], tuple(sorted(row["params"].items())), row["metric"])
            counts[key] = counts.get(key, 0) + 1
    return counts


def run_night(
    manifest: Manifest,
    *,
    jar: Path,
    java: str,
    ledger: Path,
    night: str,
    commit: str,
    out: Path,
    runner_spec: str | None = None,
    processes: int = bench_ledger.NIGHTLY_MIN_PROCESSES,
    budget_seconds: float | None = None,
    conditions: Path | None = None,
    execute: Execute = execute_process,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
    capture: Mapping[str, Any] | None = None,
) -> Summary:
    """Measure the set `processes` times, record every figure, append the run to the ledger.

    `capture` hands bench_ledger.begin the machine to read (root, runner, now) in tests.
    """
    if not _NIGHT.match(night) or not bench_ledger._is_night(night):  # noqa: SLF001 - one definition of a night
        raise RunError(f"--night must be a real calendar date YYYY-MM-DD, got {night!r}")
    if not _COMMIT.match(commit):
        raise RunError(f"--commit must be 40 lowercase hexadecimal digits, got {commit!r}")
    if processes < bench_ledger.NIGHTLY_MIN_PROCESSES:
        raise RunError(
            f"a night needs at least {bench_ledger.NIGHTLY_MIN_PROCESSES} fresh processes (CONV-304), got {processes}"
        )
    if night_recorded(ledger, night):
        raise NightRecorded(f"night {night} is already in {ledger}; two runs of one night make the trend analysis fail")
    run_dir = out / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    started = clock()
    run = bench_ledger.begin(
        run_dir,
        tier="nightly",
        kit=KIT,
        arm_specs=[f"head,commit={commit},binary=jvm,java={java}"],
        night=night,
        benchmarks=manifest.names,
        ledger=ledger,
        conditions=conditions,
        runner_spec=runner_spec,
        **dict(capture or {}),
    )
    failures: list[str] = []
    completed = 0
    stopped = False
    longest = 0.0
    for process in range(1, processes + 1):
        if budget_seconds is not None and process > 1 and (clock() - started) + longest > budget_seconds:
            stopped = True
            log(f"time budget {budget_seconds:.0f}s reached after {completed} process(es); stopping")
            break
        process_started = clock()
        for index, entry in enumerate(manifest.entries, start=1):
            legs = [("timed", timed_command, {"cpu_leg": True})]
            if entry.allocation:
                legs.append(("alloc", allocation_command, {"allocation_leg": True}))
            for leg, build, options in legs:
                result = out / "jmh" / f"p{process:02d}-e{index:02d}-{leg}.json"
                command = build(manifest, entry, java, jar, result)
                code = execute(command, result.with_suffix(".log"))
                label = f"process {process} {entry.benchmark}{dict(entry.params)} {leg}"
                if code != 0:
                    failures.append(f"{label}: exit {code}")
                    log(f"FAILED {failures[-1]}")
                    continue
                try:
                    rows = bench_ledger.jmh_rows(
                        json.loads(result.read_text(encoding="utf-8")),
                        arm=bench_ledger.HEAD,
                        pair=None,
                        position=None,
                        process=process,
                        source=result.name,
                        **options,
                    )
                    bench_ledger.record(run_dir, rows)
                except (OSError, json.JSONDecodeError, LedgerError) as error:
                    failures.append(f"{label}: {error}")
                    log(f"FAILED {failures[-1]}")
        completed = process
        longest = max(longest, clock() - process_started)
        log(f"process {process}/{processes} done in {clock() - process_started:.0f}s")
        if budget_seconds is not None and process == 1 and longest * processes > budget_seconds:
            stopped = True
            log(f"{processes} processes need ~{longest * processes:.0f}s but the budget is {budget_seconds:.0f}s")
            break
    bench_ledger.commit(run_dir, ledger)
    counts = _series_counts(bench_ledger.load_measurements(run_dir))
    short = sorted(
        f"{benchmark}{dict(params)} {metric}: {count}/{bench_ledger.NIGHTLY_MIN_PROCESSES}"
        for (benchmark, params, metric), count in counts.items()
        if count < bench_ledger.NIGHTLY_MIN_PROCESSES
    )
    expected = sum(entry.series for entry in manifest.entries)
    if len(counts) < expected:
        short.append(f"{expected - len(counts)} series have no accepted measurement at all")
    summary = Summary(night, commit, run["run"], processes, completed, failures, clock() - started, short, stopped)
    (out / "summary.json").write_text(
        json.dumps({**summary.__dict__, "complete": summary.complete}, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def _git_head() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise RunError(f"--commit was not given and git cannot name HEAD: {error}") from error


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record one night of the nightly benchmark tier.")
    parser.add_argument("--jar", required=True, type=Path, help="the :sirix-benchmarks JMH jar")
    parser.add_argument("--java", required=True, help="the java launcher of the runtime that runs it")
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--night", help="YYYY-MM-DD; default: today's UTC date")
    parser.add_argument("--out", required=True, type=Path, help="a fresh directory for the run and the JMH output")
    parser.add_argument("--commit", help="the 40-digit commit under test; default: git HEAD")
    parser.add_argument("--runner", metavar="SPEC", help="kind=hosted,provider=github,label=ubuntu-24.04,image=TEXT")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--processes", type=int, default=bench_ledger.NIGHTLY_MIN_PROCESSES)
    parser.add_argument("--budget-seconds", type=float)
    parser.add_argument("--conditions", type=Path)
    args = parser.parse_args(argv)
    try:
        summary = run_night(
            load_manifest(args.manifest),
            jar=args.jar,
            java=args.java,
            ledger=args.ledger,
            night=args.night or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
            commit=args.commit or _git_head(),
            out=args.out,
            runner_spec=args.runner,
            processes=args.processes,
            budget_seconds=args.budget_seconds,
            conditions=args.conditions,
        )
    except NightRecorded as error:
        print(f"nightly_run.py: {error}", file=sys.stderr)
        return NIGHT_RECORDED
    except (RunError, LedgerError, OSError) as error:
        print(f"nightly_run.py: {error}", file=sys.stderr)
        return 2
    print(
        f"night {summary.night} commit {summary.commit[:12]}: {summary.processes_completed}/{summary.processes_wanted} "
        f"process(es) in {summary.seconds:.0f}s, {len(summary.failures)} failure(s)"
    )
    for item in summary.series_short:
        print(f"short: {item}")
    return 0 if summary.complete else INCOMPLETE


if __name__ == "__main__":
    raise SystemExit(main())
