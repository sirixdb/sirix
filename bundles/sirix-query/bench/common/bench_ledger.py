#!/usr/bin/env python3
"""Results ledger, environment capture and conditions record of the benchmark protocol
(spec/CONV-conventions.md, CONV-303), and the run directory a timed run is recorded through.

    bench_ledger.py capture --out DIR [--data PATH] [--java NAME=PATH ...]
        Write DIR/environment.json. No ledger is needed; a kit without one still captures.
    bench_ledger.py fingerprint
        Print the fingerprint of this machine's environment and the fields it is made of.
    bench_ledger.py begin --out DIR --tier gate|control|nightly --kit NAME --arm SPEC [--arm SPEC]
                          [--data PATH] [--night YYYY-MM-DD] [--min-pairs N] [--benchmark NAME ...]
        Open a run: capture the environment, find its conditions entry, write DIR/run.json.
        Refuses, before any timed process, when no conditions entry holds the fingerprint.
        SPEC is name,commit=<40 hex>,binary=jvm|native|native-pgo[,dirty=true][,java=PATH|,runtime=TEXT].
    bench_ledger.py record --run DIR --benchmark NAME --metric NAME --unit UNIT --arm ARM --value V ...
    bench_ledger.py import-jmh --run DIR --json FILE --arm ARM --process N [--pair N --position P | --allocation-leg]
    bench_ledger.py import-clickbench --run DIR --json FILE --arm ARM --process N [--pair N --position P] [--log FILE]
    bench_ledger.py import-jsonbench --run DIR --json FILE --query N --attempt N --process N [--log FILE]
        Add measurement rows to DIR/measurements.jsonl.
    bench_ledger.py stop --run DIR
        Print the stop-rule state; the last line is `stop` or `continue` (CONV-302).
    bench_ledger.py judge --run DIR
        Judge the run once, append everything to the ledger, then print the verdicts.
    bench_ledger.py reject --run DIR --reason TEXT
        Mark every measurement of a run that is not in the ledger yet as rejected (the whole run is
        no evidence, for instance because its answers were wrong). It cannot be undone.
    bench_ledger.py commit --run DIR
        Append the run's rows to the ledger without judging it (single-arm, rejected or aborted runs).
    bench_ledger.py report --run DIR
        Print the verdicts of a run that has been judged and recorded.

The ledger is the file SIRIX_BENCH_LEDGER names: JSON Lines, append-only. The conditions record is
docs/bench/CONDITIONS.md of this checkout (SIRIX_BENCH_CONDITIONS overrides the path). Standard
library only.
"""

from __future__ import annotations

import argparse
import datetime
import fcntl
import hashlib
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import timed_gate
from timed_gate import GateError

SCHEMA = 1
LEDGER_ENV = "SIRIX_BENCH_LEDGER"
CONDITIONS_ENV = "SIRIX_BENCH_CONDITIONS"
TIERS = ("gate", "control", "nightly")
HEAD = "head"
PAIRED_ARMS = (timed_gate.BASELINE, timed_gate.CANDIDATE)
BINARIES = ("jvm", "native", "native-pgo")
NIGHTLY_MIN_PROCESSES = 20
QUANTILE = timed_gate.QUANTILE_PERCENT / 100

ENVIRONMENT_FILE = "environment.json"
RUN_FILE = "run.json"
MEASUREMENTS_FILE = "measurements.jsonl"
VERDICTS_FILE = "verdicts.jsonl"
RECORDED_FILE = "ledger-recorded.json"

CONDITION_ROWS = (
    "Machine",
    "CPU",
    "Memory",
    "Storage",
    "Firmware",
    "Kernel and OS",
    "Frequency policy",
    "Runtimes",
    "Load control",
    "Benchmark set",
    "Changed",
    "Comparable with",
    "Fingerprint",
)
_HEADING = re.compile(r"^## (\d{4}-\d{2}-\d{2}) — ([a-z0-9]+(?:-[a-z0-9]+)*) \((new baseline|continues)\)$")
_RUN_ID = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_NIGHT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SERVED = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)=(-?\d+)\b")

Runner = Callable[[Sequence[str]], tuple[int, str, str]]


class LedgerError(ValueError):
    """The run cannot be recorded as asked; nothing was appended."""


# ---------------------------------------------------------------------------------------------
# environment capture
# ---------------------------------------------------------------------------------------------


def unavailable(reason: str) -> dict[str, str]:
    """A probe that could not be read: recorded with its reason, never omitted and never guessed."""
    return {"unavailable": reason}


def _system_runner(argv: Sequence[str]) -> tuple[int, str, str]:
    completed = subprocess.run(
        list(argv), check=False, capture_output=True, text=True, timeout=20, stdin=subprocess.DEVNULL
    )
    return completed.returncode, completed.stdout, completed.stderr


def _read(root: Path, path: str) -> str | dict[str, str]:
    try:
        return (root / path.lstrip("/")).read_text(encoding="utf-8", errors="replace").strip()
    except OSError as error:
        return unavailable(f"{path}: {error.strerror or type(error).__name__}")


def _command(runner: Runner, argv: Sequence[str], *, stderr: bool = False) -> str | dict[str, str]:
    name = Path(argv[0]).name
    try:
        status, out, err = runner(argv)
    except FileNotFoundError:
        return unavailable(f"{name}: command not found")
    except (OSError, subprocess.SubprocessError) as error:
        return unavailable(f"{name}: {type(error).__name__}")
    text = (err if stderr else out).strip()
    if status != 0:
        return unavailable(f"{name}: exit status {status}")
    return text if text else unavailable(f"{name}: no output")


def _os_name(root: Path) -> str | dict[str, str]:
    text = _read(root, "/etc/os-release")
    if isinstance(text, dict):
        return text
    for line in text.splitlines():
        if line.startswith("PRETTY_NAME="):
            return line.split("=", 1)[1].strip().strip('"')
    return unavailable("/etc/os-release: no PRETTY_NAME")


def _cpu(root: Path, runner: Runner) -> dict[str, Any]:
    text = _read(root, "/proc/cpuinfo")
    lscpu = _command(runner, ["lscpu"])
    if isinstance(text, dict):
        return {"model": text, "sockets": text, "cores": text, "threads": text, "lscpu": lscpu}
    model: str | dict[str, str] = unavailable("/proc/cpuinfo: no model name")
    threads, sockets, cores = 0, set(), set()
    physical = core = None
    for line in text.splitlines() + [""]:
        name, _, value = (part.strip() for part in line.partition(":"))
        if name == "processor":
            threads += 1
        elif name == "model name" and isinstance(model, dict):
            model = value
        elif name == "physical id":
            physical = value
        elif name == "core id":
            core = value
        elif not line.strip():
            if physical is not None:
                sockets.add(physical)
                cores.add((physical, core))
            physical = core = None
    missing = unavailable("/proc/cpuinfo: no physical id")
    return {
        "model": model,
        "sockets": len(sockets) if sockets else missing,
        "cores": len(cores) if cores else missing,
        "threads": threads,
        "lscpu": lscpu,
    }


def _cpufreq(root: Path) -> dict[str, Any]:
    base = root / "sys/devices/system/cpu/cpufreq"
    try:
        policies = sorted(
            (entry.name for entry in base.iterdir() if entry.name.startswith("policy")),
            key=lambda name: (len(name), name),
        )
    except OSError as error:
        return unavailable(f"/sys/devices/system/cpu/cpufreq: {error.strerror or type(error).__name__}")
    if not policies:
        return unavailable("/sys/devices/system/cpu/cpufreq: no policy")
    files = (
        ("driver", "scaling_driver"),
        ("governor", "scaling_governor"),
        ("min_khz", "scaling_min_freq"),
        ("max_khz", "scaling_max_freq"),
        ("energy_performance_preference", "energy_performance_preference"),
    )
    return {
        policy: {field: _read(root, f"/sys/devices/system/cpu/cpufreq/{policy}/{name}") for field, name in files}
        for policy in policies
    }


def _powercap(root: Path) -> dict[str, Any]:
    base = root / "sys/class/powercap"
    try:
        zones = sorted(entry.name for entry in base.iterdir())
    except OSError as error:
        return unavailable(f"/sys/class/powercap: {error.strerror or type(error).__name__}")
    limits = {}
    for zone in zones:
        try:
            names = sorted(
                entry.name
                for entry in (base / zone).iterdir()
                if re.fullmatch(r"constraint_\d+_power_limit_uw", entry.name)
            )
        except OSError:
            continue
        for name in names:
            limits[f"{zone}/{name}"] = _read(root, f"/sys/class/powercap/{zone}/{name}")
    return limits if limits else unavailable("/sys/class/powercap: no power limit")


def _memory_kb(root: Path) -> int | dict[str, str]:
    text = _read(root, "/proc/meminfo")
    if isinstance(text, dict):
        return text
    match = re.search(r"^MemTotal:\s+(\d+) kB$", text, re.MULTILINE)
    return int(match.group(1)) if match else unavailable("/proc/meminfo: no MemTotal")


def _allowed_cpus(root: Path) -> str | dict[str, str]:
    text = _read(root, "/proc/self/status")
    if isinstance(text, dict):
        return text
    match = re.search(r"^Cpus_allowed_list:\s+(\S+)$", text, re.MULTILINE)
    return match.group(1) if match else unavailable("/proc/self/status: no Cpus_allowed_list")


def _filesystem(root: Path, data: Path | None) -> str | dict[str, str]:
    if data is None:
        return unavailable("no benchmark data path was given")
    text = _read(root, "/proc/self/mountinfo")
    if isinstance(text, dict):
        return text
    try:
        target = data.resolve()
    except (OSError, RuntimeError) as error:  # a symbolic-link loop is a RuntimeError before Python 3.13
        return unavailable(f"benchmark data path: {type(error).__name__}")
    best: tuple[int, str] | None = None
    for line in text.splitlines():
        left, separator, right = line.partition(" - ")
        fields = left.split()
        if not separator or len(fields) < 5 or not right.split():
            continue
        mount = Path(fields[4].replace("\\040", " "))
        if target == mount or mount in target.parents:
            if best is None or len(mount.parts) >= best[0]:
                best = (len(mount.parts), right.split()[0])
    return best[1] if best else unavailable("/proc/self/mountinfo: no mount holds the benchmark data path")


def capture_environment(
    *,
    data: Path | None = None,
    javas: Mapping[str, str] | None = None,
    runtimes: Mapping[str, str] | None = None,
    root: Path = Path("/"),
    runner: Runner = _system_runner,
    now: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Everything CONV-303 asks a run to record about the machine, read before the first timed process.

    javas maps an arm to the java launcher it runs on, whose `-version` output is captured; runtimes
    maps an arm to a version text the caller already knows. Nothing that names the host, the user or
    a home directory is recorded: conditions are published.
    """
    stamp = (now or datetime.datetime.now(datetime.timezone.utc)).astimezone(datetime.timezone.utc)
    versions: dict[str, Any] = dict(runtimes or {})
    for arm, java in (javas or {}).items():
        versions[arm] = _command(runner, [java, "-version"], stderr=True)
    turbo = {
        "intel_pstate_no_turbo": _read(root, "/sys/devices/system/cpu/intel_pstate/no_turbo"),
        "cpufreq_boost": _read(root, "/sys/devices/system/cpu/cpufreq/boost"),
    }
    return {
        "captured_at": stamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "kernel_release": _read(root, "/proc/sys/kernel/osrelease"),
        "os": _os_name(root),
        "architecture": os.uname().machine,
        "firmware": {
            "vendor": _read(root, "/sys/class/dmi/id/bios_vendor"),
            "version": _read(root, "/sys/class/dmi/id/bios_version"),
            "date": _read(root, "/sys/class/dmi/id/bios_date"),
        },
        "product_name": _read(root, "/sys/class/dmi/id/product_name"),
        "cpu": _cpu(root, runner),
        "cpufreq": _cpufreq(root),
        "turbo": turbo,
        "powercap": _powercap(root),
        "cpupower_frequency_info": _command(runner, ["cpupower", "frequency-info"]),
        "sensors": _command(runner, ["sensors"]),
        "memory_total_kb": _memory_kb(root),
        "allowed_cpus": _allowed_cpus(root),
        "runtimes": versions,
        "data_filesystem": _filesystem(root, data),
    }


def _distinct(cpufreq: Mapping[str, Mapping[str, Any]], field: str) -> list[str]:
    """The distinct values of one field over all cpufreq policies; an unreadable one keeps its reason."""
    values = (policy[field] for policy in cpufreq.values())
    return sorted({value if isinstance(value, str) else json.dumps(value) for value in values})


def fingerprint_fields(environment: Mapping[str, Any]) -> dict[str, Any]:
    """The part of a capture that identifies a condition; readings that vary within one are left out."""
    cpufreq = environment["cpufreq"]
    unread = "unavailable" in cpufreq
    memory = environment["memory_total_kb"]
    return {
        "kernel_release": environment["kernel_release"],
        "os": environment["os"],
        "architecture": environment["architecture"],
        "firmware_version": environment["firmware"]["version"],
        "product_name": environment["product_name"],
        "cpu_model": environment["cpu"]["model"],
        "logical_cpus": environment["cpu"]["threads"],
        "cpufreq_drivers": cpufreq if unread else _distinct(cpufreq, "driver"),
        "governors": cpufreq if unread else _distinct(cpufreq, "governor"),
        "turbo": environment["turbo"],
        "memory_gib": round(memory / (1024 * 1024)) if isinstance(memory, int) else memory,
    }


def fingerprint(environment: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical JSON of the fingerprint fields."""
    canonical = json.dumps(fingerprint_fields(environment), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------------------------
# conditions record
# ---------------------------------------------------------------------------------------------


def default_conditions_path() -> Path:
    override = os.environ.get(CONDITIONS_ENV)
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[4] / "docs" / "bench" / "CONDITIONS.md"


def _condition_entries(text: str) -> list[dict[str, Any]]:
    """The headings and table rows of a conditions record, before any cross-entry check."""
    entries: list[dict[str, Any]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if line.startswith("## "):
            match = _HEADING.match(line)
            if not match:
                raise LedgerError(
                    f"conditions line {number}: an entry heading is "
                    f"'## YYYY-MM-DD — <id> (new baseline)' or '(continues)', got {line!r}"
                )
            entries.append({"date": match.group(1), "id": match.group(2), "kind": match.group(3), "rows": {}})
        elif entries and line.startswith("|"):
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            rows = entries[-1]["rows"]
            if len(cells) >= 2 and cells[0] in CONDITION_ROWS:
                if cells[0] in rows:
                    raise LedgerError(
                        f"conditions entry {entries[-1]['date']} {entries[-1]['id']}: row '{cells[0]}' occurs twice"
                    )
                rows[cells[0]] = " | ".join(cells[1:]).strip()
    return entries


def _entry_fingerprints(entry: Mapping[str, Any], name: str) -> list[str]:
    cell = entry["rows"]["Fingerprint"]
    prints = re.findall(r"[0-9a-f]{64}", cell)
    if not prints or re.sub(r"[0-9a-f]{64}|[\s,`]", "", cell):
        raise LedgerError(f"{name}: the Fingerprint row holds 64-digit lowercase hexadecimal fingerprints only")
    return prints


def parse_conditions(text: str) -> list[dict[str, Any]]:
    """The entries of a conditions record, in order; a malformed record is refused whole."""
    entries = _condition_entries(text)
    started: set[str] = set()
    owners: dict[str, str] = {}
    previous_date = ""
    for entry in entries:
        name = f"conditions entry {entry['date']} {entry['id']}"
        try:
            datetime.date.fromisoformat(entry["date"])
        except ValueError as error:
            raise LedgerError(f"{name}: not a calendar date") from error
        if entry["date"] < previous_date:
            raise LedgerError(f"{name}: entries must be appended in date order")
        previous_date = entry["date"]
        missing = [row for row in CONDITION_ROWS if not entry["rows"].get(row)]
        if missing:
            raise LedgerError(f"{name}: missing or empty row(s): {', '.join(missing)}")
        if entry["kind"] == "new baseline" and entry["id"] in started:
            raise LedgerError(f"{name}: a new baseline needs a new id; '{entry['id']}' already exists")
        if entry["kind"] == "continues" and entry["id"] not in started:
            raise LedgerError(f"{name}: continues a condition that no earlier entry started")
        started.add(entry["id"])
        entry["fingerprints"] = _entry_fingerprints(entry, name)
        for value in entry["fingerprints"]:
            if owners.setdefault(value, entry["id"]) != entry["id"]:
                raise LedgerError(f"{name}: fingerprint {value[:12]} already belongs to condition '{owners[value]}'")
    return entries


def conditions_id(conditions: Path, value: str) -> str | None:
    """The id of the conditions entry that holds this fingerprint, or None."""
    try:
        text = conditions.read_text(encoding="utf-8")
    except OSError as error:
        raise LedgerError(f"conditions record {conditions}: {error.strerror or type(error).__name__}") from error
    for entry in parse_conditions(text):
        if value in entry["fingerprints"]:
            return entry["id"]
    return None


def _field_differences(ledger: Path, environment: Mapping[str, Any]) -> str:
    previous = None
    if ledger.exists():
        for row in read_ledger(ledger):
            if row["kind"] == "run":
                previous = row
    if previous is None:
        return "the ledger holds no earlier run to compare with"
    before, now = fingerprint_fields(previous["environment"]), fingerprint_fields(environment)
    changed = [
        f"{name}: {json.dumps(before.get(name))} -> {json.dumps(now[name])}"
        for name in now
        if before.get(name) != now[name]
    ]
    return (
        "differs from the previous run in the ledger (" + previous["run"] + ") in " + "; ".join(changed)
        if changed
        else f"matches the previous run in the ledger ({previous['run']}), whose entry is gone"
    )


# ---------------------------------------------------------------------------------------------
# rows and the ledger file
# ---------------------------------------------------------------------------------------------


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _is_text(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def _is_params(value: Any) -> bool:
    return isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items())


def _check(condition: bool, row: Mapping[str, Any], message: str) -> None:
    if not condition:
        raise LedgerError(f"{row.get('kind', 'row')} row of run {row.get('run')!r}: {message}")


def _validate_arms(row: Mapping[str, Any]) -> None:
    arms = row.get("arms")
    _check(isinstance(arms, dict) and bool(arms), row, "'arms' must be an object")
    names = tuple(sorted(arms))
    paired, single = names == tuple(sorted(PAIRED_ARMS)), names == (HEAD,)
    _check(paired or single, row, "the arms are baseline and candidate, or head")
    _check(not (row["tier"] == "gate" and not paired), row, "a gate run compares baseline and candidate")
    _check(not (row["tier"] == "nightly" and not single), row, "a nightly run measures the single arm head")
    for name, arm in arms.items():
        _check(isinstance(arm, dict), row, f"arm {name} must be an object")
        _check(
            isinstance(arm.get("commit"), str) and bool(_HEX40.match(arm["commit"])),
            row,
            f"arm {name}: 'commit' must be 40 lowercase hexadecimal digits",
        )
        _check(isinstance(arm.get("dirty"), bool), row, f"arm {name}: 'dirty' must be a boolean")
        _check(arm.get("binary") in BINARIES, row, f"arm {name}: 'binary' must be one of {', '.join(BINARIES)}")
        _check(_is_text(arm.get("runtime")), row, f"arm {name}: 'runtime' must be the runtime's version line")


def _validate_plan(row: Mapping[str, Any]) -> None:
    plan = row.get("plan")
    _check(isinstance(plan, dict), row, "'plan' must be an object")
    benchmarks = plan.get("benchmarks")
    _check(
        benchmarks is None
        or (isinstance(benchmarks, list) and bool(benchmarks) and all(_is_text(b) for b in benchmarks)),
        row,
        "plan 'benchmarks' must be a non-empty list of names, or null for every measured benchmark",
    )
    if HEAD in row["arms"]:
        _check(
            _is_int(plan.get("min_processes")) and plan["min_processes"] >= 1,
            row,
            "plan 'min_processes' must be a positive integer",
        )
        return
    for name in ("floor", "stop_precision", "quantile"):
        _check(_is_number(plan.get(name)) and plan[name] > 0, row, f"plan '{name}' must be a positive number")
    for name in ("min_pairs", "max_pairs"):
        _check(_is_int(plan.get(name)) and plan[name] >= 1, row, f"plan '{name}' must be a positive integer")


def _is_place(pair: Any, position: Any) -> bool:
    """A pair number from 1 with a place of 1 or 2, or both null."""
    if pair is None and position is None:
        return True
    return _is_int(pair) and pair >= 1 and _is_int(position) and position in (1, 2)


def _is_samples(samples: Any) -> bool:
    return (
        isinstance(samples, dict)
        and _is_int(samples.get("count"))
        and samples["count"] >= 1
        and _is_number(samples.get("min"))
        and _is_number(samples.get("max"))
        and samples["min"] <= samples["max"]
    )


def _is_counters(counters: Any) -> bool:
    return isinstance(counters, dict) and all(isinstance(k, str) and _is_number(v) for k, v in counters.items())


def _is_allocation(allocation: Any) -> bool:
    if allocation is None:
        return True
    return (
        isinstance(allocation, dict)
        and _is_int(allocation.get("baseline_bytes"))
        and _is_int(allocation.get("candidate_bytes"))
        and isinstance(allocation.get("pass"), bool)
    )


def _is_value(row: Mapping[str, Any]) -> bool:
    value = row.get("value")
    if not _is_number(value):
        return False
    return value > 0 or (row["metric"] == timed_gate.ALLOCATION_METRIC and value >= 0)


def _validate_run(row: Mapping[str, Any]) -> None:
    _check(_is_text(row.get("started_at")), row, "'started_at' must be a UTC time")
    _check(row.get("tier") in TIERS, row, f"'tier' must be one of {', '.join(TIERS)}")
    nightly = row["tier"] == "nightly"
    _check(("night" in row) == nightly, row, "'night' belongs to a nightly run and to no other")
    _check(
        not nightly or (isinstance(row["night"], str) and bool(_NIGHT.match(row["night"]))),
        row,
        "'night' must be YYYY-MM-DD",
    )
    _check(_is_text(row.get("kit")), row, "'kit' must name the kit")
    _validate_arms(row)
    _validate_plan(row)
    _check(_is_text(row.get("conditions")), row, "'conditions' must be the id of a conditions entry")
    _check(
        isinstance(row.get("fingerprint"), str) and bool(_HEX64.match(row["fingerprint"])),
        row,
        "'fingerprint' must be 64 lowercase hexadecimal digits",
    )
    _check(isinstance(row.get("environment"), dict), row, "'environment' must be the capture")
    _check(
        fingerprint(row["environment"]) == row["fingerprint"],
        row,
        "'fingerprint' is not the fingerprint of 'environment'",
    )


def _validate_series_identity(row: Mapping[str, Any]) -> None:
    _check(_is_text(row.get("benchmark")), row, "'benchmark' must be a name")
    _check(_is_params(row.get("params")), row, "'params' must map strings to strings")
    _check(_is_text(row.get("metric")) and _is_text(row.get("unit")), row, "'metric' and 'unit' must be names")


def _validate_measurement(row: Mapping[str, Any]) -> None:
    _validate_series_identity(row)
    _check(row.get("arm") in PAIRED_ARMS + (HEAD,), row, "'arm' must be baseline, candidate or head")
    _check(
        _is_place(row.get("pair"), row.get("position")),
        row,
        "'pair' and 'position' are a pair number from 1 and a place of 1 or 2, or both null",
    )
    _check(_is_int(row.get("process")) and row["process"] >= 1, row, "'process' must be an ordinal from 1")
    _check(_is_value(row), row, "'value' must be finite, and greater than zero unless it is an allocation figure")
    _check("samples" not in row or _is_samples(row["samples"]), row, "'samples' must hold count, min and max")
    _check("counters" not in row or _is_counters(row["counters"]), row, "'counters' must map names to numbers")
    _check(isinstance(row.get("accepted"), bool), row, "'accepted' must be a boolean")
    _check(
        ("reject_reason" in row) == (row["accepted"] is False),
        row,
        "'reject_reason' is present exactly when 'accepted' is false",
    )
    _check(row["accepted"] or _is_text(row["reject_reason"]), row, "'reject_reason' must say why")


def _validate_verdict(row: Mapping[str, Any]) -> None:
    _validate_series_identity(row)
    _check(_is_int(row.get("pairs")) and row["pairs"] >= 1, row, "'pairs' must be a positive integer")
    _check(_is_number(row.get("difference")), row, "'difference' and 'threshold' must be finite log ratios")
    _check(
        _is_number(row.get("threshold")) and row["threshold"] >= 0,
        row,
        "'difference' and 'threshold' must be finite log ratios",
    )
    _check(
        row.get("verdict")
        in (timed_gate.REGRESSION, timed_gate.IMPROVEMENT, timed_gate.UNSTABLE, timed_gate.UNCHANGED),
        row,
        "'verdict' must be regression, improvement, unstable or unchanged",
    )
    _check(
        "allocation" in row and _is_allocation(row["allocation"]),
        row,
        "'allocation' must be null or hold baseline_bytes, candidate_bytes and pass",
    )


_ROW_VALIDATORS = {"run": _validate_run, "measurement": _validate_measurement, "verdict": _validate_verdict}


def validate_row(row: Mapping[str, Any]) -> None:
    """Refuse a row that is not a schema-1 ledger row. Unknown fields are allowed."""
    if not isinstance(row, dict):
        raise LedgerError("a ledger row must be a JSON object")
    if row.get("schema") != SCHEMA or isinstance(row.get("schema"), bool):
        raise LedgerError(f"unknown ledger schema {row.get('schema')!r}; this reader knows schema {SCHEMA}")
    _check(
        isinstance(row.get("run"), str) and bool(_RUN_ID.match(row["run"])),
        row,
        "'run' must be YYYYMMDDTHHMMSSZ-<8 hexadecimal digits>",
    )
    validator = _ROW_VALIDATORS.get(row.get("kind"))
    if validator is None:
        raise LedgerError(f"row of run {row.get('run')!r}: unknown kind {row.get('kind')!r}")
    validator(row)


def read_ledger(path: Path) -> list[dict[str, Any]]:
    """Every row of a ledger file, validated. A row of an unknown schema is refused, not skipped."""
    rows = []
    with path.open(encoding="utf-8") as source:
        for number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise LedgerError(f"{path}:{number}: not a JSON object: {error.msg}") from error
            try:
                validate_row(row)
            except LedgerError as error:
                raise LedgerError(f"{path}:{number}: {error}") from error
            rows.append(row)
    return rows


def _lines(rows: Iterable[Mapping[str, Any]]) -> str:
    """Rows as JSON Lines text, one compact object per line."""
    return "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)


def append_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    """Append rows to the ledger in one write under an exclusive lock. Existing lines are never touched."""
    if not rows:
        return
    for row in rows:
        validate_row(row)
    payload = _lines(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        size = os.fstat(descriptor).st_size
        if size:
            with path.open("rb") as existing:
                existing.seek(size - 1)
                if existing.read(1) != b"\n":
                    raise LedgerError(
                        f"{path}: the last line is incomplete; the ledger is not appended to until it is repaired"
                    )
        data = payload.encode("utf-8")
        written = os.write(descriptor, data)
        if written != len(data):
            raise LedgerError(f"{path}: short write ({written} of {len(data)} bytes)")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


# ---------------------------------------------------------------------------------------------
# the run directory
# ---------------------------------------------------------------------------------------------


def _write_json(path: Path, document: Any) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def parse_arm(spec: str) -> tuple[str, dict[str, str]]:
    """name,commit=..,binary=..[,dirty=true|false][,java=PATH][,runtime=TEXT]"""
    parts = re.split(r",(?=(?:commit|binary|dirty|java|runtime)=)", spec)
    name, fields = parts[0].strip(), {}
    for part in parts[1:]:
        key, _, value = part.partition("=")
        if key in fields:
            raise LedgerError(f"arm {name}: '{key}' is given twice")
        fields[key] = value
    if "java" in fields and "runtime" in fields:
        raise LedgerError(f"arm {name}: give java=PATH or runtime=TEXT, not both")
    if fields.get("dirty", "false") not in ("true", "false"):
        raise LedgerError(f"arm {name}: dirty must be true or false")
    return name, fields


def _ledger_path() -> Path:
    value = os.environ.get(LEDGER_ENV)
    if not value:
        raise LedgerError(f"{LEDGER_ENV} is not set: a run is recorded only into a named results ledger")
    return Path(value)


def _arms_of(parsed: Mapping[str, Mapping[str, str]], environment: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The run row's arms; an arm whose runtime nobody stated says so instead of guessing."""
    unknown = "the kit does not know which runtime this arm runs on"
    arms = {}
    for name, fields in parsed.items():
        captured = environment["runtimes"].setdefault(name, unavailable(unknown))
        runtime = (
            captured.splitlines()[0].strip() if isinstance(captured, str) else f"unavailable: {captured['unavailable']}"
        )
        arms[name] = {
            "commit": fields.get("commit", ""),
            "dirty": fields.get("dirty", "false") == "true",
            "binary": fields.get("binary", ""),
            "runtime": runtime,
        }
    return arms


def _plan_of(arms: Mapping[str, Any], tier: str, min_pairs: int, benchmarks: Sequence[str] | None) -> dict[str, Any]:
    judged = list(benchmarks) if benchmarks else None
    if HEAD in arms:
        return {"min_processes": NIGHTLY_MIN_PROCESSES if tier == "nightly" else 1, "benchmarks": judged}
    if not _is_int(min_pairs) or not timed_gate.MIN_PAIRS <= min_pairs <= timed_gate.MAX_PAIRS:
        raise LedgerError(
            f"the plan's minimum number of pairs must be {timed_gate.MIN_PAIRS} to "
            f"{timed_gate.MAX_PAIRS}, got {min_pairs!r}"
        )
    return {
        "floor": timed_gate.FLOOR,
        "stop_precision": timed_gate.STOP_PRECISION,
        "quantile": QUANTILE,
        "min_pairs": min_pairs,
        "max_pairs": timed_gate.MAX_PAIRS,
        "benchmarks": judged,
    }


def begin(
    out: Path,
    *,
    tier: str,
    kit: str,
    arm_specs: Sequence[str],
    data: Path | None = None,
    night: str | None = None,
    min_pairs: int = timed_gate.MIN_PAIRS,
    benchmarks: Sequence[str] | None = None,
    ledger: Path | None = None,
    conditions: Path | None = None,
    root: Path = Path("/"),
    runner: Runner = _system_runner,
    now: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Open a run in a fresh directory: capture, resolve the conditions entry, write run.json."""
    ledger = ledger or _ledger_path()
    conditions = conditions or default_conditions_path()
    if not out.is_dir():
        raise LedgerError(f"run directory does not exist: {out}")
    if (out / RUN_FILE).exists():
        raise LedgerError(f"{out} already holds a run; every run needs a fresh directory")
    parsed = dict(parse_arm(spec) for spec in arm_specs)
    if len(parsed) != len(arm_specs):
        raise LedgerError("an arm is given twice")
    stamp = (now or datetime.datetime.now(datetime.timezone.utc)).astimezone(datetime.timezone.utc)
    environment = capture_environment(
        data=data,
        root=root,
        runner=runner,
        now=stamp,
        javas={name: fields["java"] for name, fields in parsed.items() if "java" in fields},
        runtimes={name: fields["runtime"] for name, fields in parsed.items() if "runtime" in fields},
    )
    arms = _arms_of(parsed, environment)
    value = fingerprint(environment)
    _write_json(out / ENVIRONMENT_FILE, environment)
    row: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": "run",
        "run": f"{stamp.strftime('%Y%m%dT%H%M%SZ')}-{os.urandom(4).hex()}",
        "started_at": stamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tier": tier,
        "kit": kit,
        "plan": _plan_of(arms, tier, min_pairs, benchmarks),
        "arms": arms,
        "conditions": "pending",
        "fingerprint": value,
        "environment": environment,
    }
    if night is not None:
        row["night"] = night
    validate_row(row)  # arms, plan and tier are refused here, before the conditions record is consulted
    entry = conditions_id(conditions, value)
    if entry is None:
        raise LedgerError(
            f"no entry of {conditions} holds this environment's fingerprint {value}; it "
            f"{_field_differences(ledger, environment)}. Add a dated entry (new baseline, or continues with the control "
            f"run that shows results stayed comparable) before measuring; `bench_ledger.py fingerprint` prints the fields."
        )
    row["conditions"] = entry
    _write_json(out / RUN_FILE, row)
    return row


def load_run(run_dir: Path) -> dict[str, Any]:
    try:
        row = json.loads((run_dir / RUN_FILE).read_text(encoding="utf-8"))
    except OSError as error:
        raise LedgerError(
            f"{run_dir} holds no opened run ({RUN_FILE}): {error.strerror or type(error).__name__}"
        ) from error
    validate_row(row)
    return row


def load_measurements(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / MEASUREMENTS_FILE
    return read_ledger(path) if path.exists() else []


def record(run_dir: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    """Add measurement rows to an open run. Nothing can be added once the run has a verdict."""
    run = load_run(run_dir)
    if (run_dir / VERDICTS_FILE).exists():
        raise LedgerError(f"{run_dir} has been judged; no measurement may be added after a verdict")
    complete = []
    seen = {
        (row["benchmark"], tuple(sorted(row["params"].items())), row["metric"], row["arm"], row["pair"], row["process"])
        for row in load_measurements(run_dir)
    }
    for row in rows:
        full = {"schema": SCHEMA, "kind": "measurement", "run": run["run"], **row}
        validate_row(full)
        if full["arm"] not in run["arms"]:
            raise LedgerError(f"measurement of arm {full['arm']!r}, which run {run['run']} does not have")
        identity = (
            full["benchmark"],
            tuple(sorted(full["params"].items())),
            full["metric"],
            full["arm"],
            full["pair"],
            full["process"],
        )
        if identity in seen:
            raise LedgerError(
                f"{full['benchmark']} {full['metric']}: arm {full['arm']}, pair {full['pair']}, process "
                f"{full['process']} is already recorded"
            )
        seen.add(identity)
        complete.append(full)
    if complete:
        with (run_dir / MEASUREMENTS_FILE).open("a", encoding="utf-8") as sink:
            sink.write(_lines(complete))
    return len(complete)


def reject_run(run_dir: Path, reason: str) -> int:
    """Reject every measurement of a run whose rows have not reached the ledger. Rejection is never reversed."""
    load_run(run_dir)
    if not reason.strip():
        raise LedgerError("a rejection needs its reason")
    if (run_dir / VERDICTS_FILE).exists() or (run_dir / RECORDED_FILE).exists():
        raise LedgerError(f"{run_dir} is already judged or recorded; rows in the ledger are never rewritten")
    rows = load_measurements(run_dir)
    for row in rows:
        if row["accepted"]:
            row["accepted"] = False
            row["reject_reason"] = reason
    temporary = run_dir / (MEASUREMENTS_FILE + ".tmp")
    temporary.write_text(_lines(rows), encoding="utf-8")
    os.replace(temporary, run_dir / MEASUREMENTS_FILE)
    return len(rows)


def commit(run_dir: Path, ledger: Path | None = None) -> dict[str, int]:
    """Append to the ledger whatever of this run is not there yet: run row first, then measurements, then verdicts."""
    ledger = Path(os.path.abspath(ledger or _ledger_path()))
    run = load_run(run_dir)
    measurements = load_measurements(run_dir)
    verdicts = read_ledger(run_dir / VERDICTS_FILE) if (run_dir / VERDICTS_FILE).exists() else []
    marker = run_dir / RECORDED_FILE
    done = (
        json.loads(marker.read_text(encoding="utf-8"))
        if marker.exists()
        else {"ledger": str(ledger), "measurements": 0, "verdicts": 0, "run": False}
    )
    if done["ledger"] != str(ledger):
        raise LedgerError(f"{run_dir} is already recorded in {done['ledger']}; one run belongs to one ledger")
    if done["measurements"] > len(measurements) or done["verdicts"] > len(verdicts):
        raise LedgerError(f"{run_dir}: rows that were recorded in the ledger are gone from the run directory")
    pending = ([] if done["run"] else [run]) + measurements[done["measurements"] :] + verdicts[done["verdicts"] :]
    append_rows(ledger, pending)
    done = {"ledger": str(ledger), "run": True, "measurements": len(measurements), "verdicts": len(verdicts)}
    _write_json(marker, done)
    return {"appended": len(pending), "measurements": len(measurements), "verdicts": len(verdicts)}


def stop(run_dir: Path) -> tuple[bool, list[timed_gate.StopState]]:
    run = load_run(run_dir)
    if HEAD in run["arms"]:
        raise LedgerError("the stop rule belongs to a paired run; this run measures the single arm head")
    return timed_gate.run_may_stop(
        load_measurements(run_dir), benchmarks=run["plan"]["benchmarks"], min_pairs=run["plan"]["min_pairs"]
    )


def judge(run_dir: Path, ledger: Path | None = None) -> timed_gate.Judgement:
    """Judge a paired run once and record it. The ledger holds every row before a verdict is reported."""
    ledger = ledger or _ledger_path()
    run = load_run(run_dir)
    if HEAD in run["arms"]:
        raise LedgerError("only a paired run of baseline and candidate is judged")
    if (run_dir / VERDICTS_FILE).exists():
        raise LedgerError(f"{run_dir} has been judged already; verdicts are computed once ({VERDICTS_FILE})")
    judgement = timed_gate.judge(
        load_measurements(run_dir),
        benchmarks=run["plan"]["benchmarks"],
        min_pairs=run["plan"]["min_pairs"],
        enforce_stop_rule=run["tier"] == "gate",
    )
    rows = [{"schema": SCHEMA, "kind": "verdict", "run": run["run"], **verdict} for verdict in judgement.verdicts]
    for row in rows:
        validate_row(row)
    temporary = run_dir / (VERDICTS_FILE + ".tmp")
    temporary.write_text(_lines(rows), encoding="utf-8")
    os.replace(temporary, run_dir / VERDICTS_FILE)
    commit(run_dir, ledger)
    return judgement


def recorded_judgement(run_dir: Path) -> timed_gate.Judgement:
    """The verdicts of a judged run, once the ledger holds them."""
    load_run(run_dir)
    if not (run_dir / VERDICTS_FILE).exists():
        raise LedgerError(f"{run_dir} has not been judged")
    verdicts = read_ledger(run_dir / VERDICTS_FILE)
    marker = run_dir / RECORDED_FILE
    done = json.loads(marker.read_text(encoding="utf-8")) if marker.exists() else {"verdicts": 0}
    if done["verdicts"] != len(verdicts):
        raise LedgerError(
            f"{run_dir}: the verdicts are not in the ledger yet; run `commit` first, a verdict is "
            f"reported only after it is recorded"
        )
    return timed_gate.Judgement(
        [{key: value for key, value in row.items() if key not in ("schema", "kind", "run")} for row in verdicts], []
    )


# ---------------------------------------------------------------------------------------------
# importers: what the kits and JMH write, as measurement rows
# ---------------------------------------------------------------------------------------------


def served_counters(log: Path | None) -> dict[str, int] | None:
    """The route counters of the `# served: name=N ...` lines a kit's runner printed, summed per name."""
    if log is None:
        return None
    counters: dict[str, int] = {}
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("# served"):
            for name, value in _SERVED.findall(line):
                counters[name] = counters.get(name, 0) + int(value)
    return counters or None


def _base_row(
    arm: str,
    pair: int | None,
    position: int | None,
    process: int,
    reject_reason: str | None,
    counters: Mapping[str, int] | None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "arm": arm,
        "pair": pair,
        "position": position,
        "process": process,
        "accepted": reject_reason is None,
    }
    if reject_reason is not None:
        row["reject_reason"] = reject_reason
    if counters:
        row["counters"] = dict(counters)
    return row


def _result_rows(document: Any, source: str) -> list[Any]:
    rows = document.get("result") or document.get("results") if isinstance(document, dict) else None
    if not isinstance(rows, list) or not rows:
        raise LedgerError(f"{source}: no 'result' array; the process produced no timings")
    return rows


def _timing(value: Any, source: str) -> float | None:
    """A timing in seconds; None when it is below the timer's resolution and so cannot be recorded."""
    if not _is_number(value) or value < 0:
        raise LedgerError(f"{source}: invalid timing {value!r}")
    return float(value) if value > 0 else None


def _hot_figure(tries: Sequence[Any], source: str) -> tuple[float, dict[str, Any]] | None:
    """The best later try of a process and its spread; None when a later try is below the timer's resolution."""
    later = [_timing(value, source) for value in tries[1:] if value is not None]
    if not later or any(value is None for value in later):
        return None
    return min(later), {"count": len(later), "min": min(later), "max": max(later)}


def clickbench_rows(
    document: Any,
    *,
    arm: str,
    pair: int | None,
    position: int | None,
    process: int,
    reject_reason: str | None = None,
    counters: Mapping[str, int] | None = None,
    source: str = "result",
) -> tuple[list[dict[str, Any]], list[str]]:
    """One process of the ClickBench runner: try 1 is the cold figure, the best later try the hot one.

    Queries are named q00.. by the zero-based ClickBench index. `suite` is the sum over all queries and
    is recorded only when the process produced that figure for every one of them. Returns the rows and
    what was left out.
    """
    results = _result_rows(document, source)
    series = {"params": {}, "unit": "s", **_base_row(arm, pair, position, process, reject_reason, counters)}
    rows, skipped = [], []
    totals = {"cold_seconds": 0.0, "hot_seconds": 0.0}
    complete = {"cold_seconds": True, "hot_seconds": True}
    for index, tries in enumerate(results):
        name = f"q{index:02d}"
        if not tries or tries[0] is None:
            complete = {"cold_seconds": False, "hot_seconds": False}
            continue
        cold = _timing(tries[0], f"{source} {name}")
        hot = _hot_figure(tries, f"{source} {name}")
        if cold is not None:
            totals["cold_seconds"] += cold
            rows.append({**series, "benchmark": name, "metric": "cold_seconds", "value": cold})
        if hot is not None:
            totals["hot_seconds"] += hot[0]
            rows.append({**series, "benchmark": name, "metric": "hot_seconds", "value": hot[0], "samples": hot[1]})
        for metric, figure, expected in (("cold_seconds", cold, True), ("hot_seconds", hot, len(tries) > 1)):
            if figure is None:
                complete[metric] = False
                if expected:
                    skipped.append(f"{name} {metric} is below the timer's resolution")
    rows.extend(
        {**series, "benchmark": "suite", "metric": metric, "value": totals[metric]}
        for metric in ("cold_seconds", "hot_seconds")
        if complete[metric]
    )
    return rows, skipped


def jsonbench_rows(
    document: Any,
    *,
    query: int,
    attempt: int,
    arm: str,
    process: int,
    reject_reason: str | None = None,
    counters: Mapping[str, int] | None = None,
    source: str = "result",
) -> tuple[list[dict[str, Any]], list[str]]:
    """One isolated JSONBench process: one query, one timing. Attempt 1 follows the eviction and is the
    cold figure; later attempts are fresh processes over resident pages and are the hot figure."""
    results = _result_rows(document, source)
    if query < 1 or query > len(results) or attempt < 1:
        raise LedgerError(f"{source}: no query {query} among {len(results)}, or attempt {attempt} is not positive")
    tries = results[query - 1]
    executed = [index + 1 for index, row in enumerate(results) if row and row[0] is not None]
    if executed != [query] or len(tries) != 1:
        raise LedgerError(
            f"{source}: an isolated process carries exactly one timing of Q{query}, got queries {executed}"
        )
    value = _timing(tries[0], f"{source} q{query}")
    metric = "cold_seconds" if attempt == 1 else "hot_seconds"
    if value is None:
        return [], [f"q{query} {metric} is below the timer's resolution"]
    return [
        {
            **_base_row(arm, None, None, process, reject_reason, counters),
            "benchmark": f"q{query}",
            "params": {},
            "metric": metric,
            "unit": "s",
            "value": value,
        }
    ], []


_JMH_TIME_MODES = ("avgt", "sample", "ss")


def _jmh_row(result: Mapping[str, Any], base: Mapping[str, Any], allocation_leg: bool, source: str) -> dict[str, Any]:
    name = ".".join(str(result["benchmark"]).split(".")[-2:])
    series = {
        **base,
        "benchmark": name,
        "params": {str(key): str(value) for key, value in (result.get("params") or {}).items()},
    }
    if result.get("forks") != 1:
        raise LedgerError(
            f"{source} {name}: one fresh process per measurement needs forks = 1, got {result.get('forks')!r}"
        )
    profiled = (result.get("secondaryMetrics") or {}).get("gc.alloc.rate.norm")
    if allocation_leg:
        if profiled is None:
            raise LedgerError(f"{source} {name}: an allocation leg needs the GC profiler's gc.alloc.rate.norm")
        return {**series, "metric": timed_gate.ALLOCATION_METRIC, "unit": "B/op", "value": profiled["score"]}
    if profiled is not None:
        raise LedgerError(f"{source} {name}: a timed process must not carry the allocation profiler")
    if result.get("mode") not in _JMH_TIME_MODES:
        raise LedgerError(
            f"{source} {name}: mode {result.get('mode')!r} is not a time per operation; "
            f"use one of {', '.join(_JMH_TIME_MODES)}"
        )
    metric = result["primaryMetric"]
    unit = str(metric["scoreUnit"])
    row = {**series, "metric": unit.replace("/", "_per_"), "unit": unit, "value": metric["score"]}
    samples = [value for fork in metric.get("rawData") or [] for value in fork]
    if samples:
        row["samples"] = {"count": len(samples), "min": min(samples), "max": max(samples)}
    return row


def jmh_rows(
    document: Any,
    *,
    arm: str,
    pair: int | None,
    position: int | None,
    process: int,
    allocation_leg: bool = False,
    reject_reason: str | None = None,
    source: str = "result",
) -> list[dict[str, Any]]:
    """The results of one JMH invocation with a single fork per benchmark.

    A timed process must not carry the allocation profiler, and an allocation leg contributes no timed
    measurement: the two are refused when mixed.
    """
    if not isinstance(document, list) or not document:
        raise LedgerError(f"{source}: not a JMH JSON result list")
    base = _base_row(
        arm, None if allocation_leg else pair, None if allocation_leg else position, process, reject_reason, None
    )
    return [_jmh_row(result, base, allocation_leg, source) for result in document]


# ---------------------------------------------------------------------------------------------
# command line
# ---------------------------------------------------------------------------------------------


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise LedgerError(f"{path}: {error.strerror or type(error).__name__}") from error
    except json.JSONDecodeError as error:
        raise LedgerError(f"{path}: not JSON: {error.msg}") from error


def _parse_javas(values: Sequence[str] | None) -> dict[str, str]:
    javas = {}
    for value in values or []:
        name, separator, path = value.partition("=")
        if not separator or not name or not path:
            raise LedgerError(f"--java expects NAME=PATH, got {value!r}")
        javas[name] = path
    return javas


def _arguments() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Results ledger and run recording (CONV-303).")
    commands = parser.add_subparsers(dest="command", required=True)

    capture = commands.add_parser("capture")
    capture.add_argument("--out", required=True, type=Path)
    capture.add_argument("--data", type=Path)
    capture.add_argument("--java", action="append")

    commands.add_parser("fingerprint")

    opening = commands.add_parser("begin")
    opening.add_argument("--out", required=True, type=Path)
    opening.add_argument("--tier", required=True, choices=TIERS)
    opening.add_argument("--kit", required=True)
    opening.add_argument("--arm", action="append", required=True)
    opening.add_argument("--data", type=Path)
    opening.add_argument("--night")
    opening.add_argument("--min-pairs", type=int, default=timed_gate.MIN_PAIRS)
    opening.add_argument("--benchmark", action="append")

    def measured(command: argparse.ArgumentParser, *, paired: bool) -> None:
        command.add_argument("--run", required=True, type=Path)
        command.add_argument("--process", required=True, type=int)
        command.add_argument("--rejected", metavar="REASON")
        if paired:
            command.add_argument("--arm", required=True)
            command.add_argument("--pair", type=int)
            command.add_argument("--position", type=int)

    single = commands.add_parser("record")
    measured(single, paired=True)
    single.add_argument("--benchmark", required=True)
    single.add_argument("--param", action="append", metavar="NAME=VALUE")
    single.add_argument("--metric", required=True)
    single.add_argument("--unit", required=True)
    single.add_argument("--value", required=True, type=float)

    jmh = commands.add_parser("import-jmh")
    measured(jmh, paired=True)
    jmh.add_argument("--json", required=True, type=Path)
    jmh.add_argument("--allocation-leg", action="store_true")

    clickbench = commands.add_parser("import-clickbench")
    measured(clickbench, paired=True)
    clickbench.add_argument("--json", required=True, type=Path)
    clickbench.add_argument("--log", type=Path)

    jsonbench = commands.add_parser("import-jsonbench")
    measured(jsonbench, paired=False)
    jsonbench.add_argument("--json", required=True, type=Path)
    jsonbench.add_argument("--query", required=True, type=int)
    jsonbench.add_argument("--attempt", required=True, type=int)
    jsonbench.add_argument("--log", type=Path)

    rejecting = commands.add_parser("reject")
    rejecting.add_argument("--run", required=True, type=Path)
    rejecting.add_argument("--reason", required=True)

    for name in ("stop", "judge", "commit", "report"):
        commands.add_parser(name).add_argument("--run", required=True, type=Path)
    return parser


def _measured(args: argparse.Namespace) -> int:
    """The four subcommands that add measurement rows to a run."""
    skipped: list[str] = []
    if args.command == "record":
        rows = [
            {
                **_base_row(args.arm, args.pair, args.position, args.process, args.rejected, None),
                "benchmark": args.benchmark,
                "params": dict(item.split("=", 1) for item in args.param or []),
                "metric": args.metric,
                "unit": args.unit,
                "value": args.value,
            }
        ]
    elif args.command == "import-jmh":
        rows = jmh_rows(
            _load_json(args.json),
            arm=args.arm,
            pair=args.pair,
            position=args.position,
            process=args.process,
            allocation_leg=args.allocation_leg,
            reject_reason=args.rejected,
            source=args.json.name,
        )
    elif args.command == "import-clickbench":
        rows, skipped = clickbench_rows(
            _load_json(args.json),
            arm=args.arm,
            pair=args.pair,
            position=args.position,
            process=args.process,
            reject_reason=args.rejected,
            counters=served_counters(args.log),
            source=args.json.name,
        )
    else:
        rows, skipped = jsonbench_rows(
            _load_json(args.json),
            query=args.query,
            attempt=args.attempt,
            arm=HEAD,
            process=args.process,
            reject_reason=args.rejected,
            counters=served_counters(args.log),
            source=args.json.name,
        )
    for item in skipped:
        print(f"bench_ledger.py: not recorded: {item}", file=sys.stderr)
    print(f"recorded {record(args.run, rows)} measurement(s)")
    return 0


def _capture(args: argparse.Namespace) -> int:
    if not args.out.is_dir():
        raise LedgerError(f"output directory does not exist: {args.out}")
    _write_json(args.out / ENVIRONMENT_FILE, capture_environment(data=args.data, javas=_parse_javas(args.java)))
    return 0


def _fingerprint(_args: argparse.Namespace) -> int:
    environment = capture_environment()
    print(fingerprint(environment))
    print(json.dumps(fingerprint_fields(environment), indent=2))
    return 0


def _begin(args: argparse.Namespace) -> int:
    row = begin(
        args.out,
        tier=args.tier,
        kit=args.kit,
        arm_specs=args.arm,
        data=args.data,
        night=args.night,
        min_pairs=args.min_pairs,
        benchmarks=args.benchmark,
    )
    print(f"run {row['run']} opened under condition {row['conditions']}")
    return 0


def _reject(args: argparse.Namespace) -> int:
    print(f"rejected {reject_run(args.run, args.reason)} measurement(s): {args.reason}")
    return 0


def _stop(args: argparse.Namespace) -> int:
    print(timed_gate.format_stop(*stop(args.run)))
    return 0


def _verdicts(args: argparse.Namespace) -> int:
    judgement = judge(args.run) if args.command == "judge" else recorded_judgement(args.run)
    print(timed_gate.format_report(judgement))
    return 1 if judgement.failed else 0


def _commit(args: argparse.Namespace) -> int:
    totals = commit(args.run)
    print(
        f"ledger: appended {totals['appended']} row(s); the run has {totals['measurements']} measurement(s) "
        f"and {totals['verdicts']} verdict(s)"
    )
    return 0


_COMMANDS = {
    "capture": _capture,
    "fingerprint": _fingerprint,
    "begin": _begin,
    "record": _measured,
    "import-jmh": _measured,
    "import-clickbench": _measured,
    "import-jsonbench": _measured,
    "reject": _reject,
    "stop": _stop,
    "judge": _verdicts,
    "report": _verdicts,
    "commit": _commit,
}


def main(argv: Sequence[str] | None = None) -> int:
    args = _arguments().parse_args(argv)
    try:
        return _COMMANDS[args.command](args)
    except (LedgerError, GateError, OSError) as error:
        print(f"bench_ledger.py: {error}", file=sys.stderr)
        return 2
    except (KeyError, TypeError, AttributeError) as error:
        print(f"bench_ledger.py: malformed input ({type(error).__name__}: {error})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
