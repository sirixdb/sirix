#!/usr/bin/env python3
"""Create fail-closed evidence for saturated async append admission."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from hft_artifact import runtime_classpath_sha256


BUILD_PREFIX = "# HFT_BUILD "
CONFIG_PREFIX = "# HFT_SATURATION_CONFIG "
SATURATION_PREFIX = "# HFT_APPEND_SATURATION "
MEASURE_START = "# HFT_MEASURE_START"
MEASURE_END = "# HFT_MEASURE_END"
MIB = 1024 * 1024
GIB = 1024 * MIB
EXPECTED_HEAP_BYTES = 4 * GIB
EXPECTED_MAX_NEW_BYTES = GIB
EXPECTED_G1_REGION_SIZE_BYTES = 4 * MIB
CONFIG_FIELDS = frozenset(
    {
        "initialHeapBytes",
        "maxHeapBytes",
        "maxNewSizeBytes",
        "g1RegionSizeBytes",
        "gcLogging",
        "safepointLogging",
    }
)
FIELDS = frozenset(
    {
        "resources",
        "versioningType",
        "records",
        "appendWorkers",
        "queueCapacity",
        "callerThreadAppendRuns",
        "submitWaitCount",
        "submitWaitTotalNs",
        "submitWaitMaxNs",
        "saturatedActiveWorkers",
        "saturatedQueuedTasks",
        "saturatedAdmissionWaiters",
        "saturatedAvailableAdmissions",
        "drainedActiveWorkers",
        "drainedQueuedTasks",
        "drainedAdmissionWaiters",
        "drainedAvailableAdmissions",
        "coldReopens",
    }
)
MIN_SATURATION_WAIT_NANOS = 100_000_000
MAX_STALL_NANOS = 250_000_000
_DURATION_RE = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ns|us|µs|ms|s)\b", re.IGNORECASE
)
_SAFEPOINT_TOTAL_RE = re.compile(
    r"\bTotal:\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ns|us|µs|ms|s)\b",
    re.IGNORECASE,
)
_SAFEPOINT_TAG_RE = re.compile(
    r"\[(?:error|warning|info|debug|trace)\s*\]\[safepoint\]", re.IGNORECASE
)
_SAFEPOINT_METADATA_RE = re.compile(
    r"^(?:Application time:|Entering safepoint region:|Leaving safepoint region|"
    r"Safepoint synchronization initiated using |Total time for which application threads were stopped:|"
    r"Waiting for \d+ thread\(s\) to block|Synchronization status:|JavaThread |VM Operation took )",
    re.IGNORECASE,
)
_HUMONGOUS_REGIONS_RE = re.compile(
    r"\bHumongous regions:\s*(?P<before>\d+)\s*->\s*(?P<after>\d+)\b",
    re.IGNORECASE,
)
_FORBIDDEN_GC_EVENTS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("full collection", re.compile(r"\b(?:Pause Full|Full GC)\b", re.IGNORECASE)),
    ("major collection", re.compile(r"\b(?:Major GC|Pause Old|G1 Old Generation)\b", re.IGNORECASE)),
    (
        "concurrent old-generation cycle",
        re.compile(
            r"\bConcurrent (?:Mark |Undo )?Cycle\b|\bPause Young \(Concurrent Start\)",
            re.IGNORECASE,
        ),
    ),
    ("old-generation remark", re.compile(r"\bPause Remark\b", re.IGNORECASE)),
    ("old-generation cleanup", re.compile(r"\bPause Cleanup\b", re.IGNORECASE)),
    ("mixed collection", re.compile(r"\bPause Young \((?:Prepare )?Mixed\)", re.IGNORECASE)),
    ("to-space exhaustion", re.compile(r"\bTo-space exhausted\b", re.IGNORECASE)),
    ("evacuation failure", re.compile(r"\bEvacuation Failure\b", re.IGNORECASE)),
    ("allocation failure", re.compile(r"\bAllocation Failure\b", re.IGNORECASE)),
    ("allocation stall", re.compile(r"\bAllocation Stall\b", re.IGNORECASE)),
    ("GCLocker collection", re.compile(r"\bGCLocker Initiated GC\b", re.IGNORECASE)),
    ("explicit collection", re.compile(r"\bSystem\.gc\(\)|Diagnostic Command", re.IGNORECASE)),
    ("preventive collection", re.compile(r"\bPreventive Collection\b", re.IGNORECASE)),
    ("humongous allocation", re.compile(r"\bHumongous Allocation\b", re.IGNORECASE)),
    ("out of memory", re.compile(r"\bOutOfMemoryError\b", re.IGNORECASE)),
)


@dataclass
class ParsedSaturation:
    builds: list[tuple[str, str]] = field(default_factory=list)
    configurations: list[dict[str, int | str]] = field(default_factory=list)
    records: list[dict[str, int | str]] = field(default_factory=list)
    gc_pause_nanos: list[int] = field(default_factory=list)
    safepoint_nanos: list[int] = field(default_factory=list)
    humongous_region_samples: int = 0
    issues: list[str] = field(default_factory=list)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _duration_nanos(value: str, unit: str) -> int:
    multipliers = {
        "ns": 1.0,
        "us": 1_000.0,
        "µs": 1_000.0,
        "ms": 1_000_000.0,
        "s": 1_000_000_000.0,
    }
    return int(float(value) * multipliers[unit.lower()])


def parse(log: Path) -> ParsedSaturation:
    parsed = ParsedSaturation()
    in_measurement = False
    start_seen = False
    end_seen = False
    for line_number, line in enumerate(log.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        stripped = line.strip()
        if stripped == MEASURE_START:
            if in_measurement or start_seen:
                parsed.issues.append(f"line {line_number}: duplicate or nested {MEASURE_START} marker")
            else:
                start_seen = True
                in_measurement = True
            continue
        if stripped == MEASURE_END:
            if not in_measurement or end_seen:
                parsed.issues.append(f"line {line_number}: {MEASURE_END} without one active region")
            else:
                in_measurement = False
                end_seen = True
            continue
        if stripped.startswith(BUILD_PREFIX):
            match = re.fullmatch(
                r"# HFT_BUILD gitSha=([0-9a-f]{40}) artifactSha256=([0-9a-f]{64})",
                stripped,
            )
            if match is None:
                parsed.issues.append(f"line {line_number}: malformed HFT build record")
            else:
                parsed.builds.append((match.group(1), match.group(2)))
            continue
        if stripped.startswith(CONFIG_PREFIX):
            if not in_measurement:
                parsed.issues.append(
                    f"line {line_number}: saturation configuration is outside the measurement region"
                )
                continue
            values: dict[str, int | str] = {}
            malformed = False
            for token in stripped[len(CONFIG_PREFIX) :].split():
                name, separator, raw_value = token.partition("=")
                if separator != "=" or not name or not raw_value or name in values:
                    malformed = True
                    continue
                if name in {"gcLogging", "safepointLogging"}:
                    if raw_value not in {"true", "false"}:
                        malformed = True
                    else:
                        values[name] = raw_value
                    continue
                try:
                    value = int(raw_value)
                except ValueError:
                    malformed = True
                    continue
                if value <= 0:
                    malformed = True
                    continue
                values[name] = value
            if malformed or set(values) != CONFIG_FIELDS:
                parsed.issues.append(f"line {line_number}: malformed saturation configuration")
            else:
                parsed.configurations.append(values)
            continue
        if stripped.startswith(SATURATION_PREFIX):
            if not in_measurement:
                parsed.issues.append(
                    f"line {line_number}: append-saturation record is outside the measurement region"
                )
                continue
            values: dict[str, int | str] = {}
            malformed = False
            for token in stripped[len(SATURATION_PREFIX) :].split():
                name, separator, raw_value = token.partition("=")
                if separator != "=" or not name or not raw_value or name in values:
                    malformed = True
                    continue
                if name == "versioningType":
                    if raw_value not in {"FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"}:
                        malformed = True
                    else:
                        values[name] = raw_value
                    continue
                try:
                    value = int(raw_value)
                except ValueError:
                    malformed = True
                    continue
                if value < 0:
                    malformed = True
                    continue
                values[name] = value
            if malformed or set(values) != FIELDS:
                parsed.issues.append(f"line {line_number}: malformed append-saturation record")
            else:
                parsed.records.append(values)
            continue
        if not in_measurement:
            continue

        humongous = _HUMONGOUS_REGIONS_RE.search(line)
        if humongous is not None:
            parsed.humongous_region_samples += 1
            if int(humongous.group("before")) > 0 or int(humongous.group("after")) > 0:
                parsed.issues.append(
                    f"line {line_number}: positive humongous-region occupancy: {stripped}"
                )

        for kind, pattern in _FORBIDDEN_GC_EVENTS:
            if pattern.search(line):
                parsed.issues.append(f"line {line_number}: forbidden {kind}: {stripped}")
                break

        if "Pause Young" in line:
            durations = list(_DURATION_RE.finditer(line))
            if not durations and "[gc,start" in line.lower():
                continue
            if not durations:
                parsed.issues.append(f"line {line_number}: could not parse young-GC pause duration")
            else:
                duration = durations[-1]
                parsed.gc_pause_nanos.append(
                    _duration_nanos(duration.group("value"), duration.group("unit"))
                )

        safepoint_tag = _SAFEPOINT_TAG_RE.search(line)
        if safepoint_tag is not None or "Safepoint " in line:
            total = _SAFEPOINT_TOTAL_RE.search(line)
            if total is not None:
                parsed.safepoint_nanos.append(
                    _duration_nanos(total.group("value"), total.group("unit"))
                )
            else:
                payload = line[safepoint_tag.end() :].strip() if safepoint_tag is not None else stripped
                if _SAFEPOINT_METADATA_RE.match(payload) is None:
                    parsed.issues.append(
                        f"line {line_number}: could not parse safepoint total duration"
                    )

    if not start_seen:
        parsed.issues.append(f"missing {MEASURE_START} marker")
    if in_measurement or (start_seen and not end_seen):
        parsed.issues.append(f"missing {MEASURE_END} marker")
    return parsed


def evaluate(log: Path, git_sha: str, artifact_sha256: str, versioning_type: str = "FULL") -> list[str]:
    return _evaluate(parse(log), git_sha, artifact_sha256, versioning_type)


def _evaluate(
    parsed: ParsedSaturation,
    git_sha: str,
    artifact_sha256: str,
    versioning_type: str,
) -> list[str]:
    issues = list(parsed.issues)
    if len(parsed.builds) != 1:
        issues.append(f"expected exactly one HFT build record, found {len(parsed.builds)}")
    elif parsed.builds[0] != (git_sha, artifact_sha256):
        issues.append("saturation build identity does not match the required commit and artifact")
    if len(parsed.configurations) != 1:
        issues.append(
            f"expected exactly one saturation configuration, found {len(parsed.configurations)}"
        )
    else:
        configuration = parsed.configurations[0]
        expected_configuration: dict[str, int | str] = {
            "initialHeapBytes": EXPECTED_HEAP_BYTES,
            "maxHeapBytes": EXPECTED_HEAP_BYTES,
            "maxNewSizeBytes": EXPECTED_MAX_NEW_BYTES,
            "g1RegionSizeBytes": EXPECTED_G1_REGION_SIZE_BYTES,
            "gcLogging": "true",
            "safepointLogging": "true",
        }
        for name, expected in expected_configuration.items():
            if configuration[name] != expected:
                issues.append(
                    f"saturation configuration {name}={configuration[name]!r}, expected {expected!r}"
                )
    if len(parsed.records) != 1:
        issues.append(f"expected exactly one append-saturation record, found {len(parsed.records)}")
        return issues
    values = parsed.records[0]
    if values["versioningType"] != versioning_type:
        issues.append("saturation versioning type does not match the required campaign arm")
    if values["resources"] < 4 or values["records"] <= 0:
        issues.append("saturation evidence requires at least four non-empty resources")
    if values["appendWorkers"] != 1 or values["queueCapacity"] != 1:
        issues.append("saturation evidence requires the canonical p=1/q=1 executor")
    if values["callerThreadAppendRuns"] != 0:
        issues.append("append work executed on a caller thread")
    if values["submitWaitCount"] < values["resources"]:
        issues.append("not every resource reached bounded append admission")
    if values["submitWaitTotalNs"] < values["submitWaitMaxNs"]:
        issues.append("append-admission maximum exceeds its total")
    if not MIN_SATURATION_WAIT_NANOS <= values["submitWaitMaxNs"] <= MAX_STALL_NANOS:
        issues.append("append-admission saturation is absent or exceeds 250 ms")

    expected_waiters = values["resources"] - values["appendWorkers"] - values["queueCapacity"]
    if (
        values["saturatedActiveWorkers"] != values["appendWorkers"]
        or values["saturatedQueuedTasks"] != values["queueCapacity"]
        or values["saturatedAdmissionWaiters"] < expected_waiters
        or values["saturatedAvailableAdmissions"] != 0
    ):
        issues.append("executor occupancy does not prove full worker, queue, and admission saturation")
    expected_admissions = values["appendWorkers"] + values["queueCapacity"]
    if (
        values["drainedActiveWorkers"] != 0
        or values["drainedQueuedTasks"] != 0
        or values["drainedAdmissionWaiters"] != 0
        or values["drainedAvailableAdmissions"] != expected_admissions
    ):
        issues.append("append executor did not return every worker, task, waiter, and admission")
    if values["coldReopens"] != values["resources"]:
        issues.append("not every saturated resource passed its cold projection reopen")

    if parsed.gc_pause_nanos and max(parsed.gc_pause_nanos) > MAX_STALL_NANOS:
        issues.append("young-GC pause exceeds 250 ms")
    if parsed.safepoint_nanos and max(parsed.safepoint_nanos) > MAX_STALL_NANOS:
        issues.append("safepoint exceeds 250 ms")
    if parsed.gc_pause_nanos and not parsed.safepoint_nanos:
        issues.append("young-GC events were present without safepoint event evidence")
    if parsed.gc_pause_nanos and parsed.humongous_region_samples < len(parsed.gc_pause_nanos):
        issues.append("young-GC events lack complete humongous-region evidence")
    return issues


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--expected-git-sha", required=True)
    parser.add_argument("--runtime-classpath", required=True)
    parser.add_argument(
        "--versioning-type",
        required=True,
        choices=("FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"),
    )
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    if re.fullmatch(r"[0-9a-f]{40}", args.expected_git_sha) is None:
        parser.error("--expected-git-sha must be a lowercase 40-character commit SHA")
    if not args.log.is_file():
        parser.error("--log must name a readable file")
    try:
        artifact_sha256 = runtime_classpath_sha256(args.runtime_classpath)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    parsed = parse(args.log)
    issues = _evaluate(parsed, args.expected_git_sha, artifact_sha256, args.versioning_type)
    evidence = parsed.records[0] if len(parsed.records) == 1 else {}
    configuration = parsed.configurations[0] if len(parsed.configurations) == 1 else {}
    manifest = {
        "kind": "append-saturation",
        "gitSha": args.expected_git_sha,
        "artifactSha256": artifact_sha256,
        "initialHeapBytes": configuration.get("initialHeapBytes"),
        "maxHeapBytes": configuration.get("maxHeapBytes"),
        "maxNewSizeBytes": configuration.get("maxNewSizeBytes"),
        "g1RegionSizeBytes": configuration.get("g1RegionSizeBytes"),
        **{name: evidence.get(name) for name in sorted(FIELDS)},
        "gcPauseCount": len(parsed.gc_pause_nanos),
        "gcPauseMaxNs": max(parsed.gc_pause_nanos, default=0),
        "safepointCount": len(parsed.safepoint_nanos),
        "safepointMaxNs": max(parsed.safepoint_nanos, default=0),
        "humongousRegionSamples": parsed.humongous_region_samples,
        "logSha256": sha256(args.log),
        "gateScriptSha256": sha256(Path(__file__)),
        "passed": not issues,
    }
    args.manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    for issue in issues:
        print(f"FAIL: {issue}")
    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())
