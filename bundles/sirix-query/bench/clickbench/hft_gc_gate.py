#!/usr/bin/env python3
"""Strict fixed-heap GC/safepoint gate for ClickBench ingestion.

The loader's combined stdout/stderr must contain exactly one measurement region:

    # HFT_MEASURE_START
    ... unified GC and safepoint log records ...
    # HFT_MEASURE_END

Only records inside that region are considered.  Startup, validation, profiler shutdown, and
Gradle noise therefore cannot make a run pass or fail.  The gate intentionally has no third-party
dependencies so it can run anywhere the ClickBench Gradle task can run.

Exit status:
    0  hard checks and retained-occupancy evidence pass
    1  a log is malformed or at least one hard/occupancy invariant is violated
    2  command-line usage error (from argparse)
    3  hard checks pass but retained-occupancy evidence is inconclusive
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

from hft_artifact import runtime_classpath_sha256


MEASURE_START = "# HFT_MEASURE_START"
MEASURE_END = "# HFT_MEASURE_END"
HFT_CONFIG_PREFIX = "# HFT_CONFIG "
HFT_BUILD_PREFIX = "# HFT_BUILD "
ASYNC_FLUSH_PREFIX = "# HFT_ASYNC_FLUSH "
PROJECTION_MAINTENANCE_PREFIX = "# HFT_PROJECTION_MAINTENANCE "
PROJECTION_EVIDENCE_PREFIX = "# HFT_PROJECTION_EVIDENCE "
PROJECTION_REVISION_PREFIX = "# HFT_PROJECTION_REVISION "

MIB = 1024 * 1024
GIB = 1024 * MIB

WARMUP_FRACTION = 0.20
DEFAULT_EXPECTED_HEAP_GIB = 4.0
DEFAULT_EXPECTED_MAX_NEW_MIB = 1024.0
DEFAULT_EXPECTED_SIDE_BATCH_MIB = 64.0
DEFAULT_MAX_PERMIT_WAIT_MS = 250.0
CANONICAL_MAX_FOREGROUND_WAIT_MS = 250.0
EXPECTED_G1_REGION_SIZE_BYTES = 4 * MIB
DEFAULT_SMALL_ROWS = 1_000_000
DEFAULT_LARGE_ROWS = 4_000_000
EXPECTED_AUTO_COMMIT_NODES = 4_194_304
EXPECTED_ASYNC_FLUSH_NODE_CAP = 16_384
EXPECTED_MAX_KVL_ATTEMPTED_PAGES_PER_EPOCH = 16
EXPECTED_MAX_NEW_BYTES = int(DEFAULT_EXPECTED_MAX_NEW_MIB * MIB)
EXPECTED_GLOBAL_DICTIONARY_MODE = "never"
EXPECTED_ARENA_STRATEGY = "shared"
EXPECTED_STORAGE = "FILE_CHANNEL"
EXPECTED_IMPORTER = "parallel-bulk"
EXPECTED_PROJECTION_MODE = "incremental"
EXPECTED_VERSIONING_TYPE = "FULL"
EXPECTED_PINNED_TRIE_SCAN_BUDGET = 1_024
EXPECTED_PINNED_TRIE_BATCH_CAPACITY = 64
DEFAULT_MIN_SMALL_SAMPLES = 5
DEFAULT_MIN_LARGE_SAMPLES = 20
MIN_PLATEAU_WINDOW = 10
MAX_PLATEAU_WINDOW = 50
PLATEAU_SPREAD_FRACTION = 0.03
PLATEAU_JITTER_REGION_MULTIPLIER = 3
POST_PLATEAU_MEDIAN_RATIO_LIMIT = 1.05
FINAL_HALF_GROWTH_FRACTION_LIMIT = 0.03
LATE_DECILE_MIN_ALLOWANCE_BYTES = 64 * MIB
LATE_DECILE_HEAP_ALLOWANCE_FRACTION = 0.03
CROSS_SCALE_MIN_ALLOWANCE_BYTES = 256 * MIB
CROSS_SCALE_HEAP_ALLOWANCE_FRACTION = 0.10


class Verdict(str, Enum):
    PASS = "PASS"
    INCONCLUSIVE = "INCONCLUSIVE"
    FAIL = "FAIL"

_SIZE_TOKEN = r"\d+(?:\.\d+)?(?:[BKMGTPE])?"
_OCCUPANCY_RE = re.compile(
    rf"(?P<before>{_SIZE_TOKEN})\s*->\s*(?P<after>{_SIZE_TOKEN})"
    rf"\s*\(\s*(?P<capacity>{_SIZE_TOKEN})\s*\)",
    re.IGNORECASE,
)
_DURATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ns|us|µs|ms|s)\b", re.IGNORECASE)
_SAFEPOINT_TOTAL_RE = re.compile(
    r"\bTotal:\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ns|us|µs|ms|s)\b",
    re.IGNORECASE,
)
_SAFEPOINT_TAG_RE = re.compile(r"\[(?:error|warning|info|debug|trace)\s*\]\[safepoint\]", re.IGNORECASE)
_SAFEPOINT_METADATA_RE = re.compile(
    r"^(?:Application time:|Entering safepoint region:|Leaving safepoint region|"
    r"Safepoint synchronization initiated using |Total time for which application threads were stopped:|"
    r"Waiting for \d+ thread\(s\) to block|Synchronization status:|JavaThread |VM Operation took )",
    re.IGNORECASE,
)
_G1_REGION_SIZE_RE = re.compile(
    rf"\bregion size\s+(?P<size>{_SIZE_TOKEN})\b",
    re.IGNORECASE,
)
_HUMONGOUS_REGIONS_RE = re.compile(
    r"\bHumongous regions:\s*(?P<before>\d+)\s*->\s*(?P<after>\d+)\b",
    re.IGNORECASE,
)

_ASYNC_FLUSH_FIELDS = (
    "combinedEpochs",
    "sideOnlyEpochs",
    "kvlPages",
    "kvlAttemptedPages",
    "kvlPromotedPages",
    "kvlAttemptedPagesMax",
    "sidePages",
    "sideBytes",
    "peakActiveSideBytes",
    "permitAcquires",
    "permitWaitTotalNs",
    "permitWaitMaxNs",
    "rotationPermitAcquires",
    "rotationPermitWaitTotalNs",
    "rotationPermitWaitMaxNs",
    "drainPermitAcquires",
    "drainPermitWaitTotalNs",
    "drainPermitWaitMaxNs",
    "workerRuns",
    "workerTotalNs",
    "workerMaxNs",
    "submitWaitCount",
    "submitWaitTotalNs",
    "submitWaitMaxNs",
    "callerThreadAppendRuns",
    "startFlushCount",
    "startFlushTotalNs",
    "startFlushMaxNs",
    "foregroundFlushCount",
    "foregroundFlushTotalNs",
    "foregroundFlushMaxNs",
    "finalDrainCount",
    "finalDrainTotalNs",
    "finalDrainMaxNs",
    "nativeReservoirCount",
    "nativeReservoirBytes",
    "kvlFrameCachePages",
    "kvlFrameCacheBytes",
    "kvlCacheFallbackPages",
    "kvlCacheFallbackBytes",
    "pinnedTrieSpillEpochs",
    "pinnedTrieSpillPages",
    "pinnedTrieSpillBatchMax",
    "pinnedTrieLiveMax",
    "pinnedTrieHighWater",
)

_HFT_CONFIG_FIELDS = (
    "globalDict",
    "autoCommitNodes",
    "asyncFlushNodeCap",
    "arenaStrategy",
    "maxNewSizeBytes",
    "initialHeapBytes",
    "maxHeapBytes",
    "g1RegionSizeBytes",
    "gcLogging",
    "safepointLogging",
    "storage",
    "importer",
    "projectionMode",
    "expectedRows",
    "pinnedTrieScanBudget",
    "pinnedTrieBatchCapacity",
    "versioningType",
    "appendWorkers",
    "appendQueueCapacity",
)

_PROJECTION_MAINTENANCE_FIELDS = (
    "commits",
    "dirtyRecords",
    "rowGroupsRead",
    "rowGroupsWritten",
    "dictionarySegments",
    "fenceChunksRead",
    "fenceChunksWritten",
    "setChunksRead",
    "setChunksWritten",
    "bloomRowGroupsRead",
    "bloomChunksWritten",
    "metadataReads",
    "metadataWrites",
    "dictionaryProbes",
    "storageReads",
    "storageWrites",
    "allocatorAllocations",
    "allocatorReleases",
    "tilReads",
    "tilWrites",
    "nativeAllocations",
    "nativeReleases",
    "asyncSubmissions",
    "asyncCompletions",
    "operations",
    "bytesRead",
    "bytesWritten",
    "fullRebuilds",
)

_PROJECTION_EVIDENCE_FIELDS = (
    "revisionsVerified",
    "historicalRevisions",
    "oracleRows",
    "servedRows",
    "oracleMatches",
    "servedMatches",
    "servedRevisions",
    "stableAnchors",
    "stableIds",
    "successorSegments",
    "introductionRevision",
    "maxProbeUnits",
)

_PROJECTION_REVISION_FIELDS = (
    "revision",
    "oracleRows",
    "servedRows",
    "oracleMatches",
    "servedMatches",
    "anchor",
    "oldId",
    "newId",
    "successorSegments",
)

# These are not warning-only events.  A fixed-heap run containing any one of them has failed to
# demonstrate that young collections alone can sustain the workload.  Keep the names separate so
# the report says exactly which invariant was violated.
_FORBIDDEN_EVENTS: tuple[tuple[str, re.Pattern[str]], ...] = (
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
    (
        "mixed collection",
        re.compile(r"\bPause Young \((?:Prepare )?Mixed\)", re.IGNORECASE),
    ),
    ("to-space exhaustion", re.compile(r"\bTo-space exhausted\b", re.IGNORECASE)),
    ("evacuation failure", re.compile(r"\bEvacuation Failure\b", re.IGNORECASE)),
    ("allocation failure", re.compile(r"\bAllocation Failure\b", re.IGNORECASE)),
    ("allocation stall", re.compile(r"\bAllocation Stall\b", re.IGNORECASE)),
    ("GCLocker collection", re.compile(r"\bGCLocker Initiated GC\b", re.IGNORECASE)),
    ("explicit collection", re.compile(r"\bSystem\.gc\(\)|Diagnostic Command", re.IGNORECASE)),
    ("preventive collection", re.compile(r"\bPreventive Collection\b", re.IGNORECASE)),
    ("humongous-allocation collection", re.compile(r"\bHumongous Allocation\b", re.IGNORECASE)),
    ("out of memory", re.compile(r"\bOutOfMemoryError\b", re.IGNORECASE)),
)


@dataclass(frozen=True)
class YoungGcSample:
    line_number: int
    before_bytes: int
    after_bytes: int
    capacity_bytes: int
    pause_nanos: int


@dataclass(frozen=True)
class ForbiddenEvent:
    line_number: int
    kind: str
    text: str


@dataclass(frozen=True)
class AsyncFlushTelemetry:
    line_number: int
    values: dict[str, int]


@dataclass(frozen=True)
class HftConfiguration:
    line_number: int
    values: dict[str, str]


@dataclass(frozen=True)
class HftBuild:
    line_number: int
    git_sha: str
    artifact_sha256: str


@dataclass(frozen=True)
class ProjectionMaintenanceTelemetry:
    line_number: int
    values: dict[str, int]


@dataclass(frozen=True)
class ProjectionEvidence:
    line_number: int
    values: dict[str, int]


@dataclass(frozen=True)
class ProjectionRevisionEvidence:
    line_number: int
    values: dict[str, int]


@dataclass
class ParsedRun:
    source: str
    young_samples: list[YoungGcSample] = field(default_factory=list)
    safepoint_nanos: list[int] = field(default_factory=list)
    hft_configurations: list[HftConfiguration] = field(default_factory=list)
    hft_builds: list[HftBuild] = field(default_factory=list)
    async_flush_telemetry: list[AsyncFlushTelemetry] = field(default_factory=list)
    projection_maintenance_telemetry: list[ProjectionMaintenanceTelemetry] = field(default_factory=list)
    projection_evidence: list[ProjectionEvidence] = field(default_factory=list)
    projection_revision_evidence: list[ProjectionRevisionEvidence] = field(default_factory=list)
    forbidden_events: list[ForbiddenEvent] = field(default_factory=list)
    g1_region_sizes: set[int] = field(default_factory=set)
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PlateauCandidate:
    start: int
    center_bytes: float
    spread_bytes: int
    spread_allowance_bytes: float
    projected_growth_bytes: float
    growth_allowance_bytes: float


@dataclass
class RunEvaluation:
    label: str
    parsed: ParsedRun
    issues: list[str] = field(default_factory=list)
    capacity_bytes: int | None = None
    warmup_samples: int = 0
    analysis_samples: int = 0
    g1_region_size_bytes: int | None = None
    plateau_sample: int | None = None
    post_plateau_samples: int = 0
    plateau_spread_bytes: int | None = None
    plateau_spread_allowance_bytes: float | None = None
    plateau_local_growth_bytes: float | None = None
    plateau_local_growth_allowance_bytes: float | None = None
    early_median_bytes: float | None = None
    late_median_bytes: float | None = None
    median_ratio: float | None = None
    steady_bytes: float | None = None
    late_decile_growth_bytes: float | None = None
    projected_final_half_growth_bytes: float | None = None
    normalized_final_half_growth: float | None = None
    max_young_pause_nanos: int | None = None
    max_safepoint_nanos: int | None = None
    max_rotation_permit_wait_nanos: int | None = None
    max_foreground_flush_nanos: int | None = None
    max_drain_permit_wait_nanos: int | None = None
    attempted_kvl_pages: int | None = None
    promoted_kvl_pages: int | None = None
    staged_side_pages: int | None = None
    staged_side_bytes: int | None = None
    peak_active_side_bytes: int | None = None
    append_worker_runs: int | None = None
    pinned_trie_spill_epochs: int | None = None
    pinned_trie_spill_pages: int | None = None
    pinned_trie_spill_batch_max: int | None = None
    pinned_trie_live_max: int | None = None
    pinned_trie_high_water: int | None = None
    zero_young_events: bool = False
    occupancy_verdict: Verdict = Verdict.INCONCLUSIVE
    inconclusive_reasons: list[str] = field(default_factory=list)

    @property
    def verdict(self) -> Verdict:
        if self.issues:
            return Verdict.FAIL
        return self.occupancy_verdict

    @property
    def passed(self) -> bool:
        return self.verdict is Verdict.PASS


@dataclass
class PairEvaluation:
    one_million: RunEvaluation
    four_million: RunEvaluation
    cross_scale_issues: list[str] = field(default_factory=list)
    cross_scale_inconclusive_reasons: list[str] = field(default_factory=list)
    cross_scale_verdict: Verdict = Verdict.INCONCLUSIVE
    cross_scale_growth_bytes: float | None = None
    cross_scale_allowance_bytes: float | None = None

    @property
    def occupancy_verdict(self) -> Verdict:
        verdicts = (
            self.one_million.occupancy_verdict,
            self.four_million.occupancy_verdict,
            self.cross_scale_verdict,
        )
        if Verdict.FAIL in verdicts:
            return Verdict.FAIL
        if Verdict.INCONCLUSIVE in verdicts:
            return Verdict.INCONCLUSIVE
        return Verdict.PASS

    @property
    def verdict(self) -> Verdict:
        verdicts = (
            self.one_million.verdict,
            self.four_million.verdict,
            self.cross_scale_verdict,
        )
        if Verdict.FAIL in verdicts or self.cross_scale_issues:
            return Verdict.FAIL
        if Verdict.INCONCLUSIVE in verdicts:
            return Verdict.INCONCLUSIVE
        return Verdict.PASS

    @property
    def passed(self) -> bool:
        return self.verdict is Verdict.PASS


def _is_marker(line: str, marker: str) -> bool:
    stripped = line.strip()
    return stripped == marker or stripped.startswith(marker + " ")


def _parse_size_bytes(token: str) -> int:
    normalized = token.strip().upper()
    if not normalized:
        raise ValueError("empty size")
    unit = normalized[-1] if normalized[-1].isalpha() else "B"
    number_text = normalized[:-1] if normalized[-1].isalpha() else normalized
    multipliers = {
        "B": 1,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
        "P": 1024**5,
        "E": 1024**6,
    }
    if unit not in multipliers:
        raise ValueError(f"unknown size unit in {token!r}")
    return int(float(number_text) * multipliers[unit])


def _duration_nanos(value: str, unit: str) -> int:
    multipliers = {
        "ns": 1.0,
        "us": 1_000.0,
        "µs": 1_000.0,
        "ms": 1_000_000.0,
        "s": 1_000_000_000.0,
    }
    return int(float(value) * multipliers[unit.lower()])


def parse_lines(lines: Iterable[str], source: str = "<memory>") -> ParsedRun:
    """Parse one combined application/unified-log stream.

    Exactly one complete measurement region is required.  All GC-like text outside that region is
    ignored deliberately.
    """

    parsed = ParsedRun(source=source)
    in_measurement = False
    start_seen = False
    end_seen = False

    for line_number, line in enumerate(lines, start=1):
        if _is_marker(line, MEASURE_START):
            if in_measurement:
                parsed.errors.append(f"line {line_number}: nested {MEASURE_START} marker")
            elif start_seen:
                parsed.errors.append(f"line {line_number}: duplicate {MEASURE_START} marker")
            else:
                start_seen = True
                in_measurement = True
            continue

        if _is_marker(line, MEASURE_END):
            if not in_measurement:
                parsed.errors.append(f"line {line_number}: {MEASURE_END} without an active measurement")
            elif end_seen:
                parsed.errors.append(f"line {line_number}: duplicate {MEASURE_END} marker")
            else:
                in_measurement = False
                end_seen = True
            continue

        if not in_measurement:
            continue

        stripped = line.strip()
        region_size = _G1_REGION_SIZE_RE.search(line)
        if region_size is not None:
            try:
                parsed.g1_region_sizes.add(_parse_size_bytes(region_size.group("size")))
            except ValueError as error:
                parsed.errors.append(f"line {line_number}: {error}")

        if stripped.startswith(HFT_CONFIG_PREFIX):
            values: dict[str, str] = {}
            malformed = False
            for token in stripped[len(HFT_CONFIG_PREFIX) :].split():
                name, separator, value = token.partition("=")
                if not separator or not name or not value:
                    parsed.errors.append(f"line {line_number}: malformed HFT config token {token!r}")
                    malformed = True
                    continue
                if name in values:
                    parsed.errors.append(f"line {line_number}: duplicate HFT config field {name!r}")
                    malformed = True
                    continue
                values[name] = value
            missing = [name for name in _HFT_CONFIG_FIELDS if name not in values]
            unknown = [name for name in values if name not in _HFT_CONFIG_FIELDS]
            if missing:
                parsed.errors.append(
                    f"line {line_number}: HFT config missing fields: {', '.join(missing)}"
                )
                malformed = True
            if unknown:
                parsed.errors.append(
                    f"line {line_number}: unknown HFT config fields: {', '.join(unknown)}"
                )
                malformed = True
            if not malformed:
                parsed.hft_configurations.append(HftConfiguration(line_number, values))
            continue

        if stripped.startswith(HFT_BUILD_PREFIX):
            tokens = stripped[len(HFT_BUILD_PREFIX) :].split()
            fields = dict(token.partition("=")[::2] for token in tokens if token.count("=") == 1)
            if len(tokens) != 2 or set(fields) != {"gitSha", "artifactSha256"}:
                parsed.errors.append(f"line {line_number}: malformed HFT build record")
            else:
                git_sha = fields["gitSha"]
                artifact_sha256 = fields["artifactSha256"]
                if re.fullmatch(r"[0-9a-f]{40}", git_sha) is None:
                    parsed.errors.append(f"line {line_number}: invalid HFT build git SHA")
                elif re.fullmatch(r"[0-9a-f]{64}", artifact_sha256) is None:
                    parsed.errors.append(f"line {line_number}: invalid HFT artifact SHA-256")
                else:
                    parsed.hft_builds.append(HftBuild(line_number, git_sha, artifact_sha256))
            continue

        if stripped.startswith(ASYNC_FLUSH_PREFIX):
            values: dict[str, int] = {}
            malformed = False
            for token in stripped[len(ASYNC_FLUSH_PREFIX) :].split():
                name, separator, raw_value = token.partition("=")
                if not separator or not name or not raw_value:
                    parsed.errors.append(f"line {line_number}: malformed async-flush token {token!r}")
                    malformed = True
                    continue
                try:
                    value = int(raw_value)
                except ValueError:
                    parsed.errors.append(f"line {line_number}: non-integer async-flush value {token!r}")
                    malformed = True
                    continue
                if value < 0:
                    parsed.errors.append(f"line {line_number}: negative async-flush value {token!r}")
                    malformed = True
                    continue
                if name in values:
                    parsed.errors.append(f"line {line_number}: duplicate async-flush field {name!r}")
                    malformed = True
                    continue
                values[name] = value
            missing = [name for name in _ASYNC_FLUSH_FIELDS if name not in values]
            unknown = [name for name in values if name not in _ASYNC_FLUSH_FIELDS]
            if missing:
                parsed.errors.append(
                    f"line {line_number}: async-flush telemetry missing fields: {', '.join(missing)}"
                )
                malformed = True
            if unknown:
                parsed.errors.append(
                    f"line {line_number}: unknown async-flush telemetry fields: {', '.join(unknown)}"
                )
                malformed = True
            if not malformed:
                parsed.async_flush_telemetry.append(AsyncFlushTelemetry(line_number, values))
            continue

        if stripped.startswith(PROJECTION_MAINTENANCE_PREFIX):
            values: dict[str, int] = {}
            malformed = False
            for token in stripped[len(PROJECTION_MAINTENANCE_PREFIX) :].split():
                name, separator, raw_value = token.partition("=")
                if not separator or not name or not raw_value:
                    parsed.errors.append(
                        f"line {line_number}: malformed projection-maintenance token {token!r}"
                    )
                    malformed = True
                    continue
                try:
                    value = int(raw_value)
                except ValueError:
                    parsed.errors.append(
                        f"line {line_number}: non-integer projection-maintenance value {token!r}"
                    )
                    malformed = True
                    continue
                if value < 0:
                    parsed.errors.append(
                        f"line {line_number}: negative projection-maintenance value {token!r}"
                    )
                    malformed = True
                    continue
                if name in values:
                    parsed.errors.append(
                        f"line {line_number}: duplicate projection-maintenance field {name!r}"
                    )
                    malformed = True
                    continue
                values[name] = value
            missing = [name for name in _PROJECTION_MAINTENANCE_FIELDS if name not in values]
            unknown = [name for name in values if name not in _PROJECTION_MAINTENANCE_FIELDS]
            if missing:
                parsed.errors.append(
                    f"line {line_number}: projection-maintenance telemetry missing fields: "
                    + ", ".join(missing)
                )
                malformed = True
            if unknown:
                parsed.errors.append(
                    f"line {line_number}: unknown projection-maintenance telemetry fields: "
                    + ", ".join(unknown)
                )
                malformed = True
            if not malformed:
                parsed.projection_maintenance_telemetry.append(
                    ProjectionMaintenanceTelemetry(line_number, values)
                )
            continue

        if stripped.startswith(PROJECTION_EVIDENCE_PREFIX):
            values: dict[str, int] = {}
            malformed = False
            for token in stripped[len(PROJECTION_EVIDENCE_PREFIX) :].split():
                name, separator, raw_value = token.partition("=")
                if not separator or not name or not raw_value:
                    parsed.errors.append(
                        f"line {line_number}: malformed projection-evidence token {token!r}"
                    )
                    malformed = True
                    continue
                try:
                    value = int(raw_value)
                except ValueError:
                    parsed.errors.append(
                        f"line {line_number}: non-integer projection-evidence value {token!r}"
                    )
                    malformed = True
                    continue
                if value < 0:
                    parsed.errors.append(
                        f"line {line_number}: negative projection-evidence value {token!r}"
                    )
                    malformed = True
                    continue
                if name in values:
                    parsed.errors.append(
                        f"line {line_number}: duplicate projection-evidence field {name!r}"
                    )
                    malformed = True
                    continue
                values[name] = value
            missing = [name for name in _PROJECTION_EVIDENCE_FIELDS if name not in values]
            unknown = [name for name in values if name not in _PROJECTION_EVIDENCE_FIELDS]
            if missing:
                parsed.errors.append(
                    f"line {line_number}: projection-evidence missing fields: " + ", ".join(missing)
                )
                malformed = True
            if unknown:
                parsed.errors.append(
                    f"line {line_number}: unknown projection-evidence fields: " + ", ".join(unknown)
                )
                malformed = True
            if not malformed:
                parsed.projection_evidence.append(ProjectionEvidence(line_number, values))
            continue

        if stripped.startswith(PROJECTION_REVISION_PREFIX):
            values: dict[str, int] = {}
            malformed = False
            for token in stripped[len(PROJECTION_REVISION_PREFIX) :].split():
                name, separator, raw_value = token.partition("=")
                if not separator or not name or not raw_value:
                    parsed.errors.append(
                        f"line {line_number}: malformed projection-revision token {token!r}"
                    )
                    malformed = True
                    continue
                try:
                    value = int(raw_value)
                except ValueError:
                    parsed.errors.append(
                        f"line {line_number}: non-integer projection-revision value {token!r}"
                    )
                    malformed = True
                    continue
                if value < 0:
                    parsed.errors.append(
                        f"line {line_number}: negative projection-revision value {token!r}"
                    )
                    malformed = True
                    continue
                if name in values:
                    parsed.errors.append(
                        f"line {line_number}: duplicate projection-revision field {name!r}"
                    )
                    malformed = True
                    continue
                values[name] = value
            missing = [name for name in _PROJECTION_REVISION_FIELDS if name not in values]
            unknown = [name for name in values if name not in _PROJECTION_REVISION_FIELDS]
            if missing:
                parsed.errors.append(
                    f"line {line_number}: projection-revision evidence missing fields: "
                    + ", ".join(missing)
                )
                malformed = True
            if unknown:
                parsed.errors.append(
                    f"line {line_number}: unknown projection-revision evidence fields: "
                    + ", ".join(unknown)
                )
                malformed = True
            if not malformed:
                parsed.projection_revision_evidence.append(
                    ProjectionRevisionEvidence(line_number, values)
                )
            continue

        humongous_regions = _HUMONGOUS_REGIONS_RE.search(line)
        if (humongous_regions is not None and (
            int(humongous_regions.group("before")) > 0
            or int(humongous_regions.group("after")) > 0
        )):
            parsed.forbidden_events.append(
                ForbiddenEvent(line_number, "humongous-region occupancy", line.strip())
            )

        for kind, pattern in _FORBIDDEN_EVENTS:
            if pattern.search(line):
                parsed.forbidden_events.append(ForbiddenEvent(line_number, kind, line.strip()))
                break

        if "Pause Young" in line:
            occupancy = _OCCUPANCY_RE.search(line)
            durations = list(_DURATION_RE.finditer(line))
            # `-Xlog:gc*` emits a `gc,start` header and a later completed `gc` record for every
            # pause. The header deliberately has no occupancy or duration; it is not a malformed
            # sample and must not make every real unified log fail closed.
            if occupancy is None and "[gc,start" in line.lower():
                continue
            if occupancy is None or not durations:
                parsed.errors.append(
                    f"line {line_number}: could not parse Pause Young occupancy/duration: {line.strip()}"
                )
            else:
                duration = durations[-1]
                try:
                    parsed.young_samples.append(
                        YoungGcSample(
                            line_number=line_number,
                            before_bytes=_parse_size_bytes(occupancy.group("before")),
                            after_bytes=_parse_size_bytes(occupancy.group("after")),
                            capacity_bytes=_parse_size_bytes(occupancy.group("capacity")),
                            pause_nanos=_duration_nanos(duration.group("value"), duration.group("unit")),
                        )
                    )
                except ValueError as error:
                    parsed.errors.append(f"line {line_number}: {error}")

        safepoint_tag = _SAFEPOINT_TAG_RE.search(line)
        if safepoint_tag is not None or "Safepoint " in line:
            total = _SAFEPOINT_TOTAL_RE.search(line)
            if total is not None:
                parsed.safepoint_nanos.append(_duration_nanos(total.group("value"), total.group("unit")))
            else:
                payload = line[safepoint_tag.end() :].strip() if safepoint_tag is not None else line.strip()
                if _SAFEPOINT_METADATA_RE.match(payload) is not None:
                    continue
                parsed.errors.append(
                    f"line {line_number}: could not parse safepoint Total duration: {line.strip()}"
                )

    if not start_seen:
        parsed.errors.append(f"missing {MEASURE_START} marker")
    if in_measurement:
        parsed.errors.append(f"missing {MEASURE_END} marker")
    elif start_seen and not end_seen:
        parsed.errors.append(f"missing {MEASURE_END} marker")
    return parsed


def parse_log(path: Path) -> ParsedRun:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as source:
            return parse_lines(source, str(path))
    except OSError as error:
        parsed = ParsedRun(source=str(path))
        parsed.errors.append(f"cannot read log: {error}")
        return parsed


def _median(values: Sequence[int]) -> float:
    return float(statistics.median(values))


def _find_plateau(
    values: Sequence[int],
    window: int,
    minimum_post_samples: int,
    g1_region_size_bytes: int,
) -> PlateauCandidate | None:
    # Search backwards. An early locally-flat shelf followed by higher stable post-young occupancy
    # is warm-up, not the run's plateau; selecting it would make a genuinely bounded run fail the
    # later/earlier ratio even when the tail is demonstrably flat. The post-plateau sample floor
    # prevents choosing a cosmetically flat final handful with too little evidence. At low
    # post-young occupancy, normal G1 survivor-target movement is quantized in regions and can
    # exceed three percent even though the retained cohort is stationary. The absolute allowance is
    # therefore floored at three actual G1 regions. Keep trend separate from jitter: the region
    # allowance must not turn a monotonic ramp into a plateau.
    latest_start = min(len(values) - window, len(values) - minimum_post_samples)
    region_jitter_allowance = g1_region_size_bytes * PLATEAU_JITTER_REGION_MULTIPLIER
    for start in range(latest_start, -1, -1):
        sample_window = values[start : start + window]
        center = _median(sample_window)
        relative_allowance = center * PLATEAU_SPREAD_FRACTION
        spread_allowance = max(relative_allowance, region_jitter_allowance)
        spread = max(sample_window) - min(sample_window)
        projected_growth = _ols_projected_growth(sample_window)
        if spread <= spread_allowance and projected_growth <= relative_allowance:
            return PlateauCandidate(
                start=start,
                center_bytes=center,
                spread_bytes=spread,
                spread_allowance_bytes=spread_allowance,
                projected_growth_bytes=projected_growth,
                growth_allowance_bytes=relative_allowance,
            )
    return None


def _ols_projected_growth(values: Sequence[int]) -> float:
    if len(values) < 2:
        return 0.0
    x_mean = (len(values) - 1) / 2.0
    y_mean = sum(values) / len(values)
    numerator = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(values))
    denominator = sum((index - x_mean) ** 2 for index in range(len(values)))
    slope = numerator / denominator if denominator else 0.0
    return slope * (len(values) - 1)


def evaluate_run(
    parsed: ParsedRun,
    label: str,
    min_samples: int = DEFAULT_MIN_LARGE_SAMPLES,
    expected_capacity_bytes: int = int(DEFAULT_EXPECTED_HEAP_GIB * GIB),
    expected_side_batch_bytes: int = int(DEFAULT_EXPECTED_SIDE_BATCH_MIB * MIB),
    max_rotation_permit_wait_nanos: int = int(DEFAULT_MAX_PERMIT_WAIT_MS * 1_000_000),
    expected_rows: int | None = None,
    expected_max_new_bytes: int = EXPECTED_MAX_NEW_BYTES,
    expected_git_sha: str | None = None,
    expected_artifact_sha256: str | None = None,
    expected_versioning_type: str = EXPECTED_VERSIONING_TYPE,
) -> RunEvaluation:
    if min_samples < 2:
        raise ValueError("min_samples must be at least 2")
    if max_rotation_permit_wait_nanos > int(CANONICAL_MAX_FOREGROUND_WAIT_MS * 1_000_000):
        raise ValueError("the canonical foreground wait bound cannot exceed 250 ms")
    evaluation = RunEvaluation(label=label, parsed=parsed)
    evaluation.issues.extend(parsed.errors)

    for event in parsed.forbidden_events:
        evaluation.issues.append(
            f"line {event.line_number}: forbidden {event.kind}: {event.text}"
        )

    if len(parsed.hft_builds) != 1:
        evaluation.issues.append(
            f"expected exactly one {HFT_BUILD_PREFIX.strip()} record, found {len(parsed.hft_builds)}"
        )
    else:
        build = parsed.hft_builds[0]
        if expected_git_sha is not None and build.git_sha != expected_git_sha:
            evaluation.issues.append(
                f"line {build.line_number}: HFT build SHA {build.git_sha} does not match {expected_git_sha}"
            )
        if expected_artifact_sha256 is not None and build.artifact_sha256 != expected_artifact_sha256:
            evaluation.issues.append(
                f"line {build.line_number}: HFT artifact SHA-256 {build.artifact_sha256} "
                f"does not match {expected_artifact_sha256}"
            )

    configuration_values: dict[str, str] | None = None
    if len(parsed.hft_configurations) != 1:
        evaluation.issues.append(
            f"expected exactly one {HFT_CONFIG_PREFIX.strip()} record, "
            f"found {len(parsed.hft_configurations)}"
        )
    else:
        configuration = parsed.hft_configurations[0]
        values = configuration.values
        configuration_values = values
        expected_values = {
            "globalDict": EXPECTED_GLOBAL_DICTIONARY_MODE,
            "autoCommitNodes": str(EXPECTED_AUTO_COMMIT_NODES),
            "asyncFlushNodeCap": str(EXPECTED_ASYNC_FLUSH_NODE_CAP),
            "arenaStrategy": EXPECTED_ARENA_STRATEGY,
            "maxNewSizeBytes": str(expected_max_new_bytes),
            "initialHeapBytes": str(expected_capacity_bytes),
            "maxHeapBytes": str(expected_capacity_bytes),
            "g1RegionSizeBytes": str(EXPECTED_G1_REGION_SIZE_BYTES),
            "storage": EXPECTED_STORAGE,
            "importer": EXPECTED_IMPORTER,
            "projectionMode": EXPECTED_PROJECTION_MODE,
            "pinnedTrieScanBudget": str(EXPECTED_PINNED_TRIE_SCAN_BUDGET),
            "pinnedTrieBatchCapacity": str(EXPECTED_PINNED_TRIE_BATCH_CAPACITY),
            "versioningType": expected_versioning_type,
        }
        if expected_rows is not None:
            expected_values["expectedRows"] = str(expected_rows)
        for name, expected_value in expected_values.items():
            if values[name] != expected_value:
                evaluation.issues.append(
                    f"line {configuration.line_number}: HFT config {name}={values[name]!r}, "
                    f"expected {expected_value!r}"
                )
        if values["gcLogging"] != "true":
            evaluation.issues.append(
                f"line {configuration.line_number}: HFT config effective GC logging is not enabled"
            )
        if values["safepointLogging"] != "true":
            evaluation.issues.append(
                f"line {configuration.line_number}: HFT config effective safepoint logging is not enabled"
            )
        try:
            configured_region_size = int(values["g1RegionSizeBytes"])
        except ValueError:
            evaluation.issues.append(
                f"line {configuration.line_number}: g1RegionSizeBytes is not an integer"
            )
        else:
            if configured_region_size <= 0:
                evaluation.issues.append(
                    f"line {configuration.line_number}: g1RegionSizeBytes must be positive"
                )
            else:
                evaluation.g1_region_size_bytes = configured_region_size
        evaluation.capacity_bytes = expected_capacity_bytes
        if values["appendWorkers"] not in {"1", "2"}:
            evaluation.issues.append(
                f"line {configuration.line_number}: HFT config appendWorkers must be 1 or 2"
            )
        if values["appendQueueCapacity"] != "1":
            evaluation.issues.append(
                f"line {configuration.line_number}: HFT config appendQueueCapacity must be 1"
            )

    if len(parsed.async_flush_telemetry) != 1:
        evaluation.issues.append(
            f"expected exactly one {ASYNC_FLUSH_PREFIX.strip()} record, "
            f"found {len(parsed.async_flush_telemetry)}"
        )
    else:
        telemetry = parsed.async_flush_telemetry[0]
        values = telemetry.values
        epochs = values["combinedEpochs"] + values["sideOnlyEpochs"]
        evaluation.staged_side_pages = values["sidePages"]
        evaluation.staged_side_bytes = values["sideBytes"]
        evaluation.peak_active_side_bytes = values["peakActiveSideBytes"]
        evaluation.append_worker_runs = values["workerRuns"]
        evaluation.attempted_kvl_pages = values["kvlAttemptedPages"]
        evaluation.promoted_kvl_pages = values["kvlPromotedPages"]
        evaluation.pinned_trie_spill_epochs = values["pinnedTrieSpillEpochs"]
        evaluation.pinned_trie_spill_pages = values["pinnedTrieSpillPages"]
        evaluation.pinned_trie_spill_batch_max = values["pinnedTrieSpillBatchMax"]
        evaluation.pinned_trie_live_max = values["pinnedTrieLiveMax"]
        evaluation.pinned_trie_high_water = values["pinnedTrieHighWater"]
        if values["sidePages"] == 0 or values["sideBytes"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: no projection side pages entered the bounded append path"
            )
        if epochs == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: staged side-page path produced no append epoch"
            )
        if values["workerRuns"] != epochs:
            evaluation.issues.append(
                f"line {telemetry.line_number}: workerRuns={values['workerRuns']} but epochs={epochs}"
            )
        if values["submitWaitCount"] != epochs:
            evaluation.issues.append(
                f"line {telemetry.line_number}: submitWaitCount={values['submitWaitCount']} but epochs={epochs}"
            )
        if values["callerThreadAppendRuns"] != 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: caller thread executed "
                f"{values['callerThreadAppendRuns']} append task(s)"
            )
        if values["permitAcquires"] < values["workerRuns"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: fewer permit acquires than worker runs"
            )
        if values["rotationPermitAcquires"] < values["workerRuns"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: fewer rotation permit acquires than worker runs"
            )
        if values["pinnedTrieSpillEpochs"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie path produced no spill epoch"
            )
        if values["pinnedTrieSpillPages"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: no pinned trie pages entered the bounded spill path"
            )
        if values["pinnedTrieSpillBatchMax"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie spill batch maximum is zero"
            )
        elif values["pinnedTrieSpillBatchMax"] > EXPECTED_PINNED_TRIE_BATCH_CAPACITY:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie spill batch maximum "
                f"{values['pinnedTrieSpillBatchMax']} exceeds configured capacity "
                f"{EXPECTED_PINNED_TRIE_BATCH_CAPACITY}"
            )
        if values["pinnedTrieSpillEpochs"] > values["pinnedTrieSpillPages"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie spill epochs exceed published pages"
            )
        if values["pinnedTrieSpillEpochs"] > values["combinedEpochs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie spill epochs exceed full snapshot epochs"
            )
        max_pages_for_reported_epochs = (
            values["pinnedTrieSpillEpochs"] * EXPECTED_PINNED_TRIE_BATCH_CAPACITY
        )
        if values["pinnedTrieSpillPages"] > max_pages_for_reported_epochs:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie spill pages "
                f"{values['pinnedTrieSpillPages']} exceed {values['pinnedTrieSpillEpochs']} epochs x "
                f"configured capacity {EXPECTED_PINNED_TRIE_BATCH_CAPACITY}"
            )
        if values["pinnedTrieSpillBatchMax"] > values["pinnedTrieSpillPages"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie spill batch maximum exceeds published pages"
            )
        if values["pinnedTrieLiveMax"] > values["pinnedTrieHighWater"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: pinned trie live maximum exceeds append-only high-water"
            )
        if values["peakActiveSideBytes"] > expected_side_batch_bytes:
            evaluation.issues.append(
                f"line {telemetry.line_number}: peak active side payload "
                f"{_format_bytes(values['peakActiveSideBytes'])} exceeds configured batch "
                f"{_format_bytes(expected_side_batch_bytes)}"
            )
        if values["nativeReservoirCount"] != 2:
            evaluation.issues.append(
                f"line {telemetry.line_number}: native reservoir count "
                f"{values['nativeReservoirCount']} is not 2"
            )
        if values["nativeReservoirBytes"] != expected_side_batch_bytes:
            evaluation.issues.append(
                f"line {telemetry.line_number}: native reservoir capacity "
                f"{_format_bytes(values['nativeReservoirBytes'])} does not match configured batch "
                f"{_format_bytes(expected_side_batch_bytes)}"
            )
        if values["kvlFrameCachePages"] == 0 or values["kvlFrameCacheBytes"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: no async KVL encoded cache reused its native frame"
            )
        accounted_attempts = values["kvlPages"] + values["kvlPromotedPages"]
        if accounted_attempts != values["kvlAttemptedPages"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: async KVL attempt coverage is incomplete: "
                f"appended={values['kvlPages']} + promoted={values['kvlPromotedPages']} "
                f"!= attempted={values['kvlAttemptedPages']}"
            )
        if values["kvlPromotedPages"] != 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: {values['kvlPromotedPages']} async KVL pages were "
                "promoted back to the live TIL instead of using the disposable native-frame path"
            )
        if values["kvlAttemptedPagesMax"] > EXPECTED_MAX_KVL_ATTEMPTED_PAGES_PER_EPOCH:
            evaluation.issues.append(
                f"line {telemetry.line_number}: maximum attempted KVL pages per epoch "
                f"{values['kvlAttemptedPagesMax']} exceeds bounded serializer window "
                f"{EXPECTED_MAX_KVL_ATTEMPTED_PAGES_PER_EPOCH}"
            )
        if values["kvlAttemptedPagesMax"] > values["kvlAttemptedPages"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: maximum attempted KVL pages per epoch exceeds "
                "the total attempted KVL pages"
            )
        if values["kvlAttemptedPages"] > 0 and values["kvlAttemptedPagesMax"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: maximum attempted KVL pages per epoch is zero "
                "despite positive attempted-page telemetry"
            )
        if values["kvlAttemptedPages"] > values["kvlAttemptedPagesMax"] * values["combinedEpochs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: total attempted KVL pages exceed the reported "
                "per-epoch maximum times combined epochs"
            )
        accounted_kvl_pages = values["kvlFrameCachePages"] + values["kvlCacheFallbackPages"]
        if accounted_kvl_pages != values["kvlPages"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: async KVL cache coverage is incomplete: "
                f"frame={values['kvlFrameCachePages']} + fallback={values['kvlCacheFallbackPages']} "
                f"!= appended KVL pages={values['kvlPages']}"
            )
        if values["kvlCacheFallbackPages"] != 0 or values["kvlCacheFallbackBytes"] != 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: {values['kvlCacheFallbackPages']} async KVL caches "
                f"({_format_bytes(values['kvlCacheFallbackBytes'])}) remained outside native frames"
            )
        if values["permitWaitTotalNs"] < values["permitWaitMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: permit wait maximum exceeds its total"
            )
        if values["rotationPermitWaitTotalNs"] < values["rotationPermitWaitMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: rotation permit wait maximum exceeds its total"
            )
        if values["drainPermitWaitTotalNs"] < values["drainPermitWaitMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: drain permit wait maximum exceeds its total"
            )
        split_acquires = values["rotationPermitAcquires"] + values["drainPermitAcquires"]
        if split_acquires != values["permitAcquires"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: split permit acquires {split_acquires} "
                f"!= aggregate {values['permitAcquires']}"
            )
        split_wait_total = values["rotationPermitWaitTotalNs"] + values["drainPermitWaitTotalNs"]
        if split_wait_total != values["permitWaitTotalNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: split permit wait total {split_wait_total} "
                f"!= aggregate {values['permitWaitTotalNs']}"
            )
        split_wait_max = max(values["rotationPermitWaitMaxNs"], values["drainPermitWaitMaxNs"])
        if split_wait_max != values["permitWaitMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: split permit wait maximum {split_wait_max} "
                f"!= aggregate {values['permitWaitMaxNs']}"
            )
        if values["workerTotalNs"] < values["workerMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: worker maximum exceeds its total"
            )
        if values["submitWaitTotalNs"] < values["submitWaitMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: submit wait maximum exceeds its total"
            )
        if values["startFlushTotalNs"] < values["startFlushMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: whole startAsyncFlush maximum exceeds its total"
            )
        if values["startFlushCount"] < epochs:
            evaluation.issues.append(
                f"line {telemetry.line_number}: fewer whole startAsyncFlush calls than append epochs"
            )
        if values["foregroundFlushCount"] != values["combinedEpochs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: foreground async-flush calls do not equal combined epochs"
            )
        if values["foregroundFlushTotalNs"] < values["foregroundFlushMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: foreground async-flush maximum exceeds its total"
            )
        if values["finalDrainTotalNs"] < values["finalDrainMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: whole final-drain maximum exceeds its total"
            )
        if values["startFlushMaxNs"] < max(
            values["rotationPermitWaitMaxNs"], values["submitWaitMaxNs"]
        ):
            evaluation.issues.append(
                f"line {telemetry.line_number}: whole startAsyncFlush maximum is smaller than a component wait"
            )
        if values["finalDrainCount"] == 0:
            evaluation.issues.append(
                f"line {telemetry.line_number}: no whole final-drain call was measured"
            )
        if values["finalDrainMaxNs"] < values["drainPermitWaitMaxNs"]:
            evaluation.issues.append(
                f"line {telemetry.line_number}: whole final-drain maximum is smaller than its component wait"
            )
        evaluation.max_rotation_permit_wait_nanos = values["startFlushMaxNs"]
        evaluation.max_foreground_flush_nanos = values["foregroundFlushMaxNs"]
        evaluation.max_drain_permit_wait_nanos = values["finalDrainMaxNs"]
        if evaluation.max_rotation_permit_wait_nanos > max_rotation_permit_wait_nanos:
            evaluation.issues.append(
                f"line {telemetry.line_number}: max whole startAsyncFlush elapsed "
                f"{_format_duration(evaluation.max_rotation_permit_wait_nanos)} exceeds "
                f"{_format_duration(max_rotation_permit_wait_nanos)}"
            )
        if evaluation.max_drain_permit_wait_nanos > max_rotation_permit_wait_nanos:
            evaluation.issues.append(
                f"line {telemetry.line_number}: max whole final-drain elapsed "
                f"{_format_duration(evaluation.max_drain_permit_wait_nanos)} exceeds "
                f"{_format_duration(max_rotation_permit_wait_nanos)}"
            )

        for field_name in (
            "permitWaitMaxNs",
            "rotationPermitWaitMaxNs",
            "drainPermitWaitMaxNs",
            "workerMaxNs",
            "submitWaitMaxNs",
            "startFlushMaxNs",
            "foregroundFlushMaxNs",
            "finalDrainMaxNs",
        ):
            if values[field_name] > max_rotation_permit_wait_nanos:
                evaluation.issues.append(
                    f"line {telemetry.line_number}: {field_name} "
                    f"{_format_duration(values[field_name])} exceeds "
                    f"{_format_duration(max_rotation_permit_wait_nanos)}"
                )

    if len(parsed.g1_region_sizes) > 1:
        formatted = ", ".join(_format_bytes(size) for size in sorted(parsed.g1_region_sizes))
        evaluation.issues.append(
            f"G1 region size changed inside fixed-heap run: {formatted}"
        )
    elif parsed.g1_region_sizes:
        logged_region_size = next(iter(parsed.g1_region_sizes))
        if evaluation.g1_region_size_bytes is not None and logged_region_size != evaluation.g1_region_size_bytes:
            evaluation.issues.append(
                "logged G1 region size does not match effective runtime configuration: "
                f"{_format_bytes(logged_region_size)} != "
                f"{_format_bytes(evaluation.g1_region_size_bytes)}"
            )

    if parsed.young_samples:
        evaluation.max_young_pause_nanos = max(sample.pause_nanos for sample in parsed.young_samples)
        if evaluation.max_young_pause_nanos > max_rotation_permit_wait_nanos:
            evaluation.issues.append(
                f"max young-GC pause {_format_duration(evaluation.max_young_pause_nanos)} exceeds "
                f"{_format_duration(max_rotation_permit_wait_nanos)}"
            )
        capacities = {sample.capacity_bytes for sample in parsed.young_samples}
        if len(capacities) != 1:
            formatted = ", ".join(_format_bytes(capacity) for capacity in sorted(capacities))
            evaluation.issues.append(f"heap capacity changed inside fixed-heap run: {formatted}")
        observed_capacity = parsed.young_samples[0].capacity_bytes
        if observed_capacity != expected_capacity_bytes:
            evaluation.issues.append(
                "fixed heap does not match gate profile: "
                f"observed {_format_bytes(observed_capacity)}, "
                f"expected {_format_bytes(expected_capacity_bytes)}"
            )
    else:
        evaluation.zero_young_events = True
        evaluation.inconclusive_reasons.append(
            "no young-GC samples; retained-occupancy behavior cannot be estimated"
        )
        if configuration_values is None or configuration_values.get("gcLogging") != "true":
            evaluation.issues.append("no young-GC samples and no valid effective GC logging evidence")

    if parsed.safepoint_nanos:
        evaluation.max_safepoint_nanos = max(parsed.safepoint_nanos)
        if evaluation.max_safepoint_nanos > max_rotation_permit_wait_nanos:
            evaluation.issues.append(
                f"max safepoint {_format_duration(evaluation.max_safepoint_nanos)} exceeds "
                f"{_format_duration(max_rotation_permit_wait_nanos)}"
            )
    else:
        if configuration_values is None or configuration_values.get("safepointLogging") != "true":
            evaluation.issues.append("no safepoint samples and no valid effective safepoint logging evidence")
        elif parsed.young_samples:
            evaluation.issues.append("young-GC events were present without safepoint event evidence")

    if (
        not parsed.young_samples
        or evaluation.capacity_bytes is None
        or evaluation.g1_region_size_bytes is None
    ):
        if parsed.young_samples and not evaluation.inconclusive_reasons:
            evaluation.inconclusive_reasons.append(
                "retained-occupancy analysis lacks valid heap-capacity or G1-region evidence"
            )
        return evaluation

    after_values = [sample.after_bytes for sample in parsed.young_samples]
    evaluation.warmup_samples = math.ceil(len(after_values) * WARMUP_FRACTION)
    # HFT_MEASURE_END is the authoritative boundary. Every collection before it belongs to the
    # measured load/close/sync lifecycle; discarding an unconditional tail can erase the exact late
    # promotion the plateau and slope checks exist to catch.
    analysis = after_values[evaluation.warmup_samples :]
    evaluation.analysis_samples = len(analysis)
    if len(analysis) < min_samples:
        evaluation.inconclusive_reasons.append(
            f"only {len(analysis)} post-warmup young-GC samples; need at least {min_samples}"
        )
        return evaluation

    minimum_plateau_window = min(MIN_PLATEAU_WINDOW, min_samples)
    plateau_window = min(
        len(analysis),
        MAX_PLATEAU_WINDOW,
        max(minimum_plateau_window, len(analysis) // 4),
    )
    plateau = _find_plateau(
        analysis,
        plateau_window,
        min_samples,
        evaluation.g1_region_size_bytes,
    )
    if plateau is None:
        region_allowance = (
            evaluation.g1_region_size_bytes * PLATEAU_JITTER_REGION_MULTIPLIER
        )
        evaluation.issues.append(
            f"post-young occupancy never plateaued: no {plateau_window}-sample window had spread within "
            f"max({PLATEAU_SPREAD_FRACTION * 100:.1f}% of its median, "
            f"{PLATEAU_JITTER_REGION_MULTIPLIER} G1 regions = "
            f"{_format_bytes(region_allowance)}) and positive local OLS growth within "
            f"{PLATEAU_SPREAD_FRACTION * 100:.1f}% of its median"
        )
        evaluation.occupancy_verdict = Verdict.FAIL
        return evaluation

    evaluation.plateau_sample = evaluation.warmup_samples + plateau.start
    evaluation.plateau_spread_bytes = plateau.spread_bytes
    evaluation.plateau_spread_allowance_bytes = plateau.spread_allowance_bytes
    evaluation.plateau_local_growth_bytes = plateau.projected_growth_bytes
    evaluation.plateau_local_growth_allowance_bytes = plateau.growth_allowance_bytes
    post_plateau = analysis[plateau.start:]
    evaluation.post_plateau_samples = len(post_plateau)
    if len(post_plateau) < min_samples:
        evaluation.inconclusive_reasons.append(
            f"only {len(post_plateau)} post-plateau samples; need at least {min_samples}"
        )
        return evaluation

    occupancy_issue_count = len(evaluation.issues)

    quarter = max(1, len(post_plateau) // 4)
    evaluation.early_median_bytes = _median(post_plateau[:quarter])
    evaluation.late_median_bytes = _median(post_plateau[-quarter:])
    evaluation.median_ratio = evaluation.late_median_bytes / max(1.0, evaluation.early_median_bytes)
    if evaluation.median_ratio > POST_PLATEAU_MEDIAN_RATIO_LIMIT:
        evaluation.issues.append(
            f"post-plateau late/early median ratio {evaluation.median_ratio:.4f} exceeds "
            f"{POST_PLATEAU_MEDIAN_RATIO_LIMIT:.4f}"
        )

    decile = min(max(3, math.ceil(len(post_plateau) * 0.10)), len(post_plateau) // 2)
    previous_decile = _median(post_plateau[-2 * decile : -decile])
    evaluation.steady_bytes = _median(post_plateau[-decile:])
    evaluation.late_decile_growth_bytes = evaluation.steady_bytes - previous_decile
    late_allowance = max(
        LATE_DECILE_MIN_ALLOWANCE_BYTES,
        evaluation.capacity_bytes * LATE_DECILE_HEAP_ALLOWANCE_FRACTION,
    )
    if evaluation.late_decile_growth_bytes > late_allowance:
        evaluation.issues.append(
            f"last-decile post-young occupancy growth "
            f"{_format_bytes(evaluation.late_decile_growth_bytes)} exceeds "
            f"allowance {_format_bytes(late_allowance)}"
        )

    final_half = post_plateau[len(post_plateau) // 2 :]
    evaluation.projected_final_half_growth_bytes = _ols_projected_growth(final_half)
    evaluation.normalized_final_half_growth = (
        evaluation.projected_final_half_growth_bytes / evaluation.capacity_bytes
    )
    if evaluation.normalized_final_half_growth > FINAL_HALF_GROWTH_FRACTION_LIMIT:
        evaluation.issues.append(
            f"final-half OLS growth {evaluation.normalized_final_half_growth * 100:.3f}% of heap exceeds "
            f"{FINAL_HALF_GROWTH_FRACTION_LIMIT * 100:.3f}%"
        )

    evaluation.occupancy_verdict = (
        Verdict.FAIL if len(evaluation.issues) > occupancy_issue_count else Verdict.PASS
    )

    return evaluation


def evaluate_pair(
    one_million: ParsedRun,
    four_million: ParsedRun,
    min_small_samples: int = DEFAULT_MIN_SMALL_SAMPLES,
    min_large_samples: int = DEFAULT_MIN_LARGE_SAMPLES,
    expected_capacity_bytes: int = int(DEFAULT_EXPECTED_HEAP_GIB * GIB),
    expected_side_batch_bytes: int = int(DEFAULT_EXPECTED_SIDE_BATCH_MIB * MIB),
    max_rotation_permit_wait_nanos: int = int(DEFAULT_MAX_PERMIT_WAIT_MS * 1_000_000),
    expected_max_new_bytes: int = EXPECTED_MAX_NEW_BYTES,
    small_rows: int = DEFAULT_SMALL_ROWS,
    large_rows: int = DEFAULT_LARGE_ROWS,
    expected_git_sha: str | None = None,
    expected_artifact_sha256: str | None = None,
    expected_versioning_type: str = EXPECTED_VERSIONING_TYPE,
) -> PairEvaluation:
    _validate_row_counts(small_rows, large_rows)
    small_label = _format_row_label(small_rows)
    large_label = _format_row_label(large_rows)
    one = evaluate_run(
        one_million,
        small_label,
        min_small_samples,
        expected_capacity_bytes,
        expected_side_batch_bytes,
        max_rotation_permit_wait_nanos,
        small_rows,
        expected_max_new_bytes,
        expected_git_sha,
        expected_artifact_sha256,
        expected_versioning_type,
    )
    four = evaluate_run(
        four_million,
        large_label,
        min_large_samples,
        expected_capacity_bytes,
        expected_side_batch_bytes,
        max_rotation_permit_wait_nanos,
        large_rows,
        expected_max_new_bytes,
        expected_git_sha,
        expected_artifact_sha256,
        expected_versioning_type,
    )
    pair = PairEvaluation(one_million=one, four_million=four)

    if one.capacity_bytes is not None and four.capacity_bytes is not None:
        if one.capacity_bytes != four.capacity_bytes:
            pair.cross_scale_issues.append(
                "fixed heap differs between runs: "
                f"{small_label}={_format_bytes(one.capacity_bytes)}, "
                f"{large_label}={_format_bytes(four.capacity_bytes)}"
            )

    if (
        one.occupancy_verdict is Verdict.PASS
        and four.occupancy_verdict is Verdict.PASS
        and one.steady_bytes is not None
        and four.steady_bytes is not None
        and four.capacity_bytes is not None
    ):
        pair.cross_scale_growth_bytes = four.steady_bytes - one.steady_bytes
        pair.cross_scale_allowance_bytes = max(
            CROSS_SCALE_MIN_ALLOWANCE_BYTES,
            four.capacity_bytes * CROSS_SCALE_HEAP_ALLOWANCE_FRACTION,
        )
        if pair.cross_scale_growth_bytes > pair.cross_scale_allowance_bytes:
            pair.cross_scale_issues.append(
                f"{large_label} steady post-young occupancy grew by "
                f"{_format_bytes(pair.cross_scale_growth_bytes)} over {small_label}; "
                f"allowance is {_format_bytes(pair.cross_scale_allowance_bytes)}"
            )
        pair.cross_scale_verdict = Verdict.FAIL if pair.cross_scale_issues else Verdict.PASS
    else:
        pair.cross_scale_inconclusive_reasons.append(
            "cross-scale retained-occupancy comparison requires conclusive occupancy in both runs"
        )

    if pair.cross_scale_issues:
        pair.cross_scale_verdict = Verdict.FAIL

    return pair


def _format_bytes(value: float | int) -> str:
    sign = "-" if value < 0 else ""
    absolute = abs(float(value))
    if absolute >= GIB:
        return f"{sign}{absolute / GIB:.2f} GiB"
    if absolute >= MIB:
        return f"{sign}{absolute / MIB:.2f} MiB"
    if absolute >= 1024:
        return f"{sign}{absolute / 1024:.2f} KiB"
    return f"{sign}{absolute:.0f} B"


def _format_row_label(rows: int) -> str:
    if rows % 1_000_000 == 0:
        return f"{rows // 1_000_000}M"
    return f"{rows:,} rows"


def _format_duration(nanos: int | None) -> str:
    if nanos is None:
        return "n/a"
    if nanos >= 1_000_000_000:
        return f"{nanos / 1_000_000_000:.3f} s"
    if nanos >= 1_000_000:
        return f"{nanos / 1_000_000:.3f} ms"
    if nanos >= 1_000:
        return f"{nanos / 1_000:.3f} us"
    return f"{nanos} ns"


def _print_run(evaluation: RunEvaluation) -> None:
    print(f"{evaluation.label} ({evaluation.parsed.source})")
    print(
        "  young samples: "
        f"{len(evaluation.parsed.young_samples)} total, {evaluation.analysis_samples} post-warmup, "
        f"{evaluation.post_plateau_samples} post-plateau"
    )
    print(f"  fixed heap: {_format_bytes(evaluation.capacity_bytes) if evaluation.capacity_bytes else 'n/a'}")
    print(
        "  G1 region size: "
        f"{_format_bytes(evaluation.g1_region_size_bytes) if evaluation.g1_region_size_bytes else 'n/a'}"
    )
    if evaluation.plateau_spread_bytes is not None:
        print(
            "  plateau candidate: "
            f"spread {_format_bytes(evaluation.plateau_spread_bytes)} / "
            f"allowance {_format_bytes(evaluation.plateau_spread_allowance_bytes or 0)}, "
            f"local OLS growth {_format_bytes(evaluation.plateau_local_growth_bytes or 0)} / "
            f"positive allowance {_format_bytes(evaluation.plateau_local_growth_allowance_bytes or 0)}"
        )
    print(f"  max young pause: {_format_duration(evaluation.max_young_pause_nanos)}")
    print(f"  max safepoint: {_format_duration(evaluation.max_safepoint_nanos)}")
    print(
        "  max whole startAsyncFlush elapsed: "
        f"{_format_duration(evaluation.max_rotation_permit_wait_nanos)}"
    )
    print(
        "  max complete foreground async-flush elapsed: "
        f"{_format_duration(evaluation.max_foreground_flush_nanos)}"
    )
    print(f"  max whole final-drain elapsed: {_format_duration(evaluation.max_drain_permit_wait_nanos)}")
    if evaluation.attempted_kvl_pages is not None and evaluation.promoted_kvl_pages is not None:
        print(
            "  async KVL outcomes: "
            f"{evaluation.attempted_kvl_pages} attempted, "
            f"{evaluation.promoted_kvl_pages} promoted"
        )
    if evaluation.staged_side_pages is not None and evaluation.staged_side_bytes is not None:
        print(
            "  bounded side-page path: "
            f"{evaluation.staged_side_pages} pages / {_format_bytes(evaluation.staged_side_bytes)}, "
            f"peak active {_format_bytes(evaluation.peak_active_side_bytes or 0)}, "
            f"{evaluation.append_worker_runs} epochs"
        )
    if evaluation.pinned_trie_spill_pages is not None:
        print(
            "  bounded pinned-trie spill: "
            f"{evaluation.pinned_trie_spill_pages} pages / "
            f"{evaluation.pinned_trie_spill_epochs} epochs, "
            f"batch max {evaluation.pinned_trie_spill_batch_max}, "
            f"live max {evaluation.pinned_trie_live_max}, "
            f"append-only high-water {evaluation.pinned_trie_high_water}"
        )
    if evaluation.steady_bytes is not None:
        print(f"  steady post-young occupancy: {_format_bytes(evaluation.steady_bytes)}")
    if evaluation.median_ratio is not None:
        print(f"  post-plateau late/early median: {evaluation.median_ratio:.4f}x")
    if evaluation.normalized_final_half_growth is not None:
        print(f"  final-half OLS growth: {evaluation.normalized_final_half_growth * 100:.3f}% of heap")
    print(f"  occupancy evidence: {evaluation.occupancy_verdict.value}")
    for reason in evaluation.inconclusive_reasons:
        print(f"    - {reason}")
    if evaluation.issues:
        print("  FAIL:")
        for issue in evaluation.issues:
            print(f"    - {issue}")
    elif evaluation.verdict is Verdict.INCONCLUSIVE:
        print("  INCONCLUSIVE")
    else:
        print("  PASS")


def print_report(pair: PairEvaluation) -> None:
    _print_run(pair.one_million)
    _print_run(pair.four_million)
    print("cross-scale")
    if pair.cross_scale_growth_bytes is not None and pair.cross_scale_allowance_bytes is not None:
        print(
            f"  {pair.four_million.label} - {pair.one_million.label} "
            "steady post-young occupancy: "
            f"{_format_bytes(pair.cross_scale_growth_bytes)} "
            f"(allowance {_format_bytes(pair.cross_scale_allowance_bytes)})"
        )
    if pair.cross_scale_issues:
        print("  FAIL:")
        for issue in pair.cross_scale_issues:
            print(f"    - {issue}")
    elif pair.cross_scale_verdict is Verdict.INCONCLUSIVE:
        print("  INCONCLUSIVE:")
        for reason in pair.cross_scale_inconclusive_reasons:
            print(f"    - {reason}")
    else:
        print("  PASS")
    print("HFT GC GATE: " + pair.verdict.value)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reject ClickBench fixed-heap runs with major GC or unbounded post-young occupancy."
    )
    parser.add_argument(
        "--one-million",
        "--small-log",
        dest="one_million",
        required=True,
        type=Path,
        metavar="LOG",
        help="combined smaller-corpus load log (--one-million is retained for compatibility)",
    )
    parser.add_argument(
        "--four-million",
        "--large-log",
        dest="four_million",
        required=True,
        type=Path,
        metavar="LOG",
        help="combined larger-corpus load log (--four-million is retained for compatibility)",
    )
    parser.add_argument(
        "--small-rows",
        type=_positive_integer,
        default=DEFAULT_SMALL_ROWS,
        metavar="N",
        help=f"expected row count in the smaller-corpus log (default: {DEFAULT_SMALL_ROWS})",
    )
    parser.add_argument(
        "--large-rows",
        type=_positive_integer,
        default=DEFAULT_LARGE_ROWS,
        metavar="N",
        help=f"expected row count in the larger-corpus log (default: {DEFAULT_LARGE_ROWS})",
    )
    parser.add_argument(
        "--expected-git-sha",
        required=True,
        metavar="SHA",
        help="lowercase 40-character commit SHA embedded by the measured process",
    )
    parser.add_argument(
        "--runtime-classpath",
        required=True,
        metavar="CLASSPATH",
        help="exact path-separated runtime classpath used for both measured JVMs",
    )
    parser.add_argument(
        "--expected-heap-gib",
        type=_positive_float,
        default=DEFAULT_EXPECTED_HEAP_GIB,
        metavar="GIB",
        help=f"required fixed heap capacity in GiB (default: {DEFAULT_EXPECTED_HEAP_GIB:g})",
    )
    parser.add_argument(
        "--expected-side-batch-mib",
        type=_positive_float,
        default=DEFAULT_EXPECTED_SIDE_BATCH_MIB,
        metavar="MIB",
        help=(
            "maximum configured active side-page payload batch in MiB "
            f"(default: {DEFAULT_EXPECTED_SIDE_BATCH_MIB:g})"
        ),
    )
    parser.add_argument(
        "--expected-max-new-mib",
        type=_positive_float,
        default=DEFAULT_EXPECTED_MAX_NEW_MIB,
        metavar="MIB",
        help=(
            "required effective HotSpot MaxNewSize in MiB "
            f"(default: {DEFAULT_EXPECTED_MAX_NEW_MIB:g})"
        ),
    )
    parser.add_argument(
        "--max-permit-wait-ms",
        type=_foreground_wait_bound,
        default=DEFAULT_MAX_PERMIT_WAIT_MS,
        metavar="MS",
        help=(
            "maximum whole startAsyncFlush and final-drain elapsed time in milliseconds; "
            "the canonical acceptance bound cannot be relaxed above 250 ms "
            f"(default: {DEFAULT_MAX_PERMIT_WAIT_MS:g})"
        ),
    )
    parser.add_argument(
        "--versioning-type",
        choices=("FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"),
        default=EXPECTED_VERSIONING_TYPE,
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument(
        "--min-small-samples",
        type=_small_sample_floor,
        default=DEFAULT_MIN_SMALL_SAMPLES,
        metavar="N",
        help=(
            "required post-warmup and post-plateau smaller-run samples; may only raise the canonical floor "
            f"(default: {DEFAULT_MIN_SMALL_SAMPLES})"
        ),
    )
    parser.add_argument(
        "--min-large-samples",
        type=_large_sample_floor,
        default=DEFAULT_MIN_LARGE_SAMPLES,
        metavar="N",
        help=(
            "required post-warmup and post-plateau larger-run samples; may only raise the canonical floor "
            f"(default: {DEFAULT_MIN_LARGE_SAMPLES})"
        ),
    )
    return parser


def _parse_arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = _argument_parser()
    args = parser.parse_args(argv)
    try:
        _validate_row_counts(args.small_rows, args.large_rows)
    except ValueError as error:
        parser.error(str(error))
    if re.fullmatch(r"[0-9a-f]{40}", args.expected_git_sha) is None:
        parser.error("--expected-git-sha must be a lowercase 40-character commit SHA")
    try:
        args.artifact_sha256 = runtime_classpath_sha256(args.runtime_classpath)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return args


def _sample_floor(raw: str, minimum: int) -> int:
    try:
        value = int(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError("sample floor must be an integer") from error
    if value < minimum:
        raise argparse.ArgumentTypeError(f"sample floor must be at least {minimum}")
    return value


def _small_sample_floor(raw: str) -> int:
    return _sample_floor(raw, DEFAULT_MIN_SMALL_SAMPLES)


def _large_sample_floor(raw: str) -> int:
    return _sample_floor(raw, DEFAULT_MIN_LARGE_SAMPLES)


def _positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return value


def _validate_row_counts(small_rows: int, large_rows: int) -> None:
    if small_rows <= 0:
        raise ValueError("small_rows must be greater than zero")
    if large_rows <= 0:
        raise ValueError("large_rows must be greater than zero")
    if small_rows >= large_rows:
        raise ValueError("small_rows must be less than large_rows")


def _positive_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a number") from error
    if not math.isfinite(value) or value <= 0.0:
        raise argparse.ArgumentTypeError("value must be finite and greater than zero")
    return value


def _foreground_wait_bound(raw: str) -> float:
    value = _positive_float(raw)
    if value > CANONICAL_MAX_FOREGROUND_WAIT_MS:
        raise argparse.ArgumentTypeError("canonical foreground wait bound cannot exceed 250 ms")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_arguments(argv)
    expected_heap_bytes = int(args.expected_heap_gib * GIB)
    expected_side_batch_bytes = int(args.expected_side_batch_mib * MIB)
    expected_max_new_bytes = int(args.expected_max_new_mib * MIB)
    pair = evaluate_pair(
        parse_log(args.one_million),
        parse_log(args.four_million),
        min_small_samples=args.min_small_samples,
        min_large_samples=args.min_large_samples,
        expected_capacity_bytes=expected_heap_bytes,
        expected_side_batch_bytes=expected_side_batch_bytes,
        max_rotation_permit_wait_nanos=int(args.max_permit_wait_ms * 1_000_000),
        expected_max_new_bytes=expected_max_new_bytes,
        small_rows=args.small_rows,
        large_rows=args.large_rows,
        expected_git_sha=args.expected_git_sha,
        expected_artifact_sha256=args.artifact_sha256,
        expected_versioning_type=args.versioning_type,
    )
    print_report(pair)
    if args.manifest is not None:
        manifest = {
            "kind": "projection-ingestion",
            "gitSha": args.expected_git_sha,
            "artifactSha256": args.artifact_sha256,
            "versioningType": args.versioning_type,
            "smallRows": args.small_rows,
            "largeRows": args.large_rows,
            "expectedHeapBytes": expected_heap_bytes,
            "expectedMaxNewBytes": expected_max_new_bytes,
            "expectedSideBatchBytes": expected_side_batch_bytes,
            "smallLogSha256": _sha256(args.one_million),
            "largeLogSha256": _sha256(args.four_million),
            "gateScriptSha256": _sha256(Path(__file__)),
            "passed": pair.passed,
            "verdict": pair.verdict.value,
            "occupancyVerdict": pair.occupancy_verdict.value,
        }
        args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return _exit_code(pair.verdict)


def _exit_code(verdict: Verdict) -> int:
    if verdict is Verdict.PASS:
        return 0
    if verdict is Verdict.INCONCLUSIVE:
        return 3
    return 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    sys.exit(main())
