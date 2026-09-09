#!/usr/bin/env python3
"""Fixed-heap AUTO-global projection-maintenance acceptance gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import hft_gc_gate as ingestion_gate
from hft_artifact import runtime_classpath_sha256


MIN_DIRTY_RECORDS = 100_001
MAX_FOREGROUND_WAIT_NANOS = 250_000_000
EXPECTED_AUTO_COMMIT_NODES = 16_384
_ASYNC_MAX_FIELDS = frozenset(
    {
        "peakActiveSideBytes",
        "permitWaitMaxNs",
        "rotationPermitWaitMaxNs",
        "drainPermitWaitMaxNs",
        "workerMaxNs",
        "submitWaitMaxNs",
        "startFlushMaxNs",
        "kvlAttemptedPagesMax",
        "foregroundFlushMaxNs",
        "finalDrainMaxNs",
        "pinnedTrieSpillBatchMax",
        "pinnedTrieLiveMax",
        "pinnedTrieHighWater",
    }
)


@dataclass
class MaintenanceEvaluation:
    label: str
    parsed: ingestion_gate.ParsedRun
    issues: list[str] = field(default_factory=list)
    operations: int | None = None
    bytes_touched: int | None = None

    @property
    def passed(self) -> bool:
        return not self.issues


def evaluate_run(
    parsed: ingestion_gate.ParsedRun,
    label: str,
    expected_rows: int,
    expected_dirty_records: int,
    expected_heap_bytes: int,
    expected_versioning_type: str,
    expected_git_sha: str,
    expected_artifact_sha256: str,
) -> MaintenanceEvaluation:
    evaluation = MaintenanceEvaluation(label, parsed)
    configured_region_size: int | None = None
    evaluation.issues.extend(parsed.errors)
    for event in parsed.forbidden_events:
        evaluation.issues.append(
            f"line {event.line_number}: forbidden {event.kind}: {event.text}"
        )

    if len(parsed.hft_builds) != 1:
        evaluation.issues.append("expected exactly one HFT build record")
    else:
        build = parsed.hft_builds[0]
        if build.git_sha != expected_git_sha:
            evaluation.issues.append(
                f"HFT build SHA {build.git_sha} does not match {expected_git_sha}"
            )
        if build.artifact_sha256 != expected_artifact_sha256:
            evaluation.issues.append(
                f"HFT artifact SHA-256 {build.artifact_sha256} does not match "
                f"{expected_artifact_sha256}"
            )

    if len(parsed.hft_configurations) != 1:
        evaluation.issues.append("expected exactly one HFT configuration record")
    else:
        values = parsed.hft_configurations[0].values
        expected = {
            "globalDict": "auto",
            "autoCommitNodes": str(EXPECTED_AUTO_COMMIT_NODES),
            "asyncFlushNodeCap": "0",
            "arenaStrategy": "shared",
            "maxNewSizeBytes": str(ingestion_gate.GIB),
            "initialHeapBytes": str(expected_heap_bytes),
            "maxHeapBytes": str(expected_heap_bytes),
            "g1RegionSizeBytes": str(ingestion_gate.EXPECTED_G1_REGION_SIZE_BYTES),
            "gcLogging": "true",
            "safepointLogging": "true",
            "importer": "ordinary-maintenance",
            "projectionMode": "incremental",
            "storage": "FILE_CHANNEL",
            "expectedRows": str(expected_rows),
            "pinnedTrieScanBudget": str(ingestion_gate.EXPECTED_PINNED_TRIE_SCAN_BUDGET),
            "pinnedTrieBatchCapacity": str(ingestion_gate.EXPECTED_PINNED_TRIE_BATCH_CAPACITY),
            "versioningType": expected_versioning_type,
            "appendQueueCapacity": "1",
        }
        for name, wanted in expected.items():
            if values[name] != wanted:
                evaluation.issues.append(
                    f"HFT config {name}={values[name]!r}, expected {wanted!r}"
                )
        if values["appendWorkers"] not in {"1", "2"}:
            evaluation.issues.append("HFT config appendWorkers must be 1 or 2")
        try:
            region_size = int(values["g1RegionSizeBytes"])
        except ValueError:
            evaluation.issues.append("HFT config g1RegionSizeBytes must be an integer")
        else:
            if region_size <= 0:
                evaluation.issues.append("HFT config g1RegionSizeBytes must be positive")
            else:
                configured_region_size = region_size

    if not parsed.projection_maintenance_telemetry:
        evaluation.issues.append("expected projection-maintenance telemetry")
    else:
        values = {
            name: sum(record.values[name] for record in parsed.projection_maintenance_telemetry)
            for name in parsed.projection_maintenance_telemetry[0].values
        }
        calculated_operations = sum(
            values[name]
            for name in (
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
            )
        )
        if values["operations"] != calculated_operations:
            evaluation.issues.append("maintenance operation total does not match its components")
        if values["dirtyRecords"] != expected_dirty_records:
            evaluation.issues.append(
                f"dirtyRecords={values['dirtyRecords']}, expected {expected_dirty_records}"
            )
        if values["dirtyRecords"] < MIN_DIRTY_RECORDS:
            evaluation.issues.append(
                f"maintenance touched fewer than {MIN_DIRTY_RECORDS} dirty records"
            )
        if values["commits"] < 3 or values["dictionarySegments"] < 3:
            evaluation.issues.append("maintenance did not persist at least three successor dictionary segments")
        if values["fullRebuilds"] != 0:
            evaluation.issues.append(f"fullRebuilds={values['fullRebuilds']}, expected 0")
        if values["rowGroupsWritten"] == 0 or values["rowGroupsWritten"] > values["dirtyRecords"]:
            evaluation.issues.append("row-group writes are not bounded by dirty records")
        if values["metadataReads"] != values["commits"] or values["metadataWrites"] != values["commits"]:
            evaluation.issues.append("metadata boundary operations do not match maintenance commits")
        if values["dictionaryProbes"] < values["dirtyRecords"]:
            evaluation.issues.append("dictionary probe accounting does not cover dirty records")
        if values["operations"] == 0 or values["bytesRead"] + values["bytesWritten"] == 0:
            evaluation.issues.append("maintenance reported no touched-unit work")
        if values["asyncSubmissions"] != values["asyncCompletions"]:
            evaluation.issues.append("actual async submissions and completions do not balance")
        evaluation.operations = values["operations"]
        evaluation.bytes_touched = values["bytesRead"] + values["bytesWritten"]

    if len(parsed.projection_evidence) != 1:
        evaluation.issues.append("expected exactly one derived projection-evidence record")
    else:
        values = parsed.projection_evidence[0].values
        if values["revisionsVerified"] < 2:
            evaluation.issues.append("projection evidence did not verify historical revisions")
        if values["historicalRevisions"] != values["revisionsVerified"] - 1:
            evaluation.issues.append("historical revision count is inconsistent")
        if values["oracleRows"] != values["servedRows"]:
            evaluation.issues.append("served projection rows differ from the record oracle")
        if values["oracleMatches"] != values["servedMatches"]:
            evaluation.issues.append("served projection predicates differ from the record oracle")
        if values["servedRevisions"] != values["revisionsVerified"]:
            evaluation.issues.append("not every revision was actually projection-served")
        if values["stableAnchors"] != 1 or values["stableIds"] != 1:
            evaluation.issues.append("global dictionary anchors or ids were not stable")
        if values["successorSegments"] < 3:
            evaluation.issues.append("fewer than three successor dictionary segments were observed")
        if values["introductionRevision"] == 0:
            evaluation.issues.append("new dictionary values have no observed introduction revision")
        if values["maxProbeUnits"] == 0 or values["maxProbeUnits"] > 18:
            evaluation.issues.append("dictionary probe depth is absent or exceeds the radix bound")
        revisions = parsed.projection_revision_evidence
        if len(revisions) != values["revisionsVerified"]:
            evaluation.issues.append("per-revision evidence count does not match revisionsVerified")
        elif revisions:
            revision_values = [record.values for record in revisions]
            revision_numbers = [record["revision"] for record in revision_values]
            expected_revisions = list(range(revision_numbers[0], revision_numbers[0] + len(revision_numbers)))
            if revision_numbers != expected_revisions:
                evaluation.issues.append("per-revision evidence is not contiguous and ordered")
            if any(record["oracleRows"] != record["servedRows"] for record in revision_values):
                evaluation.issues.append("a historical projection result differs from its record oracle")
            if any(record["oracleMatches"] != record["servedMatches"] for record in revision_values):
                evaluation.issues.append("a historical projection predicate differs from its record oracle")
            if sum(record["oracleRows"] for record in revision_values) != values["oracleRows"]:
                evaluation.issues.append("per-revision oracle sum does not match aggregate evidence")
            if sum(record["servedRows"] for record in revision_values) != values["servedRows"]:
                evaluation.issues.append("per-revision served sum does not match aggregate evidence")
            if sum(record["oracleMatches"] for record in revision_values) != values["oracleMatches"]:
                evaluation.issues.append("per-revision predicate oracle does not match aggregate evidence")
            if sum(record["servedMatches"] for record in revision_values) != values["servedMatches"]:
                evaluation.issues.append("per-revision served predicates do not match aggregate evidence")
            anchors = {record["anchor"] for record in revision_values}
            old_ids = {record["oldId"] for record in revision_values}
            if len(anchors) != 1 or 0 in anchors or len(old_ids) != 1 or 0 in old_ids:
                evaluation.issues.append("per-revision anchors or established ids are unstable")
            segment_counts = [record["successorSegments"] for record in revision_values]
            if segment_counts != sorted(segment_counts):
                evaluation.issues.append("dictionary successor links regress across revisions")
            introduced = [record for record in revision_values if record["newId"] > 0]
            if not introduced or introduced[0]["revision"] != values["introductionRevision"]:
                evaluation.issues.append("new dictionary id introduction revision is inconsistent")
            elif any(record["newId"] != introduced[0]["newId"] for record in introduced):
                evaluation.issues.append("new dictionary id changes after its introduction")
            if max(segment_counts) != values["successorSegments"]:
                evaluation.issues.append("successor segment maximum does not match aggregate evidence")

    if not parsed.async_flush_telemetry:
        evaluation.issues.append("expected at least one async-flush telemetry record")
    else:
        values = {name: 0 for name in ingestion_gate._ASYNC_FLUSH_FIELDS}
        for telemetry in parsed.async_flush_telemetry:
            if telemetry.values["callerThreadAppendRuns"] != 0:
                evaluation.issues.append(
                    f"line {telemetry.line_number}: caller thread executed snapshot append work"
                )
            for name, value in telemetry.values.items():
                values[name] = max(values[name], value) if name in _ASYNC_MAX_FIELDS else values[name] + value
        epochs = values["combinedEpochs"] + values["sideOnlyEpochs"]
        full_epoch_fields = (
            "combinedEpochs",
            "kvlPages",
            "kvlAttemptedPages",
            "kvlPromotedPages",
            "kvlAttemptedPagesMax",
            "foregroundFlushCount",
            "foregroundFlushTotalNs",
            "foregroundFlushMaxNs",
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
        nonzero_full_epoch_fields = [name for name in full_epoch_fields if values[name] != 0]
        if nonzero_full_epoch_fields:
            evaluation.issues.append(
                "async-commit maintenance reported full TIL epoch work: "
                + ", ".join(nonzero_full_epoch_fields)
            )
        if values["workerRuns"] != epochs:
            evaluation.issues.append("append worker count does not match append epochs")
        if values["submitWaitCount"] != epochs:
            evaluation.issues.append("append admission count does not match append epochs")
        if values["submitWaitTotalNs"] < values["submitWaitMaxNs"]:
            evaluation.issues.append("append admission maximum exceeds its total")
        if values["startFlushTotalNs"] < values["startFlushMaxNs"]:
            evaluation.issues.append("whole startAsyncFlush maximum exceeds its total")
        if values["startFlushCount"] < epochs:
            evaluation.issues.append("whole startAsyncFlush count is smaller than append epochs")
        if values["foregroundFlushCount"] != values["combinedEpochs"]:
            evaluation.issues.append(
                "whole foreground async-flush count does not match combined epochs"
            )
        if values["foregroundFlushTotalNs"] < values["foregroundFlushMaxNs"]:
            evaluation.issues.append("whole foreground async-flush maximum exceeds its total")
        if values["finalDrainTotalNs"] < values["finalDrainMaxNs"]:
            evaluation.issues.append("whole final-drain maximum exceeds its total")
        if values["startFlushMaxNs"] < max(
            values["rotationPermitWaitMaxNs"], values["submitWaitMaxNs"]
        ):
            evaluation.issues.append("whole startAsyncFlush maximum is smaller than a component wait")
        if values["finalDrainCount"] == 0:
            evaluation.issues.append("no whole final-drain call was measured")
        if values["finalDrainMaxNs"] < values["drainPermitWaitMaxNs"]:
            evaluation.issues.append("whole final-drain maximum is smaller than its component wait")
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
            if values[field_name] > MAX_FOREGROUND_WAIT_NANOS:
                evaluation.issues.append(f"{field_name}={values[field_name]} ns exceeds 250 ms")

    if parsed.young_samples:
        capacities = {sample.capacity_bytes for sample in parsed.young_samples}
        if capacities != {expected_heap_bytes}:
            evaluation.issues.append(
                f"maintenance heap capacities {sorted(capacities)} do not equal {expected_heap_bytes}"
            )
        max_pause = max(sample.pause_nanos for sample in parsed.young_samples)
        if max_pause > MAX_FOREGROUND_WAIT_NANOS:
            evaluation.issues.append(f"young-GC pause {max_pause} ns exceeds 250 ms")
    if parsed.safepoint_nanos:
        max_safepoint = max(parsed.safepoint_nanos)
        if max_safepoint > MAX_FOREGROUND_WAIT_NANOS:
            evaluation.issues.append(f"safepoint {max_safepoint} ns exceeds 250 ms")
    elif parsed.young_samples:
        evaluation.issues.append("young-GC events were present without safepoint event evidence")
    if len(parsed.g1_region_sizes) > 1:
        evaluation.issues.append("G1 region size changed inside the maintenance run")
    elif parsed.g1_region_sizes and configured_region_size is not None:
        logged_region_size = next(iter(parsed.g1_region_sizes))
        if logged_region_size != configured_region_size:
            evaluation.issues.append("logged G1 region size differs from effective configuration")
    return evaluation


def evaluate_pair(small: MaintenanceEvaluation, large: MaintenanceEvaluation) -> list[str]:
    issues: list[str] = []
    if small.operations is not None and large.operations is not None:
        allowance = max(16, small.operations // 4)
        if large.operations > small.operations + allowance:
            issues.append(
                f"large-base operations {large.operations} exceed small-base {small.operations} + {allowance}"
            )
    if small.bytes_touched is not None and large.bytes_touched is not None:
        allowance = max(16 * ingestion_gate.MIB, small.bytes_touched // 4)
        if large.bytes_touched > small.bytes_touched + allowance:
            issues.append(
                f"large-base touched bytes {large.bytes_touched} exceed small-base "
                f"{small.bytes_touched} + {allowance}"
            )
    return issues


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--small-log", required=True, type=Path)
    parser.add_argument("--large-log", required=True, type=Path)
    parser.add_argument("--small-rows", required=True, type=_positive_int)
    parser.add_argument("--large-rows", required=True, type=_positive_int)
    parser.add_argument("--dirty-records", required=True, type=_positive_int)
    parser.add_argument("--expected-heap-gib", type=float, default=4.0)
    parser.add_argument(
        "--versioning-type",
        choices=("FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"),
        default="FULL",
    )
    parser.add_argument("--expected-git-sha", required=True)
    parser.add_argument("--runtime-classpath", required=True)
    parser.add_argument("--manifest", required=True, type=Path)
    return parser


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_output(*arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"git {' '.join(arguments)} failed: {result.stdout.strip()}")
    return result.stdout.strip()


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    if args.dirty_records < MIN_DIRTY_RECORDS:
        raise SystemExit(f"--dirty-records must be at least {MIN_DIRTY_RECORDS}")
    heap_bytes = int(args.expected_heap_gib * ingestion_gate.GIB)
    if len(args.expected_git_sha) != 40 or any(
        character not in "0123456789abcdef" for character in args.expected_git_sha
    ):
        raise SystemExit("--expected-git-sha must be a lowercase 40-character commit SHA")
    actual_git_sha = _git_output("rev-parse", "HEAD")
    if actual_git_sha != args.expected_git_sha:
        raise SystemExit(f"gate commit {actual_git_sha} does not match {args.expected_git_sha}")
    if _git_output("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("HFT evidence requires a clean tracked worktree")
    try:
        artifact_sha256 = runtime_classpath_sha256(args.runtime_classpath)
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error
    small = evaluate_run(
        ingestion_gate.parse_log(args.small_log),
        "small",
        args.small_rows,
        args.dirty_records,
        heap_bytes,
        args.versioning_type,
        args.expected_git_sha,
        artifact_sha256,
    )
    large = evaluate_run(
        ingestion_gate.parse_log(args.large_log),
        "large",
        args.large_rows,
        args.dirty_records,
        heap_bytes,
        args.versioning_type,
        args.expected_git_sha,
        artifact_sha256,
    )
    pair_issues = evaluate_pair(small, large)
    for evaluation in (small, large):
        verdict = "PASS" if evaluation.passed else "FAIL"
        print(f"{evaluation.label}: {verdict}")
        for issue in evaluation.issues:
            print(f"  - {issue}")
    for issue in pair_issues:
        print(f"cross-scale: FAIL\n  - {issue}")
    passed = small.passed and large.passed and not pair_issues
    manifest = {
        "kind": "projection-maintenance",
        "gitSha": args.expected_git_sha,
        "artifactSha256": artifact_sha256,
        "versioningType": args.versioning_type,
        "dirtyRecords": args.dirty_records,
        "smallRows": args.small_rows,
        "largeRows": args.large_rows,
        "expectedHeapBytes": heap_bytes,
        "smallLogSha256": _sha256(args.small_log),
        "largeLogSha256": _sha256(args.large_log),
        "gateScriptSha256": _sha256(Path(__file__)),
        "passed": passed,
    }
    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
