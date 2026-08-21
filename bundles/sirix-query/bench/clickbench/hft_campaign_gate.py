#!/usr/bin/env python3
"""Require canonical all-version maintenance and append-saturation artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence

from hft_artifact import runtime_classpath_sha256


VERSIONS = frozenset({"FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
MAINTENANCE_SCHEMA = frozenset(
    {
        "kind", "gitSha", "artifactSha256", "versioningType", "dirtyRecords",
        "smallRows", "largeRows", "smallLogSha256", "largeLogSha256",
        "gateScriptSha256", "passed",
    }
)
INGESTION_SCHEMA = frozenset(
    {
        "kind", "gitSha", "artifactSha256", "versioningType", "smallRows", "largeRows",
        "smallLogSha256", "largeLogSha256", "gateScriptSha256", "passed",
    }
)
SATURATION_SCHEMA = frozenset(
    {
        "kind", "gitSha", "artifactSha256", "versioningType", "resources", "records", "appendWorkers",
        "queueCapacity", "callerThreadAppendRuns", "submitWaitCount", "submitWaitTotalNs",
        "submitWaitMaxNs", "saturatedActiveWorkers", "saturatedQueuedTasks",
        "saturatedAdmissionWaiters", "saturatedAvailableAdmissions", "drainedActiveWorkers",
        "drainedQueuedTasks", "drainedAdmissionWaiters", "drainedAvailableAdmissions", "coldReopens",
        "gcPauseCount", "gcPauseMaxNs", "safepointCount", "safepointMaxNs",
        "humongousRegionSamples", "logSha256", "gateScriptSha256", "passed",
    }
)
CANONICAL_SMALL_ROWS = 1_000_000
CANONICAL_LARGE_ROWS = 4_000_000
CANONICAL_DIRTY_RECORDS = 100_001
CANONICAL_SATURATION_RECORDS = 4_096
SHA256_RE = re.compile(r"[0-9a-f]{64}")
MAINTENANCE_GATE_PATH = "bundles/sirix-query/bench/clickbench/hft_maintenance_gate.py"
INGESTION_GATE_PATH = "bundles/sirix-query/bench/clickbench/hft_gc_gate.py"
SATURATION_GATE_PATH = "bundles/sirix-query/bench/clickbench/hft_saturation_gate.py"
CAMPAIGN_GATE_PATH = "bundles/sirix-query/bench/clickbench/hft_campaign_gate.py"
ARTIFACT_HELPER_PATH = "bundles/sirix-query/bench/clickbench/hft_artifact.py"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tracked_blob_sha256(git_sha: str, repository_path: str) -> str:
    result = subprocess.run(
        ("git", "show", f"{git_sha}:{repository_path}"),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"cannot read committed gate {repository_path}: {detail}")
    return hashlib.sha256(result.stdout).hexdigest()


def load_manifest(path: Path, issues: list[str]) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"{path}: cannot read manifest: {error}")
        return None
    if not isinstance(value, dict):
        issues.append(f"{path}: manifest root must be an object")
        return None
    return value


def evaluate(
    maintenance_paths: Sequence[Path], ingestion_paths: Sequence[Path], saturation_paths: Sequence[Path], git_sha: str,
    maintenance_artifact_sha256: str, ingestion_artifact_sha256: str, saturation_artifact_sha256: str,
    maintenance_small_logs: Sequence[Path], maintenance_large_logs: Sequence[Path],
    ingestion_small_logs: Sequence[Path], ingestion_large_logs: Sequence[Path],
    maintenance_gate_script: Path, ingestion_gate_script: Path, saturation_logs: Sequence[Path],
    saturation_gate_script: Path, committed_maintenance_gate_sha256: str,
    committed_ingestion_gate_sha256: str, committed_saturation_gate_sha256: str,
) -> list[str]:
    issues: list[str] = []
    versions: set[str] = set()
    if len(maintenance_paths) != len(VERSIONS):
        issues.append("exactly four maintenance manifests are required")
    if len(maintenance_small_logs) != len(maintenance_paths) or len(maintenance_large_logs) != len(maintenance_paths):
        issues.append("every maintenance manifest requires its bound small and large log")
    if len(ingestion_paths) != len(VERSIONS):
        issues.append("exactly four ingestion manifests are required")
    if len(ingestion_small_logs) != len(ingestion_paths) or len(ingestion_large_logs) != len(ingestion_paths):
        issues.append("every ingestion manifest requires its bound small and large log")
    if len(saturation_paths) != len(VERSIONS) or len(saturation_logs) != len(saturation_paths):
        issues.append("exactly four saturation manifests and bound logs are required")
    if not maintenance_gate_script.is_file() or sha256(maintenance_gate_script) != committed_maintenance_gate_sha256:
        issues.append("maintenance gate script differs from its committed campaign blob")
    if not saturation_gate_script.is_file() or sha256(saturation_gate_script) != committed_saturation_gate_sha256:
        issues.append("saturation gate script differs from its committed campaign blob")
    if not ingestion_gate_script.is_file() or sha256(ingestion_gate_script) != committed_ingestion_gate_sha256:
        issues.append("ingestion gate script differs from its committed campaign blob")
    for index, path in enumerate(maintenance_paths):
        manifest = load_manifest(path, issues)
        if manifest is None:
            continue
        if set(manifest) != MAINTENANCE_SCHEMA:
            issues.append(f"{path}: maintenance manifest schema is not canonical")
        if manifest.get("kind") != "projection-maintenance" or manifest.get("passed") is not True:
            issues.append(f"{path}: maintenance manifest is not a passing canonical artifact")
        version = manifest.get("versioningType")
        if not isinstance(version, str) or version not in VERSIONS or version in versions:
            issues.append(f"{path}: versioningType is missing, unsupported, or duplicated")
        else:
            versions.add(version)
        if (manifest.get("gitSha") != git_sha
                or manifest.get("artifactSha256") != maintenance_artifact_sha256):
            issues.append(f"{path}: build identity differs from the campaign identity")
        expected_numbers = {
            "dirtyRecords": CANONICAL_DIRTY_RECORDS,
            "smallRows": CANONICAL_SMALL_ROWS,
            "largeRows": CANONICAL_LARGE_ROWS,
        }
        for name, expected in expected_numbers.items():
            if type(manifest.get(name)) is not int or manifest.get(name) != expected:
                issues.append(f"{path}: {name} must equal canonical value {expected}")
        for name in ("smallLogSha256", "largeLogSha256", "gateScriptSha256"):
            if not isinstance(manifest.get(name), str) or SHA256_RE.fullmatch(manifest[name]) is None:
                issues.append(f"{path}: {name} must be a lowercase SHA-256")
        if index < len(maintenance_small_logs) and index < len(maintenance_large_logs):
            for name, evidence_path in (
                ("smallLogSha256", maintenance_small_logs[index]),
                ("largeLogSha256", maintenance_large_logs[index]),
                ("gateScriptSha256", maintenance_gate_script),
            ):
                if not evidence_path.is_file() or manifest.get(name) != sha256(evidence_path):
                    issues.append(f"{path}: {name} does not bind the supplied child evidence")
    if versions != VERSIONS:
        issues.append(f"maintenance versions are incomplete: {sorted(VERSIONS - versions)}")

    ingestion_versions: set[str] = set()
    for index, path in enumerate(ingestion_paths):
        manifest = load_manifest(path, issues)
        if manifest is None:
            continue
        if set(manifest) != INGESTION_SCHEMA:
            issues.append(f"{path}: ingestion manifest schema is not canonical")
        if manifest.get("kind") != "projection-ingestion" or manifest.get("passed") is not True:
            issues.append(f"{path}: ingestion manifest is not a passing canonical artifact")
        version = manifest.get("versioningType")
        if not isinstance(version, str) or version not in VERSIONS or version in ingestion_versions:
            issues.append(f"{path}: ingestion versioningType is missing, unsupported, or duplicated")
        else:
            ingestion_versions.add(version)
        if manifest.get("gitSha") != git_sha or manifest.get("artifactSha256") != ingestion_artifact_sha256:
            issues.append(f"{path}: ingestion build identity differs from the campaign identity")
        for name, expected in (("smallRows", CANONICAL_SMALL_ROWS), ("largeRows", CANONICAL_LARGE_ROWS)):
            if type(manifest.get(name)) is not int or manifest.get(name) != expected:
                issues.append(f"{path}: {name} must equal canonical value {expected}")
        if index < len(ingestion_small_logs) and index < len(ingestion_large_logs):
            for name, evidence_path in (
                ("smallLogSha256", ingestion_small_logs[index]),
                ("largeLogSha256", ingestion_large_logs[index]),
                ("gateScriptSha256", ingestion_gate_script),
            ):
                if not evidence_path.is_file() or manifest.get(name) != sha256(evidence_path):
                    issues.append(f"{path}: {name} does not bind the supplied ingestion evidence")
    if ingestion_versions != VERSIONS:
        issues.append(f"ingestion versions are incomplete: {sorted(VERSIONS - ingestion_versions)}")

    saturation_versions: set[str] = set()
    for index, saturation_path in enumerate(saturation_paths):
        saturation = load_manifest(saturation_path, issues)
        if saturation is None:
            continue
        if set(saturation) != SATURATION_SCHEMA:
            issues.append(f"{saturation_path}: saturation manifest schema is not canonical")
        if saturation.get("kind") != "append-saturation" or saturation.get("passed") is not True:
            issues.append(f"{saturation_path}: saturation manifest is not a passing canonical artifact")
        version = saturation.get("versioningType")
        if not isinstance(version, str) or version not in VERSIONS or version in saturation_versions:
            issues.append(f"{saturation_path}: saturation versioningType is missing, unsupported, or duplicated")
        else:
            saturation_versions.add(version)
        if (saturation.get("gitSha") != git_sha
                or saturation.get("artifactSha256") != saturation_artifact_sha256):
            issues.append("saturation build identity differs from the campaign identity")
        expected_saturation = {
            "records": CANONICAL_SATURATION_RECORDS,
            "appendWorkers": 1,
            "queueCapacity": 1,
            "callerThreadAppendRuns": 0,
        }
        for name, expected in expected_saturation.items():
            if type(saturation.get(name)) is not int or saturation.get(name) != expected:
                issues.append(f"saturation {name} must equal canonical value {expected}")
        resources = saturation.get("resources")
        if type(resources) is not int or resources < 4:
            issues.append("saturation resources must be at least four")
            resources = 4
        if saturation.get("coldReopens") != resources:
            issues.append("saturation coldReopens must equal resources")
        if type(saturation.get("submitWaitCount")) is not int or saturation.get("submitWaitCount", 0) < resources:
            issues.append("saturation submitWaitCount must cover every resource")
        wait_total = saturation.get("submitWaitTotalNs")
        wait_max = saturation.get("submitWaitMaxNs")
        if type(wait_total) is not int or type(wait_max) is not int or wait_total < wait_max:
            issues.append("saturation wait totals are malformed")
        elif not 100_000_000 <= wait_max <= 250_000_000:
            issues.append("saturation submitWaitMaxNs must be between 100 and 250 ms")
        expected_waiters = resources - 2
        if (saturation.get("saturatedActiveWorkers") != 1
                or saturation.get("saturatedQueuedTasks") != 1
                or type(saturation.get("saturatedAdmissionWaiters")) is not int
                or saturation.get("saturatedAdmissionWaiters", 0) < expected_waiters
                or saturation.get("saturatedAvailableAdmissions") != 0):
            issues.append("saturation executor occupancy is not canonical")
        if (saturation.get("drainedActiveWorkers") != 0
                or saturation.get("drainedQueuedTasks") != 0
                or saturation.get("drainedAdmissionWaiters") != 0
                or saturation.get("drainedAvailableAdmissions") != 2):
            issues.append("saturation executor did not drain canonically")
        for count_name, max_name in (
            ("gcPauseCount", "gcPauseMaxNs"),
            ("safepointCount", "safepointMaxNs"),
        ):
            count = saturation.get(count_name)
            maximum = saturation.get(max_name)
            if (type(count) is not int or count < 0 or type(maximum) is not int
                    or maximum < 0 or maximum > 250_000_000 or (count == 0 and maximum != 0)):
                issues.append(f"saturation {count_name}/{max_name} evidence is malformed")
        if (type(saturation.get("gcPauseCount")) is int
                and saturation.get("gcPauseCount", 0) > 0
                and saturation.get("safepointCount") == 0):
            issues.append("saturation young-GC evidence has no matching safepoint evidence")
        humongous_samples = saturation.get("humongousRegionSamples")
        gc_pause_count = saturation.get("gcPauseCount")
        required_humongous_samples = gc_pause_count if type(gc_pause_count) is int else 0
        if type(humongous_samples) is not int or humongous_samples < required_humongous_samples:
            issues.append("saturation humongous-region evidence is incomplete")
        for name, evidence_path in (
            ("logSha256", saturation_logs[index] if index < len(saturation_logs) else Path("")),
            ("gateScriptSha256", saturation_gate_script),
        ):
            if not isinstance(saturation.get(name), str) or SHA256_RE.fullmatch(saturation[name]) is None:
                issues.append(f"saturation {name} must be a lowercase SHA-256")
            elif not evidence_path.is_file() or saturation[name] != sha256(evidence_path):
                issues.append(f"saturation {name} does not bind the supplied child evidence")
    if saturation_versions != VERSIONS:
        issues.append(f"saturation versions are incomplete: {sorted(VERSIONS - saturation_versions)}")
    return issues


def manifest_digests_by_version(paths: Sequence[Path]) -> dict[str, str | None]:
    digests: dict[str, str | None] = {version: None for version in sorted(VERSIONS)}
    for path in paths:
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        version = manifest.get("versioningType") if isinstance(manifest, dict) else None
        if version in VERSIONS and digests[version] is None and path.is_file():
            digests[version] = sha256(path)
    return digests


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maintenance-manifest", action="append", required=True, type=Path)
    parser.add_argument("--maintenance-small-log", action="append", required=True, type=Path)
    parser.add_argument("--maintenance-large-log", action="append", required=True, type=Path)
    parser.add_argument("--maintenance-gate-script", required=True, type=Path)
    parser.add_argument("--ingestion-manifest", action="append", required=True, type=Path)
    parser.add_argument("--ingestion-small-log", action="append", required=True, type=Path)
    parser.add_argument("--ingestion-large-log", action="append", required=True, type=Path)
    parser.add_argument("--ingestion-gate-script", required=True, type=Path)
    parser.add_argument("--saturation-manifest", action="append", required=True, type=Path)
    parser.add_argument("--saturation-log", action="append", required=True, type=Path)
    parser.add_argument("--saturation-gate-script", required=True, type=Path)
    parser.add_argument("--expected-git-sha", required=True)
    parser.add_argument("--maintenance-runtime-classpath", required=True)
    parser.add_argument("--ingestion-runtime-classpath", required=True)
    parser.add_argument("--saturation-runtime-classpath", required=True)
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    if re.fullmatch(r"[0-9a-f]{40}", args.expected_git_sha) is None:
        parser.error("--expected-git-sha must be a lowercase 40-character commit SHA")
    try:
        maintenance_artifact_sha256 = runtime_classpath_sha256(args.maintenance_runtime_classpath)
        ingestion_artifact_sha256 = runtime_classpath_sha256(args.ingestion_runtime_classpath)
        saturation_artifact_sha256 = runtime_classpath_sha256(args.saturation_runtime_classpath)
        committed_maintenance_gate_sha256 = tracked_blob_sha256(
            args.expected_git_sha, MAINTENANCE_GATE_PATH
        )
        committed_saturation_gate_sha256 = tracked_blob_sha256(
            args.expected_git_sha, SATURATION_GATE_PATH
        )
        committed_ingestion_gate_sha256 = tracked_blob_sha256(
            args.expected_git_sha, INGESTION_GATE_PATH
        )
        committed_campaign_gate_sha256 = tracked_blob_sha256(
            args.expected_git_sha, CAMPAIGN_GATE_PATH
        )
        committed_artifact_helper_sha256 = tracked_blob_sha256(
            args.expected_git_sha, ARTIFACT_HELPER_PATH
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    issues = evaluate(
        args.maintenance_manifest,
        args.ingestion_manifest,
        args.saturation_manifest,
        args.expected_git_sha,
        maintenance_artifact_sha256,
        ingestion_artifact_sha256,
        saturation_artifact_sha256,
        args.maintenance_small_log,
        args.maintenance_large_log,
        args.ingestion_small_log,
        args.ingestion_large_log,
        args.maintenance_gate_script,
        args.ingestion_gate_script,
        args.saturation_log,
        args.saturation_gate_script,
        committed_maintenance_gate_sha256,
        committed_ingestion_gate_sha256,
        committed_saturation_gate_sha256,
    )
    if sha256(Path(__file__)) != committed_campaign_gate_sha256:
        issues.append("campaign gate script differs from its committed campaign blob")
    artifact_helper = Path(__file__).with_name("hft_artifact.py")
    artifact_helper_sha256 = sha256(artifact_helper) if artifact_helper.is_file() else None
    if artifact_helper_sha256 != committed_artifact_helper_sha256:
        issues.append("artifact identity helper differs from its committed campaign blob")
    manifest = {
        "kind": "hft-campaign",
        "gitSha": args.expected_git_sha,
        "maintenanceArtifactSha256": maintenance_artifact_sha256,
        "ingestionArtifactSha256": ingestion_artifact_sha256,
        "saturationArtifactSha256": saturation_artifact_sha256,
        "maintenanceManifestSha256": manifest_digests_by_version(args.maintenance_manifest),
        "ingestionManifestSha256": manifest_digests_by_version(args.ingestion_manifest),
        "saturationManifestSha256": manifest_digests_by_version(args.saturation_manifest),
        "artifactHelperSha256": artifact_helper_sha256,
        "gateScriptSha256": sha256(Path(__file__)),
        "passed": not issues,
    }
    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for issue in issues:
        print(f"FAIL: {issue}")
    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())
