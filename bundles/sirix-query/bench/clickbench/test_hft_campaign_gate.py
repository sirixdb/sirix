#!/usr/bin/env python3
"""Behavior tests for canonical HFT campaign artifacts."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import hft_campaign_gate as campaign_gate
import hft_saturation_gate as saturation_gate


GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
MAINTENANCE_ARTIFACT_SHA = "1" * 64
SATURATION_ARTIFACT_SHA = "2" * 64
INGESTION_ARTIFACT_SHA = "3" * 64


class HftCampaignGateTest(unittest.TestCase):

    def test_saturation_record_proves_bounded_worker_only_admission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory, "saturation.log")
            log.write_text(
                f"# HFT_BUILD gitSha={GIT_SHA} artifactSha256={SATURATION_ARTIFACT_SHA}\n"
                "# HFT_MEASURE_START\n"
                f"# HFT_SATURATION_CONFIG initialHeapBytes={4 * campaign_gate.GIB} "
                f"maxHeapBytes={4 * campaign_gate.GIB} maxNewSizeBytes={campaign_gate.GIB} "
                f"g1RegionSizeBytes={4 * campaign_gate.MIB} gcLogging=true safepointLogging=true\n"
                "# HFT_APPEND_SATURATION versioningType=FULL resources=4 records=4096 appendWorkers=1 "
                "queueCapacity=1 callerThreadAppendRuns=0 submitWaitCount=4 submitWaitTotalNs=500000000 "
                "submitWaitMaxNs=150000000 saturatedActiveWorkers=1 saturatedQueuedTasks=1 "
                "saturatedAdmissionWaiters=2 saturatedAvailableAdmissions=0 drainedActiveWorkers=0 "
                "drainedQueuedTasks=0 drainedAdmissionWaiters=0 drainedAvailableAdmissions=2 coldReopens=4\n"
                "# HFT_MEASURE_END\n",
                encoding="utf-8",
            )

            issues = saturation_gate.evaluate(log, GIT_SHA, SATURATION_ARTIFACT_SHA, "FULL")

            self.assertEqual([], issues)

    def test_campaign_requires_canonical_all_version_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            maintenance_gate = root / "maintenance-gate.py"
            ingestion_gate = root / "ingestion-gate.py"
            saturation_gate_script = root / "saturation-gate.py"
            maintenance_gate.write_bytes(b"maintenance gate")
            ingestion_gate.write_bytes(b"ingestion gate")
            saturation_gate_script.write_bytes(b"saturation gate")
            maintenance_manifests: list[Path] = []
            ingestion_manifests: list[Path] = []
            saturation_manifests: list[Path] = []
            maintenance_small_logs: list[Path] = []
            maintenance_large_logs: list[Path] = []
            ingestion_small_logs: list[Path] = []
            ingestion_large_logs: list[Path] = []
            saturation_logs: list[Path] = []
            for index, version in enumerate(sorted(campaign_gate.VERSIONS)):
                maintenance_small = root / f"maintenance-small-{version}.log"
                maintenance_large = root / f"maintenance-large-{version}.log"
                ingestion_small = root / f"ingestion-small-{version}.log"
                ingestion_large = root / f"ingestion-large-{version}.log"
                saturation_log = root / f"saturation-{version}.log"
                for path, value in (
                    (maintenance_small, f"maintenance small {index}"),
                    (maintenance_large, f"maintenance large {index}"),
                    (ingestion_small, f"ingestion small {index}"),
                    (ingestion_large, f"ingestion large {index}"),
                    (saturation_log, f"saturation {index}"),
                ):
                    path.write_text(value, encoding="utf-8")
                maintenance_small_logs.append(maintenance_small)
                maintenance_large_logs.append(maintenance_large)
                ingestion_small_logs.append(ingestion_small)
                ingestion_large_logs.append(ingestion_large)
                saturation_logs.append(saturation_log)

                maintenance_manifest = root / f"maintenance-{version}.json"
                maintenance_manifest.write_text(json.dumps({
                    "kind": "projection-maintenance", "passed": True, "versioningType": version,
                    "gitSha": GIT_SHA, "artifactSha256": MAINTENANCE_ARTIFACT_SHA,
                    "dirtyRecords": campaign_gate.CANONICAL_DIRTY_RECORDS,
                    "smallRows": campaign_gate.CANONICAL_SMALL_ROWS,
                    "largeRows": campaign_gate.CANONICAL_LARGE_ROWS,
                    "expectedHeapBytes": campaign_gate.CANONICAL_HEAP_BYTES,
                    "smallLogSha256": campaign_gate.sha256(maintenance_small),
                    "largeLogSha256": campaign_gate.sha256(maintenance_large),
                    "gateScriptSha256": campaign_gate.sha256(maintenance_gate),
                }), encoding="utf-8")
                maintenance_manifests.append(maintenance_manifest)

                ingestion_manifest = root / f"ingestion-{version}.json"
                ingestion_manifest.write_text(json.dumps({
                    "kind": "projection-ingestion", "passed": True, "versioningType": version,
                    "verdict": "PASS", "occupancyVerdict": "PASS",
                    "gitSha": GIT_SHA, "artifactSha256": INGESTION_ARTIFACT_SHA,
                    "smallRows": campaign_gate.CANONICAL_SMALL_ROWS,
                    "largeRows": campaign_gate.CANONICAL_LARGE_ROWS,
                    "expectedHeapBytes": campaign_gate.CANONICAL_HEAP_BYTES,
                    "expectedMaxNewBytes": campaign_gate.CANONICAL_MAX_NEW_BYTES,
                    "expectedSideBatchBytes": campaign_gate.CANONICAL_SIDE_BATCH_BYTES,
                    "smallLogSha256": campaign_gate.sha256(ingestion_small),
                    "largeLogSha256": campaign_gate.sha256(ingestion_large),
                    "gateScriptSha256": campaign_gate.sha256(ingestion_gate),
                }), encoding="utf-8")
                ingestion_manifests.append(ingestion_manifest)

                saturation_manifest = root / f"saturation-{version}.json"
                saturation_manifest.write_text(json.dumps({
                    "kind": "append-saturation", "passed": True, "versioningType": version,
                    "gitSha": GIT_SHA, "artifactSha256": SATURATION_ARTIFACT_SHA,
                    "resources": 4, "records": campaign_gate.CANONICAL_SATURATION_RECORDS,
                    "appendWorkers": 1, "queueCapacity": 1, "callerThreadAppendRuns": 0,
                    "submitWaitCount": 4, "submitWaitTotalNs": 500_000_000,
                    "submitWaitMaxNs": 150_000_000, "saturatedActiveWorkers": 1,
                    "saturatedQueuedTasks": 1, "saturatedAdmissionWaiters": 2,
                    "saturatedAvailableAdmissions": 0, "drainedActiveWorkers": 0,
                    "drainedQueuedTasks": 0, "drainedAdmissionWaiters": 0,
                    "drainedAvailableAdmissions": 2, "coldReopens": 4,
                    "gcPauseCount": 0, "gcPauseMaxNs": 0, "safepointCount": 0,
                    "safepointMaxNs": 0, "humongousRegionSamples": 0,
                    "initialHeapBytes": campaign_gate.CANONICAL_HEAP_BYTES,
                    "maxHeapBytes": campaign_gate.CANONICAL_HEAP_BYTES,
                    "maxNewSizeBytes": campaign_gate.CANONICAL_MAX_NEW_BYTES,
                    "g1RegionSizeBytes": 4 * campaign_gate.MIB,
                    "logSha256": campaign_gate.sha256(saturation_log),
                    "gateScriptSha256": campaign_gate.sha256(saturation_gate_script),
                }), encoding="utf-8")
                saturation_manifests.append(saturation_manifest)

            def evaluate(maintenance: list[Path] | None = None) -> list[str]:
                selected = maintenance_manifests if maintenance is None else maintenance
                return campaign_gate.evaluate(
                    selected, ingestion_manifests, saturation_manifests, GIT_SHA,
                    MAINTENANCE_ARTIFACT_SHA, INGESTION_ARTIFACT_SHA, SATURATION_ARTIFACT_SHA,
                    maintenance_small_logs[:len(selected)], maintenance_large_logs[:len(selected)],
                    ingestion_small_logs, ingestion_large_logs, maintenance_gate, ingestion_gate,
                    saturation_logs, saturation_gate_script, campaign_gate.sha256(maintenance_gate),
                    campaign_gate.sha256(ingestion_gate), campaign_gate.sha256(saturation_gate_script),
                )

            self.assertEqual([], evaluate())

            first_ingestion = json.loads(ingestion_manifests[0].read_text(encoding="utf-8"))
            first_ingestion["passed"] = False
            first_ingestion["verdict"] = "INCONCLUSIVE"
            first_ingestion["occupancyVerdict"] = "INCONCLUSIVE"
            ingestion_manifests[0].write_text(json.dumps(first_ingestion), encoding="utf-8")

            first_ingestion["expectedHeapBytes"] = 8 * campaign_gate.GIB
            ingestion_manifests[0].write_text(json.dumps(first_ingestion), encoding="utf-8")
            self.assertTrue(any("expectedHeapBytes" in issue for issue in evaluate()))
            first_ingestion["expectedHeapBytes"] = campaign_gate.CANONICAL_HEAP_BYTES
            ingestion_manifests[0].write_text(json.dumps(first_ingestion), encoding="utf-8")
            self.assertTrue(any("not a passing canonical artifact" in issue for issue in evaluate()))
            first_ingestion["passed"] = True
            first_ingestion["verdict"] = "PASS"
            first_ingestion["occupancyVerdict"] = "PASS"
            ingestion_manifests[0].write_text(json.dumps(first_ingestion), encoding="utf-8")

            incomplete = evaluate(maintenance_manifests[:-1])
            self.assertTrue(any("four maintenance" in issue for issue in incomplete))
            self.assertTrue(any("maintenance versions are incomplete" in issue for issue in incomplete))

            maintenance_small_logs[0].write_bytes(b"tampered")
            self.assertTrue(any("does not bind" in issue for issue in evaluate()))
            maintenance_small_logs[0].write_text("maintenance small 0", encoding="utf-8")

            uncommitted = campaign_gate.evaluate(
                maintenance_manifests, ingestion_manifests, saturation_manifests, GIT_SHA,
                MAINTENANCE_ARTIFACT_SHA, INGESTION_ARTIFACT_SHA, SATURATION_ARTIFACT_SHA,
                maintenance_small_logs, maintenance_large_logs, ingestion_small_logs, ingestion_large_logs,
                maintenance_gate, ingestion_gate, saturation_logs, saturation_gate_script,
                "a" * 64, campaign_gate.sha256(ingestion_gate), campaign_gate.sha256(saturation_gate_script),
            )
            self.assertTrue(any("committed campaign blob" in issue for issue in uncommitted))

    def test_manifest_digests_are_keyed_by_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for version in sorted(campaign_gate.VERSIONS):
                path = root / version / "manifest.json"
                path.parent.mkdir()
                path.write_text(json.dumps({"versioningType": version}), encoding="utf-8")
                paths.append(path)

            digests = campaign_gate.manifest_digests_by_version(paths)

            self.assertEqual(campaign_gate.VERSIONS, set(digests))
            self.assertTrue(all(digest is not None for digest in digests.values()))


if __name__ == "__main__":
    unittest.main()
