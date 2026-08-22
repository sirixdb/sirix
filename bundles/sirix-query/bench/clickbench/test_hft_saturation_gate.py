#!/usr/bin/env python3
"""Behavior tests for fail-closed append-saturation evidence."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import hft_saturation_gate as gate


GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
ARTIFACT_SHA = "a" * 64


def saturation_config(**overrides: int | str) -> str:
    values: dict[str, int | str] = {
        "initialHeapBytes": gate.EXPECTED_HEAP_BYTES,
        "maxHeapBytes": gate.EXPECTED_HEAP_BYTES,
        "maxNewSizeBytes": gate.EXPECTED_MAX_NEW_BYTES,
        "g1RegionSizeBytes": gate.EXPECTED_G1_REGION_SIZE_BYTES,
        "gcLogging": "true",
        "safepointLogging": "true",
    }
    values.update(overrides)
    return gate.CONFIG_PREFIX + " ".join(f"{name}={value}" for name, value in values.items())


def saturation_record(**overrides: int | str) -> str:
    values: dict[str, int | str] = {
        "versioningType": "FULL",
        "resources": 4,
        "records": 4096,
        "appendWorkers": 1,
        "queueCapacity": 1,
        "callerThreadAppendRuns": 0,
        "submitWaitCount": 4,
        "submitWaitTotalNs": 500_000_000,
        "submitWaitMaxNs": 150_000_000,
        "saturatedActiveWorkers": 1,
        "saturatedQueuedTasks": 1,
        "saturatedAdmissionWaiters": 2,
        "saturatedAvailableAdmissions": 0,
        "drainedActiveWorkers": 0,
        "drainedQueuedTasks": 0,
        "drainedAdmissionWaiters": 0,
        "drainedAvailableAdmissions": 2,
        "coldReopens": 4,
    }
    values.update(overrides)
    fields = " ".join(f"{name}={value}" for name, value in values.items())
    return f"{gate.SATURATION_PREFIX}{fields}"


def write_log(root: Path, *measurement_lines: str, include_end: bool = True) -> Path:
    lines = [
        f"{gate.BUILD_PREFIX}gitSha={GIT_SHA} artifactSha256={ARTIFACT_SHA}",
        gate.MEASURE_START,
        saturation_config(),
        *measurement_lines,
    ]
    if include_end:
        lines.append(gate.MEASURE_END)
    path = root / "saturation.log"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class HftSaturationGateTest(unittest.TestCase):

    def test_zero_gc_fully_saturated_and_drained_run_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = write_log(Path(directory), saturation_record())

            self.assertEqual([], gate.evaluate(log, GIT_SHA, ARTIFACT_SHA))

    def test_executor_occupancy_and_drain_must_be_observed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            not_full = write_log(root, saturation_record(saturatedQueuedTasks=0))
            self.assertTrue(any("occupancy" in issue for issue in gate.evaluate(
                not_full, GIT_SHA, ARTIFACT_SHA
            )))

            not_drained = write_log(root, saturation_record(drainedAvailableAdmissions=1))
            self.assertTrue(any("did not return" in issue for issue in gate.evaluate(
                not_drained, GIT_SHA, ARTIFACT_SHA
            )))

    def test_positive_humongous_regions_and_long_pauses_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = write_log(
                Path(directory),
                "[1s][info][gc] GC(0) Pause Young (Normal) (G1 Evacuation Pause) "
                "10M->1M(4096M) 251.001ms",
                "[1s][debug][gc,heap] GC(0) Humongous regions: 1->0",
                '[1s][info][safepoint] Safepoint "G1CollectForAllocation", Total: 251.001 ms',
                saturation_record(),
            )

            issues = gate.evaluate(log, GIT_SHA, ARTIFACT_SHA)
            self.assertTrue(any("humongous-region occupancy" in issue for issue in issues))
            self.assertTrue(any("young-GC pause exceeds" in issue for issue in issues))
            self.assertTrue(any("safepoint exceeds" in issue for issue in issues))

    def test_measurement_region_must_be_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = write_log(Path(directory), saturation_record(), include_end=False)

            self.assertTrue(any(gate.MEASURE_END in issue for issue in gate.evaluate(
                log, GIT_SHA, ARTIFACT_SHA
            )))

    def test_effective_heap_nursery_and_region_must_be_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases = (
                ("initialHeapBytes", 8 * gate.GIB),
                ("maxHeapBytes", 8 * gate.GIB),
                ("maxNewSizeBytes", 512 * gate.MIB),
                ("g1RegionSizeBytes", 2 * gate.MIB),
            )
            for name, value in cases:
                with self.subTest(name=name):
                    log = write_log(root, saturation_record())
                    lines = log.read_text(encoding="utf-8").splitlines()
                    config_index = lines.index(saturation_config())
                    lines[config_index] = saturation_config(**{name: value})
                    log.write_text("\n".join(lines) + "\n", encoding="utf-8")

                    issues = gate.evaluate(log, GIT_SHA, ARTIFACT_SHA)

                    self.assertTrue(any(name in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()
