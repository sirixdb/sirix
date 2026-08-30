#!/usr/bin/env python3
"""Standard-library tests for hft_maintenance_gate.py."""

from __future__ import annotations

import unittest

import hft_gc_gate as ingestion_gate
import hft_maintenance_gate as maintenance_gate
from test_hft_gc_gate import (
    ARTIFACT_SHA,
    async_flush_line,
    g1_region_line,
    hft_build_line,
    hft_config_line,
    safepoint_line,
    young_line,
)


MIB = 1024 * 1024
GIT_SHA = "0123456789abcdef0123456789abcdef01234567"


def maintenance_line(
    *,
    operations: int = 301_271,
    bytes_read: int = 32 * MIB,
    bytes_written: int = 16 * MIB,
    full_rebuilds: int = 0,
) -> str:
    boundary_operations = 100 + 90 + 100_000 + 100_001 + 2 + 2 + 8 + 8
    storage_operations = operations - boundary_operations
    if storage_operations < 0:
        raise ValueError("operations is smaller than the boundary fixture")
    return (
        f"{ingestion_gate.PROJECTION_MAINTENANCE_PREFIX}"
        "commits=8 dirtyRecords=100001 rowGroupsRead=100 rowGroupsWritten=100 "
        "dictionarySegments=8 fenceChunksRead=16 fenceChunksWritten=8 "
        "setChunksRead=4 setChunksWritten=4 bloomRowGroupsRead=200000 "
        f"bloomChunksWritten=1014 metadataReads=8 metadataWrites=8 dictionaryProbes=100001 "
        f"storageReads={storage_operations // 2} "
        f"storageWrites={storage_operations - storage_operations // 2} "
        "allocatorAllocations=100 allocatorReleases=90 tilReads=100000 tilWrites=100001 "
        "nativeAllocations=2 nativeReleases=2 asyncSubmissions=8 asyncCompletions=8 "
        f"operations={operations} bytesRead={bytes_read} "
        f"bytesWritten={bytes_written} fullRebuilds={full_rebuilds}"
    )


def evidence_line(rows: int) -> str:
    return (
        f"{ingestion_gate.PROJECTION_EVIDENCE_PREFIX}"
        f"revisionsVerified=9 historicalRevisions=8 oracleRows={rows * 9} servedRows={rows * 9} "
        "oracleMatches=23 servedMatches=23 "
        "servedRevisions=9 stableAnchors=1 stableIds=1 successorSegments=8 "
        "introductionRevision=3 maxProbeUnits=8"
    )


def revision_lines(rows: int) -> list[str]:
    return [
        f"{ingestion_gate.PROJECTION_REVISION_PREFIX}revision={revision} oracleRows={rows} "
        f"servedRows={rows} oracleMatches={1 if revision < 3 else 3} "
        f"servedMatches={1 if revision < 3 else 3} anchor=17 oldId=1 "
        f"newId={0 if revision < 3 else 1001} "
        f"successorSegments={max(0, revision - 1)}"
        for revision in range(1, 10)
    ]


def maintenance_log(
    *,
    rows: int,
    operations: int = 301_271,
    bytes_read: int = 32 * MIB,
    bytes_written: int = 16 * MIB,
    full_rebuilds: int = 0,
    submit_wait_max_ns: int = 500_000,
    start_flush_max_ns: int = 10_500_000,
) -> list[str]:
    return [
        ingestion_gate.MEASURE_START,
        hft_build_line(),
        hft_config_line(
            global_dict="auto",
            auto_commit_nodes=16_384,
            async_flush_node_cap=0,
            importer="ordinary-maintenance",
            expected_rows=rows,
        ),
        g1_region_line(),
        young_line(0, 100),
        safepoint_line(0),
        maintenance_async_flush_line(
            submit_wait_max_ns=submit_wait_max_ns,
            start_flush_max_ns=start_flush_max_ns,
        ),
        maintenance_line(
            operations=operations,
            bytes_read=bytes_read,
            bytes_written=bytes_written,
            full_rebuilds=full_rebuilds,
        ),
        *revision_lines(rows),
        evidence_line(rows),
        ingestion_gate.MEASURE_END,
    ]


def maintenance_async_flush_line(**overrides: int) -> str:
    values = {
        "combined_epochs": 0,
        "side_only_epochs": 101,
        "kvl_pages": 0,
        "kvl_attempted_pages": 0,
        "kvl_attempted_pages_max": 0,
        "foreground_flush_count": 0,
        "foreground_flush_total_ns": 0,
        "foreground_flush_max_ns": 0,
        "kvl_frame_cache_pages": 0,
        "kvl_frame_cache_bytes": 0,
        "pinned_trie_spill_epochs": 0,
        "pinned_trie_spill_pages": 0,
        "pinned_trie_spill_batch_max": 0,
        "pinned_trie_live_max": 0,
        "pinned_trie_high_water": 0,
    }
    values.update(overrides)
    return async_flush_line(**values)


class MaintenanceGateTest(unittest.TestCase):

    def evaluate(self, lines: list[str], rows: int = 1_000_000) -> maintenance_gate.MaintenanceEvaluation:
        return maintenance_gate.evaluate_run(
            ingestion_gate.parse_lines(lines),
            "maintenance",
            rows,
            100_001,
            4 * ingestion_gate.GIB,
            "FULL",
            GIT_SHA,
            ARTIFACT_SHA,
        )

    def test_auto_global_touched_unit_contract_passes(self) -> None:
        evaluation = self.evaluate(maintenance_log(rows=1_000_000))

        self.assertTrue(evaluation.passed, evaluation.issues)

    def test_full_rebuild_and_foreground_wait_fail_closed(self) -> None:
        evaluation = self.evaluate(
            maintenance_log(
                rows=1_000_000,
                full_rebuilds=1,
                start_flush_max_ns=250_000_001,
            )
        )

        self.assertTrue(any("fullRebuilds" in issue for issue in evaluation.issues))
        self.assertTrue(any("250 ms" in issue for issue in evaluation.issues))

    def test_whole_foreground_flush_contract_fails_closed(self) -> None:
        cases = (
            ({"foreground_flush_max_ns": 250_000_001}, "250 ms"),
            (
                {
                    "foreground_flush_total_ns": 19_999_999,
                    "foreground_flush_max_ns": 20_000_000,
                },
                "maximum exceeds its total",
            ),
            ({"foreground_flush_count": 99}, "does not match combined epochs"),
        )
        for overrides, expected_issue in cases:
            with self.subTest(expected_issue=expected_issue):
                lines = maintenance_log(rows=1_000_000)
                telemetry_index = next(
                    index
                    for index, line in enumerate(lines)
                    if line.startswith(ingestion_gate.ASYNC_FLUSH_PREFIX)
                )
                lines[telemetry_index] = maintenance_async_flush_line(**overrides)

                evaluation = self.evaluate(lines)

                self.assertFalse(evaluation.passed)
                self.assertTrue(any(expected_issue in issue for issue in evaluation.issues))

    def test_multi_writer_telemetry_is_aggregated(self) -> None:
        lines = maintenance_log(rows=1_000_000)
        telemetry = maintenance_async_flush_line(
            side_pages=500,
            side_bytes=16 * MIB,
            peak_active_side_bytes=16 * MIB,
        )
        lines.insert(lines.index(maintenance_line()), telemetry)

        evaluation = self.evaluate(lines)

        self.assertTrue(evaluation.passed, evaluation.issues)

    def test_async_commit_mode_rejects_full_til_epochs(self) -> None:
        lines = maintenance_log(rows=1_000_000)
        telemetry_index = next(
            index
            for index, line in enumerate(lines)
            if line.startswith(ingestion_gate.ASYNC_FLUSH_PREFIX)
        )
        lines[telemetry_index] = async_flush_line()

        evaluation = self.evaluate(lines)

        self.assertFalse(evaluation.passed)
        self.assertTrue(any("full TIL epoch work" in issue for issue in evaluation.issues))

    def test_zero_gc_and_zero_safepoints_is_valid(self) -> None:
        lines = maintenance_log(rows=1_000_000)
        lines.remove(young_line(0, 100))
        lines.remove(safepoint_line(0))

        evaluation = self.evaluate(lines)

        self.assertTrue(evaluation.passed, evaluation.issues)

    def test_mode_specific_cap_and_canonical_region_are_required(self) -> None:
        cases = (
            ("asyncFlushNodeCap=0", "asyncFlushNodeCap=16384", "asyncFlushNodeCap"),
            ("importer=ordinary-maintenance", "importer=parallel-bulk", "importer"),
            (
                f"g1RegionSizeBytes={4 * MIB}",
                f"g1RegionSizeBytes={2 * MIB}",
                "g1RegionSizeBytes",
            ),
        )
        for original, replacement, expected_issue in cases:
            with self.subTest(field=expected_issue):
                lines = maintenance_log(rows=1_000_000)
                config_index = next(
                    index
                    for index, line in enumerate(lines)
                    if line.startswith(ingestion_gate.HFT_CONFIG_PREFIX)
                )
                lines[config_index] = lines[config_index].replace(original, replacement)
                if expected_issue == "g1RegionSizeBytes":
                    lines[lines.index(g1_region_line())] = g1_region_line(2)

                evaluation = self.evaluate(lines)

                self.assertFalse(evaluation.passed)
                self.assertTrue(any(expected_issue in issue for issue in evaluation.issues))

    def test_artifact_identity_and_drain_component_fail_closed(self) -> None:
        lines = maintenance_log(rows=1_000_000)
        lines[lines.index(hft_build_line())] = hft_build_line("0" * 64)
        telemetry_index = next(
            index for index, line in enumerate(lines) if line.startswith(ingestion_gate.ASYNC_FLUSH_PREFIX)
        )
        lines[telemetry_index] = maintenance_async_flush_line(
            drain_permit_wait_max_ns=250_000_001,
            final_drain_max_ns=250_000_001,
        )

        evaluation = self.evaluate(lines)

        self.assertTrue(any("artifact SHA-256" in issue for issue in evaluation.issues))
        self.assertTrue(any("drainPermitWaitMaxNs" in issue for issue in evaluation.issues))

    def test_historical_predicate_mismatch_fails_closed(self) -> None:
        lines = maintenance_log(rows=1_000_000)
        revision = revision_lines(1_000_000)[4]
        lines[lines.index(revision)] = revision.replace("servedMatches=3", "servedMatches=2")

        evaluation = self.evaluate(lines)

        self.assertTrue(any("historical projection predicate" in issue for issue in evaluation.issues))

    def test_major_collection_fails_closed(self) -> None:
        lines = maintenance_log(rows=1_000_000)
        lines.insert(-1, "[2.000s][info][gc] Major GC")

        evaluation = self.evaluate(lines)

        self.assertTrue(any("major collection" in issue for issue in evaluation.issues))

    def test_cross_scale_work_must_be_independent_of_base_size(self) -> None:
        small = self.evaluate(maintenance_log(rows=1_000_000), 1_000_000)
        large = self.evaluate(
            maintenance_log(
                rows=4_000_000,
                operations=450_000,
                bytes_read=96 * MIB,
                bytes_written=32 * MIB,
            ),
            4_000_000,
        )

        issues = maintenance_gate.evaluate_pair(small, large)
        self.assertTrue(any("operations" in issue for issue in issues))
        self.assertTrue(any("bytes" in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()
