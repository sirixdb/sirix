#!/usr/bin/env python3
"""Standard-library tests for hft_gc_gate.py."""

from __future__ import annotations

import contextlib
import io
import unittest

import hft_gc_gate as gate


MIB = 1024 * 1024
GIT_SHA = "0123456789abcdef0123456789abcdef01234567"
ARTIFACT_SHA = "89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567"
CLI_IDENTITY = ["--expected-git-sha", GIT_SHA, "--runtime-classpath", __file__]


def hft_build_line(artifact_sha256: str = ARTIFACT_SHA) -> str:
    return f"{gate.HFT_BUILD_PREFIX}gitSha={GIT_SHA} artifactSha256={artifact_sha256}"


def young_line(
    index: int,
    after_mib: int | float,
    capacity_mib: int = 4096,
    pause_ms: float = 0.75,
) -> str:
    return (
        f"[{index + 1}.000s][info][gc] GC({index}) Pause Young (Normal) (G1 Evacuation Pause) "
        f"{after_mib + 64}M->{after_mib}M({capacity_mib}M) {pause_ms:.3f}ms"
    )


def g1_region_line(region_size_mib: int = 4) -> str:
    region_size_kib = region_size_mib * 1024
    return (
        "[1.000s][debug][gc,heap] GC(0) "
        f"region size {region_size_kib}K, 8 young (16384K), 8 survivors (16384K)"
    )


def safepoint_line(index: int, total_ns: int = 900_000) -> str:
    return (
        f"[{index + 1}.001s][info][safepoint] Safepoint \"G1CollectForAllocation\", "
        f"Reaching safepoint: 1000 ns, Cleanup: 1000 ns, At safepoint: {total_ns - 2000} ns, "
        f"Total: {total_ns} ns"
    )


def hft_config_line(
    *,
    global_dict: str = "never",
    auto_commit_nodes: int = 4_194_304,
    async_flush_node_cap: int = 16_384,
    arena_strategy: str = "shared",
    max_new_size_bytes: int = 1024 * MIB,
    initial_heap_bytes: int = 4096 * MIB,
    max_heap_bytes: int = 4096 * MIB,
    g1_region_size_bytes: int = 4 * MIB,
    gc_logging: bool = True,
    safepoint_logging: bool = True,
    storage: str = "FILE_CHANNEL",
    importer: str = "parallel-bulk",
    projection_mode: str = "incremental",
    expected_rows: int = 1_000_000,
    pinned_trie_scan_budget: int = 1_024,
    pinned_trie_batch_capacity: int = 64,
    versioning_type: str = "FULL",
    append_workers: int = 2,
    append_queue_capacity: int = 1,
) -> str:
    return (
        f"{gate.HFT_CONFIG_PREFIX}globalDict={global_dict} autoCommitNodes={auto_commit_nodes} "
        f"asyncFlushNodeCap={async_flush_node_cap} "
        f"arenaStrategy={arena_strategy} maxNewSizeBytes={max_new_size_bytes} "
        f"initialHeapBytes={initial_heap_bytes} maxHeapBytes={max_heap_bytes} "
        f"g1RegionSizeBytes={g1_region_size_bytes} "
        f"gcLogging={str(gc_logging).lower()} safepointLogging={str(safepoint_logging).lower()} "
        f"storage={storage} importer={importer} "
        f"projectionMode={projection_mode} expectedRows={expected_rows} "
        f"pinnedTrieScanBudget={pinned_trie_scan_budget} "
        f"pinnedTrieBatchCapacity={pinned_trie_batch_capacity} versioningType={versioning_type} "
        f"appendWorkers={append_workers} appendQueueCapacity={append_queue_capacity}"
    )


def async_flush_line(
    *,
    combined_epochs: int = 100,
    side_only_epochs: int = 1,
    kvl_pages: int = 1_600,
    side_pages: int = 1_000,
    side_bytes: int = 32 * MIB,
    peak_active_side_bytes: int = 32 * MIB,
    rotation_permit_wait_max_ns: int = 10_000_000,
    drain_permit_wait_max_ns: int = 100_000_000,
    kvl_attempted_pages: int = 1_600,
    kvl_promoted_pages: int = 0,
    kvl_attempted_pages_max: int = 16,
    native_reservoir_count: int = 2,
    native_reservoir_bytes: int = 64 * MIB,
    pinned_trie_spill_epochs: int = 4,
    pinned_trie_spill_pages: int = 173,
    pinned_trie_spill_batch_max: int = 64,
    pinned_trie_live_max: int = 311,
    pinned_trie_high_water: int = 487,
    worker_runs: int | None = None,
    submit_wait_max_ns: int = 500_000,
    submit_wait_count: int | None = None,
    start_flush_max_ns: int = 10_500_000,
    start_flush_count: int | None = None,
    foreground_flush_count: int = 100,
    foreground_flush_total_ns: int = 100_000_000,
    foreground_flush_max_ns: int = 20_000_000,
    final_drain_max_ns: int = 200_000_000,
    kvl_frame_cache_pages: int | None = None,
    kvl_frame_cache_bytes: int = 157_286_400,
    caller_thread_append_runs: int = 0,
) -> str:
    epochs = combined_epochs + side_only_epochs
    worker_runs = epochs if worker_runs is None else worker_runs
    submit_wait_count = epochs if submit_wait_count is None else submit_wait_count
    start_flush_count = epochs if start_flush_count is None else start_flush_count
    kvl_frame_cache_pages = kvl_pages if kvl_frame_cache_pages is None else kvl_frame_cache_pages
    rotation_wait_total_ns = max(20_000_000, rotation_permit_wait_max_ns)
    drain_wait_total_ns = max(110_000_000, drain_permit_wait_max_ns)
    permit_wait_total_ns = rotation_wait_total_ns + drain_wait_total_ns
    permit_wait_max_ns = max(rotation_permit_wait_max_ns, drain_permit_wait_max_ns)
    return (
        f"{gate.ASYNC_FLUSH_PREFIX}"
        f"combinedEpochs={combined_epochs} sideOnlyEpochs={side_only_epochs} kvlPages={kvl_pages} "
        f"kvlAttemptedPages={kvl_attempted_pages} kvlPromotedPages={kvl_promoted_pages} "
        f"kvlAttemptedPagesMax={kvl_attempted_pages_max} "
        f"sidePages={side_pages} sideBytes={side_bytes} "
        f"peakActiveSideBytes={peak_active_side_bytes} "
        f"permitAcquires=103 permitWaitTotalNs={permit_wait_total_ns} "
        f"permitWaitMaxNs={permit_wait_max_ns} "
        f"rotationPermitAcquires=101 rotationPermitWaitTotalNs={rotation_wait_total_ns} "
        f"rotationPermitWaitMaxNs={rotation_permit_wait_max_ns} "
        f"drainPermitAcquires=2 drainPermitWaitTotalNs={drain_wait_total_ns} "
        f"drainPermitWaitMaxNs={drain_permit_wait_max_ns} "
        f"workerRuns={worker_runs} workerTotalNs=1200000000 workerMaxNs=250000000 "
        f"submitWaitCount={submit_wait_count} submitWaitTotalNs={max(1_000_000, submit_wait_max_ns)} "
        f"submitWaitMaxNs={submit_wait_max_ns} "
        f"callerThreadAppendRuns={caller_thread_append_runs} "
        f"startFlushCount={start_flush_count} startFlushTotalNs={max(30_000_000, start_flush_max_ns)} "
        f"startFlushMaxNs={start_flush_max_ns} "
        f"foregroundFlushCount={foreground_flush_count} "
        f"foregroundFlushTotalNs={foreground_flush_total_ns} "
        f"foregroundFlushMaxNs={foreground_flush_max_ns} "
        f"finalDrainCount=1 finalDrainTotalNs={max(200_000_000, final_drain_max_ns)} "
        f"finalDrainMaxNs={final_drain_max_ns} "
        f"nativeReservoirCount={native_reservoir_count} "
        f"nativeReservoirBytes={native_reservoir_bytes} "
        f"kvlFrameCachePages={kvl_frame_cache_pages} kvlFrameCacheBytes={kvl_frame_cache_bytes} "
        "kvlCacheFallbackPages=0 kvlCacheFallbackBytes=0 "
        f"pinnedTrieSpillEpochs={pinned_trie_spill_epochs} "
        f"pinnedTrieSpillPages={pinned_trie_spill_pages} "
        f"pinnedTrieSpillBatchMax={pinned_trie_spill_batch_max} "
        f"pinnedTrieLiveMax={pinned_trie_live_max} "
        f"pinnedTrieHighWater={pinned_trie_high_water}"
    )


def stable_log(
    after_mib: int,
    samples: int = 60,
    outside_event: str | None = None,
    expected_rows: int = 1_000_000,
    max_new_size_bytes: int = 1024 * MIB,
) -> list[str]:
    lines = []
    if outside_event is not None:
        lines.append(outside_event)
    lines.append(gate.MEASURE_START)
    lines.append(hft_build_line())
    lines.append(
        hft_config_line(
            expected_rows=expected_rows,
            max_new_size_bytes=max_new_size_bytes,
        )
    )
    lines.append(g1_region_line())
    for index in range(samples):
        value = after_mib + ((index % 3) - 1)
        lines.append(young_line(index, value))
        lines.append(safepoint_line(index))
    lines.append(async_flush_line())
    lines.append(gate.MEASURE_END)
    if outside_event is not None:
        lines.append(outside_event)
    return lines


def occupancy_log(
    values_mib: list[int | float],
    *,
    expected_rows: int = 1_000_000,
    max_new_size_bytes: int = 1024 * MIB,
    region_size_mib: int = 4,
) -> list[str]:
    lines = [
        gate.MEASURE_START,
        hft_build_line(),
        hft_config_line(
            expected_rows=expected_rows,
            max_new_size_bytes=max_new_size_bytes,
            g1_region_size_bytes=region_size_mib * MIB,
        ),
        g1_region_line(region_size_mib),
    ]
    for index, value in enumerate(values_mib):
        lines.append(young_line(index, value))
        lines.append(safepoint_line(index))
    lines.append(async_flush_line())
    lines.append(gate.MEASURE_END)
    return lines


class ParseTest(unittest.TestCase):

    def test_unified_gc_start_record_is_not_a_malformed_sample(self) -> None:
        start = "[1.000s][info][gc,start] GC(0) Pause Young (Normal) (G1 Evacuation Pause)"
        parsed = gate.parse_lines(
            [gate.MEASURE_START, start, young_line(0, 100), safepoint_line(0), gate.MEASURE_END]
        )

        self.assertEqual([], parsed.errors)
        self.assertEqual(1, len(parsed.young_samples))

    def test_only_measurement_region_is_parsed(self) -> None:
        outside = "[0.1s][info][gc] GC(0) Pause Full (System.gc()) 10M->1M(4096M) 5ms"
        parsed = gate.parse_lines(stable_log(100, outside_event=outside))

        self.assertEqual(60, len(parsed.young_samples))
        self.assertEqual(60, len(parsed.safepoint_nanos))
        self.assertEqual(1, len(parsed.hft_configurations))
        self.assertEqual(1, len(parsed.async_flush_telemetry))
        self.assertEqual([], parsed.forbidden_events)
        self.assertEqual([], parsed.errors)
        self.assertEqual(100 * MIB, parsed.young_samples[1].after_bytes)
        self.assertEqual(750_000, parsed.young_samples[0].pause_nanos)

    def test_missing_end_marker_is_an_error(self) -> None:
        parsed = gate.parse_lines([gate.MEASURE_START, young_line(0, 100)])

        self.assertTrue(any(gate.MEASURE_END in error for error in parsed.errors))

    def test_duplicate_region_is_an_error(self) -> None:
        parsed = gate.parse_lines(
            [gate.MEASURE_START, gate.MEASURE_END, gate.MEASURE_START, gate.MEASURE_END]
        )

        self.assertTrue(any("duplicate" in error for error in parsed.errors))

    def test_forbidden_major_and_pressure_events_are_rejected(self) -> None:
        events = (
            "GC(1) Pause Full (G1 Compaction Pause) 1G->1G(4G) 10ms",
            "GC(2) Concurrent Mark Cycle",
            "GC(3) Pause Young (Concurrent Start) (G1 Evacuation Pause) 1G->900M(4G) 2ms",
            "GC(4) Pause Remark 900M->900M(4G) 1ms",
            "GC(5) Pause Young (Prepare Mixed) (G1 Evacuation Pause) 1G->900M(4G) 2ms",
            "GC(6) To-space exhausted",
            "GC(7) Evacuation Failure",
            "GC(8) Allocation Failure",
            "GC(9) Pause Young (Normal) (G1 Humongous Allocation) 1G->900M(4G) 2ms",
        )
        for event in events:
            with self.subTest(event=event):
                parsed = gate.parse_lines([gate.MEASURE_START, event, gate.MEASURE_END])
                self.assertEqual(1, len(parsed.forbidden_events))

    def test_positive_humongous_region_occupancy_is_rejected(self) -> None:
        positive = gate.parse_lines(
            [gate.MEASURE_START, "[1s][debug][gc,heap] GC(0) Humongous regions: 1->0", gate.MEASURE_END]
        )
        zero = gate.parse_lines(
            [gate.MEASURE_START, "[1s][debug][gc,heap] GC(0) Humongous regions: 0->0", gate.MEASURE_END]
        )

        self.assertEqual(1, len(positive.forbidden_events))
        self.assertEqual("humongous-region occupancy", positive.forbidden_events[0].kind)
        self.assertEqual([], zero.forbidden_events)

    def test_malformed_safepoint_record_fails_closed(self) -> None:
        parsed = gate.parse_lines(
            [gate.MEASURE_START, '[1s][info][safepoint] Safepoint "broken"', gate.MEASURE_END]
        )

        self.assertTrue(any("could not parse safepoint" in error for error in parsed.errors))

    def test_every_tagged_safepoint_event_requires_a_total(self) -> None:
        for event in (
            "[1s][info][safepoint] Safepoint",
            "[1s][info][safepoint] Reaching safepoint: 12 ns",
            "[1s][info][safepoint] truncated unified record",
        ):
            with self.subTest(event=event):
                parsed = gate.parse_lines([gate.MEASURE_START, event, gate.MEASURE_END])
                self.assertTrue(any("could not parse safepoint" in error for error in parsed.errors))

        metadata = gate.parse_lines(
            [gate.MEASURE_START, "[1s][debug][safepoint] Application time: 0.012 seconds", gate.MEASURE_END]
        )
        self.assertEqual([], metadata.errors)


class EvaluationTest(unittest.TestCase):

    def test_default_row_counts_preserve_legacy_flags_and_generalized_aliases(self) -> None:
        legacy = gate._parse_arguments(
            ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY]
        )
        aliases = gate._parse_arguments(
            ["--small-log", "small.log", "--large-log", "large.log", *CLI_IDENTITY]
        )

        self.assertEqual(1_000_000, legacy.small_rows)
        self.assertEqual(4_000_000, legacy.large_rows)
        self.assertEqual("1m.log", str(legacy.one_million))
        self.assertEqual("4m.log", str(legacy.four_million))
        self.assertEqual("small.log", str(aliases.one_million))
        self.assertEqual("large.log", str(aliases.four_million))

    def test_canonical_foreground_bound_accepts_250_and_rejects_any_relaxation(self) -> None:
        exact = gate._parse_arguments(
            ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY,
             "--max-permit-wait-ms", "250"]
        )
        self.assertEqual(250.0, exact.max_permit_wait_ms)
        with self.assertRaises(SystemExit):
            gate._parse_arguments(
                ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY,
                 "--max-permit-wait-ms", "250.001"]
            )

    def test_sample_floor_overrides_cannot_weaken_the_canonical_evidence(self) -> None:
        exact = gate._parse_arguments(
            ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY,
             "--min-small-samples", "5", "--min-large-samples", "20"]
        )
        raised = gate._parse_arguments(
            ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY,
             "--min-small-samples", "6", "--min-large-samples", "21"]
        )

        self.assertEqual(5, exact.min_small_samples)
        self.assertEqual(20, exact.min_large_samples)
        self.assertEqual(6, raised.min_small_samples)
        self.assertEqual(21, raised.min_large_samples)
        for option, weakened in (("--min-small-samples", "4"), ("--min-large-samples", "19")):
            with self.subTest(option=option):
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        gate._parse_arguments(
                            ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY,
                             option, weakened]
                        )

    def test_custom_four_and_twelve_million_pair_passes_with_dynamic_report(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100, expected_rows=4_000_000), "4m.log"),
            gate.parse_lines(stable_log(130, expected_rows=12_000_000), "12m.log"),
            small_rows=4_000_000,
            large_rows=12_000_000,
        )
        report = io.StringIO()
        with contextlib.redirect_stdout(report):
            gate.print_report(result)

        self.assertTrue(result.passed, result)
        self.assertEqual("4M", result.one_million.label)
        self.assertEqual("12M", result.four_million.label)
        self.assertIn("12M - 4M steady post-young occupancy", report.getvalue())

    def test_invalid_custom_row_arguments_fail_before_log_parsing(self) -> None:
        base = ["--one-million", "small.log", "--four-million", "large.log", *CLI_IDENTITY]
        cases = (
            ["--small-rows", "0"],
            ["--large-rows", "not-an-integer"],
            ["--small-rows", "12000000", "--large-rows", "12000000"],
            ["--small-rows", "12000000", "--large-rows", "4000000"],
        )
        for custom_rows in cases:
            with self.subTest(custom_rows=custom_rows):
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        gate._parse_arguments([*base, *custom_rows])

        parsed_small = gate.parse_lines(stable_log(100), "small.log")
        parsed_large = gate.parse_lines(
            stable_log(130, expected_rows=4_000_000),
            "large.log",
        )
        with self.assertRaisesRegex(ValueError, "small_rows must be less than large_rows"):
            gate.evaluate_pair(
                parsed_small,
                parsed_large,
                small_rows=4_000_000,
                large_rows=4_000_000,
            )

    def test_custom_expected_rows_mismatch_fails_closed(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100, expected_rows=4_000_000), "4m.log"),
            gate.parse_lines(stable_log(130, expected_rows=10_000_000), "wrong-12m.log"),
            small_rows=4_000_000,
            large_rows=12_000_000,
        )

        self.assertFalse(result.passed)
        self.assertTrue(result.one_million.passed, result.one_million.issues)
        self.assertTrue(
            any(
                "expectedRows='10000000', expected '12000000'" in issue
                for issue in result.four_million.issues
            ),
            result.four_million.issues,
        )

    def test_default_expected_max_new_size_is_one_gib(self) -> None:
        args = gate._argument_parser().parse_args(
            ["--one-million", "1m.log", "--four-million", "4m.log", *CLI_IDENTITY]
        )

        self.assertEqual(1024.0, args.expected_max_new_mib)

    def test_explicit_512_mib_max_new_profile_passes(self) -> None:
        expected_max_new_bytes = 512 * MIB
        args = gate._argument_parser().parse_args(
            [
                "--one-million",
                "1m.log",
                "--four-million",
                "4m.log",
                *CLI_IDENTITY,
                "--expected-max-new-mib",
                "512",
            ]
        )
        result = gate.evaluate_pair(
            gate.parse_lines(
                stable_log(100, max_new_size_bytes=expected_max_new_bytes),
                "1m.log",
            ),
            gate.parse_lines(
                stable_log(
                    130,
                    expected_rows=4_000_000,
                    max_new_size_bytes=expected_max_new_bytes,
                ),
                "4m.log",
            ),
            expected_max_new_bytes=expected_max_new_bytes,
        )

        self.assertEqual(512.0, args.expected_max_new_mib)
        self.assertTrue(result.passed, result)

    def test_explicit_max_new_profile_mismatch_fails_closed(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100), "1m.log"),
            gate.parse_lines(stable_log(130, expected_rows=4_000_000), "4m.log"),
            expected_max_new_bytes=512 * MIB,
        )

        self.assertFalse(result.passed)
        self.assertTrue(
            any("maxNewSizeBytes" in issue for issue in result.one_million.issues),
            result.one_million.issues,
        )

    def test_expected_max_new_size_must_be_positive(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                gate._argument_parser().parse_args(
                    [
                        "--one-million",
                        "1m.log",
                        "--four-million",
                        "4m.log",
                        *CLI_IDENTITY,
                        "--expected-max-new-mib",
                        "0",
                    ]
                )

    def test_stable_fixed_heap_pair_passes(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100), "1m.log"),
            gate.parse_lines(stable_log(130, expected_rows=4_000_000), "4m.log"),
        )

        self.assertTrue(result.passed, result)
        self.assertEqual([], result.one_million.issues)
        self.assertEqual([], result.four_million.issues)
        self.assertEqual([], result.cross_scale_issues)
        self.assertAlmostEqual(30 * MIB, result.cross_scale_growth_bytes, delta=2 * MIB)

    def test_real_256_mib_profile_survivor_jitter_passes_region_aware_plateau(self) -> None:
        expected_max_new_bytes = 256 * MIB
        one_million_values = [
            88,
            90,
            88,
            94,
            91,
            94,
            92,
            95,
            96,
            90,
            96,
            92,
            96,
            94,
            95,
            97,
        ]
        four_million_values = [
            89,
            91,
            90,
            96,
            93,
            95,
            94,
            96,
            98,
            93,
            97,
            95,
            98,
            97,
            93,
            99,
            96,
            98,
            98,
            99,
            102,
            96,
            100,
            98,
            101,
            100,
            96,
            102,
            99,
            101,
            100,
            102,
            104,
            100,
            103,
            101,
            104,
            105,
            104,
            106,
            101,
            107,
            104,
            105,
            105,
            102,
            107,
            102,
            108,
            105,
            107,
            107,
            103,
            110,
            106,
            108,
            107,
            110,
            111,
            109,
            111,
            107,
            111,
            109,
        ]
        result = gate.evaluate_pair(
            gate.parse_lines(
                occupancy_log(
                    one_million_values,
                    max_new_size_bytes=expected_max_new_bytes,
                ),
                "1m-256m.log",
            ),
            gate.parse_lines(
                occupancy_log(
                    four_million_values,
                    expected_rows=4_000_000,
                    max_new_size_bytes=expected_max_new_bytes,
                ),
                "4m-256m.log",
            ),
            expected_max_new_bytes=expected_max_new_bytes,
        )

        self.assertTrue(result.passed, result)
        self.assertEqual(10, result.one_million.plateau_sample)
        self.assertEqual(43, result.four_million.plateau_sample)
        self.assertEqual(6, result.one_million.post_plateau_samples)
        self.assertEqual(21, result.four_million.post_plateau_samples)
        self.assertEqual(12 * MIB, result.four_million.plateau_spread_allowance_bytes)
        self.assertAlmostEqual(14 * MIB, result.cross_scale_growth_bytes)

    def test_region_jitter_allowance_does_not_admit_half_mib_per_gc_ramp(self) -> None:
        values = [90 + 0.5 * index for index in range(64)]
        first_analysis_window = values[13:25]
        result = gate.evaluate_run(
            gate.parse_lines(
                occupancy_log(values, expected_rows=4_000_000),
                "low-live-ramp.log",
            ),
            "low-live-ramp",
            min_samples=20,
            expected_rows=4_000_000,
        )

        self.assertEqual(5.5, max(first_analysis_window) - min(first_analysis_window))
        self.assertLessEqual(5.5 * MIB, 3 * 4 * MIB)
        self.assertFalse(result.passed)
        self.assertIsNone(result.plateau_sample)
        self.assertTrue(
            any("positive local OLS growth" in issue for issue in result.issues),
            result.issues,
        )

    def test_diagnostic_command_full_gc_fails_despite_flat_post_full_occupancy(self) -> None:
        lines = stable_log(100)
        lines.insert(
            3,
            "[1.500s][info][gc] GC(0) Pause Full (Diagnostic Command) "
            "220M->100M(4096M) 10.000ms",
        )

        result = gate.evaluate_run(
            gate.parse_lines(lines, "diagnostic-full.log"),
            "diagnostic-full",
            min_samples=5,
            expected_rows=1_000_000,
        )

        self.assertIsNotNone(result.plateau_sample)
        self.assertIsNotNone(result.steady_bytes)
        self.assertFalse(result.passed)
        self.assertEqual(1, len(result.issues), result.issues)
        self.assertIn("forbidden full collection", result.issues[0])
        self.assertIn("Diagnostic Command", result.issues[0])

    def test_more_than_three_regions_of_low_live_jitter_does_not_plateau(self) -> None:
        values = [100, 100, 100, 87, 100, 87, 100]
        result = gate.evaluate_run(
            gate.parse_lines(occupancy_log(values), "wide-jitter.log"),
            "wide-jitter",
            min_samples=5,
            expected_rows=1_000_000,
        )

        self.assertGreater(13 * MIB, 3 * 4 * MIB)
        self.assertFalse(result.passed)
        self.assertTrue(any("never plateaued" in issue for issue in result.issues), result.issues)

    def test_high_live_plateau_keeps_the_relative_three_percent_allowance(self) -> None:
        values = [500, 500, 500, 487, 500, 487, 500]
        result = gate.evaluate_run(
            gate.parse_lines(occupancy_log(values), "high-live-jitter.log"),
            "high-live-jitter",
            min_samples=5,
            expected_rows=1_000_000,
        )

        self.assertTrue(result.passed, result.issues)
        self.assertEqual(13 * MIB, result.plateau_spread_bytes)
        self.assertEqual(15 * MIB, result.plateau_spread_allowance_bytes)

    def test_effective_g1_region_size_allows_zero_gc_and_rejects_log_mismatch(self) -> None:
        missing = stable_log(100)
        missing.remove(g1_region_line())
        inconsistent = stable_log(100)
        inconsistent.insert(inconsistent.index(g1_region_line()) + 1, g1_region_line(2))

        missing_result = gate.evaluate_run(
            gate.parse_lines(missing), "effective-region-size", min_samples=5, expected_rows=1_000_000
        )
        inconsistent_result = gate.evaluate_run(
            gate.parse_lines(inconsistent), "bad-region-size", min_samples=5, expected_rows=1_000_000
        )

        self.assertTrue(missing_result.passed, missing_result.issues)
        self.assertFalse(inconsistent_result.passed)
        self.assertTrue(
            any("G1 region size changed" in issue for issue in inconsistent_result.issues),
            inconsistent_result.issues,
        )

    def test_noncanonical_g1_region_size_fails_even_when_the_log_matches(self) -> None:
        result = gate.evaluate_run(
            gate.parse_lines(occupancy_log([100] * 13, region_size_mib=2)),
            "noncanonical-region-size",
            min_samples=5,
            expected_rows=1_000_000,
        )

        self.assertFalse(result.passed)
        self.assertTrue(
            any("g1RegionSizeBytes" in issue for issue in result.issues),
            result.issues,
        )

    def test_small_run_can_use_five_sample_cross_scale_baseline(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100, samples=13), "1m.log"),
            gate.parse_lines(stable_log(130, expected_rows=4_000_000), "4m.log"),
        )

        self.assertTrue(result.passed, result)
        self.assertEqual(10, result.one_million.analysis_samples)
        self.assertEqual(5, result.one_million.post_plateau_samples)

    def test_final_five_ingestion_collections_are_not_discarded(self) -> None:
        lines = [gate.MEASURE_START, hft_build_line(), hft_config_line(), g1_region_line()]
        values = [100] * 15 + [180, 260, 340, 420, 500]
        for index, value in enumerate(values):
            lines.append(young_line(index, value))
            lines.append(safepoint_line(index))
        lines.append(async_flush_line())
        lines.append(gate.MEASURE_END)

        result = gate.evaluate_run(gate.parse_lines(lines), "late-promotion", min_samples=5)

        self.assertFalse(result.passed)
        self.assertEqual(16, result.analysis_samples)
        self.assertTrue(any("median ratio" in issue or "growth" in issue for issue in result.issues))

    def test_late_stable_plateau_supersedes_an_earlier_warmup_shelf(self) -> None:
        lines = [gate.MEASURE_START, hft_build_line(), hft_config_line(), g1_region_line()]
        values = [80] * 8 + [120] * 8 + [160, 180, 200] + [240] * 21
        for index, value in enumerate(values):
            lines.append(young_line(index, value))
            lines.append(safepoint_line(index))
        lines.append(async_flush_line())
        lines.append(gate.MEASURE_END)

        result = gate.evaluate_run(gate.parse_lines(lines), "two-shelves", min_samples=5)

        self.assertTrue(result.passed, result.issues)
        self.assertGreaterEqual(result.plateau_sample or 0, 19)
        self.assertAlmostEqual(240 * MIB, result.steady_bytes)

    def test_missing_runtime_evidence_fails_closed(self) -> None:
        parsed = gate.parse_lines([gate.MEASURE_START, "ordinary application output", gate.MEASURE_END])
        result = gate.evaluate_run(parsed, "empty", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("HFT_BUILD" in issue for issue in result.issues))
        self.assertTrue(any("HFT_CONFIG" in issue for issue in result.issues))

    def test_initial_heap_must_equal_the_fixed_heap(self) -> None:
        lines = stable_log(100)
        config_index = next(index for index, line in enumerate(lines) if line.startswith(gate.HFT_CONFIG_PREFIX))
        lines[config_index] = hft_config_line(initial_heap_bytes=2048 * MIB)

        result = gate.evaluate_run(gate.parse_lines(lines), "non-fixed-heap", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("initialHeapBytes" in issue for issue in result.issues), result.issues)

    def test_genuine_zero_gc_pair_is_valid_but_occupancy_inconclusive(self) -> None:
        def zero_event_log(rows: int) -> list[str]:
            return [
                gate.MEASURE_START,
                hft_build_line(),
                hft_config_line(expected_rows=rows),
                async_flush_line(),
                gate.MEASURE_END,
            ]

        result = gate.evaluate_pair(
            gate.parse_lines(zero_event_log(1_000_000), "zero-small.log"),
            gate.parse_lines(zero_event_log(4_000_000), "zero-large.log"),
            expected_git_sha=GIT_SHA,
            expected_artifact_sha256=ARTIFACT_SHA,
        )

        self.assertFalse(result.passed)
        self.assertEqual(gate.Verdict.INCONCLUSIVE, result.verdict)
        self.assertEqual(gate.Verdict.INCONCLUSIVE, result.occupancy_verdict)
        self.assertTrue(result.one_million.zero_young_events)

    def test_low_gc_pair_is_inconclusive_instead_of_failed(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100, samples=1), "one-young-gc.log"),
            gate.parse_lines(
                stable_log(130, samples=4, expected_rows=4_000_000),
                "four-young-gcs.log",
            ),
        )

        self.assertEqual(gate.Verdict.INCONCLUSIVE, result.verdict)
        self.assertEqual([], result.one_million.issues)
        self.assertEqual([], result.four_million.issues)
        self.assertTrue(result.one_million.inconclusive_reasons)
        self.assertTrue(result.four_million.inconclusive_reasons)

    def test_under_sampled_occupancy_does_not_hide_hard_latency_failure(self) -> None:
        lines = stable_log(100, samples=1)
        lines[lines.index(async_flush_line())] = async_flush_line(
            foreground_flush_total_ns=250_000_001,
            foreground_flush_max_ns=250_000_001,
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "slow-low-gc", min_samples=5)

        self.assertEqual(gate.Verdict.FAIL, result.verdict)
        self.assertTrue(any("foregroundFlushMaxNs" in issue for issue in result.issues))
        self.assertTrue(result.inconclusive_reasons)

    def test_verdict_exit_codes_are_distinct(self) -> None:
        self.assertEqual(0, gate._exit_code(gate.Verdict.PASS))
        self.assertEqual(1, gate._exit_code(gate.Verdict.FAIL))
        self.assertEqual(3, gate._exit_code(gate.Verdict.INCONCLUSIVE))

    def test_unbounded_late_slope_fails(self) -> None:
        lines = [gate.MEASURE_START, hft_build_line(), hft_config_line(), g1_region_line()]
        values = [100] * 30 + [100 + 8 * index for index in range(30)]
        for index, value in enumerate(values):
            lines.append(young_line(index, value))
            lines.append(safepoint_line(index))
        lines.append(async_flush_line())
        lines.append(gate.MEASURE_END)

        result = gate.evaluate_run(gate.parse_lines(lines), "ramp", min_samples=20)

        self.assertFalse(result.passed)
        self.assertTrue(
            any("late/early" in issue or "OLS growth" in issue for issue in result.issues),
            result.issues,
        )

    def test_cross_scale_growth_fails_even_when_each_run_plateaus(self) -> None:
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100), "1m.log"),
            gate.parse_lines(stable_log(700, expected_rows=4_000_000), "4m.log"),
        )

        self.assertTrue(result.one_million.passed)
        self.assertTrue(result.four_million.passed)
        self.assertFalse(result.passed)
        self.assertTrue(
            any(
                "4M steady post-young occupancy grew" in issue
                for issue in result.cross_scale_issues
            )
        )

    def test_fixed_heap_capacity_mismatch_fails(self) -> None:
        four_million = stable_log(130, expected_rows=4_000_000)
        four_million = [line.replace("(4096M)", "(6144M)") for line in four_million]
        result = gate.evaluate_pair(
            gate.parse_lines(stable_log(100), "1m.log"),
            gate.parse_lines(four_million, "4m.log"),
        )

        self.assertFalse(result.passed)
        self.assertTrue(any("fixed heap does not match" in issue for issue in result.four_million.issues))

    def test_present_gc_and_safepoint_stalls_are_independently_bounded(self) -> None:
        young_stall = stable_log(100)
        young_index = next(index for index, line in enumerate(young_stall) if "Pause Young" in line)
        young_stall[young_index] = young_stall[young_index].replace("0.750ms", "250.001ms")
        safepoint_stall = stable_log(100)
        safepoint_index = next(index for index, line in enumerate(safepoint_stall) if 'Safepoint "' in line)
        safepoint_stall[safepoint_index] = safepoint_line(0, 250_000_001)

        young_result = gate.evaluate_run(gate.parse_lines(young_stall), "young-stall", min_samples=5)
        safepoint_result = gate.evaluate_run(
            gate.parse_lines(safepoint_stall), "safepoint-stall", min_samples=5
        )

        self.assertTrue(any("young-GC pause" in issue for issue in young_result.issues))
        self.assertTrue(any("max safepoint" in issue for issue in safepoint_result.issues))

    def test_embedded_artifact_identity_must_match(self) -> None:
        result = gate.evaluate_run(
            gate.parse_lines(stable_log(100)),
            "wrong-artifact",
            min_samples=5,
            expected_git_sha=GIT_SHA,
            expected_artifact_sha256="0" * 64,
        )

        self.assertFalse(result.passed)
        self.assertTrue(any("artifact SHA-256" in issue for issue in result.issues))

    def test_missing_async_flush_telemetry_fails(self) -> None:
        lines = stable_log(100)
        lines.remove(async_flush_line())

        result = gate.evaluate_run(gate.parse_lines(lines), "no-telemetry", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("expected exactly one" in issue for issue in result.issues))

    def test_side_batch_and_foreground_rotation_wait_are_hard_limits(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(
            peak_active_side_bytes=65 * MIB,
            rotation_permit_wait_max_ns=251_000_000,
            start_flush_max_ns=251_000_000,
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "slow-unbounded", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("peak active side payload" in issue for issue in result.issues))
        self.assertTrue(any("whole startAsyncFlush" in issue for issue in result.issues))

    def test_submission_wait_and_caller_execution_are_hard_failures(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(
            submit_wait_max_ns=251_000_000,
            start_flush_max_ns=251_000_000,
            caller_thread_append_runs=1,
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "bad-submit", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("whole startAsyncFlush" in issue for issue in result.issues))
        self.assertTrue(any("caller thread executed" in issue for issue in result.issues))

    def test_sequential_waits_are_bounded_by_whole_call_elapsed(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(
            rotation_permit_wait_max_ns=150_000_000,
            submit_wait_max_ns=150_000_000,
            start_flush_max_ns=300_000_000,
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "composed-waits", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("whole startAsyncFlush" in issue for issue in result.issues))

    def test_whole_final_drain_is_bounded(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(final_drain_max_ns=250_000_001)

        result = gate.evaluate_run(gate.parse_lines(lines), "slow-final-drain", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("whole final-drain" in issue for issue in result.issues))

    def test_pinned_trie_spill_must_be_exercised_and_bounded(self) -> None:
        cases = (
            ({"pinned_trie_spill_pages": 0}, "no pinned trie pages"),
            ({"pinned_trie_spill_batch_max": 65}, "exceeds configured capacity"),
            ({"pinned_trie_spill_epochs": 101}, "exceed full snapshot epochs"),
            ({"pinned_trie_spill_pages": 257}, "epochs x configured capacity"),
        )
        for overrides, expected_issue in cases:
            with self.subTest(overrides=overrides):
                lines = stable_log(100)
                lines[lines.index(async_flush_line())] = async_flush_line(**overrides)

                result = gate.evaluate_run(gate.parse_lines(lines), "bad-trie-spill", min_samples=5)

                self.assertFalse(result.passed)
                self.assertTrue(any(expected_issue in issue for issue in result.issues), result.issues)

    def test_pinned_trie_live_and_high_water_are_reported_without_an_invented_limit(self) -> None:
        result = gate.evaluate_run(gate.parse_lines(stable_log(100)), "trie-evidence", min_samples=5)

        self.assertTrue(result.passed, result.issues)
        self.assertEqual(311, result.pinned_trie_live_max)
        self.assertEqual(487, result.pinned_trie_high_water)

    def test_drain_component_wait_is_independently_bounded(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(
            drain_permit_wait_max_ns=1_500_000_000,
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "slow-final-drain", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("drainPermitWaitMaxNs" in issue for issue in result.issues))

    def test_promoted_kvl_pages_fail_the_direct_frame_coverage_gate(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(
            kvl_attempted_pages=1_607,
            kvl_promoted_pages=7,
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "promoted-kvl", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("promoted back to the live TIL" in issue for issue in result.issues))
        self.assertEqual(1_607, result.attempted_kvl_pages)
        self.assertEqual(7, result.promoted_kvl_pages)

    def test_kvl_attempt_accounting_must_be_exact(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line(kvl_attempted_pages=1_601)

        result = gate.evaluate_run(gate.parse_lines(lines), "bad-kvl-attempt-total", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("attempt coverage is incomplete" in issue for issue in result.issues))

    def test_kvl_attempted_pages_per_epoch_is_bounded_and_covered_by_the_total(self) -> None:
        cases = (
            ({"kvl_attempted_pages_max": 17}, "bounded serializer window"),
            (
                {"kvl_pages": 15, "kvl_attempted_pages": 15, "kvl_attempted_pages_max": 16},
                "exceeds the total attempted KVL pages",
            ),
            ({"kvl_attempted_pages_max": 0}, "is zero despite positive"),
            (
                {"kvl_attempted_pages_max": 15},
                "per-epoch maximum times combined epochs",
            ),
        )
        for overrides, expected_issue in cases:
            with self.subTest(overrides=overrides):
                lines = stable_log(100)
                lines[lines.index(async_flush_line())] = async_flush_line(**overrides)

                result = gate.evaluate_run(gate.parse_lines(lines), "bad-kvl-epoch", min_samples=5)

                self.assertFalse(result.passed)
                self.assertTrue(any(expected_issue in issue for issue in result.issues), result.issues)

    def test_foreground_async_flush_telemetry_is_complete_and_bounded(self) -> None:
        cases = (
            ({"foreground_flush_count": 99}, "do not equal combined epochs"),
            ({"foreground_flush_count": 101}, "do not equal combined epochs"),
            (
                {"foreground_flush_total_ns": 19_000_000, "foreground_flush_max_ns": 20_000_000},
                "foreground async-flush maximum exceeds its total",
            ),
            (
                {
                    "foreground_flush_total_ns": 250_000_001,
                    "foreground_flush_max_ns": 250_000_001,
                },
                "foregroundFlushMaxNs",
            ),
        )
        for overrides, expected_issue in cases:
            with self.subTest(overrides=overrides):
                lines = stable_log(100)
                lines[lines.index(async_flush_line())] = async_flush_line(**overrides)

                result = gate.evaluate_run(gate.parse_lines(lines), "bad-foreground-flush", min_samples=5)

                self.assertFalse(result.passed)
                self.assertTrue(any(expected_issue in issue for issue in result.issues), result.issues)

    def test_split_permit_accounting_must_equal_the_aggregate(self) -> None:
        lines = stable_log(100)
        telemetry_index = lines.index(async_flush_line())
        lines[telemetry_index] = lines[telemetry_index].replace(
            "drainPermitAcquires=2", "drainPermitAcquires=1"
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "bad-permit-split", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("split permit acquires" in issue for issue in result.issues))

    def test_canonical_hft_configuration_is_mandatory(self) -> None:
        mismatches = {
            "globalDict=never": "globalDict=auto",
            "autoCommitNodes=4194304": "autoCommitNodes=1048576",
            "asyncFlushNodeCap=16384": "asyncFlushNodeCap=1048576",
            "arenaStrategy=shared": "arenaStrategy=auto",
            f"maxNewSizeBytes={1024 * MIB}": f"maxNewSizeBytes={512 * MIB}",
            f"maxHeapBytes={4096 * MIB}": f"maxHeapBytes={2048 * MIB}",
            f"g1RegionSizeBytes={4 * MIB}": f"g1RegionSizeBytes={2 * MIB}",
            "gcLogging=true": "gcLogging=false",
            "safepointLogging=true": "safepointLogging=false",
            "storage=FILE_CHANNEL": "storage=MEMORY_MAPPED",
            "importer=parallel-bulk": "importer=jackson",
            "projectionMode=incremental": "projectionMode=second-pass",
            "expectedRows=1000000": "expectedRows=999999",
            "pinnedTrieScanBudget=1024": "pinnedTrieScanBudget=2048",
            "pinnedTrieBatchCapacity=64": "pinnedTrieBatchCapacity=63",
            "appendWorkers=2": "appendWorkers=3",
            "appendQueueCapacity=1": "appendQueueCapacity=2",
        }
        for original, replacement in mismatches.items():
            with self.subTest(replacement=replacement):
                lines = stable_log(100)
                config_index = lines.index(hft_config_line())
                lines[config_index] = lines[config_index].replace(original, replacement)

                result = gate.evaluate_pair(
                    gate.parse_lines(lines, "bad-1m.log"),
                    gate.parse_lines(stable_log(130, expected_rows=4_000_000), "4m.log"),
                )

                self.assertFalse(result.passed)
                self.assertTrue(any("HFT config" in issue for issue in result.one_million.issues))

    def test_native_reservoir_topology_is_mandatory(self) -> None:
        for field, value in (("count", 1), ("bytes", 32 * MIB)):
            with self.subTest(field=field):
                lines = stable_log(100)
                telemetry_index = lines.index(async_flush_line())
                lines[telemetry_index] = async_flush_line(
                    native_reservoir_count=value if field == "count" else 2,
                    native_reservoir_bytes=value if field == "bytes" else 64 * MIB,
                )

                result = gate.evaluate_run(gate.parse_lines(lines), "bad-reservoir", min_samples=5)

                self.assertFalse(result.passed)
                self.assertTrue(any("native reservoir" in issue for issue in result.issues))

    def test_kvl_cache_must_reuse_the_disposable_native_frame(self) -> None:
        for original, replacement in (
            ("kvlFrameCachePages=1600", "kvlFrameCachePages=0"),
            ("kvlCacheFallbackPages=0", "kvlCacheFallbackPages=1"),
        ):
            with self.subTest(replacement=replacement):
                lines = stable_log(100)
                telemetry_index = lines.index(async_flush_line())
                lines[telemetry_index] = lines[telemetry_index].replace(original, replacement)

                result = gate.evaluate_run(gate.parse_lines(lines), "bad-kvl-cache", min_samples=5)

                self.assertFalse(result.passed)
                self.assertTrue(any("KVL" in issue for issue in result.issues))

    def test_kvl_cache_accounting_must_cover_every_appended_kvl_page(self) -> None:
        lines = stable_log(100)
        telemetry_index = lines.index(async_flush_line())
        lines[telemetry_index] = lines[telemetry_index].replace(
            "kvlFrameCachePages=1600", "kvlFrameCachePages=1599"
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "incomplete-kvl-coverage", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("KVL cache coverage is incomplete" in issue for issue in result.issues))

    def test_malformed_async_flush_telemetry_fails_closed(self) -> None:
        lines = stable_log(100)
        lines[lines.index(async_flush_line())] = async_flush_line() + " futureField=1"

        result = gate.evaluate_run(gate.parse_lines(lines), "unknown-field", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("unknown async-flush" in issue for issue in result.issues))

    def test_missing_split_wait_field_fails_closed(self) -> None:
        lines = stable_log(100)
        telemetry_index = lines.index(async_flush_line())
        lines[telemetry_index] = lines[telemetry_index].replace(
            " drainPermitWaitMaxNs=100000000", ""
        )

        result = gate.evaluate_run(gate.parse_lines(lines), "missing-split-field", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("missing fields" in issue for issue in result.issues))

    def test_missing_kvl_promotion_field_fails_closed(self) -> None:
        lines = stable_log(100)
        telemetry_index = lines.index(async_flush_line())
        lines[telemetry_index] = lines[telemetry_index].replace(" kvlPromotedPages=0", "")

        result = gate.evaluate_run(gate.parse_lines(lines), "missing-kvl-promotion-field", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("kvlPromotedPages" in issue for issue in result.issues))

    def test_missing_pinned_trie_telemetry_field_fails_closed(self) -> None:
        lines = stable_log(100)
        telemetry_index = lines.index(async_flush_line())
        lines[telemetry_index] = lines[telemetry_index].replace(" pinnedTrieLiveMax=311", "")

        result = gate.evaluate_run(gate.parse_lines(lines), "missing-trie-field", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("pinnedTrieLiveMax" in issue for issue in result.issues))

    def test_missing_pinned_trie_config_field_fails_closed(self) -> None:
        lines = stable_log(100)
        config_index = lines.index(hft_config_line())
        lines[config_index] = lines[config_index].replace(" pinnedTrieScanBudget=1024", "")

        result = gate.evaluate_run(gate.parse_lines(lines), "missing-trie-config", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("pinnedTrieScanBudget" in issue for issue in result.issues))

    def test_missing_async_flush_node_cap_fails_closed(self) -> None:
        lines = stable_log(100)
        config_index = lines.index(hft_config_line())
        lines[config_index] = lines[config_index].replace(" asyncFlushNodeCap=16384", "")

        result = gate.evaluate_run(gate.parse_lines(lines), "missing-async-cap", min_samples=5)

        self.assertFalse(result.passed)
        self.assertTrue(any("asyncFlushNodeCap" in issue for issue in result.issues))


if __name__ == "__main__":
    unittest.main()
