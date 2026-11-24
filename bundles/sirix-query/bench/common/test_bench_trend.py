#!/usr/bin/env python3
"""Behaviour tests of the nightly trend tier (spec/CONV-conventions.md, CONV-304)."""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Sequence
from unittest import mock

import bench_ledger
import bench_trend
from bench_trend import TrendError
from test_bench_ledger import fake_environment

FIRST_NIGHT = datetime.date(2026, 12, 1)
# Night-to-night deviations of a quiet series, as log ratios around its level; they repeat every 7 nights.
RIPPLE = [0.001, -0.002, 0.0015, -0.0005, 0.002, -0.001, 0.0]


def night_name(index: int) -> str:
    return (FIRST_NIGHT + datetime.timedelta(days=index)).isoformat()


def commit_of(index: int) -> str:
    """The commit a night was built from; distinct in its first digits."""
    return f"{index + 1:02x}" * 20


class LedgerBuilder:
    """Builds the rows of nightly runs: one run per night, twenty processes per benchmark."""

    def __init__(self, environment: dict[str, Any]) -> None:
        self.environment = environment
        self.rows: list[dict[str, Any]] = []
        self.sequence = 0

    def night(
        self,
        index: int,
        levels: dict[str, float],
        *,
        conditions: str = "box-1",
        processes: int = 20,
        runtime: str = "GraalVM 25.0.3",
        binary: str = "native",
        accepted: bool = True,
        tier: str = "nightly",
        metric: str = "cold_seconds",
        exact: bool = False,
        params: dict[str, str] | None = None,
    ) -> str:
        self.sequence += 1
        run = f"202612{self.sequence // 100 + 1:02d}T0100{self.sequence % 100:02d}Z-{self.sequence:08x}"
        commit = commit_of(index)
        row: dict[str, Any] = {
            "schema": 1,
            "kind": "run",
            "run": run,
            "started_at": f"{night_name(index)}T01:00:00Z",
            "tier": tier,
            "kit": "jsonbench",
            "plan": {"min_processes": 20 if tier == "nightly" else 1, "benchmarks": None},
            "arms": {"head": {"commit": commit, "dirty": False, "binary": binary, "runtime": runtime}},
            "conditions": conditions,
            "fingerprint": bench_ledger.fingerprint(self.environment),
            "environment": self.environment,
        }
        if tier == "nightly":
            row["night"] = night_name(index)
        self.rows.append(row)
        for benchmark, level in levels.items():
            for process in range(1, processes + 1):
                # Spread the processes evenly around the level, so that their median is the level. `exact`
                # gives every process the level itself, for tests that need it to the last bit.
                value = level if exact else level * (1 + 0.001 * (process - (processes + 1) / 2))
                measurement = {
                    "schema": 1,
                    "kind": "measurement",
                    "run": run,
                    "benchmark": benchmark,
                    "params": dict(params or {}),
                    "metric": metric,
                    "unit": "s",
                    "arm": "head",
                    "pair": None,
                    "position": None,
                    "process": process,
                    "value": value,
                    "accepted": accepted,
                }
                if not accepted:
                    measurement["reject_reason"] = "foreign load"
                self.rows.append(measurement)
        return run

    def nights(self, levels: Sequence[float], *, start: int = 0, benchmark: str = "q1", **options: Any) -> None:
        for offset, level in enumerate(levels):
            self.night(start + offset, {benchmark: level}, **options)


def quiet(count: int, level: float = 1.0, start: int = 0) -> list[float]:
    return [level * math.exp(RIPPLE[(start + index) % len(RIPPLE)]) for index in range(count)]


class TrendTestCase(unittest.TestCase):

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.builder = LedgerBuilder(fake_environment(self.base))

    def states(self, night: int | None = None) -> dict[str, str]:
        trend = bench_trend.report(self.builder.rows, None if night is None else night_name(night))
        return {key[2]: state.state for key, state in trend.states.items()}

    def state(self, night: int, benchmark: str = "q1") -> bench_trend.NightState:
        trend = bench_trend.report(self.builder.rows, night_name(night))
        return next(state for key, state in trend.states.items() if key[2] == benchmark)


class QuartileTest(unittest.TestCase):

    def test_quartiles_interpolate_between_order_statistics(self) -> None:
        self.assertAlmostEqual(1.75, bench_trend.quartile([4.0, 1.0, 3.0, 2.0], 0.25), places=15)
        self.assertAlmostEqual(3.25, bench_trend.quartile([4.0, 1.0, 3.0, 2.0], 0.75), places=15)
        self.assertEqual(3.0, bench_trend.quartile([1.0, 2.0, 3.0, 4.0, 5.0], 0.5))
        self.assertEqual(7.0, bench_trend.quartile([7.0], 0.75))
        thirteen = [float(value) for value in range(13)]
        self.assertEqual((3.0, 9.0), (bench_trend.quartile(thirteen, 0.25), bench_trend.quartile(thirteen, 0.75)))


class StateTest(TrendTestCase):

    def test_a_quiet_series_is_steady(self) -> None:
        self.builder.nights(quiet(20))
        for night in range(14, 20):
            self.assertEqual("steady", self.state(night).state, night)
        self.assertLess(abs(self.state(19).drift), 0.004)

    def test_fewer_than_fourteen_earlier_nights_is_insufficient_history(self) -> None:
        self.builder.nights(quiet(14) + [1.5])
        self.assertEqual("insufficient-history", self.state(13).state)
        self.assertEqual((None, None), (self.state(13).drift, self.state(13).fence))
        self.assertEqual("unconfirmed", self.state(14).state, "the fifteenth night is the first that can be judged")

    def test_a_step_is_unconfirmed_on_its_first_night_and_drift_on_the_second(self) -> None:
        self.builder.nights(quiet(16) + quiet(4, level=1.03, start=16))
        self.assertEqual("steady", self.state(15).state)
        first, second, fourth = self.state(16), self.state(17), self.state(19)
        self.assertEqual(("unconfirmed", None), (first.state, first.since))
        self.assertEqual("drift", second.state)
        self.assertEqual((night_name(15), commit_of(15)), (second.since.night, second.since.commit))
        self.assertEqual(
            (night_name(15), "drift"), (fourth.since.night, fourth.state), "the run of exceeding nights is unbroken"
        )
        self.assertAlmostEqual(math.log(1.03), second.drift, delta=0.004)
        self.assertEqual(commit_of(17), second.night.commit)

    def test_a_single_outlying_night_is_never_confirmed(self) -> None:
        self.builder.nights(quiet(16) + [1.05] + quiet(3, start=17))
        self.assertEqual(
            ["steady", "unconfirmed", "steady", "steady", "steady"],
            [self.state(night).state for night in range(15, 20)],
        )

    def test_a_slow_drift_adds_up_past_the_bound(self) -> None:
        # +0.3% every night from night 16 on: no single step is near 2%, the sum is.
        levels = quiet(16) + [math.exp(RIPPLE[index % 7] + math.log(1.003) * (index - 15)) for index in range(16, 40)]
        self.builder.nights(levels)
        states = [self.state(night).state for night in range(14, 40)]
        first_alarm = states.index("drift") + 14
        self.assertEqual({"steady"}, set(states[: first_alarm - 14 - 1]), "nothing is reported while the sum is small")
        self.assertEqual("unconfirmed", states[first_alarm - 14 - 1])
        self.assertLessEqual(first_alarm, 16 + 11, "about two percent of drift is confirmed within eleven nights")
        self.assertGreater(self.state(first_alarm).drift, bench_trend.DRIFT_BOUND)
        self.assertLess(math.log(levels[first_alarm] / levels[first_alarm - 1]), 0.01)

    def test_a_speedup_mirrors_a_slowdown(self) -> None:
        self.builder.nights(quiet(16) + quiet(3, level=0.96, start=16))
        self.assertEqual(
            ["faster-unconfirmed", "faster", "faster"], [self.state(night).state for night in (16, 17, 18)]
        )
        self.assertEqual(night_name(15), self.state(18).since.night)
        trend = bench_trend.report(self.builder.rows)
        self.assertEqual([], trend.drifting, "a confirmed speedup is reported, it is not an alarm")

    def test_a_reversal_is_not_a_confirmation(self) -> None:
        self.builder.nights(quiet(16) + [1.05, 0.95])
        self.assertEqual(("unconfirmed", "faster-unconfirmed"), (self.state(16).state, self.state(17).state))

    def test_a_noisy_series_is_unstable_and_its_changes_stay_inside_its_fence(self) -> None:
        noisy = [math.exp(0.03 * math.sin(2.1 * index)) for index in range(18)] + [1.04, 1.04]
        self.builder.nights(noisy)
        for night in range(14, 20):
            state = self.state(night)
            self.assertEqual("unstable", state.state, night)
            self.assertGreater(state.fence, bench_trend.DRIFT_BOUND)
        self.assertGreater(
            self.state(19).drift, bench_trend.DRIFT_BOUND, "above 2%, and still inside this series' own noise"
        )

    def test_the_drift_must_exceed_two_percent_and_the_fence(self) -> None:
        # Fourteen nights that alternate between 1 and e^c make every adjacent change c, so the fence is c
        # and the baseline (median of four low and three high nights) is 1.
        def judged(change: float, last: float) -> bench_trend.NightState:
            builder = LedgerBuilder(self.builder.environment)
            builder.nights([1.0 if index % 2 == 0 else math.exp(change) for index in range(14)] + [last])
            return bench_trend.state_of(next(iter(bench_trend.nightly_series(builder.rows).values())), 14)

        bound = bench_trend.DRIFT_BOUND
        self.assertEqual("steady", judged(0.001, math.exp(bound * 0.999)).state)
        self.assertEqual("unconfirmed", judged(0.001, math.exp(bound * 1.001)).state)
        self.assertEqual("faster-unconfirmed", judged(0.001, math.exp(-bound * 1.001)).state)
        self.assertEqual("steady", judged(0.001, math.exp(-bound * 0.999)).state)
        noisy = judged(0.03, math.exp(0.03 * 0.999))
        self.assertEqual(("unstable", 0.03), (noisy.state, round(noisy.fence, 12)), "above 2% but not above the fence")
        self.assertEqual("unconfirmed", judged(0.03, math.exp(0.03 * 1.001)).state)
        self.assertAlmostEqual(2.0, 100 * math.expm1(bound), places=12)

    def judged_exactly(self, levels: list[float]) -> bench_trend.NightState:
        """The state of the last of these nights, with every level reproduced to the last bit."""
        builder = LedgerBuilder(self.builder.environment)
        builder.nights(levels, exact=True)
        return bench_trend.state_of(next(iter(bench_trend.nightly_series(builder.rows).values())), len(levels) - 1)

    def test_a_drift_exactly_at_the_bound_or_at_the_fence_does_not_exceed(self) -> None:
        bound, doubling = bench_trend.DRIFT_BOUND, math.log(2.0)
        at_bound = self.judged_exactly([1.0] * 14 + [1.02])
        self.assertEqual((bound, 0.0, "steady"), (at_bound.drift, at_bound.fence, at_bound.state))
        # Nights that alternate between 1 and 2 make every change, and so the fence, exactly ln 2.
        at_fence = self.judged_exactly([1.0, 2.0] * 7 + [2.0])
        self.assertEqual((doubling, doubling, "unstable"), (at_fence.drift, at_fence.fence, at_fence.state))
        at_fence_below = self.judged_exactly([1.0, 2.0] * 7 + [0.5])
        self.assertEqual((-doubling, "unstable"), (at_fence_below.drift, at_fence_below.state))
        self.assertEqual("unconfirmed", self.judged_exactly([1.0, 2.0] * 7 + [2.5]).state)
        self.assertEqual("faster-unconfirmed", self.judged_exactly([1.0, 2.0] * 7 + [0.4]).state)

    def test_a_fence_exactly_at_the_bound_is_not_unstable(self) -> None:
        # A history in which every night is exactly 1.02 times the night before, in floating point.
        bound = bench_trend.DRIFT_BOUND
        levels = [1.0]
        while len(levels) < 14:
            candidate = levels[-1] * 1.02
            for _ in range(64):
                if math.log(candidate / levels[-1]) == bound:
                    break
                candidate = math.nextafter(candidate, math.inf if math.log(candidate / levels[-1]) < bound else 0.0)
            self.assertEqual(
                bound, math.log(candidate / levels[-1]), "no exact step found; the fixture needs another start"
            )
            levels.append(candidate)
        state = self.judged_exactly(levels + [levels[3]])  # tonight equals the baseline, the median of the oldest seven
        self.assertEqual((bound, 0.0, "steady"), (state.fence, state.drift, state.state))

    def test_the_fence_is_the_upper_quartile_plus_one_and_a_half_interquartile_ranges(self) -> None:
        # Thirteen changes, seven of 0.002 and six of 0.010: Q1 = 0.002, Q3 = 0.010, fence = 0.010 + 1.5 * 0.008.
        steps = [0.002, -0.002, 0.010, -0.010] * 3 + [0.002]
        levels = [1.0]
        for step in steps:
            levels.append(levels[-1] * math.exp(step))

        def judged(drift: float) -> bench_trend.NightState:
            builder = LedgerBuilder(self.builder.environment)
            builder.nights(levels + [math.exp(drift)])
            return bench_trend.state_of(next(iter(bench_trend.nightly_series(builder.rows).values())), 14)

        inside, outside = judged(0.0215), judged(0.0225)
        self.assertAlmostEqual(0.022, inside.fence, places=9)
        self.assertGreater(inside.drift, bench_trend.DRIFT_BOUND)
        self.assertEqual(("unstable", "unconfirmed"), (inside.state, outside.state))

    def test_levels_are_compared_as_ratios_whatever_their_size(self) -> None:
        for level in (0.004, 250.0):
            with self.subTest(level=level):
                builder = LedgerBuilder(self.builder.environment)
                builder.nights(quiet(16, level=level) + quiet(2, level=level * 1.03, start=16))
                nights = next(iter(bench_trend.nightly_series(builder.rows).values()))
                self.assertEqual(
                    ["steady", "steady", "unconfirmed", "drift"],
                    [bench_trend.state_of(nights, index).state for index in (14, 15, 16, 17)],
                )
                self.assertAlmostEqual(math.log(1.03), bench_trend.state_of(nights, 17).drift, delta=0.004)

    def test_the_baseline_is_the_older_half_of_the_window(self) -> None:
        # Seven nights at 1.00, seven at 1.015: against the older half tonight's 1.03 is +3%, against the
        # whole window or the newer half it would stay under the bound.
        self.builder.nights([1.0] * 7 + [1.015] * 7 + [1.03])
        state = self.state(14)
        self.assertAlmostEqual(math.log(1.03), state.drift, places=12)
        self.assertEqual("unconfirmed", state.state)


class SeriesTest(TrendTestCase):

    def test_a_window_never_crosses_a_conditions_id(self) -> None:
        self.builder.nights(quiet(16))
        self.builder.nights(quiet(3, level=1.10, start=16), start=16, conditions="box-2")
        trend = bench_trend.report(self.builder.rows, night_name(18))
        self.assertEqual(
            {("box-2", "insufficient-history")}, {(key[0], state.state) for key, state in trend.states.items()}
        )
        self.assertEqual([("box-1", "q1")], [(key[0], key[2]) for key in trend.missing])

    def test_runtime_binary_kit_and_metric_each_start_their_own_series(self) -> None:
        self.builder.nights(quiet(16))
        self.builder.night(16, {"q1": 1.10}, runtime="GraalVM 25.0.4")
        self.builder.night(17, {"q1": 1.10}, binary="native-pgo")
        self.builder.night(18, {"q1": 1.10}, metric="hot_seconds")
        series = bench_trend.nightly_series(self.builder.rows)
        self.assertEqual([1, 1, 1, 16], sorted(len(nights) for nights in series.values()))

    def test_a_night_with_fewer_than_twenty_accepted_processes_is_not_a_recorded_night(self) -> None:
        self.builder.nights(quiet(15))
        self.builder.night(15, {"q1": 1.30}, processes=19)
        self.builder.night(16, {"q1": 1.30}, accepted=False)
        self.builder.night(17, {"q1": 1.0})
        nights = next(iter(bench_trend.nightly_series(self.builder.rows).values()))
        self.assertEqual([night_name(index) for index in list(range(15)) + [17]], [night.night for night in nights])
        self.assertEqual({20}, {night.processes for night in nights})
        self.assertEqual("steady", self.state(17).state)
        with self.assertRaisesRegex(TrendError, f"no series has a recorded night {night_name(15)}"):
            bench_trend.report(self.builder.rows, night_name(15))

    def test_the_level_is_the_median_of_the_nights_processes(self) -> None:
        run = self.builder.night(0, {"q1": 2.0})
        for row in self.builder.rows:
            if row["kind"] == "measurement" and row["process"] in (19, 20):
                row["value"] = 500.0  # two wild processes do not move a median of twenty
        night = next(iter(bench_trend.nightly_series(self.builder.rows).values()))[0]
        self.assertAlmostEqual(2.0, night.level, delta=0.002)
        self.assertEqual((run, commit_of(0)), (night.run, night.commit))

    def test_two_runs_with_accepted_measurements_for_one_night_fail_the_analysis(self) -> None:
        self.builder.nights(quiet(3))
        first = self.builder.rows[0]["run"]
        second = self.builder.night(0, {"q1": 1.0})
        with self.assertRaisesRegex(
            TrendError,
            f"night {night_name(0)} has accepted measurements from more than one run " f"\\({first}, {second}\\)",
        ):
            bench_trend.nightly_series(self.builder.rows)

    def test_a_rejected_rerun_of_a_night_is_not_a_second_run(self) -> None:
        self.builder.nights(quiet(3))
        self.builder.night(0, {"q1": 9.0}, accepted=False)
        self.assertEqual(3, len(next(iter(bench_trend.nightly_series(self.builder.rows).values()))))

    def test_gate_and_control_runs_are_not_nights(self) -> None:
        self.builder.nights(quiet(2))
        self.builder.night(2, {"q1": 1.0}, tier="control")
        self.assertEqual(2, len(next(iter(bench_trend.nightly_series(self.builder.rows).values()))))

    def test_an_empty_history_is_an_error(self) -> None:
        with self.assertRaisesRegex(TrendError, "holds no recorded nightly night"):
            bench_trend.report([])


class ReportTest(TrendTestCase):

    def test_a_ledger_with_an_impossible_night_is_refused(self) -> None:
        self.builder.night(0, {"q1": 1.0})
        invalid_run = self.builder.night(1, {"q1": 1.0})
        self.builder.night(2, {"q1": 1.0})
        for row in self.builder.rows:
            if row["kind"] == "run" and row["run"] == invalid_run:
                row["night"] = "2026-13-01"
        ledger = self.base / "invalid-night.jsonl"
        ledger.write_text("".join(json.dumps(row) + "\n" for row in self.builder.rows), encoding="utf-8")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = bench_trend.main(["report", "--ledger", str(ledger)])
        self.assertEqual(2, status)
        self.assertEqual("", out.getvalue())
        self.assertIn("'night'", err.getvalue())

    def test_an_incomplete_latest_run_cannot_select_an_earlier_night(self) -> None:
        for processes, accepted in ((19, True), (20, False), (0, True)):
            with self.subTest(processes=processes, accepted=accepted):
                builder = LedgerBuilder(self.builder.environment)
                builder.nights(quiet(16))
                builder.night(16, {"q1": 1.30}, processes=processes, accepted=accepted)
                builder.night(17, {"q1": 1.0}, tier="control")
                ledger = self.base / f"ledger-{processes}-{accepted}.jsonl"
                bench_ledger.append_rows(ledger, builder.rows)
                for date in ([], ["--night", night_name(16)]):
                    out, err = io.StringIO(), io.StringIO()
                    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                        status = bench_trend.main(["report", "--ledger", str(ledger), *date])
                    self.assertEqual(2, status)
                    self.assertEqual("", out.getvalue())
                    self.assertIn(f"no series has a recorded night {night_name(16)}", err.getvalue())
                trend = bench_trend.report(iter(builder.rows), night_name(15))
                self.assertEqual(night_name(15), trend.night)
                self.assertEqual(["steady"], [state.state for state in trend.states.values()])

    def test_an_incomplete_series_is_missing_when_another_series_is_recorded(self) -> None:
        self.builder.night(0, {"q1": 1.0, "q2": 1.0})
        self.builder.night(1, {"q1": 1.0, "q2": 1.0}, processes=19)
        self.builder.night(1, {"q3": 1.0})
        trend = bench_trend.report(iter(self.builder.rows))
        self.assertEqual(night_name(1), trend.night)
        self.assertEqual(["q3"], [key[2] for key in trend.states])
        self.assertEqual(["q1", "q2"], [key[2] for key in trend.missing])
        text = bench_trend.format_report(trend)
        for benchmark in ("q1", "q2"):
            self.assertIn(f"not measured         jsonbench {benchmark} ", text)

    def test_a_latest_run_without_any_recorded_history_names_its_night(self) -> None:
        self.builder.night(0, {"q1": 1.0}, processes=19)
        with self.assertRaisesRegex(TrendError, f"no series has a recorded night {night_name(0)}"):
            bench_trend.report(iter(self.builder.rows))

    def test_the_report_names_state_drift_fence_commits_and_the_night_before_the_change(self) -> None:
        for index in range(18):
            self.builder.night(
                index,
                {"q1": quiet(18)[index], "q2": (quiet(16) + [1.03, 1.03])[index], "q3": 1.0 if index < 17 else 1.05},
            )
        text = bench_trend.format_report(bench_trend.report(self.builder.rows))
        lines = text.splitlines()
        self.assertEqual(f"nightly trend for {night_name(17)}", lines[0])
        self.assertEqual(
            f"drift                jsonbench q2 cold_seconds [native, box-1]: drift +3.00%, fence 0.60%, "
            f"commit {commit_of(17)[:12]}, unbroken since the night after {night_name(15)} "
            f"(commit {commit_of(15)[:12]})",
            lines[1],
        )
        self.assertRegex(
            lines[2], "^unconfirmed +jsonbench q3 cold_seconds \\[native, box-1\\]: drift \\+5\\.00%, fence 0\\.00%"
        )
        self.assertEqual("drift 1; unconfirmed 1; steady 1", lines[3])
        self.assertIn("alarm: confirmed drift", lines[4])
        self.assertNotIn("q1", text, "a steady series is counted, not listed")

    def test_a_series_is_named_with_its_parameters(self) -> None:
        for index in range(3):
            self.builder.night(index, {"load": 30.0}, params={"tier": "1m", "engine": "native"})
        text = bench_trend.format_report(bench_trend.report(self.builder.rows))
        self.assertIn("jsonbench load{engine=native,tier=1m} cold_seconds [native, box-1], commit", text)

    def test_a_series_that_vanished_is_reported_as_not_measured(self) -> None:
        for index in range(3):
            self.builder.night(index, {"q1": 1.0, "q2": 1.0} if index < 2 else {"q1": 1.0})
        text = bench_trend.format_report(bench_trend.report(self.builder.rows))
        self.assertIn(
            f"not measured         jsonbench q2 cold_seconds [native, box-1]: it has earlier nights and none for {night_name(2)}",
            text,
        )
        self.assertIn("insufficient-history 1", text)
        self.assertNotIn("alarm", text)

    def test_only_a_series_that_began_before_the_night_can_be_missing_from_it(self) -> None:
        self.builder.night(0, {"q1": 1.0, "once": 1.0})
        self.builder.night(1, {"q1": 1.0})
        self.builder.night(2, {"q1": 1.0, "later": 1.0})
        self.assertEqual(["once"], [key[2] for key in bench_trend.report(self.builder.rows, night_name(1)).missing])
        self.assertEqual(["once"], [key[2] for key in bench_trend.report(self.builder.rows, night_name(2)).missing])
        self.assertEqual([], bench_trend.report(self.builder.rows, night_name(0)).missing, "`later` did not exist yet")

    def test_the_command_line_reports_the_latest_or_the_named_night(self) -> None:
        self.builder.nights(quiet(16) + quiet(2, level=1.03, start=16))
        ledger = self.base / "ledger.jsonl"
        bench_ledger.append_rows(ledger, self.builder.rows)

        def cli(*arguments: str) -> tuple[int, str, str]:
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                status = bench_trend.main(["report", *arguments])
            return status, out.getvalue(), err.getvalue()

        status, text, _ = cli("--ledger", str(ledger))
        self.assertEqual((1, f"nightly trend for {night_name(17)}"), (status, text.splitlines()[0]))
        status, text, _ = cli("--ledger", str(ledger), "--night", night_name(16))
        self.assertEqual(0, status, "an unconfirmed change is not an alarm")
        self.assertIn("unconfirmed", text)
        with mock.patch.dict(os.environ, {bench_ledger.LEDGER_ENV: str(ledger)}):
            self.assertEqual(1, cli()[0])
        with mock.patch.dict(os.environ):
            os.environ.pop(bench_ledger.LEDGER_ENV, None)
            status, _, err = cli()
            self.assertEqual((2, True), (status, "no ledger: pass --ledger or set SIRIX_BENCH_LEDGER" in err))
        self.assertEqual(2, cli("--ledger", str(ledger), "--night", "2020-01-01")[0])
        self.assertEqual(2, cli("--ledger", str(self.base / "absent.jsonl"))[0])
        with self.assertRaises(SystemExit) as usage, contextlib.redirect_stderr(io.StringIO()):
            bench_trend.main([])
        self.assertEqual(2, usage.exception.code, "a command is required")
        script = subprocess.run(
            [sys.executable, "-B", str(Path(bench_trend.__file__)), "report", "--ledger", str(ledger)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual((1, True), (script.returncode, "alarm: confirmed drift" in script.stdout))
        ledger.write_text(json.dumps({"schema": 9}) + "\n", encoding="utf-8")
        self.assertEqual(2, cli("--ledger", str(ledger))[0])


if __name__ == "__main__":
    unittest.main()
