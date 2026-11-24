#!/usr/bin/env python3
"""Behaviour tests of the timed acceptance verdict (spec/CONV-conventions.md, CONV-302)."""

from __future__ import annotations

import contextlib
import io
import itertools
import json
import math
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import timed_gate
from timed_gate import GateError

HERE = Path(__file__).resolve().parent
AA_PAIRS = HERE / "testdata" / "aa-control-2026-11-23.tsv"
AA_ALLOCATION = HERE / "testdata" / "aa-control-2026-11-23-allocation.tsv"

# Ten small, fixed deviations of a candidate from its baseline, as log ratios. Their mean is zero.
QUIET = [0.004, -0.003, 0.001, -0.002, 0.0, 0.003, -0.004, 0.002, -0.001, 0.0]
# The same shape, forty times larger: a benchmark whose threshold stays above the 4% floor at 20 pairs.
NOISY = [40 * value for value in QUIET]


def oracle_threshold(ratios: list[float]) -> float:
    """CONV-302 written out directly: every one of the 2^n sign assignments, sorted, rank ceil(0.99 * 2^n)."""
    count = len(ratios)
    mean = sum(ratios) / count
    residuals = [(ratio - mean) * math.sqrt(count / (count - 1)) for ratio in ratios]
    values = sorted(
        abs(sum(sign * residual for sign, residual in zip(signs, residuals))) / count
        for signs in itertools.product((1, -1), repeat=count)
    )
    return values[math.ceil(0.99 * len(values)) - 1]


def pair_rows(
    benchmark: str,
    pairs: list[tuple[float, float]],
    *,
    params: dict[str, str] | None = None,
    metric: str = "ns_per_op",
    unit: str = "ns/op",
    rejected: tuple[int, ...] = (),
    first_pair: int = 1,
) -> list[dict]:
    rows = []
    for offset, (baseline, candidate) in enumerate(pairs):
        pair = first_pair + offset
        for position, (arm, value) in enumerate((("baseline", baseline), ("candidate", candidate)), start=1):
            row = {
                "kind": "measurement",
                "benchmark": benchmark,
                "params": dict(params or {}),
                "metric": metric,
                "unit": unit,
                "arm": arm,
                "pair": pair,
                "position": position,
                "process": pair,
                "value": value,
                "accepted": pair not in rejected,
            }
            if pair in rejected:
                row["reject_reason"] = "foreign load"
            rows.append(row)
    return rows


def shifted(deviations: list[float], shift: float = 0.0, base: float = 1000.0) -> list[tuple[float, float]]:
    """Pairs whose candidate is the baseline times e^(deviation + shift)."""
    return [(base, base * math.exp(deviation + shift)) for deviation in deviations]


def allocation_rows(benchmark: str, baseline: list[float], candidate: list[float]) -> list[dict]:
    return [
        {
            "kind": "measurement",
            "benchmark": benchmark,
            "params": {},
            "metric": "alloc_bytes_per_op",
            "unit": "B/op",
            "arm": arm,
            "pair": None,
            "position": None,
            "process": process,
            "value": value,
            "accepted": True,
        }
        for arm, values in (("baseline", baseline), ("candidate", candidate))
        for process, value in enumerate(values, start=1)
    ]


def load_aa(scale_candidate: float = 1.0) -> list[dict]:
    rows = []
    for line in AA_PAIRS.read_text(encoding="utf-8").splitlines():
        if line.startswith("#") or line.startswith("pair\t"):
            continue
        pair, order, benchmark, params, baseline, candidate = line.split("\t")
        first = "baseline" if order == "A-first" else "candidate"
        parsed = dict(item.split("=") for item in params.split(","))
        for arm, value in (("baseline", float(baseline)), ("candidate", float(candidate) * scale_candidate)):
            rows.append(
                {
                    "kind": "measurement",
                    "benchmark": benchmark,
                    "params": parsed,
                    "metric": "ns_per_op",
                    "unit": "ns/op",
                    "arm": arm,
                    "pair": int(pair),
                    "position": 1 if arm == first else 2,
                    "process": int(pair),
                    "value": value,
                    "accepted": True,
                }
            )
    return rows


def load_aa_allocation() -> list[dict]:
    rows = []
    for line in AA_ALLOCATION.read_text(encoding="utf-8").splitlines():
        if line.startswith("#") or line.startswith("benchmark\t"):
            continue
        benchmark, params, arm, value = line.split("\t")
        rows.append(
            {
                "kind": "measurement",
                "benchmark": benchmark,
                "params": dict(item.split("=") for item in params.split(",")),
                "metric": "alloc_bytes_per_op",
                "unit": "B/op",
                "arm": arm,
                "pair": None,
                "position": None,
                "process": 1,
                "value": float(value),
                "accepted": True,
            }
        )
    return rows


def aa_name(row: dict) -> str:
    """'coldMaterialize REPEATED codec': method, content and which of the two benchmark classes."""
    owner, method = row["benchmark"].split(".")
    return f"{method} {row['params']['content']} {'alloc' if 'Allocation' in owner else 'codec'}"


def verdicts_by_name(judgement: timed_gate.Judgement) -> dict[str, str]:
    return {
        f"{row['benchmark']} {','.join(f'{k}={v}' for k, v in sorted(row['params'].items()))}".strip(): row["verdict"]
        for row in judgement.verdicts
    }


class ThresholdTest(unittest.TestCase):

    def test_two_pairs_by_hand(self) -> None:
        # r = [0, x]: residuals are -x/sqrt(2) and +x/sqrt(2); the four relabelings give 0, 0 and twice
        # x/sqrt(2), and rank ceil(0.99 * 4) = 4 is the largest.
        difference, threshold = timed_gate.difference_and_threshold([0.0, 0.2])
        self.assertAlmostEqual(0.1, difference, places=15)
        self.assertAlmostEqual(0.2 / math.sqrt(2), threshold, places=15)

    def test_three_pairs_by_hand(self) -> None:
        # r = [-a, 0, a]: residuals -c, 0, c with c = a * sqrt(3/2); the relabelings give 0 or 2c/3.
        difference, threshold = timed_gate.difference_and_threshold([-0.03, 0.0, 0.03])
        self.assertAlmostEqual(0.0, difference, places=15)
        self.assertAlmostEqual(2 * 0.03 * math.sqrt(1.5) / 3, threshold, places=15)

    def test_ten_pairs_take_the_eleventh_largest_of_all_relabelings(self) -> None:
        # One pair deviates by x, nine do not. With the 0.9 residual's sign fixed, the sums of the nine
        # 0.1 residuals put one relabeling at 1.8, nine at 1.6 and thirty-six at 1.4 (times x*s/10,
        # s = sqrt(10/9)); each stands for two of the 1024. Rank ceil(0.99 * 1024) = 1014 is the
        # eleventh largest, which is still in the 1.6 group.
        _, threshold = timed_gate.difference_and_threshold([0.1] + [0.0] * 9)
        self.assertAlmostEqual(0.16 * 0.1 * math.sqrt(10 / 9), threshold, places=14)

    def test_the_rank_matches_a_full_enumeration_for_every_pair_count(self) -> None:
        ratios = [0.031 * math.sin(1.7 * index + 0.3) + 0.004 * index for index in range(14)]
        for count in range(2, 15):
            with self.subTest(pairs=count):
                self.assertAlmostEqual(
                    oracle_threshold(ratios[:count]), timed_gate.difference_and_threshold(ratios[:count])[1], places=14
                )

    def test_pinned_value_at_the_pair_limit(self) -> None:
        ratios = [0.02 * math.sin(index) for index in range(20)]
        difference, threshold = timed_gate.difference_and_threshold(ratios)
        # Checked once against a loop over all 2^20 sign assignments.
        self.assertAlmostEqual(8.52766336921545e-05, difference, places=15)
        self.assertAlmostEqual(0.007930902640734847, threshold, places=14)

    def test_identical_pairs_have_a_zero_threshold(self) -> None:
        difference, threshold = timed_gate.difference_and_threshold([0.05] * 10)
        self.assertAlmostEqual(0.05, difference, places=15)
        self.assertEqual(0.0, threshold)

    def test_the_threshold_does_not_move_when_one_arm_is_scaled(self) -> None:
        pairs = [
            (1000.0 + 7 * index, (1000.0 + 7 * index) * math.exp(deviation)) for index, deviation in enumerate(NOISY)
        ]
        _, reference = timed_gate.difference_and_threshold(timed_gate.log_ratios(pairs))
        for factor in (0.5, 0.9, 1.1, 1.3, 4.0):
            with self.subTest(factor=factor):
                slower = timed_gate.log_ratios([(a, b * factor) for a, b in pairs])
                other_arm = timed_gate.log_ratios([(a * factor, b) for a, b in pairs])
                self.assertAlmostEqual(reference, timed_gate.difference_and_threshold(slower)[1], places=13)
                self.assertAlmostEqual(reference, timed_gate.difference_and_threshold(other_arm)[1], places=13)

    def test_pair_counts_outside_the_defined_range_are_refused(self) -> None:
        with self.assertRaisesRegex(GateError, "at least 2 pairs"):
            timed_gate.difference_and_threshold([0.1])
        with self.assertRaisesRegex(GateError, "at most 20 pairs"):
            timed_gate.difference_and_threshold([0.01] * 21)

    def test_measurements_must_be_finite_and_positive_numbers(self) -> None:
        for bad in (0.0, -1.0, math.inf, math.nan, "12", None, True):
            with self.subTest(value=bad):
                with self.assertRaisesRegex(GateError, "pair 2: the candidate measurement"):
                    timed_gate.log_ratios([(1.0, 1.0), (1.0, bad)])
                with self.assertRaisesRegex(GateError, "pair 1: the baseline measurement"):
                    timed_gate.log_ratios([(bad, 1.0)])


class VerdictTest(unittest.TestCase):

    def test_a_regression_needs_the_floor_and_the_threshold(self) -> None:
        floor = timed_gate.FLOOR
        self.assertEqual("regression", timed_gate.classify(floor + 0.001, 0.01))
        self.assertEqual("unchanged", timed_gate.classify(floor, 0.01), "the floor itself is not exceeded")
        self.assertEqual("unchanged", timed_gate.classify(floor - 0.001, 0.01))
        self.assertEqual("unstable", timed_gate.classify(0.06, 0.06), "the threshold itself is not exceeded")
        self.assertEqual("regression", timed_gate.classify(0.0601, 0.06))

    def test_an_improvement_mirrors_a_regression(self) -> None:
        floor = timed_gate.FLOOR
        self.assertEqual("improvement", timed_gate.classify(-floor - 0.001, 0.01))
        self.assertEqual("unchanged", timed_gate.classify(-floor, 0.01))
        self.assertEqual("unstable", timed_gate.classify(-0.06, 0.06))
        self.assertEqual("improvement", timed_gate.classify(-0.0601, 0.06))

    def test_unstable_means_the_floor_could_not_have_been_seen(self) -> None:
        floor = timed_gate.FLOOR
        self.assertEqual("unstable", timed_gate.classify(0.0, floor + 0.001))
        self.assertEqual("unchanged", timed_gate.classify(0.0, floor))
        self.assertEqual("unstable", timed_gate.classify(0.05, 0.08), "above the floor but inside the noise")

    def test_the_floor_is_four_percent_and_the_stop_precision_three(self) -> None:
        self.assertAlmostEqual(4.0, timed_gate.percent(timed_gate.FLOOR), places=12)
        self.assertAlmostEqual(3.0, timed_gate.percent(timed_gate.STOP_PRECISION), places=12)

    def test_a_shift_above_the_floor_is_a_regression_and_one_below_is_not(self) -> None:
        cases = (
            (math.log(1.06), "regression"),
            (math.log(1.03), "unchanged"),
            (0.0, "unchanged"),
            (-math.log(1.06), "improvement"),
        )
        for shift, expected in cases:
            with self.subTest(shift=shift):
                judgement = timed_gate.judge(pair_rows("scan", shifted(QUIET, shift)))
                self.assertEqual([expected], [row["verdict"] for row in judgement.verdicts])
                self.assertAlmostEqual(shift, judgement.verdicts[0]["difference"], places=12)
                self.assertEqual(expected == "regression", judgement.failed)

    def test_a_noisy_benchmark_is_unstable_until_the_slowdown_exceeds_its_noise(self) -> None:
        quiet_rows = pair_rows("quiet", shifted(QUIET + QUIET))
        for shift, expected in ((0.0, "unstable"), (math.log(1.05), "unstable"), (math.log(1.30), "regression")):
            with self.subTest(shift=shift):
                judgement = timed_gate.judge(quiet_rows + pair_rows("noisy", shifted(NOISY + NOISY, shift)))
                by_name = {row["benchmark"]: row["verdict"] for row in judgement.verdicts}
                self.assertEqual({"quiet": "unchanged", "noisy": expected}, by_name)
                self.assertEqual(expected == "regression", judgement.failed, "unstable does not fail the comparison")
                self.assertEqual(expected == "unstable", bool(judgement.unstable))

    def test_the_report_names_unstable_benchmarks_as_not_judged(self) -> None:
        judgement = timed_gate.judge(
            pair_rows("quiet", shifted(QUIET + QUIET)) + pair_rows("noisy", shifted(NOISY + NOISY))
        )
        report = timed_gate.format_report(judgement)
        self.assertIn("outcome: no regression in 1 judged benchmark(s)", report)
        self.assertIn("not judged: noisy ns_per_op is unstable", report)
        self.assertIn("not judged (unstable) 1", report)
        failing = timed_gate.format_report(timed_gate.judge(pair_rows("scan", shifted(QUIET, math.log(1.06)))))
        self.assertIn("outcome: FAIL - 1 regression(s), 0 failed allocation check(s)", failing)


class AAControlTest(unittest.TestCase):
    """The 2026-11-23 A/A control: main against itself, 17 accepted pairs, 12 benchmarks."""

    UNSTABLE = {
        "HOTLeafCodecAllocationBenchmark.compactMaterialize content=REPEATED",
        "HOTLeafCodecAllocationBenchmark.compactMaterialize content=ZERO",
    }

    def test_identical_code_is_never_judged_a_regression(self) -> None:
        judgement = timed_gate.judge(load_aa(), enforce_stop_rule=False)
        verdicts = verdicts_by_name(judgement)
        self.assertEqual(12, len(verdicts))
        self.assertEqual(self.UNSTABLE, {name for name, verdict in verdicts.items() if verdict == "unstable"})
        self.assertEqual({"unchanged"}, {verdict for name, verdict in verdicts.items() if name not in self.UNSTABLE})
        self.assertFalse(judgement.failed)
        self.assertEqual({17}, {row["pairs"] for row in judgement.verdicts})

    def test_thresholds_of_the_control(self) -> None:
        judgement = timed_gate.judge(load_aa(), enforce_stop_rule=False)
        thresholds = {aa_name(row): round(timed_gate.percent(row["threshold"]), 2) for row in judgement.verdicts}
        self.assertEqual(
            {
                "coldVerifiedCopy REPEATED alloc": 0.92,
                "coldVerifiedCopy ZERO alloc": 0.95,
                "compactMaterialize REPEATED alloc": 6.28,
                "compactMaterialize ZERO alloc": 7.86,
                "uncachedSelection REPEATED alloc": 1.90,
                "uncachedSelection ZERO alloc": 1.70,
                "warmImageLookup REPEATED alloc": 2.35,
                "warmImageLookup ZERO alloc": 1.87,
                "coldMaterialize REPEATED codec": 2.64,
                "warmImageLookup REPEATED codec": 2.04,
                "warmLeafLookup REPEATED codec": 1.91,
                "warmLeafValueRef REPEATED codec": 1.17,
            },
            thresholds,
        )

    def test_every_ten_pair_window_of_the_control_is_free_of_regressions(self) -> None:
        rows = load_aa()
        pairs = sorted({row["pair"] for row in rows})
        for start in range(len(pairs) - 9):
            window = set(pairs[start : start + 10])
            judgement = timed_gate.judge([row for row in rows if row["pair"] in window], enforce_stop_rule=False)
            self.assertNotIn("regression", {row["verdict"] for row in judgement.verdicts}, f"pairs {sorted(window)}")

    def test_a_candidate_slowed_by_eight_percent_is_a_regression_wherever_the_benchmark_can_see_it(self) -> None:
        verdicts = verdicts_by_name(timed_gate.judge(load_aa(scale_candidate=1.08), enforce_stop_rule=False))
        noisiest = "HOTLeafCodecAllocationBenchmark.compactMaterialize content=ZERO"
        self.assertEqual(
            "unstable", verdicts.pop(noisiest), "8% is inside this benchmark's 7.9% threshold plus its offset"
        )
        self.assertEqual({"regression"}, set(verdicts.values()))
        self.assertEqual(11, len(verdicts))

    def test_a_candidate_slowed_by_thirty_percent_is_a_regression_everywhere(self) -> None:
        judgement = timed_gate.judge(load_aa(scale_candidate=1.30), enforce_stop_rule=False)
        self.assertEqual({"regression"}, {row["verdict"] for row in judgement.verdicts})
        self.assertTrue(judgement.failed)

    def test_the_exact_allocation_delta_rule_flags_both_fixture_defects(self) -> None:
        judgement = timed_gate.judge(load_aa() + load_aa_allocation(), enforce_stop_rule=False)
        checks = {aa_name(row): row["allocation"] for row in judgement.verdicts}
        failing = {name: check for name, check in checks.items() if not check["pass"]}
        self.assertEqual(
            {
                "compactMaterialize REPEATED alloc": {
                    "baseline_bytes": 995, "candidate_bytes": 995, "delta_bytes": 1, "pass": False
                },
                "compactMaterialize ZERO alloc": {
                    "baseline_bytes": 995, "candidate_bytes": 1019, "delta_bytes": 25, "pass": False
                },
            },
            failing,
        )
        self.assertEqual(12, len(checks))
        self.assertTrue(
            judgement.failed, "allocation stays exact: identical code with unequal legs is a fixture defect"
        )
        self.assertNotIn("regression", {row["verdict"] for row in judgement.verdicts})


class StopRuleTest(unittest.TestCase):

    def test_a_run_never_stops_below_ten_pairs(self) -> None:
        for count in (1, 2, 9):
            with self.subTest(pairs=count):
                may_stop, states = timed_gate.run_may_stop(pair_rows("scan", shifted([0.0] * count)))
                self.assertFalse(may_stop)
                self.assertEqual([count], [state.pairs for state in states])

    def test_a_precise_benchmark_stops_at_ten_pairs(self) -> None:
        may_stop, states = timed_gate.run_may_stop(pair_rows("scan", shifted(QUIET)))
        self.assertTrue(may_stop)
        self.assertLess(states[0].threshold, timed_gate.STOP_PRECISION)

    def test_a_noisy_benchmark_continues_until_twenty_pairs(self) -> None:
        for count, expected in ((10, False), (19, False), (20, True)):
            with self.subTest(pairs=count):
                may_stop, states = timed_gate.run_may_stop(pair_rows("scan", shifted((NOISY + NOISY)[:count])))
                self.assertEqual(expected, may_stop)
                self.assertGreater(states[0].threshold, timed_gate.STOP_PRECISION)

    def test_the_precision_boundary_is_three_percent(self) -> None:
        # Ten deviations of +-x have a threshold proportional to x; find the scale that puts it at 3%.
        unit = timed_gate.difference_and_threshold([1.0, -1.0] * 5)[1]
        for factor, expected in ((0.999, True), (1.001, False)):
            scale = factor * timed_gate.STOP_PRECISION / unit
            may_stop, _ = timed_gate.run_may_stop(pair_rows("scan", shifted([scale, -scale] * 5)))
            self.assertEqual(expected, may_stop, f"threshold at {factor} of the stop precision")

    def test_the_run_stops_only_when_every_judged_benchmark_has_met_its_condition(self) -> None:
        rows = pair_rows("quiet", shifted(QUIET)) + pair_rows("noisy", shifted(NOISY))
        may_stop, states = timed_gate.run_may_stop(rows)
        self.assertFalse(may_stop)
        self.assertEqual({"noisy": False, "quiet": True}, {state.series.benchmark: state.met for state in states})
        self.assertTrue(timed_gate.run_may_stop(rows, benchmarks=["quiet"])[0], "the plan judges only the quiet one")

    def test_the_stop_decision_does_not_read_the_difference(self) -> None:
        for deviations in (QUIET, NOISY):
            reference = timed_gate.run_may_stop(pair_rows("scan", shifted(deviations)))[0]
            for shift in (-0.5, -0.05, 0.05, 0.5):
                with self.subTest(deviations=deviations[0], shift=shift):
                    self.assertEqual(
                        reference, timed_gate.run_may_stop(pair_rows("scan", shifted(deviations, shift)))[0]
                    )

    def test_a_plan_may_raise_the_minimum_and_nothing_else(self) -> None:
        rows = pair_rows("scan", shifted(QUIET + QUIET[:4]))
        self.assertTrue(timed_gate.run_may_stop(rows)[0])
        self.assertFalse(timed_gate.run_may_stop(rows, min_pairs=15)[0])
        self.assertTrue(timed_gate.run_may_stop(pair_rows("scan", shifted(QUIET + QUIET[:5])), min_pairs=15)[0])
        for bad in (9, 21, 0, True, 10.0):
            with self.subTest(min_pairs=bad):
                with self.assertRaisesRegex(GateError, "minimum number of pairs must be 10 to 20"):
                    timed_gate.run_may_stop(rows, min_pairs=bad)

    def test_rejected_pairs_do_not_count(self) -> None:
        rows = pair_rows("scan", shifted(QUIET), rejected=(4,))
        may_stop, states = timed_gate.run_may_stop(rows)
        self.assertFalse(may_stop)
        self.assertEqual(9, states[0].pairs)
        self.assertEqual(10, states[0].series.attempted)

    def test_more_than_twenty_accepted_pairs_are_refused(self) -> None:
        with self.assertRaisesRegex(GateError, "21 accepted pairs, more than the limit of 20"):
            timed_gate.run_may_stop(pair_rows("scan", shifted(QUIET + QUIET + QUIET[:1])))

    def test_an_empty_run_continues(self) -> None:
        self.assertEqual((False, []), timed_gate.run_may_stop([]))


class JudgeTest(unittest.TestCase):

    def test_a_verdict_needs_ten_to_twenty_accepted_pairs(self) -> None:
        with self.assertRaisesRegex(
            GateError, "scan ns_per_op: a verdict needs 10 to 20 accepted pairs, the run has 9 of 10"
        ):
            timed_gate.judge(pair_rows("scan", shifted(QUIET), rejected=(2,)), enforce_stop_rule=False)
        with self.assertRaisesRegex(GateError, "the run has 21 of 21"):
            timed_gate.judge(pair_rows("scan", shifted(QUIET + QUIET + QUIET[:1])), enforce_stop_rule=False)
        with self.assertRaisesRegex(GateError, "no timed pair to judge"):
            timed_gate.judge([])

    def test_a_gate_run_that_ended_before_its_stop_condition_has_no_verdict(self) -> None:
        rows = pair_rows("scan", shifted((NOISY + NOISY)[:12]))
        with self.assertRaisesRegex(GateError, "the run ended before its stop condition \\(12 accepted pairs"):
            timed_gate.judge(rows)
        self.assertEqual(
            ["unstable"], [row["verdict"] for row in timed_gate.judge(rows, enforce_stop_rule=False).verdicts]
        )

    def test_a_gate_run_that_continued_past_its_stop_point_has_no_verdict(self) -> None:
        with self.assertRaisesRegex(GateError, "continued past its stop point"):
            timed_gate.judge(pair_rows("scan", shifted(QUIET + QUIET[:1])))
        self.assertEqual(
            11,
            timed_gate.judge(pair_rows("scan", shifted(QUIET + QUIET[:1])), enforce_stop_rule=False).verdicts[0][
                "pairs"
            ],
        )

    def test_an_earlier_stop_is_not_erased_by_later_noise(self) -> None:
        rows = pair_rows("scan", shifted([0.05] * 10 + [-0.10] * 10))
        self.assertEqual("regression", timed_gate.judge(rows[:20]).verdicts[0]["verdict"])
        with self.assertRaisesRegex(GateError, "the run continued past its stop point"):
            timed_gate.judge(rows)

    def test_a_benchmark_that_met_its_condition_keeps_running_with_the_others(self) -> None:
        rows = pair_rows("quiet", shifted(QUIET + QUIET)) + pair_rows("noisy", shifted(NOISY + NOISY))
        judgement = timed_gate.judge(rows)
        self.assertEqual({"quiet": 20, "noisy": 20}, {row["benchmark"]: row["pairs"] for row in judgement.verdicts})

    def test_a_rejected_last_pair_is_still_the_runs_last_pair(self) -> None:
        # Pairs 1 to 10 are accepted and precise, so the run had to stop there; pair 11 was run anyway.
        rows = pair_rows("scan", shifted(QUIET + QUIET[:1]), rejected=(11,))
        with self.assertRaisesRegex(GateError, "continued past its stop point"):
            timed_gate.judge(rows)

    def test_a_pair_rejected_in_one_arm_is_rejected(self) -> None:
        rows = pair_rows("scan", shifted(QUIET + QUIET[:1]))
        rows[2]["accepted"] = False  # pair 2, baseline only
        judgement = timed_gate.judge(rows)
        self.assertEqual(10, judgement.verdicts[0]["pairs"])

    def test_series_are_kept_apart_by_parameters_and_metric(self) -> None:
        rows = (
            pair_rows("scan", shifted(QUIET), params={"keyBytes": "8"})
            + pair_rows("scan", shifted(QUIET, math.log(1.06)), params={"keyBytes": "64"})
            + pair_rows(
                "scan", shifted(QUIET, -math.log(1.06)), params={"keyBytes": "8"}, metric="cold_seconds", unit="s"
            )
        )
        verdicts = {
            (row["params"]["keyBytes"], row["metric"]): row["verdict"] for row in timed_gate.judge(rows).verdicts
        }
        self.assertEqual(
            {("8", "ns_per_op"): "unchanged", ("64", "ns_per_op"): "regression", ("8", "cold_seconds"): "improvement"},
            verdicts,
        )

    def test_the_plan_selects_the_judged_benchmarks(self) -> None:
        rows = pair_rows("scan", shifted(QUIET)) + pair_rows("probe", shifted(QUIET, math.log(1.06)))
        judgement = timed_gate.judge(rows, benchmarks=["scan"])
        self.assertEqual(["scan"], [row["benchmark"] for row in judgement.verdicts])
        self.assertEqual(["probe"], judgement.outside_plan)
        self.assertFalse(judgement.failed)
        self.assertIn("measured outside the plan's judged set: probe", timed_gate.format_report(judgement))
        with self.assertRaisesRegex(GateError, "the plan judges load, but the run holds no measurement of it"):
            timed_gate.judge(rows, benchmarks=["scan", "load"])

    def test_malformed_pairs_fail_the_whole_analysis(self) -> None:
        good = pair_rows("other", shifted(QUIET))
        cases = {
            "pair 3 has no candidate measurement": lambda rows: rows.pop(5),
            "pair 3 holds two baseline measurements": lambda rows: rows.append(dict(rows[4])),
            "must be finite and greater than zero": lambda rows: rows[4].update(value=0.0),
            "is not a number": lambda rows: rows[4].update(value="fast"),
            "needs a pair number of 1 or more": lambda rows: rows[4].update(pair=None),
            "has the arms baseline and candidate, got 'head'": lambda rows: rows[4].update(arm="head"),
            "the unit changes within the run": lambda rows: rows[4].update(unit="us/op"),
            "params must map strings to strings": lambda rows: rows[4].update(params={"keyBytes": 8}),
            "a measurement row lacks 'value'": lambda rows: rows[4].pop("value"),
        }
        for message, damage in cases.items():
            with self.subTest(message=message):
                rows = pair_rows("scan", shifted(QUIET))
                damage(rows)
                with self.assertRaisesRegex(GateError, message):
                    timed_gate.judge(good + rows, enforce_stop_rule=False)

    def test_a_broken_measurement_in_a_rejected_pair_is_not_silently_dropped(self) -> None:
        rows = pair_rows("scan", shifted(QUIET + QUIET[:1]), rejected=(11,))
        rows[-1]["value"] = math.nan
        with self.assertRaisesRegex(GateError, "must be finite and greater than zero"):
            timed_gate.judge(rows, enforce_stop_rule=False)


def series_with_ratios(ratios: list[float]) -> timed_gate.Series:
    """A series whose log ratios are exactly these, where pair values could only approximate them."""

    class Fixed(timed_gate.Series):
        def ratios(self) -> list[float]:
            return list(ratios)

    pairs = [(pair, 1.0, 1.0) for pair in range(1, len(ratios) + 1)]
    return Fixed("scan", {}, "ns_per_op", "ns/op", pairs, len(pairs))


class BoundaryTest(unittest.TestCase):

    def test_a_threshold_exactly_at_the_stop_precision_meets_the_condition(self) -> None:
        # Ten ratios of +-x have a threshold proportional to x; walk x to the value whose threshold is
        # the stop precision to the last bit.
        target = timed_gate.STOP_PRECISION
        scale = target / timed_gate.difference_and_threshold([1.0, -1.0] * 5)[1]
        for _ in range(400):
            threshold = timed_gate.difference_and_threshold([scale, -scale] * 5)[1]
            if threshold == target:
                break
            scale = math.nextafter(scale, math.inf if threshold < target else 0.0)
        self.assertEqual(
            target, threshold, "no ratio gives the stop precision exactly; the fixture needs another shape"
        )
        (state,) = timed_gate.stop_states([series_with_ratios([scale, -scale] * 5)])
        self.assertEqual((target, True), (state.threshold, state.met))
        above = math.nextafter(scale, math.inf)
        (state,) = timed_gate.stop_states([series_with_ratios([above, -above] * 5)])
        self.assertEqual((True, False), (state.threshold > target, state.met))

    def test_the_minimum_may_be_raised_to_the_pair_limit(self) -> None:
        rows = pair_rows("scan", shifted(QUIET + QUIET[:9]))
        self.assertFalse(timed_gate.run_may_stop(rows, min_pairs=20)[0], "19 precise pairs, 20 asked for")
        self.assertTrue(timed_gate.run_may_stop(pair_rows("scan", shifted(QUIET + QUIET)), min_pairs=20)[0])
        self.assertTrue(timed_gate.run_may_stop(pair_rows("scan", shifted(QUIET)), min_pairs=10)[0])

    def test_a_threshold_exists_from_two_pairs_on(self) -> None:
        states = {
            count: timed_gate.run_may_stop(pair_rows("scan", shifted(QUIET[:count])))[1][0] for count in (1, 2, 3)
        }
        self.assertIsNone(states[1].threshold)
        self.assertAlmostEqual(0.007 / math.sqrt(2), states[2].threshold, places=12)
        self.assertGreater(states[3].threshold, 0.0)
        text = timed_gate.format_stop(False, [states[1], states[2]])
        self.assertEqual(
            [
                f"{'scan ns_per_op':<58} pairs  1  threshold       -  continue",
                f"{'scan ns_per_op':<58} pairs  2  threshold   0.50%  continue",
                "continue",
            ],
            text.splitlines(),
        )


class ReportTextTest(unittest.TestCase):

    def test_the_outcome_counts_judged_and_unjudged_benchmarks(self) -> None:
        rows = (
            pair_rows("slower", shifted(QUIET + QUIET, math.log(1.06)))
            + pair_rows("faster", shifted(QUIET + QUIET, -math.log(1.06)))
            + pair_rows("same", shifted(QUIET + QUIET))
            + pair_rows("noisy", shifted(NOISY + NOISY))
        )
        report = timed_gate.format_report(timed_gate.judge(rows))
        self.assertIn("judged 3, not judged (unstable) 1", report)
        self.assertIn("outcome: FAIL - 1 regression(s), 0 failed allocation check(s)", report)
        passing = timed_gate.format_report(timed_gate.judge([row for row in rows if row["benchmark"] != "slower"]))
        self.assertIn("judged 2, not judged (unstable) 1", passing)
        self.assertIn("outcome: no regression in 2 judged benchmark(s)", passing)

    def test_a_failed_allocation_check_fails_the_outcome_by_itself(self) -> None:
        rows = (
            pair_rows("scan", shifted(QUIET))
            + allocation_rows("scan", [168.0], [169.0])
            + pair_rows("probe", shifted(QUIET))
            + allocation_rows("probe", [64.0], [64.0])
            + pair_rows("walk", shifted(QUIET))
            + allocation_rows("walk", [0.0], [0.0])
        )
        report = timed_gate.format_report(timed_gate.judge(rows))
        self.assertIn("outcome: FAIL - 0 regression(s), 1 failed allocation check(s)", report)
        self.assertIn("168 -> 169 B/op delta +1 FAIL", report)
        self.assertIn("64 -> 64 B/op delta +0 pass", report)

    def test_a_series_is_named_with_its_parameters_everywhere(self) -> None:
        params = {"keyBytes": "8", "content": "ZERO"}
        rows = pair_rows("scan", shifted(QUIET), params=params)
        self.assertIn("scan{content=ZERO,keyBytes=8} ns_per_op ", timed_gate.format_report(timed_gate.judge(rows)))
        self.assertIn(
            "scan{content=ZERO,keyBytes=8} ns_per_op ", timed_gate.format_stop(*timed_gate.run_may_stop(rows))
        )
        with self.assertRaisesRegex(GateError, "scan\\{content=ZERO,keyBytes=8\\} ns_per_op: pair 3 has no candidate"):
            timed_gate.judge(rows[:5] + rows[6:], enforce_stop_rule=False)
        with self.assertRaisesRegex(GateError, "^scan\\{content=ZERO,keyBytes=8\\} ns_per_op: a verdict needs"):
            timed_gate.judge(rows[:8], enforce_stop_rule=False)


class OtherRowsTest(unittest.TestCase):
    """A run directory and a ledger hold more than accepted timed measurements."""

    def test_a_row_that_does_not_say_otherwise_is_accepted(self) -> None:
        timed = pair_rows("scan", shifted(QUIET))
        legs = allocation_rows("scan", [168.0], [168.0])
        for row in timed + legs:
            del row["accepted"]
        verdict = timed_gate.judge(timed + legs).verdicts[0]
        self.assertEqual((10, 168), (verdict["pairs"], verdict["allocation"]["candidate_bytes"]))

    def test_allocation_legs_outside_the_plans_judged_set_are_left_alone(self) -> None:
        rows = (
            pair_rows("scan", shifted(QUIET))
            + allocation_rows("scan", [168.0], [168.0])
            + pair_rows("probe", shifted(QUIET))
            + allocation_rows("probe", [64.0], [900.0])
        )
        judgement = timed_gate.judge(rows, benchmarks=["scan"])
        self.assertEqual(
            [{"baseline_bytes": 168, "candidate_bytes": 168, "delta_bytes": 0, "pass": True}],
            [row["allocation"] for row in judgement.verdicts],
        )
        self.assertFalse(judgement.failed)

    def test_rows_of_other_kinds_are_not_measurements(self) -> None:
        timed = pair_rows("scan", shifted(QUIET))
        others = [
            {"kind": "run", "run": "20261123T183005Z-0a1b2c3d", "tier": "gate"},
            {"kind": "verdict", "benchmark": "scan", "params": {}, "metric": "ns_per_op", "verdict": "regression"},
        ]
        self.assertEqual([10], [row["pairs"] for row in timed_gate.judge(others + timed).verdicts])
        self.assertTrue(timed_gate.run_may_stop(timed + others)[0])

    def test_a_rejected_allocation_leg_is_not_part_of_an_arms_figure(self) -> None:
        legs = allocation_rows("scan", [168.0], [168.0, 9000.0])
        legs[-1].update(accepted=False, reject_reason="identity check failed")
        check = timed_gate.judge(pair_rows("scan", shifted(QUIET)) + legs).verdicts[0]["allocation"]
        self.assertEqual({"baseline_bytes": 168, "candidate_bytes": 168, "delta_bytes": 0, "pass": True}, check)

    def test_an_arm_without_a_leg_has_no_figure(self) -> None:
        with self.assertRaisesRegex(GateError, "an allocation figure needs at least one measurement"):
            timed_gate.allocation_median([])

    def test_measured_allocation_needs_an_accepted_leg_for_each_arm(self) -> None:
        for rejected in (("baseline",), ("candidate",), ("baseline", "candidate")):
            with self.subTest(rejected=rejected):
                legs = allocation_rows("scan", [168.0], [168.0])
                for row in legs:
                    if row["arm"] in rejected:
                        row.update(accepted=False, reject_reason="foreign load")
                with self.assertRaisesRegex(
                    GateError, f"scan alloc_bytes_per_op: no accepted {rejected[0]} allocation leg"
                ):
                    timed_gate.judge(pair_rows("scan", shifted(QUIET)) + legs)

    def test_rejected_allocation_outside_the_plan_does_not_require_a_check(self) -> None:
        legs = allocation_rows("probe", [168.0], [168.0])
        for row in legs:
            row.update(accepted=False, reject_reason="foreign load")
        judgement = timed_gate.judge(pair_rows("scan", shifted(QUIET)) + legs, benchmarks=["scan"])
        self.assertIsNone(judgement.verdicts[0]["allocation"])
        self.assertFalse(judgement.failed)


class AllocationTest(unittest.TestCase):

    def test_arm_figures_are_unrounded_medians(self) -> None:
        self.assertEqual(994.6127919781989, timed_gate.allocation_median([994.6127919781989]))
        self.assertEqual(168.2, timed_gate.allocation_median([167.9, 168.2, 4000.0]))

    def test_only_the_median_delta_is_rounded_with_ties_to_even(self) -> None:
        cases = (
            ([994.6128], [995.2031], 1),
            ([100.5], [101.0], 0),
            ([100.0], [101.5], 2),
            ([100.0], [102.5], 2),
            ([100.0], [99.5], 0),
            ([100.0], [98.5], -2),
            ([100.0, 101.0, 900.0], [100.0, 101.5, 900.0], 0),
        )
        for baseline, candidate, delta in cases:
            with self.subTest(baseline=baseline, candidate=candidate):
                judgement = timed_gate.judge(
                    pair_rows("scan", shifted(QUIET)) + allocation_rows("scan", baseline, candidate)
                )
                check = judgement.verdicts[0]["allocation"]
                self.assertEqual(delta, check["delta_bytes"])
                self.assertEqual(delta <= 0, check["pass"])
                self.assertEqual("unchanged", judgement.verdicts[0]["verdict"])

    def test_the_candidate_may_not_allocate_one_byte_more(self) -> None:
        timed = pair_rows("scan", shifted(QUIET))
        cases = (([168.0], [168.4], True), ([168.0], [167.0], True), ([168.0], [168.6], False), ([0.0], [0.6], False))
        for baseline, candidate, passes in cases:
            with self.subTest(baseline=baseline, candidate=candidate):
                judgement = timed_gate.judge(timed + allocation_rows("scan", baseline, candidate))
                self.assertEqual(passes, judgement.verdicts[0]["allocation"]["pass"])
                self.assertEqual(not passes, judgement.failed)
                self.assertEqual(
                    "unchanged", judgement.verdicts[0]["verdict"], "allocation never changes the timed verdict"
                )

    def test_a_plan_without_allocation_legs_has_no_allocation_check(self) -> None:
        self.assertIsNone(timed_gate.judge(pair_rows("scan", shifted(QUIET))).verdicts[0]["allocation"])

    def test_malformed_allocation_legs_fail_the_analysis(self) -> None:
        timed = pair_rows("scan", shifted(QUIET))
        one_arm = [row for row in allocation_rows("scan", [168.0], [168.0]) if row["arm"] == "baseline"]
        with self.assertRaisesRegex(GateError, "scan alloc_bytes_per_op: no accepted candidate allocation leg"):
            timed_gate.judge(timed + one_arm)
        with self.assertRaisesRegex(GateError, "must be finite and not negative"):
            timed_gate.judge(timed + allocation_rows("scan", [168.0], [-1.0]))
        with self.assertRaisesRegex(GateError, "probe alloc_bytes_per_op: an allocation leg without timed pairs"):
            timed_gate.judge(timed + allocation_rows("probe", [168.0], [168.0]))
        with self.assertRaisesRegex(GateError, "belongs to baseline or candidate, got 'head'"):
            timed_gate.judge(timed + [dict(allocation_rows("scan", [1.0], [1.0])[0], arm="head")])

    def test_allocation_legs_do_not_enter_the_timed_pairs(self) -> None:
        rows = pair_rows("scan", shifted(QUIET)) + allocation_rows("scan", [168.0, 168.0], [168.0, 168.0])
        self.assertEqual(10, timed_gate.judge(rows).verdicts[0]["pairs"])
        self.assertTrue(timed_gate.run_may_stop(rows)[0])


class CommandLineTest(unittest.TestCase):

    def run_cli(self, rows: list[dict] | str, *arguments: str) -> tuple[int, str, str]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.jsonl"
            path.write_text(
                rows if isinstance(rows, str) else "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                status = timed_gate.main([arguments[0], str(path), *arguments[1:]])
            return status, out.getvalue(), err.getvalue()

    def test_judge_exit_status_is_zero_one_or_two(self) -> None:
        status, out, _ = self.run_cli(pair_rows("scan", shifted(QUIET)), "judge")
        self.assertEqual(0, status)
        self.assertIn("unchanged", out)
        status, out, _ = self.run_cli(pair_rows("scan", shifted(QUIET, math.log(1.06))), "judge")
        self.assertEqual(1, status)
        self.assertIn("outcome: FAIL", out)
        status, out, err = self.run_cli(pair_rows("scan", shifted(QUIET[:5])), "judge")
        self.assertEqual((2, ""), (status, out))
        self.assertIn("a verdict needs 10 to 20 accepted pairs", err)
        spaced = "\n\n".join(json.dumps(row) for row in pair_rows("scan", shifted(QUIET))) + "\n\n"
        self.assertEqual(0, self.run_cli(spaced, "judge")[0], "blank lines are not rows")
        status, _, err = self.run_cli("not json\n", "judge")
        self.assertEqual(2, status)
        self.assertIn("rows.jsonl:1: not a JSON object", err)
        status, _, err = self.run_cli("[1]\n", "judge")
        self.assertEqual(2, status)

    def test_the_script_exits_with_the_verdicts_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.jsonl"
            rows = pair_rows("scan", shifted(QUIET, math.log(1.06)))
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            script = subprocess.run(
                [sys.executable, "-B", str(Path(timed_gate.__file__)), "judge", str(path)],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual((1, True), (script.returncode, "outcome: FAIL" in script.stdout))
        with self.assertRaises(SystemExit) as usage, contextlib.redirect_stderr(io.StringIO()):
            timed_gate.main([])
        self.assertEqual(2, usage.exception.code, "a command is required")

    def test_judge_options_reach_the_analysis(self) -> None:
        rows = pair_rows("scan", shifted(QUIET + QUIET[:1])) + pair_rows(
            "probe", shifted(QUIET + QUIET[:1], math.log(1.06))
        )
        self.assertEqual(2, self.run_cli(rows, "judge")[0], "past the stop point")
        self.assertEqual(1, self.run_cli(rows, "judge", "--control")[0])
        self.assertEqual(0, self.run_cli(rows, "judge", "--control", "--benchmark", "scan")[0])
        self.assertEqual(0, self.run_cli(rows, "judge", "--min-pairs", "11", "--benchmark", "scan")[0])

    def test_stop_prints_the_decision_as_its_last_line(self) -> None:
        status, out, _ = self.run_cli(pair_rows("scan", shifted(QUIET)), "stop")
        self.assertEqual((0, "stop"), (status, out.splitlines()[-1]))
        status, out, _ = self.run_cli(pair_rows("scan", shifted(NOISY)), "stop")
        self.assertEqual((0, "continue"), (status, out.splitlines()[-1]))
        self.assertIn("pairs 10", out)
        status, out, _ = self.run_cli(pair_rows("scan", shifted(QUIET)), "stop", "--min-pairs", "12")
        self.assertEqual("continue", out.splitlines()[-1])
        self.assertEqual(2, self.run_cli(pair_rows("scan", shifted(QUIET)), "stop", "--min-pairs", "3")[0])


if __name__ == "__main__":
    unittest.main()
