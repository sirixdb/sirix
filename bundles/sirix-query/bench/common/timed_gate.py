#!/usr/bin/env python3
"""Timed acceptance verdict of the benchmark protocol (spec/CONV-conventions.md, CONV-302).

A timed comparison is judged from alternating fresh-process pairs. For one benchmark, pair i holds a
baseline measurement a_i and a candidate measurement b_i. With r_i = ln(b_i / a_i):

    difference  d = mean(r_i)
    threshold   T = the 0.99 quantile of |mean(s_i * e_i)| over all 2^n sign assignments s, where
                    e_i = (r_i - d) * sqrt(n / (n - 1))

A sign assignment swaps the two arm labels within the pairs, so T is how far the paired difference
moves by spread alone. The mean is removed first, which makes T blind to the sign and the size of the
difference: scaling one arm by a constant leaves it unchanged. That is what allows the stop rule to
read thresholds without reading results.

    regression   d > FLOOR and d > T
    improvement  -d > FLOOR and -d > T
    unstable     neither, and T > FLOOR  (the run could not have seen a slowdown the size of the floor)
    unchanged    otherwise

The module is pure analysis over measurement rows (the ledger's row shape, CONV-303); bench_ledger.py
owns files, the environment capture and the ledger. Standard library only.

    timed_gate.py stop  ROWS.jsonl [--min-pairs N] [--benchmark NAME ...]
    timed_gate.py judge ROWS.jsonl [--min-pairs N] [--benchmark NAME ...] [--control]

Exit status of judge: 0 no regression, 1 a regression or a failed allocation check, 2 invalid input.
"""

from __future__ import annotations

import argparse
import functools
import heapq
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

# The smallest slowdown the per-change gate is asked to stop: 4%, as a natural-log ratio.
FLOOR = math.log(1.04)
# A benchmark may stop before MAX_PAIRS once its threshold is at most 3%.
STOP_PRECISION = math.log(1.03)
# Below 8 pairs the 0.99 quantile of the relabelings is their maximum; 10 is the gate's minimum.
MIN_PAIRS = 10
# All 2^20 relabelings are still enumerated exactly, so the threshold needs no random numbers.
MAX_PAIRS = 20
QUANTILE_PERCENT = 99

ALLOCATION_METRIC = "alloc_bytes_per_op"
BASELINE = "baseline"
CANDIDATE = "candidate"

REGRESSION = "regression"
IMPROVEMENT = "improvement"
UNSTABLE = "unstable"
UNCHANGED = "unchanged"

SeriesKey = tuple[str, tuple[tuple[str, str], ...], str]


class GateError(ValueError):
    """The input cannot be judged; no verdict is produced from the rest of it."""


def log_ratios(pairs: Sequence[tuple[float, float]]) -> list[float]:
    """ln(candidate / baseline) for every (baseline, candidate) pair."""
    ratios = []
    for index, (baseline, candidate) in enumerate(pairs, start=1):
        for arm, value in ((BASELINE, baseline), (CANDIDATE, candidate)):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise GateError(f"pair {index}: the {arm} measurement is not a number: {value!r}")
            if not math.isfinite(value) or value <= 0:
                raise GateError(
                    f"pair {index}: the {arm} measurement must be finite and greater than zero, got {value!r}"
                )
        ratios.append(math.log(candidate / baseline))
    return ratios


def _signed_sums(values: Sequence[float]) -> list[float]:
    """Every sum of the values under all sign assignments, 2^len(values) of them."""
    sums = [0.0]
    for value in values:
        sums = [total + value for total in sums] + [total - value for total in sums]
    return sums


def difference_and_threshold(ratios: Sequence[float]) -> tuple[float, float]:
    """The paired difference d and the noise threshold T of CONV-302, both as natural-log ratios."""
    count = len(ratios)
    if count < 2:
        raise GateError(f"a threshold needs at least 2 pairs, got {count}")
    if count > MAX_PAIRS:
        raise GateError(f"a threshold is defined for at most {MAX_PAIRS} pairs, got {count}")
    return _difference_and_threshold(tuple(ratios))


@functools.lru_cache(maxsize=512)
def _difference_and_threshold(ratios: tuple[float, ...]) -> tuple[float, float]:
    count = len(ratios)
    difference = math.fsum(ratios) / count
    scale = math.sqrt(count / (count - 1))
    residuals = [(ratio - difference) * scale for ratio in ratios]
    # An assignment and its negation give the same absolute sum, so the last sign stays positive
    # and every value below stands for two of the 2^n assignments. The two halves are summed
    # separately and combined once, which keeps each sum within a few additions of exact.
    split = count // 2
    low = _signed_sums(residuals[:split])
    high = [total + residuals[-1] for total in _signed_sums(residuals[split:-1])]
    values = [abs(left + right) for right in high for left in low]
    assignments = 1 << count
    rank = assignments - assignments * (100 - QUANTILE_PERCENT) // 100  # ceil(0.99 * 2^n), 1-based
    half_rank = (rank + 1) // 2  # 1-based, among the half enumerated here
    threshold = heapq.nlargest(len(values) - half_rank + 1, values)[-1] / count
    return difference, threshold


def classify(difference: float, threshold: float) -> str:
    """The verdict for one benchmark."""
    if difference > FLOOR and difference > threshold:
        return REGRESSION
    if -difference > FLOOR and -difference > threshold:
        return IMPROVEMENT
    if threshold > FLOOR:
        return UNSTABLE
    return UNCHANGED


def percent(log_ratio: float) -> float:
    """A natural-log ratio as a percentage change."""
    return 100.0 * math.expm1(log_ratio)


def allocation_median(values: Sequence[float]) -> float:
    """An arm's allocation figure: the unrounded median of its accepted legs."""
    if not values:
        raise GateError("an allocation figure needs at least one measurement")
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise GateError(f"an allocation measurement must be finite and not negative, got {value!r}")
    return median(values)


@dataclass(frozen=True)
class Series:
    """The pairs of one judged benchmark, parameter set and metric."""

    benchmark: str
    params: dict[str, str]
    metric: str
    unit: str
    accepted: list[tuple[int, float, float]]  # (pair, baseline, candidate), in pair order
    attempted: int

    @property
    def label(self) -> str:
        return series_label(self.benchmark, self.params, self.metric)

    def ratios(self) -> list[float]:
        return log_ratios([(baseline, candidate) for _, baseline, candidate in self.accepted])


def _key(row: Mapping[str, Any]) -> SeriesKey:
    params = row.get("params") or {}
    if not isinstance(params, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in params.items()):
        raise GateError(f"{row.get('benchmark')!r}: params must map strings to strings")
    return str(row["benchmark"]), tuple(sorted(params.items())), str(row["metric"])


def series_label(benchmark: str, params: Mapping[str, str], metric: str) -> str:
    """`scan{keyBytes=8} ns_per_op`: how a series is named in reports and messages."""
    rendered = ",".join(f"{name}={value}" for name, value in sorted(params.items()))
    return f"{benchmark}{'{' + rendered + '}' if rendered else ''} {metric}"


def _describe(key: SeriesKey) -> str:
    return series_label(key[0], dict(key[1]), key[2])


def _selected(
    rows: Iterable[Mapping[str, Any]], benchmarks: Sequence[str] | None
) -> tuple[list[Mapping[str, Any]], list[str]]:
    """The rows of the plan's judged set and the names measured outside it."""
    wanted = None if benchmarks is None else set(benchmarks)
    kept, outside = [], set()
    for row in rows:
        if row.get("kind", "measurement") != "measurement":
            continue
        for field in ("benchmark", "metric", "arm", "value"):
            if field not in row:
                raise GateError(f"a measurement row lacks '{field}': {dict(row)!r}")
        if wanted is not None and row["benchmark"] not in wanted:
            outside.add(str(row["benchmark"]))
            continue
        kept.append(row)
    if wanted is not None:
        missing = sorted(wanted - {str(row["benchmark"]) for row in kept})
        if missing:
            raise GateError(f"the plan judges {', '.join(missing)}, but the run holds no measurement of it")
    return kept, sorted(outside)


def collect_series(
    rows: Iterable[Mapping[str, Any]], benchmarks: Sequence[str] | None = None
) -> tuple[list[Series], list[str]]:
    """Group timed measurement rows into paired series; also return benchmarks measured outside the plan."""
    kept, outside = _selected(rows, benchmarks)
    cells: dict[SeriesKey, dict[int, dict[str, Mapping[str, Any]]]] = {}
    units: dict[SeriesKey, str] = {}
    for row in kept:
        if row["metric"] == ALLOCATION_METRIC:
            continue
        key = _key(row)
        arm, pair = row["arm"], row.get("pair")
        if arm not in (BASELINE, CANDIDATE):
            raise GateError(f"{_describe(key)}: a paired comparison has the arms baseline and candidate, got {arm!r}")
        if isinstance(pair, bool) or not isinstance(pair, int) or pair < 1:
            raise GateError(f"{_describe(key)}: a timed measurement needs a pair number of 1 or more, got {pair!r}")
        arms = cells.setdefault(key, {}).setdefault(pair, {})
        if arm in arms:
            raise GateError(f"{_describe(key)}: pair {pair} holds two {arm} measurements")
        arms[arm] = row
        unit = str(row.get("unit", ""))
        if units.setdefault(key, unit) != unit:
            raise GateError(f"{_describe(key)}: the unit changes within the run ({units[key]!r} and {unit!r})")
    series = [
        Series(key[0], dict(key[1]), key[2], units[key], _accepted_pairs(key, cells[key]), len(cells[key]))
        for key in sorted(cells)
    ]
    return series, outside


def _accepted_pairs(
    key: SeriesKey, pairs: Mapping[int, Mapping[str, Mapping[str, Any]]]
) -> list[tuple[int, float, float]]:
    """The accepted pairs of one series in pair order. A pair is accepted when both of its measurements are."""
    accepted = []
    for pair in sorted(pairs):
        arms = pairs[pair]
        for arm in (BASELINE, CANDIDATE):
            if arm not in arms:
                raise GateError(f"{_describe(key)}: pair {pair} has no {arm} measurement")
        # Validate every pair, rejected ones too: a broken measurement is never silently dropped.
        log_ratios([(arms[BASELINE]["value"], arms[CANDIDATE]["value"])])
        if all(arms[arm].get("accepted", True) is True for arm in (BASELINE, CANDIDATE)):
            accepted.append((pair, float(arms[BASELINE]["value"]), float(arms[CANDIDATE]["value"])))
    return accepted


def collect_allocation(
    rows: Iterable[Mapping[str, Any]], benchmarks: Sequence[str] | None = None
) -> dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]]:
    """Per benchmark and parameters: rounded arm figures, rounded median delta and whether it passes."""
    kept, _ = _selected(rows, None)
    wanted = None if benchmarks is None else set(benchmarks)
    legs: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, list[float]]] = {}
    for row in kept:
        if row["metric"] != ALLOCATION_METRIC or (wanted is not None and row["benchmark"] not in wanted):
            continue
        key = _key(row)
        if row["arm"] not in (BASELINE, CANDIDATE):
            raise GateError(f"{_describe(key)}: an allocation leg belongs to baseline or candidate, got {row['arm']!r}")
        accepted = legs.setdefault(key[:2], {}).setdefault(row["arm"], [])
        if row.get("accepted", True) is True:
            accepted.append(row["value"])
    checks = {}
    for key in sorted(legs):
        for arm in (BASELINE, CANDIDATE):
            if not legs[key].get(arm):
                raise GateError(f"{_describe((key[0], key[1], ALLOCATION_METRIC))}: no accepted {arm} allocation leg")
        baseline = allocation_median(legs[key][BASELINE])
        candidate = allocation_median(legs[key][CANDIDATE])
        delta = round(candidate - baseline)
        checks[key] = {
            "baseline_bytes": round(baseline),
            "candidate_bytes": round(candidate),
            "delta_bytes": delta,
            "pass": delta <= 0,
        }
    return checks


@dataclass(frozen=True)
class StopState:
    """Where one series stands against its stop condition."""

    series: Series
    pairs: int
    threshold: float | None  # None below 2 pairs or once the pair limit made it irrelevant
    met: bool


def _minimum_pairs(min_pairs: int) -> int:
    if isinstance(min_pairs, bool) or not isinstance(min_pairs, int) or not MIN_PAIRS <= min_pairs <= MAX_PAIRS:
        raise GateError(f"the plan's minimum number of pairs must be {MIN_PAIRS} to {MAX_PAIRS}, got {min_pairs!r}")
    return min_pairs


def stop_states(series: Sequence[Series], min_pairs: int = MIN_PAIRS) -> list[StopState]:
    """Per series: has it at least min_pairs accepted pairs and a threshold within the stop precision, or MAX_PAIRS?"""
    minimum = _minimum_pairs(min_pairs)
    states = []
    for item in series:
        pairs = len(item.accepted)
        if pairs > MAX_PAIRS:
            raise GateError(f"{item.label}: {pairs} accepted pairs, more than the limit of {MAX_PAIRS}")
        threshold = difference_and_threshold(item.ratios())[1] if 2 <= pairs else None
        met = pairs == MAX_PAIRS or (pairs >= minimum and threshold is not None and threshold <= STOP_PRECISION)
        states.append(StopState(item, pairs, threshold, met))
    return states


def run_may_stop(
    rows: Iterable[Mapping[str, Any]], *, benchmarks: Sequence[str] | None = None, min_pairs: int = MIN_PAIRS
) -> tuple[bool, list[StopState]]:
    """Whether every judged series has met its stop condition. Reads pair counts and thresholds only."""
    series, _ = collect_series(rows, benchmarks)
    if not series:
        return False, []
    states = stop_states(series, min_pairs)
    return all(state.met for state in states), states


@dataclass(frozen=True)
class Judgement:
    """The verdicts of one run."""

    verdicts: list[dict[str, Any]]  # verdict rows without the run's identity
    outside_plan: list[str]

    @property
    def failed(self) -> bool:
        return any(
            row["verdict"] == REGRESSION or (row["allocation"] is not None and not row["allocation"]["pass"])
            for row in self.verdicts
        )

    @property
    def unstable(self) -> list[dict[str, Any]]:
        return [row for row in self.verdicts if row["verdict"] == UNSTABLE]


def _require_stop_point(
    rows: Sequence[Mapping[str, Any]], series: Sequence[Series], minimum: int, benchmarks: Sequence[str] | None
) -> None:
    """Refuse a run that did not end exactly where the stop rule ends it."""
    pending = [state for state in stop_states(series, minimum) if not state.met]
    if pending:
        state = pending[0]
        raise GateError(
            f"{state.series.label}: the run ended before its stop condition ({state.pairs} accepted pairs, "
            f"threshold {percent(state.threshold):.2f}%, needs {minimum} pairs within "
            f"{percent(STOP_PRECISION):.0f}% or {MAX_PAIRS} pairs)"
        )
    kept, _ = _selected(rows, benchmarks)
    boundaries = sorted({row["pair"] for row in kept if row["metric"] != ALLOCATION_METRIC})
    for boundary in boundaries[minimum - 1 : -1]:
        earlier, _ = collect_series([row for row in kept if row.get("pair") is None or row["pair"] <= boundary])
        if len(earlier) == len(series) and all(state.met for state in stop_states(earlier, minimum)):
            raise GateError(
                "the run continued past its stop point: every benchmark had met its stop condition "
                "before the last pair was run"
            )


def judge(
    rows: Iterable[Mapping[str, Any]],
    *,
    benchmarks: Sequence[str] | None = None,
    min_pairs: int = MIN_PAIRS,
    enforce_stop_rule: bool = True,
) -> Judgement:
    """Judge every series of a run once, from all its accepted pairs.

    With enforce_stop_rule the run must have ended exactly where the stop rule ends it: every series
    has met its stop condition, and that was not yet so at any earlier pair boundary. A control run
    with a pair count fixed by its plan passes False.
    """
    rows = list(rows)
    series, outside = collect_series(rows, benchmarks)
    if not series:
        raise GateError("the run holds no timed pair to judge")
    minimum = _minimum_pairs(min_pairs)
    for item in series:
        pairs = len(item.accepted)
        if pairs < MIN_PAIRS or pairs > MAX_PAIRS:
            raise GateError(
                f"{item.label}: a verdict needs {MIN_PAIRS} to {MAX_PAIRS} accepted pairs, "
                f"the run has {pairs} of {item.attempted} attempted"
            )
    if enforce_stop_rule:
        _require_stop_point(rows, series, minimum, benchmarks)
    allocation = collect_allocation(rows, benchmarks)
    verdicts = []
    for item in series:
        difference, threshold = difference_and_threshold(item.ratios())
        verdicts.append(
            {
                "benchmark": item.benchmark,
                "params": item.params,
                "metric": item.metric,
                "unit": item.unit,
                "pairs": len(item.accepted),
                "difference": difference,
                "threshold": threshold,
                "verdict": classify(difference, threshold),
                "allocation": allocation.get((item.benchmark, tuple(sorted(item.params.items())))),
            }
        )
    judged = {(row["benchmark"], tuple(sorted(row["params"].items()))) for row in verdicts}
    stray = sorted(_describe((key[0], key[1], ALLOCATION_METRIC)) for key in allocation if key not in judged)
    if stray:
        raise GateError(f"{stray[0]}: an allocation leg without timed pairs of the same benchmark")
    return Judgement(verdicts, outside)


def _verdict_line(row: Mapping[str, Any]) -> str:
    label = series_label(row["benchmark"], row["params"], row["metric"])
    check = row["allocation"]
    allocation = (
        "-"
        if check is None
        else (
            f"{check['baseline_bytes']} -> {check['candidate_bytes']} B/op "
            f"delta {check['delta_bytes']:+d} {'pass' if check['pass'] else 'FAIL'}"
        )
    )
    return (
        f"{label:<58} {row['pairs']:>5} {percent(row['difference']):>+9.2f}% "
        f"{percent(row['threshold']):>8.2f}%  {row['verdict']:<11} {allocation}"
    )


def format_report(judgement: Judgement) -> str:
    """The run's verdict table and its outcome line."""
    lines = [f"{'benchmark':<58} {'pairs':>5} {'difference':>10} {'threshold':>9}  {'verdict':<11} allocation"]
    lines.extend(_verdict_line(row) for row in judgement.verdicts)
    counts = {
        name: sum(1 for row in judgement.verdicts if row["verdict"] == name)
        for name in (REGRESSION, IMPROVEMENT, UNCHANGED, UNSTABLE)
    }
    failed_allocation = len([row for row in judgement.verdicts if row["allocation"] and not row["allocation"]["pass"]])
    lines.append("")
    lines.append(
        f"floor {percent(FLOOR):.0f}%, threshold = 0.99 quantile of the relabelled pairs; "
        f"judged {counts[UNCHANGED] + counts[IMPROVEMENT] + counts[REGRESSION]}, "
        f"not judged (unstable) {counts[UNSTABLE]}"
    )
    if judgement.failed:
        lines.append(
            f"outcome: FAIL - {counts[REGRESSION]} regression(s), {failed_allocation} failed allocation check(s)"
        )
    else:
        lines.append(f"outcome: no regression in {counts[UNCHANGED] + counts[IMPROVEMENT]} judged benchmark(s)")
    for row in judgement.unstable:
        lines.append(
            f"not judged: {row['benchmark']} {row['metric']} is unstable (threshold "
            f"{percent(row['threshold']):.2f}% is above the floor); it needs a fixture issue, "
            f"and it is no evidence that the change is free"
        )
    if judgement.outside_plan:
        lines.append(f"measured outside the plan's judged set: {', '.join(judgement.outside_plan)}")
    return "\n".join(lines)


def format_stop(may_stop: bool, states: Sequence[StopState]) -> str:
    lines = []
    for state in states:
        threshold = "-" if state.threshold is None else f"{percent(state.threshold):.2f}%"
        lines.append(
            f"{state.series.label:<58} pairs {state.pairs:>2}  threshold {threshold:>7}  "
            f"{'met' if state.met else 'continue'}"
        )
    lines.append("stop" if may_stop else "continue")
    return "\n".join(lines)


def read_rows(path: Path) -> list[dict[str, Any]]:
    """Measurement rows from a JSON Lines file."""
    rows = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise GateError(f"{path}:{number}: not a JSON object: {error.msg}") from error
        if not isinstance(row, dict):
            raise GateError(f"{path}:{number}: not a JSON object")
        rows.append(row)
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Timed acceptance verdict (CONV-302).")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("stop", "judge"):
        command = commands.add_parser(name)
        command.add_argument("rows", type=Path, help="measurement rows, JSON Lines")
        command.add_argument("--min-pairs", type=int, default=MIN_PAIRS)
        command.add_argument("--benchmark", action="append", help="judged benchmark; repeat. Default: all measured")
        if name == "judge":
            command.add_argument(
                "--control",
                action="store_true",
                help="a control run with a fixed pair count: do not enforce the stop rule",
            )
    args = parser.parse_args(argv)
    try:
        rows = read_rows(args.rows)
        if args.command == "stop":
            may_stop, states = run_may_stop(rows, benchmarks=args.benchmark, min_pairs=args.min_pairs)
            print(format_stop(may_stop, states))
            return 0
        judgement = judge(rows, benchmarks=args.benchmark, min_pairs=args.min_pairs, enforce_stop_rule=not args.control)
    except (GateError, OSError) as error:
        print(f"timed_gate.py: {error}", file=sys.stderr)
        return 2
    print(format_report(judgement))
    return 1 if judgement.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
