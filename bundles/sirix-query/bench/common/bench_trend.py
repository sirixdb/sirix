#!/usr/bin/env python3
"""Nightly trend tier of the benchmark protocol (spec/CONV-conventions.md, CONV-304).

Reads the results ledger and reports, per series, whether the latest night drifted against the
nights before it. A series is the accepted nightly measurements that share conditions id, kit,
benchmark, params, metric, binary kind and runtime; a night's level is their median.

    window    the 14 most recent earlier recorded nights
    baseline  the median level of the 7 oldest nights of the window
    drift     D = ln(level / baseline)
    fence     Q3 + 1.5 * (Q3 - Q1) of the absolute log ratios of adjacent nights of the window
    exceeds   upward when D > ln 1.02 and D > fence; downward when -D > ln 1.02 and -D > fence

A night that exceeds is `unconfirmed`; when the previous recorded night exceeded the same way it is
`drift` (or `faster`). The tier reports and alarms; it never passes or fails a change.

    bench_trend.py report [--ledger FILE] [--night YYYY-MM-DD]

Exit status: 0 no confirmed drift, 1 at least one series in `drift`,
2 invalid input or no usable series for the requested night.
Standard library only.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

import bench_ledger
from bench_ledger import LedgerError

DRIFT_BOUND = math.log(1.02)
WINDOW_NIGHTS = 14
BASELINE_NIGHTS = 7
FENCE_FACTOR = 1.5
MIN_PROCESSES = bench_ledger.NIGHTLY_MIN_PROCESSES

INSUFFICIENT = "insufficient-history"
DRIFT = "drift"
UNCONFIRMED = "unconfirmed"
FASTER = "faster"
FASTER_UNCONFIRMED = "faster-unconfirmed"
UNSTABLE = "unstable"
STEADY = "steady"

UPWARD = "upward"
DOWNWARD = "downward"

SeriesKey = tuple[str, str, str, tuple[tuple[str, str], ...], str, str, str]


class TrendError(ValueError):
    """The ledger cannot be read as a nightly history."""


@dataclass(frozen=True)
class Night:
    """One recorded night of a series."""

    night: str
    run: str
    commit: str
    level: float
    processes: int


@dataclass(frozen=True)
class NightState:
    """A series' state on one night."""

    night: Night
    state: str
    drift: float | None  # D, None with insufficient history
    fence: float | None
    since: Night | None  # the night before the first of the unbroken run of exceeding nights


def quartile(values: Sequence[float], fraction: float) -> float:
    """A quantile by linear interpolation between order statistics."""
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (position - lower) * (ordered[upper] - ordered[lower])


def describe(key: SeriesKey) -> str:
    conditions, kit, benchmark, params, metric, binary, _runtime = key
    rendered = ",".join(f"{name}={value}" for name, value in params)
    return f"{kit} {benchmark}{'{' + rendered + '}' if rendered else ''} {metric} [{binary}, {conditions}]"


def nightly_series(rows: Iterable[Mapping[str, Any]]) -> dict[SeriesKey, list[Night]]:
    """The recorded nights of every nightly series in a ledger, oldest first."""
    runs: dict[str, Mapping[str, Any]] = {}
    values: dict[SeriesKey, dict[str, dict[str, list[float]]]] = {}
    for row in rows:
        if row["kind"] == "run" and row["tier"] == "nightly":
            runs[row["run"]] = row
    for row in rows:
        if row["kind"] != "measurement" or row["run"] not in runs or not row["accepted"]:
            continue
        run = runs[row["run"]]
        head = run["arms"][bench_ledger.HEAD]
        key = (
            run["conditions"],
            run["kit"],
            row["benchmark"],
            tuple(sorted(row["params"].items())),
            row["metric"],
            head["binary"],
            head["runtime"],
        )
        values.setdefault(key, {}).setdefault(run["night"], {}).setdefault(row["run"], []).append(row["value"])
    series: dict[SeriesKey, list[Night]] = {}
    for key, nights in values.items():
        recorded = []
        for night in sorted(nights):
            if len(nights[night]) > 1:
                raise TrendError(
                    f"{describe(key)}: night {night} has accepted measurements from more than one run "
                    f"({', '.join(sorted(nights[night]))})"
                )
            ((run_id, measured),) = nights[night].items()
            if len(measured) < MIN_PROCESSES:
                continue  # fewer than the tier's processes: not a recorded night of this series
            recorded.append(
                Night(night, run_id, runs[run_id]["arms"][bench_ledger.HEAD]["commit"], median(measured), len(measured))
            )
        if recorded:
            series[key] = recorded
    return series


def _exceeds(nights: Sequence[Night], index: int) -> tuple[str | None, float, float] | None:
    """(UPWARD, DOWNWARD or None, drift, fence) for a night; None when it has too little history."""
    if index < WINDOW_NIGHTS:
        return None
    window = nights[index - WINDOW_NIGHTS : index]
    baseline = median(night.level for night in window[:BASELINE_NIGHTS])
    drift = math.log(nights[index].level / baseline)
    changes = [abs(math.log(later.level / earlier.level)) for earlier, later in zip(window, window[1:])]
    first, third = quartile(changes, 0.25), quartile(changes, 0.75)
    fence = third + FENCE_FACTOR * (third - first)
    if _beyond(drift, fence):
        return UPWARD, drift, fence
    if _beyond(-drift, fence):
        return DOWNWARD, drift, fence
    return None, drift, fence


def _beyond(change: float, fence: float) -> bool:
    """Above the drift bound and above the series' own night-to-night noise."""
    return change > DRIFT_BOUND and change > fence


def state_of(nights: Sequence[Night], index: int) -> NightState:
    """The state of one night of a series, judged on its own window."""
    judged = _exceeds(nights, index)
    if judged is None:
        return NightState(nights[index], INSUFFICIENT, None, None, None)
    direction, drift, fence = judged
    if direction is None:
        return NightState(nights[index], UNSTABLE if fence > DRIFT_BOUND else STEADY, drift, fence, None)
    # Walk back over the unbroken run of nights that exceed the same way. It ends at the latest where
    # the history becomes too short to judge a night.
    first = index
    while (earlier := _exceeds(nights, first - 1)) is not None and earlier[0] == direction:
        first -= 1
    confirmed, unconfirmed = (DRIFT, UNCONFIRMED) if direction == UPWARD else (FASTER, FASTER_UNCONFIRMED)
    if first == index:
        return NightState(nights[index], unconfirmed, drift, fence, None)
    return NightState(nights[index], confirmed, drift, fence, nights[first - 1])


@dataclass(frozen=True)
class TrendReport:
    night: str
    states: dict[SeriesKey, NightState]
    missing: list[SeriesKey]  # series with a history and nothing recorded for this night

    @property
    def drifting(self) -> list[SeriesKey]:
        return [key for key, state in self.states.items() if state.state == DRIFT]


def report(rows: Iterable[Mapping[str, Any]], night: str | None = None) -> TrendReport:
    """Every series' state on one night; by default the latest night the ledger holds."""
    rows = list(rows)
    run_nights = [row["night"] for row in rows if row["kind"] == "run" and row["tier"] == "nightly"]
    if not run_nights:
        raise TrendError("the ledger holds no recorded nightly night")
    night = night or max(run_nights)
    series = nightly_series(rows)
    states, missing = {}, []
    for key in sorted(series):
        nights = series[key]
        index = next((position for position, entry in enumerate(nights) if entry.night == night), None)
        if index is None:
            if nights[0].night < night:
                missing.append(key)
            continue
        states[key] = state_of(nights, index)
    if not states:
        raise TrendError(f"no series has a recorded night {night}")
    return TrendReport(night, states, missing)


def _percent(value: float, sign: str = "+") -> str:
    """A natural-log ratio as a percentage change."""
    return f"{100.0 * math.expm1(value):{sign}.2f}%"


def format_report(trend: TrendReport) -> str:
    counts: dict[str, int] = {}
    lines = [f"nightly trend for {trend.night}"]
    for key, state in trend.states.items():
        counts[state.state] = counts.get(state.state, 0) + 1
        if state.state == STEADY:
            continue
        line = f"{state.state:<20} {describe(key)}"
        if state.drift is not None:
            line += f": drift {_percent(state.drift)}, fence {_percent(state.fence, '')}"
        line += f", commit {state.night.commit[:12]}"
        if state.since is not None:
            line += f", unbroken since the night after {state.since.night} (commit {state.since.commit[:12]})"
        lines.append(line)
    for key in trend.missing:
        lines.append(f"{'not measured':<20} {describe(key)}: it has earlier nights and none for {trend.night}")
    lines.append(
        "; ".join(
            f"{name} {counts[name]}"
            for name in (DRIFT, UNCONFIRMED, FASTER, FASTER_UNCONFIRMED, UNSTABLE, INSUFFICIENT, STEADY)
            if name in counts
        )
    )
    if trend.drifting:
        lines.append(
            "alarm: confirmed drift; investigate and record the cause in an issue. This is not a verdict on a change."
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Nightly trend tier (CONV-304).")
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("report")
    command.add_argument("--ledger", type=Path, help=f"default: ${bench_ledger.LEDGER_ENV}")
    command.add_argument("--night", help="the night to report, YYYY-MM-DD; default: the latest in the ledger")
    args = parser.parse_args(argv)
    try:
        ledger = args.ledger or (
            Path(os.environ[bench_ledger.LEDGER_ENV]) if os.environ.get(bench_ledger.LEDGER_ENV) else None
        )
        if ledger is None:
            raise TrendError(f"no ledger: pass --ledger or set {bench_ledger.LEDGER_ENV}")
        trend = report(bench_ledger.read_ledger(ledger), args.night)
    except (TrendError, LedgerError, OSError) as error:
        print(f"bench_trend.py: {error}", file=sys.stderr)
        return 2
    print(format_report(trend))
    return 1 if trend.drifting else 0


if __name__ == "__main__":
    raise SystemExit(main())
