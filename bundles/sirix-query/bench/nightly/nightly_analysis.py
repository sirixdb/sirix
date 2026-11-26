#!/usr/bin/env python3
"""The nightly tier's analysis: CONV-304 states and Apache Otava change points over one ledger.

For every nightly series the ledger holds, this module computes

    * the CONV-304 state of every recorded night (bench_trend.state_of), the normative judge, and
    * the change points Apache Otava (E-divisive means over the nights' levels) finds,

and merges the two into one list of *changes*, each with the nights and the commit range it lies
in. Otava is an advisory second opinion unless the specification says otherwise: a change that
only Otava reports is labelled so, and when the two disagree both answers are kept, never one
silently dropped. The result is a JSON document (`analysis.json`) that the dashboard and the issue
step both read, so they cannot disagree with each other.

    nightly_analysis.py --ledger FILE --out analysis.json [--tags FILE] [--require-otava]

A series is judged only where it may be: on a hosted runner the wall-clock series are analysed and
shown but never produce a change to act on (bench_trend.is_informational).
Needs the standard library; Otava (`apache-otava`, numpy, scipy) is imported when it is installed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import bench_ledger  # noqa: E402
import bench_trend  # noqa: E402
from bench_ledger import LedgerError  # noqa: E402
from bench_trend import Night, SeriesKey  # noqa: E402

SCHEMA = 1
OTAVA_P_VALUE = 0.001  # Otava's own default significance
OTAVA_WINDOW = 50  # Otava's default window of nights
OTAVA_MIN_NIGHTS = bench_trend.WINDOW_NIGHTS  # no opinion before the baseline CONV-304 needs exists
# A change is actionable once at least this many nights have been recorded at the new level, the
# same "two nights, not one" confirmation CONV-304 demands before it speaks of drift.
ACTIONABLE_NEW_NIGHTS = 2
# Two detectors that place a change within this many nights of each other found the same change.
MERGE_NIGHTS = 3
# Only changes this recent are reported for action; older ones are history on the dashboard.
RECENT_NIGHTS = 10

CONV_304 = "conv-304"
OTAVA = "otava"


class OtavaUnavailable(RuntimeError):
    """Apache Otava is not installed in this interpreter."""


@dataclass(frozen=True)
class Detection:
    """One detector's statement about one series."""

    detector: str
    direction: str  # bench_trend.UPWARD or DOWNWARD
    night: str  # the first night at the new level
    previous_night: str  # the last night at the old level
    commit: str  # the commit built on `night`
    previous_commit: str
    relative_change: float | None  # fraction; None when the old level was zero
    detail: str


def _percent(logratio: float) -> str:
    return f"{100.0 * math.expm1(logratio):+.2f}%"


def series_id(key: SeriesKey) -> str:
    """A stable short identity of a series, used in issue markers and dashboard anchors."""
    canonical = json.dumps(list(key), separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def require_otava() -> None:
    """Raise OtavaUnavailable unless Otava's change point analysis can be imported."""
    try:
        from otava.analysis import compute_change_points  # noqa: F401, PLC0415 - optional, heavy dependency
    except ImportError as error:
        raise OtavaUnavailable(f"apache-otava is not importable: {error}") from error


def otava_detect(levels: Sequence[float]) -> list[tuple[int, float | None, float, float, float]]:
    """(index of the first night at the new level, relative change, p-value, mean before, mean after).

    The relative change is None when the level before is zero. Raises OtavaUnavailable when Otava is
    not installed; a series too short for an opinion yields nothing.
    """
    if len(levels) < OTAVA_MIN_NIGHTS:
        return []
    require_otava()
    from otava.analysis import compute_change_points  # noqa: PLC0415 - optional, heavy dependency

    found, _weak = compute_change_points(
        [float(level) for level in levels], window_len=OTAVA_WINDOW, max_pvalue=OTAVA_P_VALUE, min_magnitude=0.0
    )
    changes = []
    for point in found:
        before, after = float(point.stats.mean_1), float(point.stats.mean_2)
        relative = None if before == 0 else after / before - 1.0
        changes.append((int(point.index), relative, float(point.stats.pvalue), before, after))
    return sorted(changes)


def _otava_detections(nights: Sequence[Night]) -> list[Detection]:
    detections = []
    for index, relative, pvalue, before, after in otava_detect([night.level for night in nights]):
        if index < 1:
            continue
        # The bound CONV-304 uses (2%) is the smallest change either detector is asked to report; Otava's
        # own significance decides whether it is real, this decides whether it is worth an investigation.
        if relative is not None and abs(relative) < math.expm1(bench_trend.DRIFT_BOUND):
            continue
        if relative is None and after == 0:
            continue
        direction = bench_trend.UPWARD if after > before else bench_trend.DOWNWARD
        shown = "from zero" if relative is None else f"{100 * relative:+.2f}%"
        detections.append(
            Detection(
                OTAVA,
                direction,
                nights[index].night,
                nights[index - 1].night,
                nights[index].commit,
                nights[index - 1].commit,
                relative,
                f"Otava change point: level {before:.6g} -> {after:.6g} ({shown}), p = {pvalue:.3g}",
            )
        )
    return detections


def _conv304_detections(nights: Sequence[Night], states: Sequence[bench_trend.NightState]) -> list[Detection]:
    """The unbroken runs of confirmed exceeding nights, each once, as the change CONV-304 reports."""
    detections: list[Detection] = []
    for index, state in enumerate(states):
        if state.state not in (bench_trend.DRIFT, bench_trend.FASTER) or state.since is None:
            continue
        upward = state.state == bench_trend.DRIFT
        first = next(i for i, night in enumerate(nights) if night.night == state.since.night) + 1
        direction = bench_trend.UPWARD if upward else bench_trend.DOWNWARD
        if any(item.night == nights[first].night and item.direction == direction for item in detections):
            continue  # a later night of the same unbroken run
        detections.append(
            Detection(
                CONV_304,
                direction,
                nights[first].night,
                state.since.night,
                nights[first].commit,
                state.since.commit,
                math.expm1(state.drift),
                f"CONV-304 {state.state}: drift {_percent(state.drift)} against a fence of {_percent(state.fence)}",
            )
        )
    return detections


def _merge(detections: Iterable[Detection], nights: Sequence[Night]) -> list[dict[str, Any]]:
    """Group detections of one direction whose change nights lie within MERGE_NIGHTS of each other."""
    position = {night.night: index for index, night in enumerate(nights)}
    groups: list[list[Detection]] = []
    for detection in sorted(detections, key=lambda item: (item.direction, position[item.night], item.detector)):
        for group in groups:
            if group[0].direction == detection.direction and abs(
                position[group[0].night] - position[detection.night]
            ) <= MERGE_NIGHTS:
                group.append(detection)
                break
        else:
            groups.append([detection])
    merged = []
    for group in groups:
        lead = next((item for item in group if item.detector == CONV_304), group[0])  # CONV-304 is the judge
        detectors = sorted({item.detector for item in group})
        last = len(nights) - 1
        merged.append(
            {
                "direction": lead.direction,
                "night": lead.night,
                "previous_night": lead.previous_night,
                "commit": lead.commit,
                "previous_commit": lead.previous_commit,
                "detectors": detectors,
                "agreement": "both" if len(detectors) == 2 else f"{detectors[0]}-only",
                "relative_change": lead.relative_change,
                "details": [item.detail for item in group],
                "nights_at_new_level": last - position[lead.night] + 1,
            }
        )
    return merged


def analyse_series(
    key: SeriesKey,
    nights: Sequence[Night],
    unit: str,
    *,
    use_otava: bool,
    images: Mapping[str, str | None] | None = None,
) -> dict[str, Any]:
    states = [bench_trend.state_of(nights, index) for index in range(len(nights))]
    detections = _conv304_detections(nights, states)
    if use_otava:
        detections += _otava_detections(nights)
    conditions, kit, benchmark, params, metric, binary, runtime, runner = key
    informational = bench_trend.is_informational(key)
    return {
        "id": series_id(key),
        "name": bench_trend.describe(key),
        "kit": kit,
        "benchmark": benchmark,
        "params": dict(params),
        "metric": metric,
        "unit": unit,
        "conditions": conditions,
        "binary": binary,
        "runtime": runtime,
        "runner": runner,
        "informational": informational,
        "nights": [
            {
                "night": night.night,
                "run": night.run,
                "commit": night.commit,
                "level": night.level,
                "processes": night.processes,
                "image": (images or {}).get(night.run),
                "state": state.state,
                "drift": state.drift,
                "fence": state.fence,
            }
            for night, state in zip(nights, states)
        ],
        "changes": _merge(detections, nights),
    }


def units_of(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str, str], str]:
    """The unit each (kit, benchmark, metric) is recorded in, from the measurement rows."""
    runs = {row["run"]: row["kit"] for row in rows if row["kind"] == "run"}
    return {
        (runs[row["run"]], row["benchmark"], row["metric"]): row["unit"]
        for row in rows
        if row["kind"] == "measurement" and row["run"] in runs
    }


def analyse(
    rows: Sequence[Mapping[str, Any]],
    *,
    releases: Sequence[Mapping[str, str]] = (),
    use_otava: bool = True,
) -> dict[str, Any]:
    """Every nightly series of a ledger analysed. Raises OtavaUnavailable when use_otava and it is missing."""
    if use_otava:
        require_otava()  # fail on a missing installation before any work, not mid-series
    units = units_of(rows)
    images = {row["run"]: (row.get("runner") or {}).get("image") for row in rows if row["kind"] == "run"}
    series = bench_trend.nightly_series(rows)
    analysed = [
        analyse_series(key, nights, units.get((key[1], key[2], key[4]), ""), use_otava=use_otava, images=images)
        for key, nights in sorted(series.items())
    ]
    nights = sorted({entry["night"] for item in analysed for entry in item["nights"]})
    return {
        "schema": SCHEMA,
        "first_night": nights[0] if nights else None,
        "latest_night": nights[-1] if nights else None,
        "otava": use_otava,
        "series": analysed,
        "releases": [dict(release) for release in releases],
    }


def actionable(analysis: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The upward changes of judged series that deserve an issue now, with the series they belong to.

    A change qualifies when it lies in a series that is judged (not informational), points upward,
    is recent, and is confirmed: CONV-304 reports it as drift, or Otava sees at least
    ACTIONABLE_NEW_NIGHTS nights at the new level. The latest night a series was recorded on is the
    one that counts as now.
    """
    found = []
    for series in analysis["series"]:
        if series["informational"] or not series["nights"]:
            continue
        for change in series["changes"]:
            confirmed = CONV_304 in change["detectors"] or change["nights_at_new_level"] >= ACTIONABLE_NEW_NIGHTS
            if change["direction"] == bench_trend.UPWARD and confirmed and change["nights_at_new_level"] <= RECENT_NIGHTS:
                found.append({"series": {key: value for key, value in series.items() if key != "nights"}, **change})
    return found


def read_releases(path: Path | None) -> list[dict[str, str]]:
    """`<tag> <YYYY-MM-DD>` per line, as `git for-each-ref --format='%(refname:short) %(creatordate:short)'` prints."""
    if path is None:
        return []
    releases = []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].startswith("sirix-") and parts[0][6:7].isdigit():
            releases.append({"tag": parts[0], "date": parts[1]})
    return sorted(releases, key=lambda release: release["date"])


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CONV-304 and Otava analysis of the nightly ledger.")
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--tags", type=Path, help="git tags, one `<tag> <date>` per line")
    parser.add_argument("--require-otava", action="store_true", help="fail when apache-otava is not installed")
    args = parser.parse_args(argv)
    try:
        rows = bench_ledger.read_ledger(args.ledger)
        try:
            analysis = analyse(rows, releases=read_releases(args.tags), use_otava=True)
        except OtavaUnavailable as error:
            if args.require_otava:
                raise
            print(f"nightly_analysis.py: {error}; continuing with CONV-304 alone", file=sys.stderr)
            analysis = analyse(rows, releases=read_releases(args.tags), use_otava=False)
    except (bench_trend.TrendError, LedgerError, OtavaUnavailable, OSError) as error:
        print(f"nightly_analysis.py: {error}", file=sys.stderr)
        return 2
    args.out.write_text(json.dumps(analysis, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"analysed {len(analysis['series'])} series, latest night {analysis['latest_night']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
