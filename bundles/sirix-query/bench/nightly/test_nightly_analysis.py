#!/usr/bin/env python3
"""Behaviour tests of the nightly analysis (nightly_analysis.py): CONV-304 states and Apache Otava
change points merged into changes, what is actionable, and what a hosted runner never judges."""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import bench_ledger  # noqa: E402
import bench_trend  # noqa: E402
import nightly_analysis  # noqa: E402
from nightly_analysis import OtavaUnavailable  # noqa: E402
from test_bench_ledger import FakeRunner, fake_environment  # noqa: E402

FIRST_NIGHT = datetime.date(2026, 12, 1)
RIPPLE = [0.001, -0.002, 0.0015, -0.0005, 0.002, -0.001, 0.0]
HOSTED = {"kind": "hosted", "provider": "github", "label": "ubuntu-24.04"}
CPU, ALLOC, WALL = "cpu_ns_per_op", "alloc_bytes_per_op", "ns_per_op"
UNITS = {CPU: "ns/op", WALL: "ns/op", ALLOC: "B/op"}


def night_name(index: int) -> str:
    return (FIRST_NIGHT + datetime.timedelta(days=index)).isoformat()


def commit_of(index: int) -> str:
    return f"{index + 1:02x}" * 20


def quiet(count: int, level: float, start: int = 0) -> list[float]:
    return [level * math.exp(RIPPLE[(start + index) % len(RIPPLE)]) for index in range(count)]


def otava_installed() -> bool:
    try:
        nightly_analysis.require_otava()
    except OtavaUnavailable:
        return False
    return True


class Ledger:
    """Nightly run rows of a hosted runner: per night one run with twenty processes per series."""

    def __init__(self, environment: dict[str, Any], runner: Mapping[str, str] | None = HOSTED) -> None:
        self.environment = environment
        self.runner = dict(runner) if runner else None
        self.rows: list[dict[str, Any]] = []
        self.count = 0

    def night(self, index: int, series: Mapping[tuple[str, str], float], image: str | None = None) -> None:
        self.count += 1
        run = f"20261201T0100{self.count % 100:02d}Z-{self.count:08x}"
        runner = {**self.runner, **({"image": image} if image else {})} if self.runner else None
        row: dict[str, Any] = {
            "schema": 1,
            "kind": "run",
            "run": run,
            "started_at": f"{night_name(index)}T20:17:00Z",
            "tier": "nightly",
            "night": night_name(index),
            "kit": "jmh-nightly",
            "plan": {"min_processes": 20, "benchmarks": None},
            "arms": {"head": {"commit": commit_of(index), "dirty": False, "binary": "jvm", "runtime": "Temurin 25"}},
            "conditions": "gh-test",
            "fingerprint": bench_ledger.fingerprint(self.environment, runner),
            "environment": self.environment,
        }
        if runner:
            row["runner"] = runner
        self.rows.append(row)
        for (benchmark, metric), level in series.items():
            for process in range(1, 21):
                value = level * (1 + 0.001 * (process - 10.5))
                self.rows.append(
                    {
                        "schema": 1, "kind": "measurement", "run": run, "benchmark": benchmark,
                        "params": {"shape": "WIDE"}, "metric": metric, "unit": UNITS[metric], "arm": "head",
                        "pair": None, "position": None, "process": process, "value": value, "accepted": True,
                    }
                )  # fmt: skip

    def history(self, levels: Mapping[tuple[str, str], list[float]]) -> None:
        count = len(next(iter(levels.values())))
        for index in range(count):
            self.night(index, {key: values[index] for key, values in levels.items()})


class AnalysisTestCase(unittest.TestCase):

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.environment = fake_environment(self.base, FakeRunner())

    def ledger(self, runner: Mapping[str, str] | None = HOSTED) -> Ledger:
        return Ledger(self.environment, runner)


def stepped(before: int, after: int, level: float, factor: float) -> list[float]:
    return quiet(before, level) + quiet(after, level * factor, before)


class ChangeTest(AnalysisTestCase):
    KEY = ("AlphaBenchmark.encode", CPU)

    def analyse(self, ledger: Ledger, *, use_otava: bool = False) -> dict[str, Any]:
        return nightly_analysis.analyse(ledger.rows, use_otava=use_otava)

    def test_conv_304_reports_a_confirmed_step_with_its_commit_range(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: stepped(20, 4, 1000.0, 1.05)})
        series = self.analyse(ledger)["series"][0]
        self.assertEqual(24, len(series["nights"]))
        self.assertEqual("steady", series["nights"][19]["state"])
        self.assertEqual("unconfirmed", series["nights"][20]["state"])
        self.assertEqual("drift", series["nights"][21]["state"])
        (change,) = series["changes"]
        self.assertEqual(["conv-304"], change["detectors"])
        self.assertEqual("upward", change["direction"])
        self.assertEqual((night_name(20), night_name(19)), (change["night"], change["previous_night"]))
        self.assertEqual((commit_of(20), commit_of(19)), (change["commit"], change["previous_commit"]))
        self.assertEqual(4, change["nights_at_new_level"])

    def test_a_steady_series_has_no_change_and_is_not_actionable(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: quiet(30, 1000.0)})
        analysis = self.analyse(ledger)
        self.assertEqual([], analysis["series"][0]["changes"])
        self.assertEqual([], nightly_analysis.actionable(analysis))

    def test_a_confirmed_upward_step_is_actionable_once(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: stepped(20, 4, 1000.0, 1.05)})
        found = nightly_analysis.actionable(self.analyse(ledger))
        self.assertEqual(1, len(found))
        self.assertEqual(self.KEY[0], found[0]["series"]["benchmark"])
        self.assertNotIn("nights", found[0]["series"])

    def test_one_night_at_a_new_level_is_unconfirmed_and_not_actionable(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: stepped(20, 1, 1000.0, 1.05)})
        analysis = self.analyse(ledger)
        self.assertEqual("unconfirmed", analysis["series"][0]["nights"][-1]["state"])
        self.assertEqual([], nightly_analysis.actionable(analysis))

    def test_a_downward_step_is_a_change_but_never_an_issue(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: stepped(20, 4, 1000.0, 0.9)})
        analysis = self.analyse(ledger)
        (change,) = analysis["series"][0]["changes"]
        self.assertEqual("downward", change["direction"])
        self.assertEqual([], nightly_analysis.actionable(analysis))

    def test_a_change_older_than_the_recent_nights_is_history(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: stepped(20, nightly_analysis.RECENT_NIGHTS + 4, 1000.0, 1.05)})
        analysis = self.analyse(ledger)
        self.assertTrue(analysis["series"][0]["changes"])
        self.assertEqual([], nightly_analysis.actionable(analysis))

    def test_a_hosted_wall_clock_series_is_analysed_but_never_acted_on(self) -> None:
        ledger = self.ledger()
        wall = ("AlphaBenchmark.encode", WALL)
        ledger.history({self.KEY: quiet(24, 1000.0), wall: stepped(20, 4, 1000.0, 1.2)})
        analysis = self.analyse(ledger)
        by_metric = {series["metric"]: series for series in analysis["series"]}
        self.assertTrue(by_metric[WALL]["informational"])
        self.assertFalse(by_metric[CPU]["informational"])
        self.assertTrue(by_metric[WALL]["changes"])
        self.assertEqual([], nightly_analysis.actionable(analysis))

    def test_a_wall_clock_series_of_a_dedicated_machine_is_judged(self) -> None:
        ledger = self.ledger(runner=None)
        wall = ("AlphaBenchmark.encode", WALL)
        ledger.history({wall: stepped(20, 4, 1000.0, 1.2)})
        analysis = self.analyse(ledger)
        self.assertFalse(analysis["series"][0]["informational"])
        self.assertEqual(1, len(nightly_analysis.actionable(analysis)))

    def test_allocation_that_starts_on_a_zero_allocation_path_is_a_regression(self) -> None:
        ledger = self.ledger()
        key = ("AlphaBenchmark.encode", ALLOC)
        ledger.history({key: [0.0] * 20 + [40.0] * 4})
        series = self.analyse(ledger)["series"][0]
        self.assertEqual("drift", series["nights"][21]["state"])
        self.assertEqual(1, len(nightly_analysis.actionable(self.analyse(ledger))))

    def test_each_series_has_its_own_stable_identity_and_unit(self) -> None:
        ledger = self.ledger()
        ledger.history({self.KEY: quiet(15, 1000.0), ("AlphaBenchmark.encode", ALLOC): quiet(15, 64.0)})
        first = self.analyse(ledger)["series"]
        second = self.analyse(ledger)["series"]
        self.assertEqual([s["id"] for s in first], [s["id"] for s in second])
        self.assertEqual(2, len({s["id"] for s in first}))
        self.assertEqual({"ns/op", "B/op"}, {s["unit"] for s in first})

    def test_the_runner_image_of_every_night_is_kept(self) -> None:
        ledger = self.ledger()
        for index in range(15):
            ledger.night(index, {self.KEY: 1000.0}, image="ubuntu24/20261101.1" if index < 10 else "ubuntu24/20261110.1")
        nights = self.analyse(ledger)["series"][0]["nights"]
        self.assertEqual("ubuntu24/20261101.1", nights[9]["image"])
        self.assertEqual("ubuntu24/20261110.1", nights[10]["image"])


class MergeTest(AnalysisTestCase):
    KEY = ("AlphaBenchmark.encode", CPU)

    def history(self, nights: int, before: int, factor: float) -> dict[str, Any]:
        ledger = Ledger(self.environment)
        ledger.history({self.KEY: stepped(before, nights - before, 1000.0, factor)})
        return {"rows": ledger.rows}

    def analyse_with(self, rows: list[dict[str, Any]], otava: list[tuple[int, float | None, float, float, float]]) -> dict[str, Any]:
        with mock.patch.object(nightly_analysis, "otava_detect", return_value=otava), mock.patch.object(
            nightly_analysis, "require_otava"
        ):
            return nightly_analysis.analyse(rows, use_otava=True)

    def test_both_detectors_on_the_same_change_make_one_change_labelled_both(self) -> None:
        rows = self.history(24, 20, 1.05)["rows"]
        analysis = self.analyse_with(rows, [(20, 0.05, 1e-9, 1000.0, 1050.0)])
        (change,) = analysis["series"][0]["changes"]
        self.assertEqual(["conv-304", "otava"], change["detectors"])
        self.assertEqual("both", change["agreement"])
        self.assertEqual(2, len(change["details"]))

    def test_a_change_only_otava_reports_is_kept_and_labelled_advisory(self) -> None:
        rows = self.history(30, 30, 1.0)["rows"]  # CONV-304 sees a steady series
        analysis = self.analyse_with(rows, [(26, 0.04, 1e-6, 1000.0, 1040.0)])
        (change,) = analysis["series"][0]["changes"]
        self.assertEqual(["otava"], change["detectors"])
        self.assertEqual("otava-only", change["agreement"])
        self.assertEqual((night_name(26), night_name(25)), (change["night"], change["previous_night"]))
        self.assertEqual(4, change["nights_at_new_level"])
        self.assertEqual(1, len(nightly_analysis.actionable(analysis)))  # two or more nights at the new level

    def test_otava_with_a_single_night_at_the_new_level_is_not_yet_actionable(self) -> None:
        rows = self.history(30, 30, 1.0)["rows"]
        analysis = self.analyse_with(rows, [(29, 0.04, 1e-6, 1000.0, 1040.0)])
        self.assertEqual(1, len(analysis["series"][0]["changes"]))
        self.assertEqual([], nightly_analysis.actionable(analysis))

    def test_otava_below_the_two_percent_bound_is_dropped(self) -> None:
        rows = self.history(30, 30, 1.0)["rows"]
        analysis = self.analyse_with(rows, [(24, 0.015, 1e-9, 1000.0, 1015.0)])
        self.assertEqual([], analysis["series"][0]["changes"])

    def test_otava_from_a_zero_level_is_reported_as_from_zero(self) -> None:
        rows = self.history(30, 30, 1.0)["rows"]
        analysis = self.analyse_with(rows, [(24, None, 0.0, 0.0, 40.0)])
        (change,) = analysis["series"][0]["changes"]
        self.assertIsNone(change["relative_change"])

    def test_two_detections_far_apart_stay_two_changes(self) -> None:
        rows = self.history(30, 30, 1.0)["rows"]
        analysis = self.analyse_with(rows, [(16, 0.05, 1e-9, 1000.0, 1050.0), (26, 0.05, 1e-9, 1050.0, 1100.0)])
        self.assertEqual(2, len(analysis["series"][0]["changes"]))

    def test_a_series_too_short_for_otava_has_no_otava_opinion(self) -> None:
        self.assertEqual([], nightly_analysis.otava_detect([1.0] * (nightly_analysis.OTAVA_MIN_NIGHTS - 1)))


class OtavaTest(AnalysisTestCase):

    @unittest.skipUnless(otava_installed(), "apache-otava is not installed")
    def test_real_otava_finds_a_step_within_a_night_of_the_true_one(self) -> None:
        found = nightly_analysis.otava_detect(stepped(24, 6, 1000.0, 1.06))
        self.assertEqual(1, len(found))
        index, relative, pvalue, before, after = found[0]
        self.assertLessEqual(abs(index - 24), 1)
        self.assertAlmostEqual(0.06, relative, delta=0.01)
        self.assertLess(pvalue, nightly_analysis.OTAVA_P_VALUE)
        self.assertLess(before, after)

    @unittest.skipUnless(otava_installed(), "apache-otava is not installed")
    def test_real_otava_leaves_a_quiet_series_alone_and_survives_a_constant_one(self) -> None:
        self.assertEqual([], nightly_analysis.otava_detect(quiet(40, 1000.0)))
        self.assertEqual([], nightly_analysis.otava_detect([64.0] * 40))
        zero = nightly_analysis.otava_detect([0.0] * 20 + [40.0] * 6)
        self.assertEqual(None, zero[0][1])

    @unittest.skipUnless(otava_installed(), "apache-otava is not installed")
    def test_both_detectors_agree_on_a_real_step(self) -> None:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): stepped(24, 6, 1000.0, 1.06)})
        analysis = nightly_analysis.analyse(ledger.rows)
        (change,) = analysis["series"][0]["changes"]
        self.assertEqual("both", change["agreement"])
        self.assertEqual(1, len(nightly_analysis.actionable(analysis)))

    def test_a_missing_installation_is_reported_and_can_be_required_or_waived(self) -> None:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): quiet(16, 1000.0)})
        path = self.base / "ledger.jsonl"
        bench_ledger.append_rows(path, ledger.rows)
        with mock.patch.dict(sys.modules, {"otava": None, "otava.analysis": None}):
            with self.assertRaises(OtavaUnavailable):
                nightly_analysis.analyse(ledger.rows)
            out = io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    0, nightly_analysis.main(["--ledger", str(path), "--out", str(self.base / "a.json")])
                )
                self.assertEqual(
                    2,
                    nightly_analysis.main(
                        ["--ledger", str(path), "--out", str(self.base / "b.json"), "--require-otava"]
                    ),
                )
        self.assertFalse(json.loads((self.base / "a.json").read_text(encoding="utf-8"))["otava"])
        self.assertFalse((self.base / "b.json").exists())


class ReleasesAndCommandLineTest(AnalysisTestCase):

    def test_release_tags_are_read_in_date_order_and_other_tags_are_ignored(self) -> None:
        tags = self.base / "tags.txt"
        tags.write_text(
            "sirix-1.0.0-beta7 2026-07-16\nsirix-parent-0.9.3 2019-07-29\nsirix-1.0.0-beta6 2026-07-11\nnoise\n"
            "v2 2026-01-01\n",
            encoding="utf-8",
        )
        self.assertEqual(
            [("sirix-1.0.0-beta6", "2026-07-11"), ("sirix-1.0.0-beta7", "2026-07-16")],
            [(r["tag"], r["date"]) for r in nightly_analysis.read_releases(tags)],
        )
        self.assertEqual([], nightly_analysis.read_releases(None))

    def test_the_command_writes_the_analysis_and_refuses_a_bad_ledger(self) -> None:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): quiet(16, 1000.0)})
        path = self.base / "ledger.jsonl"
        bench_ledger.append_rows(path, ledger.rows)
        out = self.base / "analysis.json"
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, nightly_analysis.main(["--ledger", str(path), "--out", str(out)]))
        document = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(1, document["schema"])
        self.assertEqual(night_name(15), document["latest_night"])
        broken = self.base / "broken.jsonl"
        broken.write_text("not json\n", encoding="utf-8")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(2, nightly_analysis.main(["--ledger", str(broken), "--out", str(self.base / "x.json")]))

    def test_two_runs_of_one_night_make_the_analysis_fail(self) -> None:
        ledger = Ledger(self.environment)
        key = ("AlphaBenchmark.encode", CPU)
        ledger.night(0, {key: 1000.0})
        ledger.night(0, {key: 1001.0})
        with self.assertRaises(bench_trend.TrendError):
            nightly_analysis.analyse(ledger.rows, use_otava=False)


class PolicyTest(AnalysisTestCase):
    """The constants that decide what is reported, pinned at their edges."""

    KEY = ("AlphaBenchmark.encode", CPU)

    def history(self, nights: int, levels: list[float] | None = None) -> list[dict[str, Any]]:
        ledger = Ledger(self.environment)
        ledger.history({self.KEY: levels or quiet(nights, 1000.0)})
        return ledger.rows

    def analyse(self, rows: list[dict[str, Any]], detections: list[tuple[int, float | None, float, float, float]]) -> dict[str, Any]:
        with mock.patch.object(nightly_analysis, "otava_detect", return_value=detections), mock.patch.object(
            nightly_analysis, "require_otava"
        ):
            return nightly_analysis.analyse(rows, use_otava=True)

    def test_the_series_identity_is_pinned(self) -> None:
        key = ("c", "k", "b", (("p", "v"),), "m", "jvm", "rt", "hosted")
        self.assertEqual("53a45ed4edbf", nightly_analysis.series_id(key))
        self.assertNotEqual(nightly_analysis.series_id(key), nightly_analysis.series_id(key[:-1] + ("dedicated",)))

    def test_two_nights_at_the_new_level_confirm_an_otava_change_and_one_does_not(self) -> None:
        rows = self.history(30)
        for index, actionable in ((29, False), (28, True), (27, True)):
            with self.subTest(index=index):
                analysis = self.analyse(rows, [(index, 0.05, 1e-6, 1000.0, 1050.0)])
                self.assertEqual(actionable, bool(nightly_analysis.actionable(analysis)))

    def test_a_change_is_recent_for_ten_nights_and_history_after_that(self) -> None:
        for nights_at_new_level, actionable in ((10, True), (11, False)):
            with self.subTest(nights_at_new_level=nights_at_new_level):
                rows = self.history(40)
                analysis = self.analyse(rows, [(40 - nights_at_new_level, 0.05, 1e-6, 1000.0, 1050.0)])
                self.assertEqual(actionable, bool(nightly_analysis.actionable(analysis)))

    def test_detectors_within_three_nights_found_the_same_change_and_four_nights_apart_they_did_not(self) -> None:
        rows = self.history(24, stepped(20, 4, 1000.0, 1.05))  # CONV-304 places its change on night 20
        together = self.analyse(rows, [(23, 0.05, 1e-6, 1000.0, 1050.0)])
        self.assertEqual(1, len(together["series"][0]["changes"]))
        apart = self.analyse(self.history(25, stepped(20, 5, 1000.0, 1.05)), [(24, 0.05, 1e-6, 1000.0, 1050.0)])
        self.assertEqual(2, len(apart["series"][0]["changes"]))

    def test_the_merged_change_is_the_one_conv_304_places_even_when_otava_places_it_earlier(self) -> None:
        analysis = self.analyse(self.history(24, stepped(20, 4, 1000.0, 1.05)), [(19, 0.05, 1e-6, 1000.0, 1050.0)])
        (change,) = analysis["series"][0]["changes"]
        self.assertEqual(night_name(20), change["night"])
        self.assertEqual(["conv-304", "otava"], change["detectors"])

    def test_opposite_directions_are_never_merged(self) -> None:
        analysis = self.analyse(self.history(24, stepped(20, 4, 1000.0, 1.05)), [(20, -0.05, 1e-6, 1050.0, 1000.0)])
        self.assertEqual(["downward", "upward"], sorted(c["direction"] for c in analysis["series"][0]["changes"]))

    def test_two_separate_regressions_of_one_series_are_two_changes(self) -> None:
        levels = quiet(20, 1000.0) + quiet(14, 1050.0, 20) + quiet(5, 1155.0, 34)
        analysis = nightly_analysis.analyse(self.history(39, levels), use_otava=False)
        changes = analysis["series"][0]["changes"]
        self.assertEqual([night_name(20), night_name(34)], [c["night"] for c in changes])

    def test_otava_sizes_are_reported_around_the_two_percent_bound(self) -> None:
        rows = self.history(30)
        kept = {}
        for relative in (0.0199, 0.0201, -0.0199, -0.0201):
            before, after = 1000.0, 1000.0 * (1 + relative)
            changes = self.analyse(rows, [(25, relative, 1e-6, before, after)])["series"][0]["changes"]
            kept[relative] = bool(changes)
        self.assertEqual({0.0199: False, 0.0201: True, -0.0199: False, -0.0201: True}, kept)

    def test_an_otava_change_names_its_nights_commits_and_size(self) -> None:
        (change,) = self.analyse(self.history(30), [(25, 0.05, 1e-6, 1000.0, 1050.0)])["series"][0]["changes"]
        self.assertEqual((night_name(25), night_name(24)), (change["night"], change["previous_night"]))
        self.assertEqual((commit_of(25), commit_of(24)), (change["commit"], change["previous_commit"]))
        self.assertEqual("upward", change["direction"])
        self.assertAlmostEqual(0.05, change["relative_change"])
        self.assertIn("level 1000 -> 1050 (+5.00%), p = 1e-06", change["details"][0])
        self.assertEqual(5, change["nights_at_new_level"])

    def test_a_decrease_is_downward_and_a_change_from_zero_to_zero_is_nothing(self) -> None:
        rows = self.history(30)
        down = self.analyse(rows, [(25, -0.05, 1e-6, 1000.0, 950.0)])["series"][0]["changes"][0]
        self.assertEqual("downward", down["direction"])
        self.assertEqual([], self.analyse(rows, [(25, None, 0.0, 0.0, 0.0)])["series"][0]["changes"])
        up = self.analyse(rows, [(25, None, 0.0, 0.0, 40.0)])["series"][0]["changes"][0]
        self.assertIn("from zero", up["details"][0])

    def test_an_otava_change_on_the_second_night_is_the_earliest_one_reported(self) -> None:
        (change,) = self.analyse(self.history(30), [(1, 0.05, 1e-6, 1000.0, 1050.0)])["series"][0]["changes"]
        self.assertEqual((night_name(1), night_name(0)), (change["night"], change["previous_night"]))

    def test_a_change_of_exactly_the_bound_is_reported(self) -> None:
        exactly = math.expm1(bench_trend.DRIFT_BOUND)
        self.assertEqual(1, len(self.analyse(self.history(30), [(25, exactly, 1e-6, 1000.0, 1000.0 * (1 + exactly))])["series"][0]["changes"]))

    def test_a_missing_otava_is_reported_even_when_no_series_is_long_enough_to_need_it(self) -> None:
        with mock.patch.dict(sys.modules, {"otava": None, "otava.analysis": None}):
            with self.assertRaises(OtavaUnavailable):
                nightly_analysis.analyse(self.history(5), use_otava=True)
            with self.assertRaises(OtavaUnavailable):
                nightly_analysis.analyse([], use_otava=True)

    def test_an_otava_change_before_the_second_night_is_ignored(self) -> None:
        self.assertEqual([], self.analyse(self.history(30), [(0, 0.05, 1e-6, 1000.0, 1050.0)])["series"][0]["changes"])

    def test_the_conv_304_detail_gives_drift_and_fence(self) -> None:
        analysis = nightly_analysis.analyse(self.history(24, stepped(20, 4, 1000.0, 1.05)), use_otava=False)
        detail = analysis["series"][0]["changes"][0]["details"][0]
        self.assertRegex(detail, r"^CONV-304 drift: drift \+5\.\d\d% against a fence of \+0\.\d\d%$")

    def test_otava_is_asked_with_its_documented_window_and_significance(self) -> None:
        calls: list[tuple[list[float], dict[str, Any]]] = []

        def compute(levels: list[float], **options: Any) -> tuple[list[Any], None]:
            calls.append((levels, options))
            return [], None

        fake_module = mock.Mock(compute_change_points=compute)
        with mock.patch.dict(sys.modules, {"otava": mock.Mock(), "otava.analysis": fake_module}):
            self.assertEqual([], nightly_analysis.otava_detect([1.0] * 13))
            self.assertEqual([], calls)
            nightly_analysis.otava_detect([1.0] * 14)
        self.assertEqual(
            [([1.0] * 14, {"window_len": 50, "max_pvalue": 0.001, "min_magnitude": 0.0})], calls
        )

    def test_without_otava_nothing_asks_for_it(self) -> None:
        with mock.patch.dict(sys.modules, {"otava": None, "otava.analysis": None}):
            analysis = nightly_analysis.analyse(self.history(16), use_otava=False)
        self.assertFalse(analysis["otava"])
        self.assertEqual(night_name(0), analysis["first_night"])
        self.assertEqual(night_name(15), analysis["latest_night"])

    def test_an_empty_ledger_has_no_nights(self) -> None:
        analysis = nightly_analysis.analyse([], use_otava=False)
        self.assertEqual((None, None, []), (analysis["first_night"], analysis["latest_night"], analysis["series"]))

    def test_the_analysis_file_is_stable_text(self) -> None:
        path = self.base / "ledger.jsonl"
        bench_ledger.append_rows(path, self.history(16))
        out = self.base / "a.json"
        for _ in range(2):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), mock.patch.dict(
                sys.modules, {"otava": None, "otava.analysis": None}
            ):
                nightly_analysis.main(["--ledger", str(path), "--out", str(out)])
        text = out.read_text(encoding="utf-8")
        self.assertTrue(text.endswith("}\n"))
        self.assertEqual(sorted(json.loads(text)), list(json.loads(text)))  # keys are sorted
        self.assertIn('\n "otava"', text)  # one space of indentation

    def test_the_required_arguments_are_required(self) -> None:
        for arguments in ([ "--out", "x"], ["--ledger", "x"]):
            with self.assertRaises(SystemExit) as caught, contextlib.redirect_stderr(io.StringIO()):
                nightly_analysis.main(arguments)
            self.assertEqual(2, caught.exception.code)


if __name__ == "__main__":
    unittest.main()
