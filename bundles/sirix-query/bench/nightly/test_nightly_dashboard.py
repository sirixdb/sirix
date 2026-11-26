#!/usr/bin/env python3
"""Behaviour tests of the static dashboard (nightly_dashboard.py)."""

from __future__ import annotations

import contextlib
import io
import json
import re
import sys
import unittest
from html.parser import HTMLParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import nightly_analysis  # noqa: E402
import nightly_dashboard  # noqa: E402
from test_nightly_analysis import ALLOC, CPU, WALL, AnalysisTestCase, Ledger, quiet, stepped  # noqa: E402

VOID = {"meta", "br", "line", "polyline", "circle", "link", "hr", "img", "input"}


class Balance(HTMLParser):
    """Counts unclosed or stray tags, so a malformed page is a failing test."""

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []
        self.problems: list[str] = []
        self.refs: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if name in ("href", "src", "action") and value is not None:
                self.refs.append((tag, value))
        if tag not in VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        pass

    def handle_endtag(self, tag: str) -> None:
        if tag in VOID:
            return
        if not self.stack or self.stack[-1] != tag:
            self.problems.append(f"</{tag}> closes {self.stack[-1] if self.stack else 'nothing'}")
            if tag in self.stack:
                while self.stack.pop() != tag:
                    pass
        else:
            self.stack.pop()


class DashboardTest(AnalysisTestCase):

    def analysis(self, use_otava: bool = False) -> dict:
        ledger = Ledger(self.environment)
        ledger.history(
            {
                ("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 1.05),
                ("AlphaBenchmark.encode", ALLOC): quiet(24, 64.0),
                ("AlphaBenchmark.encode", WALL): stepped(20, 4, 1000.0, 1.2),
                ("BetaBenchmark.lookup", CPU): quiet(24, 20.0),
            }
        )
        releases = [
            {"tag": "sirix-1.0.0-beta7", "date": "2026-12-10"},
            {"tag": "sirix-1.0.0-beta6", "date": "2026-07-11"},
        ]
        return nightly_analysis.analyse(ledger.rows, releases=releases, use_otava=use_otava)

    def page(self, **options: str) -> str:
        return nightly_dashboard.render(self.analysis(), repository="sirixdb/sirix", generated="2026-12-24 20:00 UTC", **options)

    def test_the_page_is_well_formed_and_titled(self) -> None:
        text = self.page()
        balance = Balance()
        balance.feed(text)
        self.assertEqual([], balance.problems)
        self.assertEqual([], balance.stack)
        self.assertIn("<title>SirixDB nightly benchmarks</title>", text)

    def test_every_benchmark_has_a_card_with_a_chart_per_series(self) -> None:
        text = self.page()
        self.assertEqual(2, text.count("<section class='card'"))
        self.assertEqual(4, text.count("<figure id='s-"))
        self.assertEqual(4, text.count("<svg viewBox"))
        self.assertIn("CPU time per operation", text)
        self.assertIn("Allocation per operation", text)
        self.assertIn("Wall-clock time per operation", text)

    def test_wall_clock_is_marked_informational_and_cpu_is_not(self) -> None:
        text = self.page()
        self.assertEqual(1, text.count("informational, not judged"))
        self.assertIn("Wall-clock time is informational", text)
        self.assertEqual(1, text.count('class="chart info"'))

    def test_changes_and_releases_are_drawn_where_they_belong(self) -> None:
        text = self.page()
        self.assertIn('class="cp-conv-304"', text)
        self.assertIn(">1.0.0-beta7<", text)  # inside the nights' range
        self.assertNotIn("1.0.0-beta6", text)  # months before the first night
        # the commit range of the change that began on night 20: the commit of night 19 to that of night 20
        self.assertIn("/compare/" + "14" * 20 + "..." + "15" * 20, text)
        self.assertIn("Detected changes", text)

    def test_nothing_on_the_page_reaches_another_site_or_runs_a_script(self) -> None:
        text = self.page()
        self.assertNotRegex(text, r"(?i)<script|<link|@import|url\(|<iframe|<img|src=")
        balance = Balance()
        balance.feed(text)
        for _tag, ref in balance.refs:
            self.assertTrue(
                ref.startswith("#") or ref == "analysis.json" or ref.startswith("https://github.com/sirixdb/sirix/"), ref
            )

    def test_names_from_the_ledger_cannot_inject_markup(self) -> None:
        analysis = self.analysis()
        analysis["series"][0]["benchmark"] = "<script>alert(1)</script>"
        analysis["series"][0]["name"] = "<b>x</b>"
        text = nightly_dashboard.render(analysis, repository="sirixdb/sirix")
        self.assertNotIn("<script>alert", text)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", text)

    def test_an_empty_ledger_still_renders(self) -> None:
        empty = nightly_analysis.analyse([], use_otava=False)
        text = nightly_dashboard.render(empty, repository="sirixdb/sirix")
        self.assertIn("none yet", text)
        self.assertIn("No change point has been detected", text)

    def test_a_flat_or_zero_series_does_not_divide_by_zero(self) -> None:
        for level in (0.0, 5.0):
            ledger = Ledger(self.environment)
            ledger.history({("AlphaBenchmark.encode", ALLOC): [level] * 16})
            analysis = nightly_analysis.analyse(ledger.rows, use_otava=False)
            text = nightly_dashboard.render(analysis, repository=None)
            self.assertIn("<polyline", text)
            self.assertNotIn("nan", text.lower().replace("nan.", ""))

    def test_a_single_night_series_draws_a_point(self) -> None:
        ledger = Ledger(self.environment)
        ledger.night(0, {("AlphaBenchmark.encode", CPU): 1000.0})
        text = nightly_dashboard.render(nightly_analysis.analyse(ledger.rows, use_otava=False), repository=None)
        self.assertIn("<circle", text)

    def test_the_summary_counts_judged_series_only(self) -> None:
        text = self.page()
        summary = re.search(r"Judged series on the latest night: (.*?)</p>", text).group(1)
        # Alpha CPU is in drift, Alpha allocation and Beta CPU are in their window: three judged series.
        self.assertEqual(3, sum(int(n) for n in re.findall(r"</span> (\d+)", summary)))

    def test_values_are_shown_in_units_that_fit(self) -> None:
        cases = {
            (999.0, "ns/op"): "999 ns/op",
            (999.96, "ns/op"): "1000 ns/op",
            (1500.0, "ns/op"): "1.5 µs/op",
            (2.5e6, "ns/op"): "2.5 ms/op",
            (3e9, "ns/op"): "3 s/op",
            (64.0, "B/op"): "64 B/op",
            (2048.0, "B/op"): "2 KiB/op",
            (3 * 2**20, "B/op"): "3 MiB/op",
        }
        for (value, unit), expected in cases.items():
            self.assertEqual(expected, nightly_dashboard.format_value(value, unit))

    def test_the_site_directory_holds_the_page_the_data_and_the_pages_marker(self) -> None:
        out = self.base / "site"
        nightly_dashboard.write_site(self.analysis(), out, repository="sirixdb/sirix")
        self.assertEqual({"index.html", "analysis.json", ".nojekyll"}, {p.name for p in out.iterdir()})
        self.assertEqual(24, len(json.loads((out / "analysis.json").read_text(encoding="utf-8"))["series"][0]["nights"]))

    def test_the_command_checks_its_inputs(self) -> None:
        analysis = self.base / "analysis.json"
        analysis.write_text(json.dumps(self.analysis()), encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(
                0,
                nightly_dashboard.main(["--analysis", str(analysis), "--out", str(self.base / "o"), "--repository", "a/b"]),
            )
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(2, nightly_dashboard.main(["--analysis", str(self.base / "none"), "--out", str(self.base / "p")]))
            self.assertEqual(
                2,
                nightly_dashboard.main(
                    ["--analysis", str(analysis), "--out", str(self.base / "q"), "--repository", "a b/c\"d"]
                ),
            )


class DashboardDetailTest(AnalysisTestCase):

    def analysis_of(self, levels: dict[tuple[str, str], list[float]], **options: object) -> dict:
        ledger = Ledger(self.environment, options.pop("runner", None) if "runner" in options else None)  # type: ignore[arg-type]
        ledger.history(levels)
        return nightly_analysis.analyse(ledger.rows, use_otava=bool(options.get("otava")), releases=options.get("releases", ()))  # type: ignore[arg-type]

    def test_units_change_exactly_at_their_boundaries(self) -> None:
        cases = {
            (1000.0, "ns/op"): "1 µs/op", (999.0, "ns/op"): "999 ns/op", (1e6, "ns/op"): "1 ms/op",
            (999999.0, "ns/op"): "1000 µs/op", (1e9, "ns/op"): "1 s/op", (1023.0, "B/op"): "1023 B/op",
            (1024.0, "B/op"): "1 KiB/op", (2**20, "B/op"): "1 MiB/op", (2**20 - 1, "B/op"): "1024 KiB/op",
            (2**30, "B/op"): "1 GiB/op", (5.0, "widgets"): "5 widgets", (-1500.0, "ns/op"): "-1.5 µs/op",
        }  # fmt: skip
        for (value, unit), expected in cases.items():
            self.assertEqual(expected, nightly_dashboard.format_value(value, unit), (value, unit))

    def test_the_polyline_runs_left_to_right_and_the_higher_the_level_the_higher_it_is_drawn(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", CPU): [10.0, 30.0, 20.0, 40.0] + [25.0] * 12})
        svg = nightly_dashboard.chart_svg(analysis["series"][0], [])
        points = [tuple(map(float, pair.split(","))) for pair in re.search(r'points="([^"]+)"', svg).group(1).split()]
        xs, ys = [x for x, _ in points], [y for _, y in points]
        self.assertEqual(sorted(xs), xs)
        self.assertEqual(84.0, xs[0])  # the plot starts at its left margin, wide enough for the axis labels
        self.assertEqual(640 - 14, xs[-1])  # and ends at its right margin
        self.assertGreater(ys[0], ys[2])  # 10 is drawn below 20
        self.assertGreater(ys[2], ys[1])  # 20 below 30
        self.assertGreater(ys[1], ys[3])  # 30 below 40
        self.assertTrue(all(12.0 <= y <= 190 - 28 for y in ys))  # inside the plot, never over the axes

    def test_equal_spacing_in_days_not_in_nights(self) -> None:
        ledger = Ledger(self.environment)
        for index in (0, 1, 5):
            ledger.night(index, {("AlphaBenchmark.encode", CPU): 100.0 + index})
        svg = nightly_dashboard.chart_svg(nightly_analysis.analyse(ledger.rows, use_otava=False)["series"][0], [])
        xs = [float(pair.split(",")[0]) for pair in re.search(r'points="([^"]+)"', svg).group(1).split()]
        self.assertAlmostEqual((xs[1] - xs[0]) * 5, xs[2] - xs[0], places=1)  # nights 0, 1 and 5

    def test_the_outer_date_labels_are_anchored_inside_the_plot(self) -> None:
        series = self.analysis_of({("AlphaBenchmark.encode", CPU): quiet(16, 1000.0)})["series"][0]
        anchors = re.findall(r'text-anchor="(\w+)">(2026-\d\d-\d\d)<', nightly_dashboard.chart_svg(series, []))
        self.assertEqual([("start", "2026-12-01"), ("middle", "2026-12-08"), ("end", "2026-12-16")], anchors)
        ledger = Ledger(self.environment)
        ledger.night(0, {("AlphaBenchmark.encode", CPU): 100.0})
        one = nightly_analysis.analyse(ledger.rows, use_otava=False)["series"][0]
        self.assertEqual([("middle", "2026-12-01")], re.findall(r'text-anchor="(\w+)">(2026-\d\d-\d\d)<', nightly_dashboard.chart_svg(one, [])))

    def test_a_single_night_is_centred(self) -> None:
        ledger = Ledger(self.environment)
        ledger.night(0, {("AlphaBenchmark.encode", CPU): 100.0})
        svg = nightly_dashboard.chart_svg(nightly_analysis.analyse(ledger.rows, use_otava=False)["series"][0], [])
        self.assertIn('cx="355.0"', svg)  # the middle of the plot between the margins 84 and 626

    def test_a_release_on_the_first_or_last_night_is_marked_and_one_a_day_outside_is_not(self) -> None:
        levels = {("AlphaBenchmark.encode", CPU): quiet(16, 1000.0)}  # nights 2026-12-01 to 2026-12-16
        series = self.analysis_of(levels)["series"][0]
        for date, marked in (("2026-11-30", False), ("2026-12-01", True), ("2026-12-16", True), ("2026-12-17", False)):
            svg = nightly_dashboard.chart_svg(series, [{"tag": "sirix-9.9", "date": date}])
            self.assertEqual(marked, 'class="release"' in svg, date)

    def test_a_downward_change_is_drawn_in_its_own_class_and_a_judged_chart_is_not_dimmed(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 0.9)})
        svg = nightly_dashboard.chart_svg(analysis["series"][0], [])
        self.assertIn('class="cp-conv-304 cp-down"', svg)
        self.assertNotIn("chart info", svg)

    def test_the_changes_table_names_direction_detectors_size_and_is_newest_first(self) -> None:
        up = self.analysis_of({("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 1.05)})
        down = self.analysis_of({("BetaBenchmark.lookup", CPU): stepped(18, 6, 1000.0, 0.9)})
        merged = {**up, "series": up["series"] + down["series"]}
        text = nightly_dashboard.render(merged, repository="sirixdb/sirix", generated="x")
        rows = re.findall(r"<tr><td class='num'>([\d-]+)</td>.*?<td>(slower|faster)</td><td>(.*?)</td><td class='num'>(.*?)</td>", text)
        self.assertEqual(
            [("2026-12-21", "slower", "conv-304"), ("2026-12-19", "faster", "conv-304")],
            [(a, b, c.split(" <")[0]) for a, b, c, _ in rows],
        )
        self.assertRegex(rows[0][3], r"^\+5\.\d%$")
        self.assertRegex(rows[1][3], r"^-10\.\d%$")

    def test_the_detector_agreement_is_shown_only_when_otava_ran_and_they_differ(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 1.05)})
        self.assertNotIn("conv-304-only", nightly_dashboard.render(analysis, repository=None))
        analysis["otava"] = True
        self.assertIn("conv-304-only", nightly_dashboard.render(analysis, repository=None))
        analysis["series"][0]["changes"][0]["agreement"] = "both"
        self.assertNotIn("conv-304-only", nightly_dashboard.render(analysis, repository=None))

    def test_a_change_from_zero_and_an_informational_series_are_marked_in_the_table(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", ALLOC): [0.0] * 20 + [40.0] * 4})
        analysis["series"][0]["changes"][0]["relative_change"] = None
        analysis["series"][0]["informational"] = True
        text = nightly_dashboard.render(analysis, repository=None)
        self.assertIn("<td class='num'>from zero</td>", text)
        self.assertIn("(informational)", text)

    def test_commits_are_plain_text_without_a_repository_and_links_with_one(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 1.05)})
        plain = nightly_dashboard.render(analysis, repository=None)
        self.assertIn(f"<code>{'14' * 6}..{'15' * 6}</code>", plain)
        self.assertNotIn("github.com", plain)
        linked = nightly_dashboard.render(analysis, repository="sirixdb/sirix")
        self.assertIn(f">{'14' * 6}..{'15' * 6}</a>", linked)
        self.assertIn("Apache Otava", nightly_dashboard.render({**analysis, "otava": True}, repository="sirixdb/sirix"))
        self.assertNotIn("Apache Otava as a second opinion", linked)

    def test_the_summary_says_when_no_judged_series_has_a_night(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", WALL): quiet(16, 1000.0)})
        for series in analysis["series"]:
            series["informational"] = True
        self.assertIn("No judged series has a recorded night yet.", nightly_dashboard.render(analysis, repository=None))

    def test_a_benchmark_card_lists_cpu_then_allocation_then_wall_clock(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", WALL): quiet(16, 1000.0), ("AlphaBenchmark.encode", ALLOC): quiet(16, 64.0),
                                     ("AlphaBenchmark.encode", CPU): quiet(16, 900.0)})  # fmt: skip
        text = nightly_dashboard.render(analysis, repository=None)
        order = [text.index(title) for title in ("<strong>CPU time per operation", "<strong>Allocation per operation", "<strong>Wall-clock time per operation")]
        self.assertEqual(sorted(order), order)

    def test_a_group_without_parameters_is_named_by_its_benchmark(self) -> None:
        analysis = self.analysis_of({("AlphaBenchmark.encode", CPU): quiet(16, 1000.0)})
        analysis["series"][0]["params"] = {}
        self.assertIn("<h2>AlphaBenchmark.encode</h2>", nightly_dashboard.render(analysis, repository=None))

    def test_the_files_the_command_writes_are_those_of_write_site(self) -> None:
        analysis = self.base / "analysis.json"
        analysis.write_text(json.dumps(nightly_analysis.analyse([], use_otava=False)), encoding="utf-8")
        out = self.base / "deep" / "site"
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(0, nightly_dashboard.main(["--analysis", str(analysis), "--out", str(out)]))
        self.assertTrue((out / "index.html").is_file())
        self.assertTrue(json.loads((out / "analysis.json").read_text(encoding="utf-8"))["series"] == [])
        for missing in ("--analysis", "--out"):
            arguments = {"--analysis": str(analysis), "--out": str(out)}
            arguments.pop(missing)
            with self.assertRaises(SystemExit) as caught, contextlib.redirect_stderr(io.StringIO()):
                nightly_dashboard.main([item for pair in arguments.items() for item in pair])
            self.assertEqual(2, caught.exception.code)


if __name__ == "__main__":
    unittest.main()
