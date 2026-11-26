#!/usr/bin/env python3
"""Behaviour tests of the regression issues (nightly_issues.py): one per change point, deduplicated,
labelled, linked, never in the reserved window, never for a fork, capped per run."""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any, Sequence
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import nightly_analysis  # noqa: E402
import nightly_issues  # noqa: E402
from test_nightly_analysis import ALLOC, CPU, WALL, AnalysisTestCase, Ledger, quiet, stepped  # noqa: E402

UTC = datetime.timezone.utc
EVENING = datetime.datetime(2026, 12, 24, 20, 17, tzinfo=UTC)  # Thursday 21:17 Berlin
MIDDAY = datetime.datetime(2026, 12, 24, 10, 0, tzinfo=UTC)  # Thursday 11:00 Berlin
DASHBOARD = "https://sirixdb.github.io/sirix/"


class FakeGitHub:
    def __init__(self, existing: Sequence[dict[str, Any]] = ()) -> None:
        self.existing = list(existing)
        self.created: list[dict[str, Any]] = []
        self.labels: list[str] = []

    def issues(self) -> list[dict[str, Any]]:
        return list(self.existing)

    def ensure_labels(self, names: Sequence[str]) -> None:
        self.labels += list(names)

    def create_issue(self, title: str, body: str, labels: Sequence[str]) -> int:
        number = 100 + len(self.created)
        self.created.append({"number": number, "title": title, "body": body, "labels": list(labels)})
        return number


class IssueTest(AnalysisTestCase):

    def analysis(self, **series: list[float]) -> dict[str, Any]:
        ledger = Ledger(self.environment)
        ledger.history(
            {
                ("AlphaBenchmark.encode", CPU): series.get("cpu", stepped(20, 4, 1000.0, 1.05)),
                ("AlphaBenchmark.encode", ALLOC): series.get("alloc", quiet(24, 64.0)),
            }
        )
        return nightly_analysis.analyse(ledger.rows, use_otava=False)

    def open(self, analysis: dict[str, Any], client: FakeGitHub | None = None, **options: Any) -> nightly_issues.Outcome:
        self.client = client or FakeGitHub()
        settings = {"repository": "sirixdb/sirix", "dashboard": DASHBOARD, "now": EVENING}
        settings.update(options)
        return nightly_issues.open_issues(analysis, self.client, **settings)

    def test_a_confirmed_regression_opens_one_labelled_issue_with_the_range_and_the_dashboard(self) -> None:
        outcome = self.open(self.analysis())
        self.assertEqual(1, len(self.client.created))
        issue = self.client.created[0]
        self.assertEqual([(100, issue["title"])], outcome.opened)
        self.assertEqual(["benchmark-regression", "nightly-benchmark"], issue["labels"])
        self.assertIn("AlphaBenchmark.encode{shape=WIDE}", issue["title"])
        self.assertIn("CPU time", issue["title"])
        self.assertIn("+5.1%", issue["title"])
        body = issue["body"]
        self.assertIn(f"sirix-nightly-change: series=", body)
        self.assertIn("/compare/" + "14" * 20 + "..." + "15" * 20, body)
        self.assertIn(f"{DASHBOARD}#s-", body)
        self.assertIn("CONV-304 (the normative judge)", body)
        self.assertIn("Apache Otava", body)

    def test_the_same_change_is_not_opened_twice_even_a_night_later(self) -> None:
        analysis = self.analysis()
        self.open(analysis)
        again = self.open(analysis, FakeGitHub(self.client.created))
        self.assertEqual([], self.client.created)
        self.assertEqual(1, len(again.covered))
        # A detector that places the change one night later is still the same change.
        shifted = json.loads(json.dumps(analysis))
        for series in shifted["series"]:
            for change in series["changes"]:
                change["night"] = "2026-12-22"
        outcome = self.open(shifted, FakeGitHub(FakeGitHub().created + [{"number": 5, "title": "t", "body": self.body_of(analysis)}]))
        self.assertEqual([], outcome.opened)

    def body_of(self, analysis: dict[str, Any]) -> str:
        client = FakeGitHub()
        nightly_issues.open_issues(
            analysis, client, repository="sirixdb/sirix", dashboard=DASHBOARD, now=EVENING
        )
        return client.created[0]["body"]

    def test_a_closed_issue_still_covers_its_change(self) -> None:
        body = self.body_of(self.analysis())
        outcome = self.open(self.analysis(), FakeGitHub([{"number": 7, "title": "x", "body": body, "state": "CLOSED"}]))
        self.assertEqual([], outcome.opened)

    def test_a_change_of_another_series_or_a_month_later_is_a_new_issue(self) -> None:
        body = self.body_of(self.analysis())
        marker_swapped = nightly_issues.MARKER.sub(lambda m: f"<!-- sirix-nightly-change: series={'0' * 12} night={m.group(2)} -->", body)
        self.assertEqual(1, len(self.open(self.analysis(), FakeGitHub([{"body": marker_swapped}])).opened))
        old = nightly_issues.MARKER.sub(lambda m: f"<!-- sirix-nightly-change: series={m.group(1)} night=2026-11-01 -->", body)
        self.assertEqual(1, len(self.open(self.analysis(), FakeGitHub([{"body": old}])).opened))

    def test_no_issue_inside_the_reserved_window(self) -> None:
        outcome = self.open(self.analysis(), now=MIDDAY)
        self.assertEqual([], self.client.created)
        self.assertIn("reserved", outcome.refused)
        self.assertIn("No issue opened", outcome.summary())

    def test_no_issue_when_the_call_could_run_into_the_window(self) -> None:
        just_before = datetime.datetime(2026, 12, 25, 2, 50, tzinfo=UTC)  # Friday 03:50 Berlin
        outcome = self.open(self.analysis(), now=just_before)
        self.assertEqual([], self.client.created)
        self.assertIsNotNone(outcome.refused)

    def test_a_steady_night_opens_nothing(self) -> None:
        outcome = self.open(self.analysis(cpu=quiet(24, 1000.0)))
        self.assertEqual([], self.client.created)
        self.assertIn("No change point needs an issue tonight.", outcome.summary())

    def test_a_hosted_wall_clock_change_opens_nothing(self) -> None:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", WALL): stepped(20, 4, 1000.0, 1.3)})
        self.open(nightly_analysis.analyse(ledger.rows, use_otava=False))
        self.assertEqual([], self.client.created)

    def test_an_otava_only_change_is_labelled_and_says_conv_304_has_not_confirmed_it(self) -> None:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): quiet(30, 1000.0)})
        with mock.patch.object(nightly_analysis, "otava_detect", return_value=[(26, 0.04, 1e-6, 1000.0, 1040.0)]), \
                mock.patch.object(nightly_analysis, "require_otava"):
            analysis = nightly_analysis.analyse(ledger.rows, use_otava=True)
        self.open(analysis)
        issue = self.client.created[0]
        self.assertIn("detector-otava-only", issue["labels"])
        self.assertIn("has **not** confirmed", issue["body"])
        self.assertIn("Otava change point", issue["body"])

    def test_a_runner_image_change_between_the_nights_is_called_out(self) -> None:
        analysis = self.analysis()
        for series in analysis["series"]:
            for entry in series["nights"]:
                entry["image"] = "ubuntu24/20261201.1" if entry["night"] < "2026-12-21" else "ubuntu24/20261215.1"
        self.open(analysis)
        self.assertIn("runner image changed", self.client.created[0]["body"])

    def test_a_flood_of_changes_is_capped_and_the_rest_waits_for_tomorrow(self) -> None:
        ledger = Ledger(self.environment)
        keys = [(f"Bench{index:02d}.run", CPU) for index in range(nightly_issues.MAX_NEW_ISSUES + 3)]
        ledger.history({key: stepped(20, 4, 1000.0, 1.05) for key in keys})
        outcome = self.open(nightly_analysis.analyse(ledger.rows, use_otava=False))
        self.assertEqual(nightly_issues.MAX_NEW_ISSUES, len(self.client.created))
        self.assertEqual(3, len(outcome.deferred))
        # The next night picks up the deferred ones: they were not recorded as seen.
        again = self.open(nightly_analysis.analyse(ledger.rows, use_otava=False), FakeGitHub(self.client.created))
        self.assertEqual(3, len(self.client.created))
        self.assertEqual(nightly_issues.MAX_NEW_ISSUES, len(again.covered))

    def test_a_dry_run_names_the_issues_and_opens_none(self) -> None:
        outcome = self.open(self.analysis(), dry_run=True)
        self.assertEqual([], self.client.created)
        self.assertEqual(1, len(outcome.opened))

    def test_two_changes_of_one_series_on_nearby_nights_are_one_issue(self) -> None:
        analysis = self.analysis()
        series = analysis["series"][0] if analysis["series"][0]["changes"] else analysis["series"][1]
        twin = dict(series["changes"][0], night="2026-12-22", detectors=["otava"], agreement="otava-only")
        series["changes"].append(twin)
        self.open(analysis)
        self.assertEqual(1, len(self.client.created))


class CommandLineTest(AnalysisTestCase):

    def test_a_fork_or_a_topic_branch_opens_nothing_and_exits_cleanly(self) -> None:
        analysis = self.base / "analysis.json"
        analysis.write_text(json.dumps(nightly_analysis.analyse([], use_otava=False)), encoding="utf-8")
        for environment in (
            {"GITHUB_REPOSITORY": "someone/sirix", "GITHUB_EVENT_NAME": "schedule", "GITHUB_REF": "refs/heads/main"},
            {"GITHUB_REPOSITORY": "sirixdb/sirix", "GITHUB_EVENT_NAME": "pull_request", "GITHUB_REF": "refs/pull/1/merge"},
            {},
        ):
            out = io.StringIO()
            with mock.patch.dict("os.environ", environment, clear=True), contextlib.redirect_stdout(out), \
                    mock.patch.object(nightly_issues, "GhCli", side_effect=AssertionError("must not be created")):
                code = nightly_issues.main(
                    ["--analysis", str(analysis), "--repository", environment.get("GITHUB_REPOSITORY", "sirixdb/sirix"),
                     "--dashboard", DASHBOARD]
                )  # fmt: skip
            self.assertEqual(0, code, environment)
            self.assertIn("No issue opened", out.getvalue())

    def test_a_repository_that_is_not_the_canonical_one_opens_nothing_even_in_a_good_context(self) -> None:
        analysis = self.base / "analysis.json"
        analysis.write_text(json.dumps(nightly_analysis.analyse([], use_otava=False)), encoding="utf-8")
        good = {"GITHUB_REPOSITORY": "someone/sirix", "GITHUB_EVENT_NAME": "schedule", "GITHUB_REF": "refs/heads/main"}
        out = io.StringIO()
        with mock.patch.dict("os.environ", good, clear=True), contextlib.redirect_stdout(out):
            code = nightly_issues.main(
                ["--analysis", str(analysis), "--repository", "someone/sirix", "--dashboard", DASHBOARD]
            )
        self.assertEqual(0, code)
        self.assertIn("canonical", out.getvalue())

    def test_the_gh_client_never_runs_without_a_label_and_creates_with_the_body_on_stdin(self) -> None:
        calls: list[tuple[list[str], str | None]] = []

        def fake_run(command: list[str], input: str | None = None, **_kwargs: Any) -> Any:  # noqa: A002
            calls.append((command, input))
            return mock.Mock(returncode=0, stdout="https://github.com/sirixdb/sirix/issues/321\n", stderr="")

        client = nightly_issues.GhCli("sirixdb/sirix")
        with mock.patch.object(nightly_issues.subprocess, "run", side_effect=fake_run):
            number = client.create_issue("title", "body text", ["benchmark-regression", "nightly-benchmark"])
        self.assertEqual(321, number)
        command, stdin = calls[0]
        self.assertEqual("body text", stdin)
        self.assertIn("--body-file", command)
        self.assertEqual(["benchmark-regression", "nightly-benchmark"], [command[i + 1] for i, a in enumerate(command) if a == "--label"])
        self.assertEqual(["--repo", "sirixdb/sirix"], command[-2:])

    def test_a_failing_gh_call_is_an_error_not_a_silent_skip(self) -> None:
        with mock.patch.object(
            nightly_issues.subprocess, "run", return_value=mock.Mock(returncode=1, stdout="", stderr="HTTP 403")
        ):
            with self.assertRaises(nightly_issues.GitHubError):
                nightly_issues.GhCli("sirixdb/sirix").issues()


class IssueDetailTest(AnalysisTestCase):

    def analysis(self) -> dict[str, Any]:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 1.05)})
        return nightly_analysis.analyse(ledger.rows, use_otava=False)

    def body(self, analysis: dict[str, Any]) -> str:
        client = FakeGitHub()
        nightly_issues.open_issues(analysis, client, repository="sirixdb/sirix", dashboard=DASHBOARD, now=EVENING)
        return client.created[0]["body"]

    def marker_for(self, analysis: dict[str, Any], night: str) -> str:
        change = nightly_analysis.actionable(analysis)[0]
        return f"<!-- sirix-nightly-change: series={change['series']['id']} night={night} -->"

    def test_a_change_is_covered_within_seven_days_and_not_at_eight(self) -> None:
        analysis = self.analysis()  # the change night is 2026-12-21
        for night, covered in (("2026-12-14", True), ("2026-12-28", True), ("2026-12-13", False), ("2026-12-29", False)):
            client = FakeGitHub([{"number": 1, "body": self.marker_for(analysis, night)}])
            outcome = nightly_issues.open_issues(analysis, client, repository="sirixdb/sirix", dashboard=DASHBOARD, now=EVENING)
            self.assertEqual(covered, not outcome.opened, night)

    def test_the_cap_is_ten_new_issues_a_run(self) -> None:
        self.assertEqual(10, nightly_issues.MAX_NEW_ISSUES)
        self.assertEqual(7, nightly_issues.DEDUP_DAYS)
        self.assertEqual(datetime.timedelta(minutes=20), nightly_issues.GUARD_MARGIN)

    def test_the_window_margin_is_twenty_minutes(self) -> None:
        ok = datetime.datetime(2026, 12, 25, 2, 39, 59, tzinfo=UTC)  # Friday 03:39:59 Berlin
        refused = datetime.datetime(2026, 12, 25, 2, 40, 0, tzinfo=UTC)
        self.assertEqual(1, len(nightly_issues.open_issues(self.analysis(), FakeGitHub(), repository="sirixdb/sirix", dashboard=DASHBOARD, now=ok).opened))
        self.assertIsNotNone(nightly_issues.open_issues(self.analysis(), FakeGitHub(), repository="sirixdb/sirix", dashboard=DASHBOARD, now=refused).refused)

    def test_the_body_states_the_nights_the_commits_and_the_series(self) -> None:
        body = self.body(self.analysis())
        self.assertIn(f"| Last good night | 2026-12-20 at `{'14' * 6}` |", body)
        self.assertIn(f"| First night at the new level | 2026-12-21 at `{'15' * 6}` |", body)
        self.assertIn(f"[{'14' * 6}...{'15' * 6}](https://github.com/sirixdb/sirix/compare/{'14' * 20}...{'15' * 20})", body)
        self.assertIn("parameters shape=WIDE", body)
        self.assertIn("state on the latest night is `drift`", body)
        self.assertIn("CONV-304 confirms this change", body)
        self.assertIn("Apache Otava:** reports no change point here", body)
        self.assertNotIn("runner image changed", body)

    def test_the_body_reports_the_state_of_the_latest_night_and_both_detectors_when_they_agree(self) -> None:
        analysis = self.analysis()
        for series in analysis["series"]:
            series["nights"][-1]["state"] = "unstable"
            series["nights"][-2]["state"] = "drift"
            for change in series["changes"]:
                change["detectors"] = ["conv-304", "otava"]
        body = self.body(analysis)
        self.assertIn("state on the latest night is `unstable`", body)
        self.assertIn("Apache Otava:** reports a change point at the same place", body)
        self.assertNotIn("reports no change point here", body)

    def test_only_the_image_of_the_last_good_night_and_of_the_first_new_night_are_compared(self) -> None:
        analysis = self.analysis()
        for series in analysis["series"]:
            for entry in series["nights"]:
                entry["image"] = "old" if entry["night"] == "2026-12-20" else "new"
        self.assertIn("`old` to `new`", self.body(analysis))

    def test_a_series_without_parameters_says_none(self) -> None:
        analysis = self.analysis()
        for series in analysis["series"]:
            series["params"] = {}
        self.assertIn("parameters none", self.body(analysis))

    def test_an_image_that_did_not_change_or_is_unknown_is_not_called_out(self) -> None:
        for before, after in (("a", "a"), ("a", None), (None, "b"), (None, None)):
            analysis = self.analysis()
            for series in analysis["series"]:
                for entry in series["nights"]:
                    entry["image"] = before if entry["night"] < "2026-12-21" else after
            self.assertNotIn("runner image changed", self.body(analysis), (before, after))

    def test_a_change_from_zero_is_titled_so(self) -> None:
        analysis = self.analysis()
        actionable = nightly_analysis.actionable(analysis)[0]
        self.assertTrue(nightly_issues.issue_title({**actionable, "relative_change": None}).endswith("from zero (2026-12-21)"))

    def test_an_analysis_without_a_change_does_not_even_ask_github(self) -> None:
        client = FakeGitHub()
        calls: list[str] = []
        client.issues = lambda: calls.append("issues") or []  # type: ignore[method-assign]
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): quiet(24, 1000.0)})
        nightly_issues.open_issues(nightly_analysis.analyse(ledger.rows, use_otava=False), client, repository="sirixdb/sirix", dashboard=DASHBOARD, now=EVENING)
        self.assertEqual([], calls)

    def test_a_dry_run_reports_what_it_would_open_and_a_second_change_of_the_series_is_covered(self) -> None:
        analysis = self.analysis()
        series = next(item for item in analysis["series"] if item["changes"])
        series["changes"].append(dict(series["changes"][0], night="2026-12-22", detectors=["otava"], agreement="otava-only"))
        outcome = nightly_issues.open_issues(analysis, FakeGitHub(), repository="sirixdb/sirix", dashboard=DASHBOARD, now=EVENING, dry_run=True)
        self.assertEqual([0], [number for number, _title in outcome.opened])
        self.assertEqual(1, len(outcome.covered))

    def test_the_labels_are_created_before_the_issue(self) -> None:
        client = FakeGitHub()
        nightly_issues.open_issues(self.analysis(), client, repository="sirixdb/sirix", dashboard=DASHBOARD, now=EVENING)
        self.assertEqual(["benchmark-regression", "nightly-benchmark"], client.labels)


class MainDetailTest(AnalysisTestCase):
    GOOD = {"GITHUB_REPOSITORY": "sirixdb/sirix", "GITHUB_EVENT_NAME": "schedule", "GITHUB_REF": "refs/heads/main"}

    def analysis_file(self) -> Path:
        ledger = Ledger(self.environment)
        ledger.history({("AlphaBenchmark.encode", CPU): stepped(20, 4, 1000.0, 1.05)})
        path = self.base / "analysis.json"
        path.write_text(json.dumps(nightly_analysis.analyse(ledger.rows, use_otava=False)), encoding="utf-8")
        return path

    def run_main(self, environment: dict[str, str], *arguments: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.dict("os.environ", environment, clear=True), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            return nightly_issues.main(list(arguments)), out.getvalue(), err.getvalue()

    def test_the_repository_argument_must_be_the_canonical_one_even_in_a_good_context(self) -> None:
        with mock.patch.object(nightly_issues, "GhCli", side_effect=AssertionError("must not be created")):
            code, text, _ = self.run_main(self.GOOD, "--analysis", str(self.analysis_file()), "--repository", "other/repo", "--dashboard", DASHBOARD)
        self.assertEqual(0, code)
        self.assertIn("other/repo is not the canonical repository sirixdb/sirix", text)

    def test_a_dry_run_works_outside_a_publishing_context_and_opens_nothing(self) -> None:
        client = FakeGitHub()
        with mock.patch.object(nightly_issues, "GhCli", return_value=client):
            code, text, _ = self.run_main(
                {}, "--analysis", str(self.analysis_file()), "--repository", "sirixdb/sirix", "--dashboard", DASHBOARD,
                "--dry-run", "--now", EVENING.isoformat(),
            )  # fmt: skip
        self.assertEqual(0, code)
        self.assertEqual([], client.created)
        self.assertIn("opened #0: Nightly benchmark regression", text)

    def test_the_instant_can_be_given_and_the_window_then_applies(self) -> None:
        code, text, _ = self.run_main(
            {}, "--analysis", str(self.analysis_file()), "--repository", "sirixdb/sirix", "--dashboard", DASHBOARD,
            "--dry-run", "--now", MIDDAY.isoformat(),
        )  # fmt: skip
        self.assertEqual(0, code)
        self.assertIn("No issue opened: reserved", text)

    def test_the_issues_are_opened_through_gh_in_a_good_context(self) -> None:
        client = FakeGitHub()
        with mock.patch.object(nightly_issues, "GhCli", return_value=client):
            code, text, _ = self.run_main(
                self.GOOD, "--analysis", str(self.analysis_file()), "--repository", "sirixdb/sirix", "--dashboard", DASHBOARD,
                "--now", EVENING.isoformat(),
            )  # fmt: skip
        self.assertEqual((0, 1), (code, len(client.created)))
        self.assertIn("opened #100", text)

    def test_an_unreadable_analysis_or_a_failing_github_is_exit_two(self) -> None:
        code, _text, err = self.run_main(self.GOOD, "--analysis", str(self.base / "none.json"), "--repository", "sirixdb/sirix", "--dashboard", DASHBOARD)
        self.assertEqual(2, code)
        self.assertTrue(err)
        broken = mock.Mock()
        broken.issues.side_effect = nightly_issues.GitHubError("HTTP 500")
        with mock.patch.object(nightly_issues, "GhCli", return_value=broken):
            code, _text, err = self.run_main(
                self.GOOD, "--analysis", str(self.analysis_file()), "--repository", "sirixdb/sirix", "--dashboard", DASHBOARD,
                "--now", EVENING.isoformat(),
            )  # fmt: skip
        self.assertEqual((2, True), (code, "HTTP 500" in err))

    def test_the_required_arguments_are_required(self) -> None:
        for missing in ("--analysis", "--repository", "--dashboard"):
            arguments = {"--analysis": "a.json", "--repository": "sirixdb/sirix", "--dashboard": DASHBOARD}
            arguments.pop(missing)
            flat = [item for pair in arguments.items() for item in pair]
            with self.subTest(missing), self.assertRaises(SystemExit) as caught, contextlib.redirect_stderr(io.StringIO()):
                nightly_issues.main(flat)
            self.assertEqual(2, caught.exception.code)

    def test_gh_runs_capture_text_and_never_raise_by_itself(self) -> None:
        captured: dict[str, Any] = {}

        def fake_run(command: list[str], **kwargs: Any) -> Any:
            captured.update(kwargs)
            return mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(nightly_issues.subprocess, "run", side_effect=fake_run):
            self.assertEqual([], nightly_issues.GhCli("sirixdb/sirix").issues())
        self.assertEqual((True, True, False), (captured["capture_output"], captured["text"], captured["check"]))

    def test_a_gh_failure_names_the_call_and_the_reason(self) -> None:
        failing = mock.Mock(returncode=1, stdout="", stderr=" HTTP 403 ")
        with mock.patch.object(nightly_issues.subprocess, "run", return_value=failing):
            with self.assertRaises(nightly_issues.GitHubError) as caught:
                nightly_issues.GhCli("sirixdb/sirix").issues()
        self.assertEqual("gh issue list failed: HTTP 403", str(caught.exception))
        failing.stderr = ""
        failing.stdout = "stdout reason"
        with mock.patch.object(nightly_issues.subprocess, "run", return_value=failing):
            with self.assertRaises(nightly_issues.GitHubError) as caught:
                nightly_issues.GhCli("sirixdb/sirix").issues()
        self.assertIn("stdout reason", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
