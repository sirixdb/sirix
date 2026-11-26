#!/usr/bin/env python3
"""Behaviour tests of the reserved-window guard (nightly_window.py) and of the nightly workflow's
schedule against it, across daylight saving time."""

from __future__ import annotations

import contextlib
import datetime
import io
import re
import sys
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))

import nightly_window  # noqa: E402
from nightly_window import WindowError, is_reserved, verdict  # noqa: E402

UTC = datetime.timezone.utc
BERLIN = ZoneInfo("Europe/Berlin")
WORKFLOW = Path(__file__).resolve().parents[4] / ".github" / "workflows" / "nightly-bench.yml"


def utc(text: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(text).replace(tzinfo=UTC)


def berlin(text: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(text).replace(tzinfo=BERLIN)


class ReservedTest(unittest.TestCase):

    def test_a_weekday_is_reserved_from_four_until_half_past_five(self) -> None:
        # 2026-11-26 is a Thursday, in winter time (UTC+1).
        cases = {
            "2026-11-26T03:59:59": False,
            "2026-11-26T04:00:00": True,
            "2026-11-26T12:00:00": True,
            "2026-11-26T17:29:59": True,
            "2026-11-26T17:30:00": False,
            "2026-11-26T23:59:59": False,
        }
        for local, expected in cases.items():
            with self.subTest(local):
                self.assertEqual(expected, is_reserved(berlin(local)))

    def test_every_hour_of_every_weekday_and_weekend_day(self) -> None:
        monday = datetime.date(2026, 11, 23)
        for offset in range(7):
            day = monday + datetime.timedelta(days=offset)
            for hour in range(24):
                moment = datetime.datetime.combine(day, datetime.time(hour, 45), BERLIN)
                # At minute 45 the window covers the hours 04 to 16 of a weekday (17:45 is past 17:30).
                expected = offset < 5 and 4 <= hour <= 16
                with self.subTest(day=day.isoformat(), hour=hour):
                    self.assertEqual(expected, is_reserved(moment))

    def test_the_weekend_is_never_reserved(self) -> None:
        for local in ("2026-11-28T04:00:00", "2026-11-28T12:00:00", "2026-11-29T17:00:00"):
            self.assertFalse(is_reserved(berlin(local)), local)

    def test_the_instant_matters_not_the_zone_it_is_written_in(self) -> None:
        # 03:00 UTC is 04:00 in winter and 05:00 in summer Berlin: both reserved. 02:59 UTC is 03:59 in
        # winter (open) and 04:59 in summer (reserved).
        self.assertTrue(is_reserved(utc("2026-11-26T03:00:00")))
        self.assertFalse(is_reserved(utc("2026-11-26T02:59:00")))
        self.assertTrue(is_reserved(utc("2026-07-16T02:00:00")))  # 04:00 CEST, a Thursday
        self.assertFalse(is_reserved(utc("2026-07-16T01:59:00")))  # 03:59 CEST
        self.assertTrue(is_reserved(utc("2026-07-16T15:29:00")))  # 17:29 CEST
        self.assertFalse(is_reserved(utc("2026-07-16T15:30:00")))  # 17:30 CEST
        self.assertTrue(is_reserved(utc("2026-11-26T16:29:00")))  # 17:29 CET
        self.assertFalse(is_reserved(utc("2026-11-26T16:30:00")))  # 17:30 CET

    def test_the_change_of_the_clock_does_not_move_the_window(self) -> None:
        # Sunday 2026-03-29 02:00 CET becomes 03:00 CEST; Monday the 30th is the first summer weekday.
        self.assertFalse(is_reserved(utc("2026-03-30T01:59:00")))  # Monday 03:59 CEST
        self.assertTrue(is_reserved(utc("2026-03-30T02:00:00")))  # Monday 04:00 CEST
        self.assertFalse(is_reserved(utc("2026-03-27T02:59:00")))  # Friday 03:59 CET, the week before
        self.assertTrue(is_reserved(utc("2026-03-27T03:00:00")))
        # Sunday 2026-10-25 03:00 CEST becomes 02:00 CET; Monday the 26th is the first winter weekday.
        self.assertFalse(is_reserved(utc("2026-10-26T02:59:00")))  # Monday 03:59 CET
        self.assertTrue(is_reserved(utc("2026-10-26T03:00:00")))  # Monday 04:00 CET
        # The ambiguous local hour of the autumn change is a Sunday and open either way.
        self.assertFalse(is_reserved(utc("2026-10-25T00:30:00")))
        self.assertFalse(is_reserved(utc("2026-10-25T01:30:00")))

    def test_a_moment_without_a_zone_is_refused(self) -> None:
        with self.assertRaises(WindowError):
            is_reserved(datetime.datetime(2026, 11, 26, 12, 0))


class VerdictTest(unittest.TestCase):

    def test_a_margin_refuses_a_start_that_would_run_into_the_window(self) -> None:
        # Monday 03:50 Berlin (02:50 UTC in winter): open now, reserved ten minutes later.
        now = utc("2026-11-30T02:50:00")
        self.assertTrue(verdict(now)[0])
        self.assertTrue(verdict(now, margin=datetime.timedelta(minutes=9))[0])
        allowed, reason = verdict(now, margin=datetime.timedelta(minutes=11))
        self.assertFalse(allowed)
        self.assertIn("04:00", reason)

    def test_the_longer_of_margin_and_duration_counts(self) -> None:
        now = utc("2026-11-30T02:50:00")
        self.assertFalse(verdict(now, duration=datetime.timedelta(minutes=30))[0])
        self.assertFalse(verdict(now, margin=datetime.timedelta(minutes=5), duration=datetime.timedelta(minutes=30))[0])

    def test_a_span_over_the_whole_window_is_refused_even_when_both_ends_are_open(self) -> None:
        self.assertFalse(verdict(utc("2026-11-26T02:00:00"), duration=datetime.timedelta(hours=20))[0])

    def test_the_reason_names_when_publication_resumes(self) -> None:
        allowed, reason = verdict(utc("2026-11-26T10:00:00"))
        self.assertFalse(allowed)
        self.assertIn("2026-11-26T17:30:00+01:00", reason)

    def test_negative_spans_are_invalid(self) -> None:
        with self.assertRaises(WindowError):
            verdict(utc("2026-11-26T20:00:00"), margin=datetime.timedelta(minutes=-1))

    def test_next_open_is_half_past_five_on_a_weekday_and_the_instant_otherwise(self) -> None:
        self.assertEqual(berlin("2026-11-26T17:30:00"), nightly_window.next_open(utc("2026-11-26T10:00:00")))
        open_moment = utc("2026-11-26T20:00:00")
        self.assertEqual(open_moment, nightly_window.next_open(open_moment))


class PublishContextTest(unittest.TestCase):
    GOOD = {"GITHUB_REPOSITORY": "sirixdb/sirix", "GITHUB_EVENT_NAME": "schedule", "GITHUB_REF": "refs/heads/main"}

    def allowed(self, **changes: str) -> bool:
        return nightly_window.publish_context({**self.GOOD, **changes}, canonical="sirixdb/sirix")[0]

    def test_only_the_canonical_default_branch_on_a_schedule_or_dispatch_publishes(self) -> None:
        self.assertTrue(self.allowed())
        self.assertTrue(self.allowed(GITHUB_EVENT_NAME="workflow_dispatch"))
        self.assertFalse(self.allowed(GITHUB_REPOSITORY="someone/sirix"))
        self.assertFalse(self.allowed(GITHUB_EVENT_NAME="pull_request"))
        self.assertFalse(self.allowed(GITHUB_EVENT_NAME="push"))
        self.assertFalse(self.allowed(GITHUB_REF="refs/heads/topic"))
        self.assertFalse(self.allowed(GITHUB_REF="refs/pull/7/merge"))

    def test_outside_github_actions_nothing_publishes(self) -> None:
        self.assertFalse(nightly_window.publish_context({}, canonical="sirixdb/sirix")[0])


class CommandLineTest(unittest.TestCase):

    def run_main(self, *arguments: str) -> tuple[int, str]:
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = nightly_window.main(list(arguments))
        return code, out.getvalue()

    def test_exit_status_tells_open_from_reserved(self) -> None:
        self.assertEqual(0, self.run_main("check", "--now", "2026-11-26T20:17:00+00:00")[0])
        code, text = self.run_main("check", "--now", "2026-11-26T10:00:00+00:00")
        self.assertEqual(nightly_window.REFUSED, code)
        self.assertIn("reserved", text)

    def test_a_malformed_or_zoneless_instant_is_exit_two(self) -> None:
        self.assertEqual(2, self.run_main("check", "--now", "yesterday")[0])
        self.assertEqual(2, self.run_main("check", "--now", "2026-11-26T20:17:00")[0])


class WorkflowScheduleTest(unittest.TestCase):
    """The cron and the time limit of nightly-bench.yml keep the whole job outside the window."""

    GITHUB_START_DELAY = datetime.timedelta(hours=2)  # scheduled runs can start late under load

    def setUp(self) -> None:
        self.text = WORKFLOW.read_text(encoding="utf-8")
        cron = re.search(r"cron:\s*'(\d+) (\d+) \* \* \*'", self.text)
        self.assertIsNotNone(cron, "the workflow has a daily cron")
        self.minute, self.hour = int(cron.group(1)), int(cron.group(2))
        limit = re.search(r"timeout-minutes:\s*(\d+)", self.text)
        self.assertIsNotNone(limit)
        self.timeout = datetime.timedelta(minutes=int(limit.group(1)))

    def test_the_job_ends_outside_the_window_on_every_day_of_three_years(self) -> None:
        day = datetime.date(2026, 1, 1)
        while day < datetime.date(2029, 1, 1):
            fire = datetime.datetime.combine(day, datetime.time(self.hour, self.minute), UTC)
            late = fire + self.GITHUB_START_DELAY
            end = late + self.timeout
            # Publication happens at the end of the job: the whole span from the scheduled time to the
            # latest finish must be open, and so must every minute of it for a weekday.
            self.assertIsNone(
                nightly_window.first_reserved(fire, end), f"{day}: {fire.isoformat()} .. {end.isoformat()}"
            )
            day += datetime.timedelta(days=1)

    def test_the_cron_does_not_fire_at_the_top_of_the_hour(self) -> None:
        # GitHub delays scheduled runs most at the top of the hour.
        self.assertNotIn(self.minute, (0, 30))

    def test_the_workflow_cannot_run_for_a_pull_request_and_publishes_only_from_the_canonical_repository(self) -> None:
        self.assertNotIn("pull_request", self.text)
        self.assertIn("if: github.repository == 'sirixdb/sirix'", self.text)
        self.assertIn("persist-credentials: false", self.text)
        for command in ("push-data", "push-pages"):
            self.assertRegex(self.text, rf"{command}[\s\S]*?--canonical")
        self.assertRegex(self.text, r"nightly_window\.py\" check --margin-minutes \d+ --canonical")

    def test_every_action_is_pinned_to_a_commit(self) -> None:
        for line in self.text.splitlines():
            match = re.match(r"\s*uses:\s*(\S+)", line)
            if match:
                self.assertRegex(match.group(1), r"@[0-9a-f]{40}$", line)

    def test_nothing_is_fetched_from_a_third_party_service(self) -> None:
        hosts = set(re.findall(r"https?://([^/\s\"')]+)", self.text))
        self.assertLessEqual(hosts, {"github.com", "sirix.io"})


if __name__ == "__main__":
    unittest.main()
