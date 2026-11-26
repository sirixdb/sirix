#!/usr/bin/env python3
"""Behaviour tests of the branch publisher (nightly_publish.py) against a local bare repository."""

from __future__ import annotations

import contextlib
import datetime
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import nightly_publish  # noqa: E402
from nightly_publish import PublishError  # noqa: E402

UTC = datetime.timezone.utc
EVENING = datetime.datetime(2026, 12, 24, 20, 17, tzinfo=UTC)  # Thursday 21:17 Berlin
MIDDAY = datetime.datetime(2026, 12, 24, 10, 0, tzinfo=UTC)  # Thursday 11:00 Berlin
GOOD_CONTEXT = {"GITHUB_REPOSITORY": "sirixdb/sirix", "GITHUB_EVENT_NAME": "schedule", "GITHUB_REF": "refs/heads/main"}


def git(*arguments: str, cwd: Path | None = None) -> str:
    return subprocess.run(["git", *arguments], cwd=cwd, capture_output=True, text=True, check=True).stdout


class PublishTestCase(unittest.TestCase):

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name)
        self.remote = str(self.base / "remote.git")
        git("init", "--quiet", "--bare", "--initial-branch", "main", self.remote)

    def show(self, branch: str, name: str) -> str:
        return git("--git-dir", self.remote, "show", f"{branch}:{name}")

    def commits(self, branch: str) -> int:
        return int(git("--git-dir", self.remote, "rev-list", "--count", branch).strip())

    def ledger(self, text: str) -> Path:
        path = self.base / "ledger.jsonl"
        path.write_text(text, encoding="utf-8")
        return path


class DataBranchTest(PublishTestCase):

    def test_a_missing_branch_pulls_as_an_empty_ledger(self) -> None:
        destination = self.base / "pulled" / "ledger.jsonl"
        self.assertFalse(nightly_publish.pull_data(self.remote, "nightly-data", destination))
        self.assertEqual("", destination.read_text(encoding="utf-8"))

    def test_the_first_push_creates_the_branch_with_the_ledger_and_a_note(self) -> None:
        self.assertTrue(nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-24", now=EVENING))
        self.assertEqual("a\n", self.show("nightly-data", "ledger.jsonl"))
        self.assertIn("append-only", self.show("nightly-data", "README.md"))

    def test_each_night_is_one_commit_and_the_ledger_only_grows(self) -> None:
        nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-24", now=EVENING)
        nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\nb\n"), "2026-12-25", now=EVENING)
        self.assertEqual("a\nb\n", self.show("nightly-data", "ledger.jsonl"))
        self.assertEqual(2, self.commits("nightly-data"))
        self.assertIn("nightly: ledger of 2026-12-25", git("--git-dir", self.remote, "log", "-1", "--format=%s", "nightly-data"))

    def test_an_unchanged_ledger_stores_nothing(self) -> None:
        nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-24", now=EVENING)
        self.assertFalse(nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-25", now=EVENING))
        self.assertEqual(1, self.commits("nightly-data"))

    def test_a_rewritten_or_shortened_ledger_is_refused(self) -> None:
        nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\nb\n"), "2026-12-24", now=EVENING)
        for text in ("a\nX\n", "a\n", "", "Xa\nb\n"):
            with self.subTest(text), self.assertRaises(PublishError) as caught:
                nightly_publish.push_data(self.remote, "nightly-data", self.ledger(text), "2026-12-25", now=EVENING)
            self.assertIn("append-only", str(caught.exception))
        self.assertEqual("a\nb\n", self.show("nightly-data", "ledger.jsonl"))

    def test_the_ledger_pulled_back_is_the_ledger_pushed(self) -> None:
        nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\nb\n"), "2026-12-24", now=EVENING)
        destination = self.base / "back.jsonl"
        self.assertTrue(nightly_publish.pull_data(self.remote, "nightly-data", destination))
        self.assertEqual("a\nb\n", destination.read_text(encoding="utf-8"))

    def test_nothing_is_pushed_inside_the_reserved_window_or_just_before_it(self) -> None:
        for now in (MIDDAY, datetime.datetime(2026, 12, 25, 2, 50, tzinfo=UTC)):  # 11:00 and Friday 03:50 Berlin
            with self.subTest(now.isoformat()), self.assertRaises(PublishError) as caught:
                nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-24", now=now)
            self.assertIn("refused: reserved", str(caught.exception))
        self.assertEqual("", git("--git-dir", self.remote, "branch", "--list", "nightly-data"))

    def test_a_missing_ledger_file_is_an_error(self) -> None:
        with self.assertRaises(PublishError):
            nightly_publish.push_data(self.remote, "nightly-data", self.base / "none", "2026-12-24", now=EVENING)


class PagesBranchTest(PublishTestCase):

    def site(self, text: str) -> Path:
        site = self.base / "site"
        site.mkdir(exist_ok=True)
        (site / "index.html").write_text(text, encoding="utf-8")
        (site / ".nojekyll").write_text("", encoding="utf-8")
        return site

    def test_the_pages_branch_is_one_commit_that_every_night_replaces(self) -> None:
        nightly_publish.push_pages(self.remote, "gh-pages", self.site("<p>one</p>"), now=EVENING)
        nightly_publish.push_pages(self.remote, "gh-pages", self.site("<p>two</p>"), now=EVENING)
        self.assertEqual("<p>two</p>", self.show("gh-pages", "index.html"))
        self.assertEqual(1, self.commits("gh-pages"))

    def test_the_window_guard_applies_to_the_dashboard_too(self) -> None:
        with self.assertRaises(PublishError):
            nightly_publish.push_pages(self.remote, "gh-pages", self.site("x"), now=MIDDAY)

    def test_a_directory_without_a_page_is_refused(self) -> None:
        with self.assertRaises(PublishError):
            nightly_publish.push_pages(self.remote, "gh-pages", self.base, now=EVENING)


class CommandLineTest(PublishTestCase):

    def run_main(self, *arguments: str, environment: dict[str, str] | None = None) -> tuple[int, str]:
        out = io.StringIO()
        with mock.patch.dict(os.environ, environment if environment is not None else GOOD_CONTEXT), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            return nightly_publish.main(list(arguments)), out.getvalue()

    def test_a_push_needs_the_canonical_publishing_context(self) -> None:
        arguments = ["push-data", "--remote", self.remote, "--ledger", str(self.ledger("a\n")), "--night", "2026-12-24",
                     "--canonical", "sirixdb/sirix", "--now", EVENING.isoformat()]  # fmt: skip
        self.assertEqual(0, self.run_main(*arguments)[0])
        for bad in (
            {**GOOD_CONTEXT, "GITHUB_REPOSITORY": "someone/sirix"},
            {**GOOD_CONTEXT, "GITHUB_REF": "refs/heads/topic"},
            {**GOOD_CONTEXT, "GITHUB_EVENT_NAME": "pull_request"},
        ):
            code, text = self.run_main(*arguments[:-2], "--now", EVENING.isoformat(), environment=bad)
            self.assertEqual(nightly_publish.nightly_window.REFUSED, code, text)

    def test_the_window_makes_the_command_exit_three(self) -> None:
        code, text = self.run_main(
            "push-data", "--remote", self.remote, "--ledger", str(self.ledger("a\n")), "--night", "2026-12-24",
            "--canonical", "sirixdb/sirix", "--now", MIDDAY.isoformat(),
        )  # fmt: skip
        self.assertEqual(3, code)
        self.assertIn("reserved", text)

    def test_pull_needs_no_context_because_it_publishes_nothing(self) -> None:
        code, _text = self.run_main("pull-data", "--remote", self.remote, "--dest", str(self.base / "l.jsonl"), environment={})
        self.assertEqual(0, code)

    def test_the_token_reaches_git_as_a_header_and_never_a_file(self) -> None:
        captured: list[list[str]] = []
        real = subprocess.run

        def spy(command: list[str], *args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
            captured.append(list(command))
            return real(command, *args, **kwargs)  # type: ignore[call-overload]

        with mock.patch.object(nightly_publish.subprocess, "run", side_effect=spy):
            nightly_publish._git(["ls-remote", self.remote], token="s3cret")  # noqa: SLF001
        self.assertTrue(any(arg.startswith("http.extraheader=AUTHORIZATION: basic ") for arg in captured[0]))
        self.assertNotIn("s3cret", " ".join(captured[0]))  # only its base64 form is on the command line


class GuardDetailTest(PublishTestCase):

    def test_the_margin_is_twenty_minutes_before_the_window_opens(self) -> None:
        # Friday 2026-12-25, Berlin = UTC+1. A push starting at 03:39:59 ends before 04:00; one at 03:40:00 would not.
        ok = datetime.datetime(2026, 12, 25, 2, 39, 59, tzinfo=UTC)
        refused = datetime.datetime(2026, 12, 25, 2, 40, 0, tzinfo=UTC)
        self.assertTrue(nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-24", now=ok))
        with self.assertRaises(PublishError):
            nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\nb\n"), "2026-12-25", now=refused)

    def test_a_git_failure_is_a_publish_error_that_says_what_failed(self) -> None:
        with self.assertRaises(PublishError) as caught:
            nightly_publish._git(["clone", "--quiet", str(self.base / "no-such-remote"), str(self.base / "x")])  # noqa: SLF001
        self.assertIn("git clone --quiet failed", str(caught.exception))

    def test_a_git_failure_message_is_text_not_bytes(self) -> None:
        with self.assertRaises(PublishError) as caught:
            nightly_publish._git(["clone", "--quiet", str(self.base / "no-such-remote"), str(self.base / "x")])  # noqa: SLF001
        self.assertNotIn("b'", str(caught.exception))

    def test_a_push_to_a_remote_that_refuses_is_an_error(self) -> None:
        refusing = self.base / "refusing.git"
        git("init", "--quiet", "--bare", "--initial-branch", "main", str(refusing))
        hook = refusing / "hooks" / "pre-receive"
        hook.write_text("#!/bin/sh\necho refused by policy >&2\nexit 1\n", encoding="utf-8")
        hook.chmod(0o755)
        with self.assertRaises(PublishError) as caught:
            nightly_publish.push_data(str(refusing), "nightly-data", self.ledger("a\n"), "2026-12-24", now=EVENING)
        self.assertIn("refused by policy", str(caught.exception))

    def test_the_commit_is_authored_by_the_actions_bot(self) -> None:
        nightly_publish.push_data(self.remote, "nightly-data", self.ledger("a\n"), "2026-12-24", now=EVENING)
        author = git("--git-dir", self.remote, "log", "-1", "--format=%an <%ae>", "nightly-data").strip()
        self.assertEqual("github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>", author)

    def test_a_pull_creates_the_directories_of_its_destination(self) -> None:
        destination = self.base / "a" / "b" / "ledger.jsonl"
        nightly_publish.pull_data(self.remote, "nightly-data", destination)
        self.assertTrue(destination.is_file())

    def test_the_token_comes_from_the_environment_only_when_set(self) -> None:
        self.assertEqual("t0ken", nightly_publish._token({"NIGHTLY_PUSH_TOKEN": "t0ken"}))  # noqa: SLF001
        self.assertIsNone(nightly_publish._token({"NIGHTLY_PUSH_TOKEN": ""}))  # noqa: SLF001
        self.assertIsNone(nightly_publish._token({}))  # noqa: SLF001


class CommandLineDetailTest(PublishTestCase):

    def run_main(self, *arguments: str) -> tuple[int, str]:
        out = io.StringIO()
        with mock.patch.dict(os.environ, GOOD_CONTEXT), contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            return nightly_publish.main(list(arguments)), out.getvalue()

    def test_the_default_branches_and_the_messages(self) -> None:
        now = ["--now", EVENING.isoformat()]
        ledger = str(self.ledger("a\n"))
        code, text = self.run_main("push-data", "--remote", self.remote, "--ledger", ledger, "--night", "2026-12-24",
                                   "--canonical", "sirixdb/sirix", *now)  # fmt: skip
        self.assertEqual((0, "ledger pushed to nightly-data\n"), (code, text))
        code, text = self.run_main("push-data", "--remote", self.remote, "--ledger", ledger, "--night", "2026-12-25",
                                   "--canonical", "sirixdb/sirix", *now)  # fmt: skip
        self.assertEqual((0, "nothing new to store\n"), (code, text))
        site = self.base / "site"
        site.mkdir()
        (site / "index.html").write_text("<p>x</p>", encoding="utf-8")
        code, text = self.run_main("push-pages", "--remote", self.remote, "--site", str(site), "--canonical", "sirixdb/sirix", *now)
        self.assertEqual((0, "dashboard pushed to gh-pages\n"), (code, text))
        self.assertEqual("<p>x</p>", self.show("gh-pages", "index.html"))
        destination = self.base / "pulled.jsonl"
        code, text = self.run_main("pull-data", "--remote", self.remote, "--dest", str(destination))
        self.assertEqual((0, f"pulled {destination}\n"), (code, text))
        code, text = self.run_main("pull-data", "--remote", self.remote, "--branch", "none", "--dest", str(destination))
        self.assertEqual((0, f"no such branch; empty ledger at {destination}\n"), (code, text))

    def test_an_error_that_is_not_a_refusal_is_exit_two(self) -> None:
        code, text = self.run_main("push-data", "--remote", self.remote, "--ledger", str(self.base / "none"),
                                   "--night", "2026-12-24", "--canonical", "sirixdb/sirix", "--now", EVENING.isoformat())  # fmt: skip
        self.assertEqual(2, code)
        self.assertIn("is not a ledger file", text)

    def test_the_required_arguments_are_required(self) -> None:
        complete = {
            "push-data": ["--remote", self.remote, "--ledger", "l", "--night", "n", "--canonical", "c"],
            "push-pages": ["--remote", self.remote, "--site", "s", "--canonical", "c"],
            "pull-data": ["--remote", self.remote, "--dest", "d"],
        }
        for command, arguments in complete.items():
            pairs = list(zip(arguments[::2], arguments[1::2]))
            for index, (name, _value) in enumerate(pairs):
                remaining = [item for k, pair in enumerate(pairs) if k != index for item in pair]
                with self.subTest(command=command, missing=name), self.assertRaises(SystemExit) as caught, \
                        contextlib.redirect_stderr(io.StringIO()):
                    nightly_publish.main([command, *remaining])
                self.assertEqual(2, caught.exception.code)


if __name__ == "__main__":
    unittest.main()
