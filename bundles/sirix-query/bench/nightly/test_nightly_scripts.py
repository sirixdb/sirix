#!/usr/bin/env python3
"""Every nightly script runs standalone, from any directory, the way the workflow calls it."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = (
    "nightly_run.py",
    "nightly_analysis.py",
    "nightly_dashboard.py",
    "nightly_issues.py",
    "nightly_publish.py",
    "nightly_window.py",
)


class ScriptTest(unittest.TestCase):

    def run_script(self, name: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as elsewhere:
            return subprocess.run(
                [sys.executable, "-B", str(HERE / name), *arguments],
                cwd=elsewhere, capture_output=True, text=True, check=False,
            )  # fmt: skip

    def test_every_script_prints_its_usage_from_another_directory(self) -> None:
        for name in SCRIPTS:
            with self.subTest(name):
                result = self.run_script(name, "--help")
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertIn("usage:", result.stdout)

    def test_the_window_guard_script_answers_with_its_exit_status(self) -> None:
        open_now = self.run_script("nightly_window.py", "check", "--now", "2026-12-24T20:17:00+00:00")
        self.assertEqual(0, open_now.returncode)
        self.assertIn("open", open_now.stdout)
        reserved = self.run_script("nightly_window.py", "check", "--now", "2026-12-24T10:00:00+00:00")
        self.assertEqual(3, reserved.returncode)

    def test_a_script_without_its_arguments_is_a_usage_error(self) -> None:
        for name in SCRIPTS:
            with self.subTest(name):
                self.assertEqual(2, self.run_script(name).returncode)


if __name__ == "__main__":
    unittest.main()
