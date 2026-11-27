#!/usr/bin/env python3
"""Open the regression issues of the nightly tier, once per change point.

Reads `analysis.json` (nightly_analysis.py) and opens an issue for every upward change of a judged
series that is confirmed (CONV-304 `drift`, or Apache Otava with two nights at the new level) and
that no issue covers yet:

    * one issue per change point: the issue carries a hidden marker with the series and the night,
      and a change that an existing issue, open or closed, names within DEDUP_DAYS of it is
      covered, so a detector that moves its change point by a night opens nothing new;
    * every issue is labelled, names both detectors' answers (CONV-304 is the normative judge, an
      Otava-only change says so), links the dashboard and the compare view of the commit range;
    * never from a fork, never from a branch other than the default branch, never inside the
      reserved weekday window (nightly_window.py), judged in code and not left to the cron;
    * at most MAX_NEW_ISSUES per run: a runner image that moves every series at once must not
      flood the tracker; what is left is retried the next night while the change is recent.

    nightly_issues.py --analysis analysis.json --repository OWNER/NAME --dashboard URL [--dry-run]

Exit status 0 (including "nothing to open" and "refused by the guard"), 2 invalid input or a failed
GitHub call. Standard library plus the `gh` command line tool.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import nightly_analysis  # noqa: E402
import nightly_window  # noqa: E402

MARKER = re.compile(r"<!-- sirix-nightly-change: series=([0-9a-f]{12}) night=(\d{4}-\d{2}-\d{2}) -->")
LABELS = {
    "benchmark-regression": ("b60205", "A nightly benchmark series drifted up"),
    "nightly-benchmark": ("1d76db", "Opened by the nightly benchmark tier"),
}
OTAVA_ONLY_LABEL = "detector-otava-only"
DEDUP_DAYS = 7
MAX_NEW_ISSUES = 10
GUARD_MARGIN = datetime.timedelta(minutes=20)


class GitHub(Protocol):
    def issues(self) -> list[dict[str, Any]]: ...

    def ensure_labels(self, names: Iterable[str]) -> None: ...

    def create_issue(self, title: str, body: str, labels: Sequence[str]) -> int: ...


class GhCli:
    """The GitHub operations the step needs, through the `gh` command line tool the runner ships."""

    def __init__(self, repository: str) -> None:
        self.repository = repository

    def _gh(self, arguments: Sequence[str], stdin: str | None = None) -> str:
        result = subprocess.run(
            ["gh", *arguments, "--repo", self.repository], input=stdin, capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            raise GitHubError(f"gh {' '.join(arguments[:2])} failed: {result.stderr.strip() or result.stdout.strip()}")
        return result.stdout

    def issues(self) -> list[dict[str, Any]]:
        listed = self._gh(
            ["issue", "list", "--label", "nightly-benchmark", "--state", "all", "--limit", "1000",
             "--json", "number,title,body,state"]  # fmt: skip
        )
        return json.loads(listed or "[]")

    def ensure_labels(self, names: Iterable[str]) -> None:
        for name in names:
            color, description = LABELS.get(name, ("fbca04", "Only Apache Otava reports this change"))
            self._gh(["label", "create", name, "--color", color, "--description", description, "--force"])

    def create_issue(self, title: str, body: str, labels: Sequence[str]) -> int:
        arguments = ["issue", "create", "--title", title, "--body-file", "-"]
        for label in labels:
            arguments += ["--label", label]
        url = self._gh(arguments, stdin=body).strip().splitlines()[-1]
        return int(url.rsplit("/", 1)[-1])


class GitHubError(RuntimeError):
    """A GitHub call failed."""


@dataclass
class Outcome:
    opened: list[tuple[int, str]] = field(default_factory=list)  # (issue number, title)
    covered: list[str] = field(default_factory=list)  # changes an existing issue already names
    deferred: list[str] = field(default_factory=list)  # over the per-run cap
    refused: str | None = None  # why nothing was opened at all

    def summary(self) -> str:
        lines = ["### Regression issues"]
        if self.refused:
            lines.append(f"No issue opened: {self.refused}")
        lines += [f"- opened #{number}: {title}" for number, title in self.opened]
        lines += [f"- already covered: {item}" for item in self.covered]
        lines += [f"- over the cap of {MAX_NEW_ISSUES}, retried tomorrow: {item}" for item in self.deferred]
        if len(lines) == 1:
            lines.append("No change point needs an issue tonight.")
        return "\n".join(lines)


def covered_by(issues: Iterable[Mapping[str, Any]], series: str, night: str) -> bool:
    """True when an issue, open or closed, names this series within DEDUP_DAYS of the change night."""
    day = datetime.date.fromisoformat(night)
    for issue in issues:
        for found_series, found_night in MARKER.findall(issue.get("body") or ""):
            distance = abs((datetime.date.fromisoformat(found_night) - day).days)
            if found_series == series and distance <= DEDUP_DAYS:
                return True
    return False


def _percent(change: Mapping[str, Any]) -> str:
    relative = change["relative_change"]
    return "from zero" if relative is None else f"{100 * relative:+.1f}%"


def issue_title(change: Mapping[str, Any]) -> str:
    series = change["series"]
    params = ",".join(f"{name}={value}" for name, value in sorted(series["params"].items()))
    benchmark = f"{series['benchmark']}{{{params}}}" if params else series["benchmark"]
    what = nightly_analysis_title(series["metric"])
    return f"Nightly benchmark regression: {benchmark} {what} {_percent(change)} ({change['night']})"


def nightly_analysis_title(metric: str) -> str:
    return {"cpu_ns_per_op": "CPU time", "alloc_bytes_per_op": "allocation"}.get(metric, metric)


def _verdict_lines(change: Mapping[str, Any], latest: Mapping[str, Any] | None) -> list[str]:
    state = latest["state"] if latest else "no recorded night"
    lines = [f"- **CONV-304 (the normative judge):** state on the latest night is `{state}`."]
    if nightly_analysis.CONV_304 in change["detectors"]:
        lines.append("  CONV-304 confirms this change: drift above the 2% bound and above the series' own noise fence, "
                     "on two nights in a row.")  # fmt: skip
    else:
        lines.append("  CONV-304 has **not** confirmed this change. Only Apache Otava reports it, so treat it as advisory "
                     "until the next nights agree or disagree.")  # fmt: skip
    if nightly_analysis.OTAVA in change["detectors"]:
        lines.append("- **Apache Otava:** reports a change point at the same place.")
    else:
        lines.append("- **Apache Otava:** reports no change point here.")
    lines += [f"  - {detail}" for detail in change["details"]]
    return lines


def issue_body(change: Mapping[str, Any], *, repository: str, dashboard: str) -> str:
    series = change["series"]
    night_entries = series.get("nights", [])
    latest = night_entries[-1] if night_entries else None
    marker = f"<!-- sirix-nightly-change: series={series['id']} night={change['night']} -->"
    compare = f"https://github.com/{repository}/compare/{change['previous_commit']}...{change['commit']}"
    params = ", ".join(f"{name}={value}" for name, value in sorted(series["params"].items())) or "none"
    lines = [
        marker,
        f"The nightly benchmark tier saw **{nightly_analysis_title(series['metric'])}** of `{series['benchmark']}` "
        f"rise by **{_percent(change)}** between the nights of {change['previous_night']} and {change['night']}.",
        "",
        "| | |",
        "|---|---|",
        f"| Series | `{series['benchmark']}`, parameters {params}, metric `{series['metric']}` ({series['unit']}) |",
        f"| Conditions | `{series['conditions']}`, {series['binary']}, {series['runner']} runner, {series['runtime']} |",
        f"| Last good night | {change['previous_night']} at `{change['previous_commit'][:12]}` |",
        f"| First night at the new level | {change['night']} at `{change['commit'][:12]}` |",
        f"| Commit range | [{change['previous_commit'][:12]}...{change['commit'][:12]}]({compare}) |",
        f"| Dashboard | [{series['id']}]({dashboard.rstrip('/')}/#s-{series['id']}) |",
        "",
        "### What the two detectors say",
        *_verdict_lines(change, latest),
    ]
    image_before = next((n.get("image") for n in night_entries if n["night"] == change["previous_night"]), None)
    image_after = next((n.get("image") for n in night_entries if n["night"] == change["night"]), None)
    if image_before and image_after and image_before != image_after:
        lines += [
            "",
            f"> **The runner image changed between these nights** (`{image_before}` to `{image_after}`). "
            "A level change on that night may be the runner and not the code; check the other series of the same night.",
        ]
    lines += [
        "",
        "### What to do",
        "Under [CONV-304](https://github.com/" + repository + "/blob/main/spec/CONV-conventions.md#conv-304--nightly-trend-tier) "
        "a drift is answered by an investigation recorded in an issue. Find the commit in the range that moved the "
        "figure, and either fix it or record why the new level is the right one. Close this issue with the cause. "
        "Do not widen a bound to silence it.",
        "",
        "_Opened automatically by the nightly benchmark workflow. One issue per change point; a change this issue "
        f"names within {DEDUP_DAYS} days is not opened again._",
    ]
    return "\n".join(lines)


def open_issues(
    analysis: Mapping[str, Any],
    client: GitHub,
    *,
    repository: str,
    dashboard: str,
    now: datetime.datetime,
    dry_run: bool = False,
) -> Outcome:
    """Open the issues the analysis calls for. The window guard is judged here, not only in the workflow."""
    outcome = Outcome()
    allowed, reason = nightly_window.verdict(now, margin=GUARD_MARGIN)
    if not allowed:
        outcome.refused = reason
        return outcome
    changes = nightly_analysis.actionable(analysis)
    if not changes:
        return outcome
    existing = client.issues()
    # The series entry is kept whole for the body: the latest night's state and the runner images.
    by_id = {series["id"]: series for series in analysis["series"]}
    for change in sorted(changes, key=lambda item: (item["night"], item["series"]["id"])):
        series_id, night = change["series"]["id"], change["night"]
        title = issue_title(change)
        if covered_by(existing, series_id, night):
            outcome.covered.append(title)
            continue
        if len(outcome.opened) >= MAX_NEW_ISSUES:
            outcome.deferred.append(title)
            continue
        full = {**change, "series": by_id[series_id]}
        body = issue_body(full, repository=repository, dashboard=dashboard)
        labels = ["benchmark-regression", "nightly-benchmark"]
        if nightly_analysis.CONV_304 not in change["detectors"]:
            labels.append(OTAVA_ONLY_LABEL)
        number = 0  # a dry run opens nothing and names no issue
        if not dry_run:
            client.ensure_labels(labels)
            number = client.create_issue(title, body, labels)
        # What this run opened covers its own later changes as well: one issue per change point.
        existing.append({"number": number, "title": title, "body": body})
        outcome.opened.append((number, title))
    return outcome


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Open the nightly tier's regression issues.")
    parser.add_argument("--analysis", required=True, type=Path)
    parser.add_argument("--repository", required=True, help="OWNER/NAME of the repository the issues belong to")
    parser.add_argument("--canonical", default="sirixdb/sirix", help="the only repository that may receive issues")
    parser.add_argument("--dashboard", required=True, help="URL of the published dashboard")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--now", help="ISO 8601 instant with offset, for tests; default: now")
    args = parser.parse_args(argv)
    allowed, reason = nightly_window.publish_context(os.environ, canonical=args.canonical)
    if args.repository != args.canonical:
        allowed, reason = False, f"{args.repository} is not the canonical repository {args.canonical}"
    if not allowed and not args.dry_run:
        print(Outcome(refused=reason).summary())
        return 0
    try:
        analysis = json.loads(args.analysis.read_text(encoding="utf-8"))
        now = datetime.datetime.fromisoformat(args.now) if args.now else datetime.datetime.now(datetime.timezone.utc)
        outcome = open_issues(
            analysis, GhCli(args.repository), repository=args.repository, dashboard=args.dashboard, now=now,
            dry_run=args.dry_run,
        )  # fmt: skip
    except (OSError, json.JSONDecodeError, ValueError, GitHubError) as error:
        print(f"nightly_issues.py: {error}", file=sys.stderr)
        return 2
    print(outcome.summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
