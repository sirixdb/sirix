#!/usr/bin/env python3
"""The reserved window of the nightly benchmark tier, enforced in code (docs/DEVELOPMENT.md).

Nothing public is committed, pushed, published or opened on a weekday between 04:00 and 17:30
Europe/Berlin: the machine and the maintainers' attention are reserved for the working day. The
nightly workflow's cron places its job in the evening; this module is the second, independent
guard, evaluated in the zone itself so that daylight saving time cannot move a job into the window.

    reserved  Monday to Friday, 04:00 <= local time < 17:30 in Europe/Berlin
    open      every other instant: evenings, nights and the whole of Saturday and Sunday

The window is judged on the *local* date and clock in Berlin, never on a UTC offset: 04:00 Berlin is
03:00 UTC in winter and 02:00 UTC in summer, and the evening Berlin hours a cron fires in move the
same way. A weekday that begins at 00:00 Berlin is judged by that weekday: Saturday 03:00 is open
because Saturday is not a weekday, Monday 03:00 is open because it is before 04:00.

    nightly_window.py check [--now ISO8601] [--margin-minutes N] [--duration-minutes N]

Exit status 0: the instant, the instant plus the margin and everything between are open.
Exit status 3: the guard refuses; the reason names the reserved interval it met.
Standard library only.
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

ZONE = ZoneInfo("Europe/Berlin")
RESERVED_START = datetime.time(4, 0)
RESERVED_END = datetime.time(17, 30)
WEEKDAYS = range(0, 5)  # Monday is 0
REFUSED = 3
STEP = datetime.timedelta(minutes=1)


class WindowError(ValueError):
    """A moment that cannot be judged: it carries no time zone."""


def _local(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise WindowError(f"{moment.isoformat()} has no time zone; the window is judged on an exact instant")
    return moment.astimezone(ZONE)


def is_reserved(moment: datetime.datetime) -> bool:
    """True when the instant falls in the reserved window: a Berlin weekday from 04:00 until 17:30."""
    local = _local(moment)
    return local.weekday() in WEEKDAYS and RESERVED_START <= local.time() < RESERVED_END


def reserved_interval(moment: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
    """The reserved interval of the Berlin date of an instant, as aware datetimes in Berlin time."""
    local = _local(moment)
    day = local.date()
    return (
        datetime.datetime.combine(day, RESERVED_START, ZONE),
        datetime.datetime.combine(day, RESERVED_END, ZONE),
    )


def next_open(moment: datetime.datetime) -> datetime.datetime:
    """The first instant at or after `moment` that is open; `moment` itself when it already is."""
    local = _local(moment)
    if not is_reserved(local):
        return local
    return reserved_interval(local)[1]


def first_reserved(start: datetime.datetime, end: datetime.datetime) -> datetime.datetime | None:
    """The first reserved instant in [start, end], or None when the whole span is open.

    The span is sampled every minute and at its end. That is exact: a reserved stretch is thirteen
    and a half hours long, so a span that meets it holds one of the samples. The arithmetic is done in
    UTC, where adding a duration is always exact, never in a zone whose clock can jump.
    """
    start, end = start.astimezone(datetime.timezone.utc), end.astimezone(datetime.timezone.utc)
    if end < start:
        raise WindowError("the span ends before it starts")
    moment = start
    while moment < end:
        if is_reserved(moment):
            return moment
        moment += STEP
    return end if is_reserved(end) else None


def verdict(
    now: datetime.datetime,
    *,
    margin: datetime.timedelta = datetime.timedelta(0),
    duration: datetime.timedelta = datetime.timedelta(0),
) -> tuple[bool, str]:
    """(allowed, reason): may something public be done now, and still be finished `margin` later.

    `duration` is the time a longer step still needs; the whole span from now until now + max(margin,
    duration) must be open, so a publish that starts at 03:55 and takes ten minutes is refused.
    """
    if margin < datetime.timedelta(0) or duration < datetime.timedelta(0):
        raise WindowError("margin and duration cannot be negative")
    end = now + max(margin, duration)
    blocked = first_reserved(now, end)
    local = _local(now)
    if blocked is None:
        return True, f"open: {local.isoformat()} to {_local(end).isoformat()} is outside Mon-Fri 04:00-17:30 Berlin"
    return False, (
        f"reserved: {_local(blocked).isoformat()} falls in the weekday window "
        f"{RESERVED_START:%H:%M}-{RESERVED_END:%H:%M} Europe/Berlin; "
        f"public actions resume at {next_open(blocked).isoformat()}"
    )


PUBLISHING_EVENTS = ("schedule", "workflow_dispatch")
DEFAULT_REF = "refs/heads/main"


def publish_context(environment: Mapping[str, str], *, canonical: str) -> tuple[bool, str]:
    """May this GitHub Actions context publish: push the data, deploy the dashboard, open an issue?

    Only the canonical repository, never a fork; only a scheduled or manually dispatched run, never a
    pull request; only on the default branch, so a dispatch from a topic branch measures and
    publishes nothing.
    """
    repository = environment.get("GITHUB_REPOSITORY", "")
    if not repository:
        return False, "not running in GitHub Actions (GITHUB_REPOSITORY is unset)"
    if repository != canonical:
        return False, f"{repository} is not the canonical repository {canonical}; forks publish nothing"
    event = environment.get("GITHUB_EVENT_NAME", "")
    if event not in PUBLISHING_EVENTS:
        return False, f"event {event!r} is not a schedule or a manual dispatch"
    if environment.get("GITHUB_REF", "") != DEFAULT_REF:
        return False, f"ref {environment.get('GITHUB_REF', '')!r} is not {DEFAULT_REF}"
    return True, f"{repository} {event} on {DEFAULT_REF}"


def _parse_now(text: str | None) -> datetime.datetime:
    if text is None:
        return datetime.datetime.now(datetime.timezone.utc)
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError as error:
        raise WindowError(f"--now must be an ISO 8601 instant with a zone offset, got {text!r}") from error


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="The nightly tier's reserved window guard.")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check")
    check.add_argument("--now", help="the instant to judge, ISO 8601 with a zone offset; default: now")
    check.add_argument("--margin-minutes", type=int, default=0)
    check.add_argument("--duration-minutes", type=int, default=0)
    check.add_argument(
        "--canonical", metavar="OWNER/NAME", help="also require a publishing GitHub Actions context of this repository"
    )
    args = parser.parse_args(argv)
    if args.canonical:
        allowed, reason = publish_context(os.environ, canonical=args.canonical)
        if not allowed:
            print(f"refused: {reason}")
            return REFUSED
    try:
        allowed, reason = verdict(
            _parse_now(args.now),
            margin=datetime.timedelta(minutes=args.margin_minutes),
            duration=datetime.timedelta(minutes=args.duration_minutes),
        )
    except WindowError as error:
        print(f"nightly_window.py: {error}", file=sys.stderr)
        return 2
    print(reason)
    return 0 if allowed else REFUSED


if __name__ == "__main__":
    raise SystemExit(main())
