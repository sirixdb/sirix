#!/usr/bin/env python3
"""Store the nightly tier's history and dashboard on branches of the repository, outside the window.

    nightly_publish.py pull-data  --remote URL [--branch nightly-data] --dest FILE
    nightly_publish.py push-data  --remote URL [--branch nightly-data] --ledger FILE --night YYYY-MM-DD
                                  --canonical OWNER/NAME
    nightly_publish.py push-pages --remote URL [--branch gh-pages]     --site DIR --canonical OWNER/NAME

`nightly-data` holds one file, `ledger.jsonl`: the results ledger of CONV-303, append-only, one
commit per night, so the history outlives any workflow artifact. `gh-pages` holds the generated
dashboard as a single commit that every night replaces; it is a build product, not a history.

Publishing is something public, so it is judged here, in the code that does it:

    * `push-data` and `push-pages` refuse inside the reserved weekday window (nightly_window.py),
      and when the push could not finish before it starts;
    * they refuse outside a scheduled or dispatched run of the default branch of the canonical
      repository, so a fork or a topic branch publishes nothing;
    * `push-data` refuses a ledger that is not an append-only extension of the one on the branch
      (CONV-303: a writer MUST only append and MUST NOT rewrite or remove a line);
    * a token is passed to git as an HTTP header for the one command, never stored in a config file.

Standard library and the `git` command line tool.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))

import nightly_window  # noqa: E402

DATA_BRANCH = "nightly-data"
PAGES_BRANCH = "gh-pages"
LEDGER_NAME = "ledger.jsonl"
MARGIN = datetime.timedelta(minutes=20)  # a push that starts now must also be over before the window opens
IDENTITY = ("github-actions[bot]", "41898282+github-actions[bot]@users.noreply.github.com")
DATA_README = """# Nightly benchmark data

`ledger.jsonl` is the results ledger of the nightly benchmark tier
(spec/CONV-conventions.md, CONV-303): JSON Lines, append-only, one commit per recorded night.
The workflow `nightly-bench.yml` on `main` is its only writer. Do not edit it by hand.
"""


class PublishError(RuntimeError):
    """A publication was refused or git failed."""


def _git(arguments: Sequence[str], cwd: Path | None = None, token: str | None = None) -> str:
    command = ["git"]
    if token:
        header = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        command += ["-c", f"http.extraheader=AUTHORIZATION: basic {header}"]
    result = subprocess.run(
        [*command, *arguments], cwd=cwd, capture_output=True, text=True, check=False,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )  # fmt: skip
    if result.returncode != 0:
        raise PublishError(f"git {' '.join(arguments[:2])} failed: {(result.stderr or result.stdout).strip()[-400:]}")
    return result.stdout


def _has_branch(remote: str, branch: str, token: str | None) -> bool:
    return bool(_git(["ls-remote", "--heads", remote, f"refs/heads/{branch}"], token=token).strip())


def _guard(now: datetime.datetime | None, what: str) -> None:
    allowed, reason = nightly_window.verdict(now or datetime.datetime.now(datetime.timezone.utc), margin=MARGIN)
    if not allowed:
        raise PublishError(f"{what} refused: {reason}")


def pull_data(remote: str, branch: str, dest: Path, *, token: str | None = None) -> bool:
    """Write the branch's ledger to `dest`; an empty file when the branch does not exist yet. True if it did."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not _has_branch(remote, branch, token):
        dest.write_text("", encoding="utf-8")
        return False
    with tempfile.TemporaryDirectory() as work:
        _git(["clone", "--quiet", "--depth", "1", "--branch", branch, remote, work], token=token)
        shutil.copyfile(Path(work) / LEDGER_NAME, dest)
    return True


def _commit_all(work: Path, message: str) -> bool:
    _git(["add", "--all"], cwd=work)
    if not _git(["status", "--porcelain"], cwd=work).strip():
        return False
    _git(
        ["-c", f"user.name={IDENTITY[0]}", "-c", f"user.email={IDENTITY[1]}", "commit", "--quiet", "-m", message],
        cwd=work,
    )
    return True


def push_data(
    remote: str,
    branch: str,
    ledger: Path,
    night: str,
    *,
    token: str | None = None,
    now: datetime.datetime | None = None,
) -> bool:
    """Commit the ledger to the data branch if it grew. False when there is nothing new to store."""
    _guard(now, "publishing the ledger")
    if not ledger.is_file():
        raise PublishError(f"{ledger} is not a ledger file")
    new = ledger.read_bytes()
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory) / "data"
        if _has_branch(remote, branch, token):
            _git(["clone", "--quiet", "--branch", branch, remote, str(work)], token=token)
            old = (work / LEDGER_NAME).read_bytes()
            if not new.startswith(old):
                raise PublishError(
                    "the ledger is not an append-only extension of the one on the branch: "
                    "a ledger line may be added, never rewritten or removed (CONV-303)"
                )
        else:
            work.mkdir()
            _git(["init", "--quiet", "--initial-branch", branch], cwd=work)
            _git(["remote", "add", "origin", remote], cwd=work)
            (work / "README.md").write_text(DATA_README, encoding="utf-8")
        (work / LEDGER_NAME).write_bytes(new)
        if not _commit_all(work, f"nightly: ledger of {night}"):
            return False
        _guard(now, "pushing the ledger")  # the clock may have moved on while git cloned and committed
        _git(["push", "--quiet", "origin", f"HEAD:refs/heads/{branch}"], cwd=work, token=token)
    return True


def push_pages(
    remote: str,
    branch: str,
    site: Path,
    *,
    token: str | None = None,
    now: datetime.datetime | None = None,
) -> None:
    """Replace the Pages branch by one commit holding exactly the generated site."""
    _guard(now, "publishing the dashboard")
    if not (site / "index.html").is_file():
        raise PublishError(f"{site} holds no index.html")
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory) / "pages"
        shutil.copytree(site, work)
        _git(["init", "--quiet", "--initial-branch", branch], cwd=work)
        _git(["remote", "add", "origin", remote], cwd=work)
        _commit_all(work, "nightly: dashboard")
        _git(["push", "--quiet", "--force", "origin", f"HEAD:refs/heads/{branch}"], cwd=work, token=token)


def _token(environment: Mapping[str, str]) -> str | None:
    return environment.get("NIGHTLY_PUSH_TOKEN") or None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish the nightly ledger and dashboard to branches.")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("pull-data", "push-data", "push-pages"):
        command = commands.add_parser(name)
        command.add_argument("--remote", required=True)
        command.add_argument("--branch", default=PAGES_BRANCH if name == "push-pages" else DATA_BRANCH)
        command.add_argument("--now", help="ISO 8601 instant with offset, for tests")
    commands.choices["pull-data"].add_argument("--dest", required=True, type=Path)
    commands.choices["push-data"].add_argument("--ledger", required=True, type=Path)
    commands.choices["push-data"].add_argument("--night", required=True)
    commands.choices["push-pages"].add_argument("--site", required=True, type=Path)
    for name in ("push-data", "push-pages"):
        commands.choices[name].add_argument("--canonical", required=True, metavar="OWNER/NAME")
    args = parser.parse_args(argv)
    token = _token(os.environ)
    try:
        now = datetime.datetime.fromisoformat(args.now) if args.now else None
        if args.command != "pull-data":
            allowed, reason = nightly_window.publish_context(os.environ, canonical=args.canonical)
            if not allowed:
                raise PublishError(f"refused: {reason}")
        if args.command == "pull-data":
            existed = pull_data(args.remote, args.branch, args.dest, token=token)
            print(f"{'pulled' if existed else 'no such branch; empty ledger at'} {args.dest}")
        elif args.command == "push-data":
            stored = push_data(args.remote, args.branch, args.ledger, args.night, token=token, now=now)
            print(f"ledger pushed to {args.branch}" if stored else "nothing new to store")
        else:
            push_pages(args.remote, args.branch, args.site, token=token, now=now)
            print(f"dashboard pushed to {args.branch}")
    except (PublishError, nightly_window.WindowError, ValueError, OSError) as error:
        print(f"nightly_publish.py: {error}", file=sys.stderr)
        return nightly_window.REFUSED if "refused: " in str(error) else 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
