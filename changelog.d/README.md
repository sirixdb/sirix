# Changelog and lesson fragments

A pull request does not edit [`CHANGELOG.md`](../CHANGELOG.md) or
[`docs/LESSONS.md`](../docs/LESSONS.md). It adds **one fragment file per entry**, and the release
collects the fragments into those files. Two pull requests then never touch the same lines, so a
merge on `main` no longer forces every other open branch to rebase and revalidate.

| Entry | Fragment directory |
| --- | --- |
| A changelog entry | `changelog.d/` |
| A lesson from a bug | [`docs/lessons.d/`](../docs/lessons.d/README.md) |

## File names

A fragment is a `.md` file named after its branch or its topic — `hot-leaf-bounds.md`,
`fmt-microsecond-timestamps.md`. Names match `[a-z0-9][a-z0-9._-]*\.md`.

A name made only of digits is rejected: a pull-request number is not known when the branch starts,
and guessing one is how two branches end up claiming the same file. `README.md` is this
documentation, not a fragment.

## Format

Front matter delimited by `---` lines, then the entry body:

```markdown
---
section: Fixed
---
- **Range scans no longer drop rows below a routed leaf.** ...
```

- The first line is exactly `---`. Front-matter lines are `key: value`; the block ends at the next
  line that is exactly `---`.
- The body is everything after that line, taken **verbatim** — the collector inserts those bytes and
  reformats nothing. Write the entry exactly as it should read in the collected file, with the same
  bullet style, wrapping and indentation as its neighbours.
- Links in the body are relative to the collected destination (`CHANGELOG.md` or
  `docs/LESSONS.md`), not to the fragment's directory.
- Leading blank lines after the front matter and trailing whitespace are stripped, and the body is
  appended directly after the entry that is already last, with no blank line inserted between them.
  An empty body is an error.

### Changelog fragments (`changelog.d/`)

One key, `section`, naming the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) section the
entry belongs in: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed` or `Security`. The collector
appends the body to that `###` section of `## [Unreleased]`, creating the section in that canonical
order if the release has none yet.

### Lesson fragments (`docs/lessons.d/`)

One key, `kind`:

- `kind: row` — the body is one or more complete table rows (`| failure | apply next time |
  evidence |`), appended to the table at the top of `docs/LESSONS.md`. Every body line starts and
  ends with `|`.
- `kind: note` — the body is markdown appended at the end of `docs/LESSONS.md`, for a lesson that
  does not fit the table.

Extending an existing lesson still edits `docs/LESSONS.md` in place, as that file asks: the
recurring failure mode belongs in the entry that already describes it, and changing a line two
branches rarely share does not cause the conflicts this convention removes.

## Collecting at release time

```bash
python3 .github/scripts/changelog_fragments.py collect        # writes and deletes the fragments
```

`collect` validates every fragment first and writes nothing if any of them is invalid. It inserts
entries and changes no other line of either target file: with no fragments present, both files are
left byte-for-byte unchanged. Commit the collected files and the fragment deletions together — that
commit is the one the guard below exempts.

The collector is an in-repo standard-library Python script rather than
[towncrier](https://towncrier.readthedocs.io/): towncrier renders a new release section from a
template and would reflow these hand-wrapped, multi-paragraph entries instead of preserving them
byte for byte, it cannot render the `docs/LESSONS.md` table at all, and it would add a Python
dependency to a repository whose build has none.

## The guard

`python3 .github/scripts/changelog_fragments.py check --base-sha <merge-base>` runs on every pull
request (the `Changelog fragments` job of `.github/workflows/gradle.yml`). It validates every
fragment, and it fails when the pull request adds an entry line — a `- ` bullet, a `|` table row or
a `##`/`###` heading — to `CHANGELOG.md` or `docs/LESSONS.md` instead of adding a fragment.
`--base-sha` is required so every check guards the shared files.

Reworking or extending a line that is already there is allowed, and so is the release commit, which
is recognized by the fragment deletions in its own diff.
