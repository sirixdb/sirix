# Changelog and lesson fragments

A pull request adds each new entry as **one fragment file** instead of appending directly to
[`CHANGELOG.md`](../CHANGELOG.md) or [`docs/LESSONS.md`](../docs/LESSONS.md). The release
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

- The first line is `---`, ignoring surrounding whitespace. Front-matter lines are `key: value`;
  blank lines are ignored, and the block ends at the next `---` line, also ignoring surrounding
  whitespace.
- The body is everything after that line, taken **verbatim** — the collector inserts those bytes and
  reformats nothing. Write the entry exactly as it should read in the collected file, with the same
  bullet style, wrapping and indentation as its neighbours.
- Links in the body are relative to the collected destination (`CHANGELOG.md` or
  `docs/LESSONS.md`), not to the fragment's directory.
- Permanent documents should link to the collected destination rather than to a fragment that
  collection deletes.
- Leading blank lines after the front matter and trailing whitespace are stripped. An empty body
  is an error. Changelog entries and lesson rows are appended directly after existing content in
  their destination section or table; lesson notes have a blank line inserted before their body.

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
fragment, and it fails when the pull request adds an entry line — a `- ` bullet or a `##`/`###`
heading in either shared file, or a `|` table row in `docs/LESSONS.md` — instead of adding a fragment.
`--base-sha` is required so every check guards the shared files.
It must be a full 40-character lowercase hexadecimal commit hash, not a revision expression or
Git option. Both commands operate only on the repository containing the script; there is no
repository-root override.

Reworking or extending a line that is already there is allowed, and so is the release commit, which
is recognized separately for each shared file by deletion of at least one validly named fragment
directly in that file's fragment directory. Deleting documentation or a fragment for the other
shared file does not exempt an entry.
