---
section: Changed
---
- **Changelog and lesson entries arrive as fragment files.** A pull request adds one file under
  `changelog.d/` or `docs/lessons.d/` instead of appending to `CHANGELOG.md` or `docs/LESSONS.md`,
  and a release collects the fragments with
  `python3 .github/scripts/changelog_fragments.py collect`. Two pull requests no longer touch the
  same lines, so merging one does not force the others to rebase and revalidate. Nothing a user of
  the library observes changes; see [`changelog.d/README.md`](changelog.d/README.md).
