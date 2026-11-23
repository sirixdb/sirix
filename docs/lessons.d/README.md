# Lesson fragments

A pull request adds its lesson as one fragment file here instead of editing
[`LESSONS.md`](../LESSONS.md), so two branches never append to the same lines. The release collects
the fragments into that file.

A fragment is a `.md` file named after its branch or topic, with front matter and a verbatim body:

```markdown
---
kind: row
---
| A borrowed reference was folded onto a lone child's own slot. | Register every reference the fold can swizzle, including the one the lone child owns. | [HOT lower bound](postmortems/2026-11-hot-lower-bound-below-leaf.md), `HotFoldBoundaryTest` |
```

- `kind: row` — one or more complete table rows for the table at the top of `LESSONS.md`.
- `kind: note` — markdown appended at the end of `LESSONS.md`, for a lesson the table cannot hold.

Links in the body are relative to the collected destination, `docs/LESSONS.md`, not to this
directory: use `postmortems/...` for evidence under `docs/postmortems/`.

Extending an existing lesson still edits `LESSONS.md` in place, as that file asks.

The full convention, the collector and the pull-request guard are documented in
[`changelog.d/README.md`](../../changelog.d/README.md).
