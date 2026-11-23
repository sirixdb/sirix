# Lesson fragments

A pull request adds a new lesson here; a release collects it into [`LESSONS.md`](../LESSONS.md).
The [fragment convention](../../changelog.d/README.md) owns the naming, format, link resolution,
collection and guard rules, including how to extend an existing lesson.

A fragment is a `.md` file named after its branch or topic, with front matter and a verbatim body:

```markdown
---
kind: row
---
| A borrowed reference was folded onto a lone child's own slot. | Register every reference the fold can swizzle, including the one the lone child owns. | [HOT lower bound](postmortems/2026-11-hot-lower-bound-below-leaf.md), `HotFoldBoundaryTest` |
```

The example's `postmortems/...` link follows the convention's
[collected-destination rule](../../changelog.d/README.md#format).
