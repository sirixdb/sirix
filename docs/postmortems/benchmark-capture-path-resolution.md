# Bug: benchmark capture guessed a filesystem for a symlink loop

- Fix / issue: PR #1491, failing `Benchmark protocol tooling` check; CONV-303.
- Symptom: the capture returned a filesystem string for an unresolved data path instead of an
  unavailable object; the regression test then raised `TypeError` on the CI runner.
- Trigger: capture a data path containing a self-link or a two-link cycle, at the leaf or in a
  parent, on Python 3.13 or later.
- Root cause: non-strict `Path.resolve()` stopped raising for symlink loops in Python 3.13.
  Mount selection therefore consumed a partially resolved path and guessed its filesystem.
  Strict resolution at the shared capture boundary rejects loops and missing targets before
  mount selection, while valid links still select the target's innermost mount.
- Why tests missed it: the loop regression passed on local Python 3.12, whose non-strict resolver
  raises `RuntimeError`. A mount-selection fixture also used a nonexistent target, obscuring the
  requirement that a filesystem reading needs a fully resolved path.
- Regression test: `CaptureDetailTest` exercises valid links, missing direct and linked targets,
  self-links and two-link cycles, including parent components. The oracle is CONV-303's prohibition
  on guessed probe values: invalid targets must be unavailable, and valid links must report the
  target mount. It rejects returning the root filesystem for an unresolved path.
  Before the fix, the missing-target test failed locally. Substituting `os.path.realpath` at the
  resolver boundary to reproduce Python 3.13's documented non-strict semantics reproduced the CI
  `TypeError` for all four loop cases. All 12 capture detail tests pass after the fix with that
  substitution. This is a compatibility simulation, not execution on a newer interpreter:
  only Python 3.12.3 is available on PATH.
- Mutation evidence: forcing the changed resolver argument from `strict=True` to `False` fails
  `test_a_missing_data_target_is_unavailable` through the public capture output. No survivor.
- Lesson: [LESSONS.md](../LESSONS.md), via `lessons.d/benchmark-capture-path-resolution.md`.
- Vision check: truthful environment evidence protects the vision's measurement discipline;
  this changes no acceptance threshold, engine path or performance default.

Verification command, from `bundles/sirix-query/bench/common`:
`python3 -B -m unittest discover -p 'test_*.py'`.
Result: all 198 tests pass on Python 3.12.3. The fragment validator and `git diff --check` also pass.

Python documents the version-dependent behavior in
[Path.resolve](https://docs.python.org/3/library/pathlib.html#pathlib.Path.resolve).
