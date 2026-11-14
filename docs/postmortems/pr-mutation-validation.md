# PR mutation tooling: argument validation and missing coverage

- Evidence: [initial implementation](https://github.com/sirixdb/sirix/commit/48f3d9d2f04def3bd4bdc74ff9eed031c449a931),
  [quality-gate run](https://github.com/sirixdb/sirix/actions/runs/43613897463).
- Symptom: Sonar rejected the new tooling for argument injection risk and 0% new-code coverage;
  the tooling's unit tests and its live PIT proof had passed. No exploit was observed.
- Trigger: CLI revision values reached `git diff` without an allowlist. Argument lists prevent
  shell interpretation but do not stop a value beginning with `-` from becoming a Git option.
  Tests ran without collecting or handing Python coverage to Sonar.
- Root cause: the subprocess boundary assumed GitHub-supplied values and Git paths were trusted,
  and the existing Java coverage handoff was assumed to cover a new language.
- Why tests missed it: fixtures supplied only ordinary refs/paths; no hostile tracked filename or
  coverage-import acceptance check existed.
- Regression: `SelectionTest` rejects option-like revisions, traversal/absolute/option-like paths,
  paths absent from the head commit, and a hostile filename committed in a temporary real Git
  repository. These assertions failed before validation was added. `ReportingTest` also executes
  a real Python child with a shell-looking literal argument and verifies it remains data.
- Prevention: validate full commit IDs, check literal paths against the repository tree, keep
  argv lists and `shell=False`, and collect/import coverage whenever a new language enters CI.
  The independent Python lane publishes branch coverage and Sonar requires its XML.
- Lesson: [validate argument semantics and coverage handoffs](../LESSONS.md).
