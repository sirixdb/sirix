# Bug: `sirix-shell -iqf` could not run any query

- Fix / issue: [#1470](https://github.com/sirixdb/sirix/issues/1470), alongside the #1430 fix
  (PR #1472).
- Symptom: `-iqf` ("query files") never executed a query.
  - `sirix-shell -iqf queries.xq`: the file path was rejected as an unknown option, so the usage
    text was printed and the process exited.
  - `sirix-shell -iqf` alone: the shell read `Path.of(null)` and failed with `Error: null`.
- Trigger: any use of `-iqf`.
- Root cause: the option was declared without a value, while its loop expected a file name from
  that value. The loop's only exit was `readFile` returning `null`, which it never does, so a
  working file would have re-run the same text forever. There was no `-` (stdin) handling, although
  the help text promised it. Earlier terminal work recorded the path as unreachable legacy code in
  mutation reviews ([shell CI boundaries](shell-ci-boundaries.md),
  [follow-up](shell-ci-follow-up.md)) instead of repairing or removing it.
- Contract now (`Main.executeQueryScript`):
  - `-iqf <file>` reads a query script from the file; `-iqf -` reads it from stdin.
  - Queries are separated by blank lines, the interactive end mark; repeated and whitespace-only
    separator lines are skipped, and the last query needs no trailing blank line.
  - Queries run in order in one session with the #1430 failure boundary: a coded report, pending
    updates discarded, and the script continues.
  - No prompt is printed. The exit status is 0 if every query succeeded and -1 otherwise; an
    unreadable script reports `I/O Error` with -1.
- Why tests missed it: no test invoked `-iqf`, and the mutation reviews accepted "no supported
  public invocation reaches this call" as a reason rather than as a defect.
- Regression test: `MainTest.queryScriptsRunEveryBlankLineSeparatedQueryInOneSessionAndReportFailuresInTheStatus`.
  It covers CRLF input, a multi-line query, repeated and whitespace-only separators, a failing query
  after a queued update, a success exit through stdin `-`, and an unreadable file. Before the fix the
  test JVM exited through `printUsage`.
- Lesson: an unreachable public entry point is a defect to repair or remove, not a mutation
  disposition; see [Lessons](../LESSONS.md).
