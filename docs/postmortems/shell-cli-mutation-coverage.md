# Bug: CLI query-error dispatch was outside the mutation baseline

- Fix / issue: PR #1357, PIT run `43771748471`, job `132990181693`.
- Symptom: the changed top-level query-error conditional had no coverage; two
  equivalent boundary mutants also survived.
- Trigger: a syntax or evaluation error through `-q`, `-qf` or redirected stdin.
- Root cause: command execution and process termination shared `main`, so an
  in-process error test would terminate PIT's baseline JVM. Subprocess tests do
  not exercise the parent JVM's mutated classes.
- Why tests missed it: in-process tests covered successful commands and interactive
  recovery, while process tests covered successful redirected output only.
- Regression tests: `MainTest` checks syntax/evaluation diagnostics and failure
  status through every batch dispatch, generic I/O failure, successful commands,
  interactive recovery and actual subprocess failure status. Expected error codes
  come from the query language and the existing CLI diagnostic/exit contract.
- Lesson: separate command outcomes from the process exit boundary so failure
  behavior is executable under mutation testing; see [Lessons](../LESSONS.md).

## Specification and path inventory

Every batch query failure must emit its existing diagnostic and return failure;
successful commands return zero. This applies to inline `-q`, file `-qf`, explicit
stdin `-qf -`, and implicit stdin. Interactive `-iq` and file-interactive `-iqf`
retain their existing per-query recovery. Generic setup/file I/O failures retain
their diagnostic and failure status. `main` translates the returned nonzero
command status into process termination. Legacy invalid-option usage handling
is unchanged. No argument, presentation or query semantics change.

Derived tests exercise malformed `1+` (XPST0003), `1 idiv 0` (FOAR0001), a
missing query file, existing successful dispatches and interactive recovery.
A wrong branch must lose the query diagnostic, status, or both; these are
observable assertions rather than source inspection.

## Equivalent mutants in the hosted report

| Class / method / original line | Mutation | Equivalence proof |
| --- | --- | --- |
| `ShellOutput.diffMode:77` | Loop boundary `<` to `<=` | The preceding guard rejects odd token counts. The loop starts at even index 2 and advances by 2; `size - 1` is odd, so equality is impossible for both JSON and XML, including all supported arities. |
| `ShellPrompt.boundPrefix:131` | Minimum token count `>= 14` to `> 14` | Only exactly 14 tokens differ. A bound document call ends at index 9 (latest) or 11 (historical); store calls end at 11. Historical/store bindings are rejected by `supportedSuffix`. Latest bindings start their suffix at 10, and `simpleUpdate` rejects `start + 4 >= size`, including size 14. Thus both comparisons ultimately return no reference for that size. |

These are individual justifications, not a scope exclusion or a weakened gate.
The reporter has no equivalence allowlist. Remove the redundant boundaries:
iterate argument indexes below the already-even token count in `diffMode`, and
check the six-token literal-call minimum once relative to the call's start in
`reference`, for both direct and bound forms. Empty/incomplete input stays neutral;
all supported JSON/XML doc/store/update forms and diff arities stay unchanged.
The prefix helper then only recognizes the binding syntax.

The first local rerun killed query-error-path mutations but sampled a previously
uncovered console `isTerminal()` conditional. The console boundary must distinguish
absent, nonterminal and terminal consoles: only the terminal case acquires the
initial shared terminal, including when colours are disabled. Explicit `-iq`
retains its existing fallback terminal. Pass the console
from `main` into the command runner and exercise both console states with external
boundary mocks, asserting output routing and terminal lifetime. Redirected input
continues to exercise the absent-console case. Default interactive dispatch uses
the acquired terminal rather than repeating the console decision.

## Trace after the correction

Batch inputs enter `main` / `run`, initialize stores/output, and dispatch through
`executeUserQueries` to `executeQuery` (via `executeFileQuery` for files).
Compilation and evaluation errors reach the outer query diagnostic and return
`-1`; file-read errors reach the I/O diagnostic and return `-1`. Resource cleanup
completes before either status returns. Successful commands return zero without
an exit call; `-iq` and terminal-default queries still catch errors in their loop.
The error formatter's generic helper has no query branch because its sole caller
already handles QueryException. All callers now reach process exit only through
the returned command status, except unchanged invalid-option usage handling.

Prompt observation still calls `reference` before metadata lookup. Its shared
relative minimum protects every literal-call access, including incomplete bound
input; unsupported short suffixes still fail `simpleUpdate`. Supported direct
doc/store and bound updates reach the same metadata and revision decisions.
`ShellOutput.result` still calls `diffMode` once before constructing the writer;
the even-size guard means the revised loop visits exactly the same arguments for
four-argument JSON/XML and five-/six-argument JSON calls, skipping the final `)`.

## Verification evidence

- `build/ci-shell-mutation-tests.log` and
  `build/ci-shell-mutation/shell-test-results/`: all 57 shell/CLI tests pass,
  including the new in-process error/console assertions and subprocess exit test.
- `build/ci-shell-mutation/first-rerun/`: the error test kills mutations in the
  command runner; the two equivalent mutants and uncovered terminal check remain.
- `build/ci-shell-mutation/summary.md`, `results.json` and
  `sirix-query/report/mutations.xml`: the final bounded CI-equivalent run completes
  with exit zero, 107 changed-line mutants, 105 killed and two detected run errors,
  zero survivors and zero uncovered mutants on changed lines. The console
  conditional is killed by the new console-state test. The whole report has 127
  mutants; one survivor and seven uncovered mutants on unchanged lines are advisory.
  Selection uses base `9a6e1f8995aa684a40372180754c970d10166289` and head
  `eb7c489e3f1a0daf090e5efc31986bee29a9ab2e`, with the working-tree line map,
  followed by `pr_mutation.py run --seconds 1320 --blocking`. No scope, threshold
  or status rule changes.
- `build/ci-shell-work-budgets.log`: the documented query work-budget command
  (`io.sirix.query.budget.*`, `IndexedArrayPositionTest`,
  `BitemporalLoaderBudgetTest`) passes 327 tests with five existing conditional
  skips, zero failures/errors. `:sirix-query:spotlessJavaCheck` also passes.
