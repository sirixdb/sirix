# Bug: shell CI retained native argument and analyzer path assumptions

- Fix / issue: PR #1357; Windows and Sonar failures in run `43745087457`.
- Symptom: the Windows output golden expected `商店😀` but received `????` despite
  explicitly UTF-8 output streams. Sonar's reliability gate still rejected the
  renderer's buffer access after the first drain simplification.
- Trigger: a Unicode query transported through native JVM argv on Windows; a
  symbolic execution path from `put()` through the empty branch in `drain()`.
- Root cause: the output test coupled its Unicode payload to native argument
  conversion, a separate boundary from output encoding. The analyzer independently
  assumed a full buffer at the caller and an empty buffer at the callee. Its trace
  is infeasible, but moving only the cursor reset did not remove the ambiguous
  branch from that call path.
- Why tests missed it: local Linux argv preserves the query. Runtime buffer tests
  cannot reproduce mutually contradictory symbolic assumptions.
- Regression tests: `MainTest.redirectedOutputIsGoldenRegardlessOfColourEnvironment`
  now supplies the same Unicode query in an explicitly UTF-8 file through `-qf`.
  The default, `--no-color`, and `NO_COLOR` modes retain the exact JSON/whitespace
  oracle. `cliQueryFileAndStdinDispatchProduceTheSamePayloadInProcess` still checks
  Unicode across argv, file and stdin. The original Windows failure is also
  reproduced locally under `LC_ALL=C`,
  where the JDK reports ASCII native encoding while keeping the default charset
  UTF-8. The original argv fixture produces question marks; the fixed query-file
  fixture passes all three colour modes in the same environment. Hosted Windows confirmation remains with
  the outer executor.
  `ShellResultWriterTest.failedFullBufferDrainRetainsPayloadAndEmptyFlushesDoNotWrite`
  injects an IOException in the downstream Writer, verifies propagation, retries,
  and asserts the exact independent payload with no zero-length writes. This
  behavior-preserving simplification passes that test before and after.
- Lesson: isolate native argument transport from an output-encoding golden;
  remove contradictory empty/full branches from a shared buffer boundary instead
  of repeatedly adjusting the same analyzer-obscured postcondition.

## Specification and complete path trace

The renderer must preserve every character and exception across all three write
interfaces, flush, finish, close and reuse. `drain()` is called only with data:
`put()` calls it for a full buffer; `flush()` checks for a nonempty buffer.
`drain()` writes and then resets the cursor unconditionally on normal return.
A failed write still leaves the cursor and buffered characters intact. Empty
flushes call only the downstream flush. `finish()` resolves deferred lexical
state and reaches `flush()`; `close()` reaches `finish()`. The uncoloured write
interfaces continue forwarding directly. No input forms or state are added.

The CLI file path reaches `executeFileQuery()`, `readFile()` (UTF-8),
`executeQuery()`, `ShellOutput.result()` and the existing charset-preserving
output adapter. The in-process test continues checking the ordinary `-q`, `-qf`
and stdin paths against the same Unicode golden.

## Mutation-check diagnosis

The hosted sampled report is complete: 141 mutants, 129 killed, seven run errors
(reported by PIT as detected), zero survivors, five uncovered. Four uncovered mutations are in unchanged CLI behavior; the
fifth is in the new outer query-error dispatch, which still exits the process:

| Class / method / original line | Mutation | Disposition |
| --- | --- | --- |
| `Main.executeFileInteractiveQuery:268` | Remove `executeQuery` | Legacy `-iqf` declares no value but expects a filename; no supported public invocation reaches this call. Repairing or removing that existing command is outside terminal presentation. |
| `Main.handleException:284` | Remove query diagnostic | The unchanged branch is unreachable from `main`: QueryException is routed to `handleQueryException` first. |
| `Main.handleException:288` | Remove generic diagnostic | Process-termination path; verifying its exit requires a subprocess, which does not supply parent-JVM PIT coverage. |
| `Main.initializeContextWithFile:153` | Remove XML initialization | Legacy `-f` initialization, unchanged by terminal presentation. |
| `Main.main:123` | Remove `handleQueryException` | Outer process-termination path; interactive recovery is independently covered in process, but a CLI error exits PIT's baseline JVM. |

These are coverage limitations, not equivalent mutants or assertion kills. The
mutation check remains non-green on that report. No scope exclusion, acceptance
change or artificial exit interception is introduced to hide them. The outer
executor owns hosted Windows/Sonar reruns and the disposition of these coverage
limitations, including the changed outer error-dispatch path.

The rebase onto `650263ffe30173ed39cf0862c6d614ae070c8a8f` conflicts only in
`docs/LESSONS.md`; preserve the new base's rows and every shell lesson.

## Verification evidence

Evidence is retained in `build/ci-current/` (ignored build output):

- `native-before.log` / `native-after.log`: the original fixture fails and the
  fixed fixture passes under the same ASCII native locale with UTF-8 stdout.
- `test-before.log`: the focused CLI/renderer baseline, including the new sink
  failure test, passes before the behavior-preserving drain simplification.
- `test-after.log`: the full shell suite and Spotless pass after the fix.
- `writer-pit/mutations.xml`: all 159 renderer mutants killed, zero survivors,
  zero uncovered and zero abnormal outcomes. This run mutates every renderer
  mutant, without sampling or method exclusions. The local init script changes
  only PIT memory limits and worker count.
- `final-checks.log`: Error Prone/NullAway, all 47 shell/CLI tests and
  327 query work-budget tests, and Spotless pass with zero failures. Five
  additional query tests are skipped by their existing conditions. The hosted Sonar reliability gate requires the
  outer executor's next analysis; this phase does not publish or rerun CI.
