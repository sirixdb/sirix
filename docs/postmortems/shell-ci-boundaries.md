# Bug: shell test evidence stopped at process and buffer boundaries

- Fix / issue: PR #1357, CI run `43737453237`.
- Symptom: Windows rejected the redirected-output test's query; PIT reported 24
  uncovered and four surviving mutations; Sonar rejected a buffer access.
- Trigger: pass embedded double quotes in a Windows `ProcessBuilder` argument;
  execute the CLI only in child JVMs during PIT; analyze a full buffer through
  `drain()`'s conditional cursor reset.
- Root cause: the subprocess fixture relied on native argument quoting and an
  implicit output encoding. A separate JVM also cannot supply PIT's in-process
  coverage. Sonar's buffer trace assumes `used == buffer.length` at `put()` and
  `used == 0` at `drain()`; those assumptions are incompatible, but the conditional
  reset obscures the normal-return postcondition.
- Why tests missed it: Linux preserves the original argument, child-process output
  tests do not instrument the parent, and long-payload tests did not explicitly
  enumerate the exact buffer boundary across all write overloads.
- Regression tests: `MainTest.redirectedOutputIsGoldenRegardlessOfColourEnvironment`
  uses JSONiq single-quoted literals and explicitly UTF-8 child streams, retaining
  the Unicode payload and all three colour-policy modes. `MainTest` also executes
  argument/file/stdin dispatch and interactive success/error/EOF through `main()`
  in PIT's JVM. Only terminal construction/input are mocked; queries, serializers,
  and captured output are real. `ShellResultWriterTest` checks 16383, 16384, 16385,
  and 32768 characters through String, char-array, and integer writes, flush,
  finish, close, and reuse. Its independent oracle is the input payload; a dropped,
  duplicated, or out-of-bounds character fails. The original Windows failure is
  recorded in the CI artifact; the boundary test already passes before the
  behavior-preserving cursor-reset simplification.
- Lesson: [test process and streaming boundaries explicitly](../LESSONS.md).

## Behaviour and invariants specified before the implementation

1. Native argument transport must preserve the test's query. Its output contract
   is explicitly UTF-8, regardless of the Windows default code page. Default,
   `--no-color`, and `NO_COLOR` all produce the same uncoloured JSON bytes.
2. A successful drain leaves `used == 0`, whether the buffer was empty, partial,
   or full. All writer overloads share this boundary. A downstream write failure
   still propagates before the cursor is cleared, as before.
3. First-line prompts and subsequent line separators depend on whether any input
   has been accumulated. An integer line counter adds no information. The lexical
   prefix must affect the next line's rendered style and reset for the next query.
4. An unmatched XML closing tag must not underflow XML depth; subsequent element
   text and tags must retain their respective styles, including one-character
   writes. This is a renderer input contract, not a claim that the string is a
   well-formed XML document.

## Path re-trace

- `executeInteractiveQuery()` calls `readStringFromScannerWithEndMark()`. Initially
  the empty buffer selects the prompt and empty lexical prefix. Every accepted
  nonempty line makes the buffer nonempty, so later lines get no prompt and exactly
  one line separator. Blank input and EOF finish the current query; interrupt
  returns null. The counter and its equivalent increment mutant are removed.
- Every coloured write overload reaches `accept()` and `put()`. At a full buffer,
  `put()` calls `drain()`, which writes the pending characters and unconditionally
  clears the cursor on success before the next array access. `flush()` and
  `finish()` reach the same drain; empty flushes still issue no zero-length write.
  Uncoloured writes still delegate directly. No new branch or buffer is needed.

- `ShellHighlighter.tokens()` still supplies `highlight()`, prompt inference, and
  diff inference. Its comment, string, number, and name scans return the same
  end offsets; name classification retains keyword/function precedence. Prompt
  helpers retain the same JSON/XML doc/store and bound-update checks, and reject
  invalid literal revisions before the final token access.
- `accept()` reaches `acceptQuoted()` only in quote state and returns immediately
  afterward. Only non-XML, unquoted input reaches `acceptJson()` and its shared
  container/number update. `acceptXml()` calls `closeXmlMarkup()` only after the
  original TAG, COMMENT, PI, or CDATA terminator predicate succeeds; previous
  character state advances afterward, preserving every chunk boundary.

## Verification scope

Local evidence lives under `build/ci-diagnosis/`. The Windows runner and the hosted
Sonar quality gate must be checked by the outer CI executor after publication.
CodeFactor initially returned HTTP 403, but a browser User-Agent and HTML Accept
header retrieved its embedded issue model. Its four complexity findings are in
`ShellHighlighter.tokens` (50), `ShellPrompt.reference` (25),
`ShellResultWriter.accept` (36), and `ShellResultWriter.acceptXml` (16).
Extract existing lexical scans, argument checks, and renderer transitions without
new state, allocations per character, or accepted input forms. All extracted
helpers remain on the original call path; no replacement implementation or
fallback is retained. Local cyclomatic checks bound each flagged method at 15.
The existing seeded lexical/payload/style regressions and allocation assertion
remain the behavioral acceptance checks for these extractions.

## Full mutation audit before the complexity extractions

`build/ci-diagnosis/pit-current/mutations.xml` contains the complete 534-mutant
run after the initial CLI/buffer fixes: 466 killed, 10 survived, 34 uncovered,
21 process exits reported as `RUN_ERROR`, and three timed out. These are not
a clean mutation result. The final focused run independently verifies the extracted logic, as recorded
below. CI scope, thresholds, and reporting have not been weakened.

The initially reported multiline-prefix and XML-depth mutations are killed.
The counter increment is removed because only zero/nonzero was observable.
The stronger final interactive test additionally exercises context use via a
real store operation, observes its next revision prompt, and asserts both
debug logging policies. The final focused report confirms that the prompt-observation removal and debug
flag inversion are killed. The legacy `createQueryContext` method is outside
that final focused scope; its null-return survivor from the earlier run is not
claimed as killed.

The following boundary survivors have equivalent observable behaviour:

| Class / method | Original line; PIT instruction index | Mutation and reason |
| --- | --- | --- |
| `ShellPrompt.observe` | 47 | `< 0` to `<= 0`: references contain -1 or a strictly positive revision; zero is rejected before construction. |
| `ShellPrompt.reference` (now `boundPrefix`) | 78 | Size 14 is a bare `let $d := jn:doc('a','b') return delete json $d`; `simpleUpdate` rejects it independently because a supported target suffix requires more tokens. |
| `ShellPrompt.unquote` | 149; 20 | Backslash index `>= 0` to `> 0`: a STRING token always starts with a quote, so a backslash at index zero is unreachable. |
| `ShellPrompt.unquote` | 150; 26 | Ampersand index `>= 0` to `> 0`: a STRING token always starts with a quote, so an ampersand at index zero is unreachable. |
| `ShellHighlighter.highlight` | 58; 111 | `< token length` to `<= token length`: at equality the appended suffix is empty; no visible character or style changes. |
| `ShellOutput.isDiff` | 76; 136 | Loop boundary `< size - 1` to `<= size - 1`: size is even, the loop index is even, and size - 1 is odd. Equality is unreachable. |

Two further survivors remove legacy `createQueryContext` file initialization
(line 140) or return a null context (line 141); that method is unchanged by this
PR and remains outside the final focused scope. The complete uncovered inventory
is below, with each limitation stated rather than counted as equivalent or killed.

| Class / method | Original line | Mutation | Scope / evidence limitation |
| --- | --- | --- | --- |
| `Main.executeFileInteractiveQuery` | 264 | negated conditional | Unchanged legacy `-iqf` path: the option has no value, while its reader expects a file name; outside the terminal-polish change. |
| `Main.executeFileInteractiveQuery` | 262 | removed call to java/io/PrintStream::println | Unchanged legacy `-iqf` path: the option has no value, while its reader expects a file name; outside the terminal-polish change. |
| `Main.executeFileInteractiveQuery` | 268 | removed call to io/sirix/query/Main::executeQuery | Unchanged legacy `-iqf` path: the option has no value, while its reader expects a file name; outside the terminal-polish change. |
| `Main.executeFileInteractiveQuery` | 270 | removed call to io/sirix/query/Main::handleQueryException | Unchanged legacy `-iqf` path: the option has no value, while its reader expects a file name; outside the terminal-polish change. |
| `Main.executeUserQueries` | 189 | negated conditional | Unchanged legacy `-iqf` dispatch or real-console-only default; the in-process JVM has no system console. |
| `Main.executeUserQueries` | 188 | removed call to io/sirix/query/Main::executeFileInteractiveQuery | Unchanged legacy `-iqf` dispatch or real-console-only default; the in-process JVM has no system console. |
| `Main.executeUserQueries` | 192 | removed call to io/sirix/query/Main::executeInteractiveQuery | Unchanged legacy `-iqf` dispatch or real-console-only default; the in-process JVM has no system console. |
| `Main.handleException` | 283 | negated conditional | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.handleException` | 285 | negated conditional | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.handleException` | 284 | removed call to java/io/PrintStream::println | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.handleException` | 286 | removed call to java/io/PrintStream::println | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.handleException` | 288 | removed call to java/io/PrintStream::println | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.handleException` | 291 | removed call to java/lang/System::exit | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.initializeContextWithFile` | 150 | negated conditional | Unchanged legacy `-f` context initialization; no presentation logic is changed in this path. |
| `Main.initializeContextWithFile` | 151 | removed call to io/sirix/query/Main::initializeJsonContext | Unchanged legacy `-f` context initialization; no presentation logic is changed in this path. |
| `Main.initializeContextWithFile` | 153 | removed call to io/sirix/query/Main::initializeXmlContext | Unchanged legacy `-f` context initialization; no presentation logic is changed in this path. |
| `Main.initializeJsonContext` | 163 | removed call to io/brackit/query/QueryContext::setContextItem | Unchanged legacy `-f` context initialization; no presentation logic is changed in this path. |
| `Main.initializeXmlContext` | 175 | removed call to io/brackit/query/QueryContext::setContextItem | Unchanged legacy `-f` context initialization; no presentation logic is changed in this path. |
| `Main.main` | 100 | negated conditional | Real-console-only or process-termination branch; requires an OS terminal/process test, which cannot supply in-process PIT coverage. |
| `Main.main` | 122 | negated conditional | Real-console-only or process-termination branch; requires an OS terminal/process test, which cannot supply in-process PIT coverage. |
| `Main.main` | 123 | removed call to io/sirix/query/Main::handleQueryException | Real-console-only or process-termination branch; requires an OS terminal/process test, which cannot supply in-process PIT coverage. |
| `Main.main` | 124 | removed call to java/lang/System::exit | Real-console-only or process-termination branch; requires an OS terminal/process test, which cannot supply in-process PIT coverage. |
| `Main.main` | 126 | removed call to io/sirix/query/Main::handleException | Real-console-only or process-termination branch; requires an OS terminal/process test, which cannot supply in-process PIT coverage. |
| `Main.parseParams` | 349 | removed call to io/sirix/query/Main::printUsage | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 373 | negated conditional | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 366 | removed call to java/io/PrintStream::println | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 368 | removed call to java/io/PrintStream::println | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 371 | removed call to java/io/PrintStream::print | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 372 | removed call to java/io/PrintStream::print | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 374 | removed call to java/io/PrintStream::print | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 376 | removed call to java/io/PrintStream::print | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 378 | removed call to java/io/PrintStream::print | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 379 | removed call to java/io/PrintStream::println | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |
| `Main.printUsage` | 382 | removed call to java/lang/System::exit | Unchanged process-termination/usage path; invoking it terminates PIT's test JVM. Do not substitute a mock of this code as evidence. |

## Final verification

- All 46 `MainTest` / `io.sirix.query.shell.*Test` tests pass, including the
  renderer allocation budget, fixed-seed adversarial inputs, all writer overloads,
  revision prompt integration, debug policy, and Unicode subprocess goldens.
- `:sirix-query:compileJava :sirix-query:compileTestJava -PerrorProne` and
  `:sirix-query:spotlessCheck` pass. `git diff --check` passes.
- Local cyclomatic complexity for the four flagged methods is now 14 (`tokens`),
  15 (`reference`), 13 (`accept`), and 10 (`acceptXml`).
- Final focused PIT report: `build/ci-diagnosis/pit-final/mutations.xml`, 413
  mutations, 403 killed, five survived, two memory errors, two timeouts, one run
  error, and zero uncovered. Its scope is the three extracted shell classes and
  Main's input/output methods; unchanged CLI dispatch/lifecycle methods were
  already included in the preceding full report. Exact targets and local-only
  resource/scope options are in `final-targets.txt` and `pit-bounded.init.gradle`
  under the same evidence directory. These options do not change repository CI.
- The five final survivors are the equivalent cases above: `boundPrefix:131`
  (instruction 6), `observe:47` (123), `unquote:168` (20), `unquote:169` (26), and
  `highlight:58` (111). No undocumented survivor remains in the focused report.
- The abnormal mutation outcomes are explicit, not counted as assertion kills:
  returning zero from `commentEnd:111` or `stringEnd:126` repeatedly emits tokens
  without advancing and exhausts the minion heap; decrementing the cursor in
  `stringEnd:118` / `stringEnd:121` prevents forward progress and times out.
  Negating the null-prompt condition in `Main.executeQuery:220` terminates the
  minion through the real CLI error/exit boundary. The unmutated baseline passes.

The full CI mutation check still needs disposition of the documented equivalent
and legacy uncovered mutants; no allowlist, threshold change, or workflow edit
silently accepts them. Windows and hosted CodeFactor/Sonar reruns belong to the
outer executor. This phase neither publishes nor controls the pipeline.
