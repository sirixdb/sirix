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
- Current usage and separator grammar: [README](../../README.md#using-the-cli-native-binaries).
- Why tests missed it: no test invoked `-iqf`, and the mutation reviews accepted "no supported
  public invocation reaches this call" as a reason rather than as a defect.
- Regression test: `MainTest.queryScriptsRunEveryBlankLineSeparatedQueryInOneSessionAndReportFailuresInTheStatus`.
  It covers CRLF input, a multi-line query, repeated and whitespace-only separators, a failing query
  after a queued update, a success exit through stdin `-`, and an unreadable file. Before the fix the
  test JVM exited through `printUsage`.
- Lesson: an unreachable public entry point is a defect to repair or remove, not a mutation
  disposition; see [Lessons](../LESSONS.md).

## Review follow-up: platform-independent query text

- Invariant (I1–I6): file and stdin scripts with LF or CRLF endings assemble query lines with LF
  on every host, retaining indentation and the shell framing's native output newlines.
- Root cause: script assembly inserted `System.lineSeparator()` into the query source, and
  the regression oracle expected that host separator inside a literal's result as well.
- CR trace: Brackit's tokenizer preserves the CR; `compiler.translator.Compiler` translates
  string literals using `Whitespace.normalizeXML11`, which maps CRLF to LF before evaluation.
  `StringSerializer` writes the resulting atomic string unchanged. Thus the observed LF is
  compiler line-end normalization, not serialization corruption.
- Why tests missed it: Linux uses LF for both query values and framing, concealing the oracle's
  conflation until the Windows job used CRLF framing.
- Regressions: `MainTest.queryScriptsUseLfInsideLiteralsForFilesAndStdin` checks LF/CRLF inputs
  and both literal quote forms through both script entry points; the existing session test
  expects LF only inside the value. `literalLineEndsNormalizeBeforeEvaluationAndRuntimeCarriageReturnsSurviveOutput`
  checks the evaluated string and serialized output separately, including runtime-generated CR.
  I1–I6 supply the script oracle; compiler line-end normalization supplies the literal oracle;
  code point 13 supplies the independent serialization control. A serializer that discards CR
  fails that control.
- Lesson: distinguish language line-end normalization from output framing, and test runtime
  values separately from source literals; see [Lessons](../LESSONS.md).

## Review follow-up: preserve non-line-break script characters

- Invariant (I1–I6): only LF and CRLF delimit script lines; empty lines or lines containing
  spaces and tabs separate queries. Every other character remains part of the query source,
  through both file and stdin input and in both single- and double-quoted literals.
- Trigger: `string-to-codepoints('a<U+2029>b')`, with an actual paragraph separator, returned
  `97 10 98` instead of `97 8233 98`; a paragraph separator occupying a literal's middle line
  could instead end the query prematurely.
- Root cause: `\R` recognized six extra line-ending aliases, and `isBlank()` independently
  treated several of those same characters as empty separator lines. Both decisions belonged
  to a broader whitespace grammar than the script protocol requires.
- Why tests missed it: LF/CRLF examples checked supported delimiters but never placed other
  separator characters inline or alone on a physical line inside a literal.
- Regression: `MainTest.queryScriptsPreserveOtherSeparatorsAsQueryContent` runs both script
  entry points with LF/CRLF, both quote forms and U+000B, U+000C, lone CR, U+0085, U+2028 and
  U+2029 inline and on their own lines (a tab follows lone CR to prevent pairing it with LF).
  The I1–I6 oracle preserves those source characters;
  independently specified XML 1.1 literal normalization maps CR, NEL and U+2028 to LF during
  compilation. Expected code points distinguish data preservation from accidental replacement
  and premature query execution; paragraph separator, vertical tab and form feed remain intact.
- Fix: remove the extra splitter aliases and restrict blank separator content to spaces/tabs.
- Lesson: apply the same delimiter grammar to line splitting and blank-line classification;
  see [Lessons](../LESSONS.md).
