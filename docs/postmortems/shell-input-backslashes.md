# Bug: single-quoted input inherited double-quoted escaping

- Fix / issue: review R2 on `fm/sirix-shell-polish`.
- Symptom: `let $p := 'C:\' return count($p)` coloured the remainder green,
  and direct JSON/XML diff calls with the same literal shape missed operation colours.
- Trigger: a backslash immediately before a closing apostrophe; a backslash before
  doubled apostrophes also broke the string boundary.
- Root cause: the shared tolerant scanner skipped the next character after every
  backslash. JSONiq single-quoted strings treat it literally. The error propagated
  into tokenization, live/multiline styles and both direct diff selectors. Prompt
  inference also consumes these tokens, but must still reject unsupported names.
- Why tests missed it: round-trip checks preserved input bytes despite wrong token
  boundaries, while escape-style checks covered only double-quoted input.
- Regression test: `ShellHighlighterTest.singleQuotedBackslashesFollowExecutableParserAndRestoreStyles`
  executes Brackit queries to check single-quoted values and the reported count result, then
  checks literal boundaries and independent styles across every prefix split and palette.
  Unterminated strings are rejected by the parser but remain one green editing token.
  `ShellOutputTest.directDiffClassificationUsesLiteralBoundariesWithoutNativeNames`
  checks both selectors and both fields without creating invalid Windows filenames;
  real-diff integration tests continue to exercise the output boundary.
  `ShellPromptTest.backslashNamesStayNeutralAfterSingleQuotedTokenBoundariesRecover`
  covers both fields, stores, reads and supported update forms.
- Lesson: distinguish each input literal's escape rules at the shared scanner; compare
  token/style boundaries with executable parser results, not payload round-trips alone.
  See [lexical language boundaries](../LESSONS.md#lexical-language-boundaries).

## Focused review evidence

The final baseline control used the original HEAD `ShellHighlighter` in an isolated
classpath. After executing the parser/value/count assertions successfully, the trailing
single-quoted backslash case failed its exact string-token assertion. The classifier
control failed with expected JSON mode versus actual NONE. The fixed implementation
passed all 44 tests: 9 highlighter, 11 output, 9 prompt and 15 renderer tests. The renderer
allocation bound and the prior real XML patch regression passed unchanged. The first two
verification attempts exposed test-oracle assumptions about lazy sequences and serialized
double-quoted values; the final oracle consumes single-quoted values through serialization,
and double-quoted cases assert the retained lexical compatibility contract.

The focused Gradle invocation ran the baseline control, these four test classes and
unsampled PIT on `ShellHighlighter` and `ShellOutput`. All JVM work used the supplied
`heavy.sh`; its supported priority slot was used after waiting for the normal slots.
`/` had at least 87 GiB available before each build. Scratch stayed inside the worktree
under ignored `build/review-input-backslashes/`, following the workspace boundary.
No query engine, read/load algorithm, CI configuration or work budget changed. Full test
and lint phases remain with the outer executor.

Evidence: `build/review-input-backslashes/verification.log`,
`build/review-input-backslashes/pitest/mutations.xml`,
`bundles/sirix-query/build/test-results/inputQuoteBaselineRegression/TEST-io.sirix.query.shell.ShellHighlighterTest.xml`,
`bundles/sirix-query/build/test-results/inputQuoteBaselineRegression/TEST-io.sirix.query.shell.ShellOutputTest.xml`
and the four shell XML reports under `bundles/sirix-query/build/test-results/test/`.

PIT reported 155 mutants: 149 KILLED, 2 SURVIVED, 2 TIMED_OUT, 2 MEMORY_ERROR and zero
uncovered. All four mutants at the changed `ShellHighlighter.stringEnd:117` condition
were KILLED: the conditional-boundary mutation and all three conditional negations.
The CLI's aggregate of 153 detected mutants includes the four abnormal exits; those
are not normal assertion kills. Every remaining outcome is individually accounted for:

| Class / method / line | Mutator / bytecode index | Status | Justification |
| --- | --- | --- | --- |
| `ShellHighlighter.highlight:58` | `ConditionalsBoundaryMutator`, 111 | SURVIVED, equivalent | Changing `<` to `<=` adds only the case `skip == token.length()`: appending an empty slice emits no characters. The next token sets its own style; a final empty slice contributes no attributed characters. Payload and styles are identical. |
| `ShellOutput.diffMode:77` | `ConditionalsBoundaryMutator`, 136 | SURVIVED, equivalent | The preceding guard requires even token count; the loop index starts at 2 and increases by 2, so it cannot equal the odd `size - 1` bound. `<` and `<=` visit identical arguments. |
| `ShellHighlighter.nameEnd:166` | `PrimitiveReturnsMutator`, 21 | MEMORY_ERROR | Returning zero resets the outer tokenizer to the first identifier. For an identifier at offset zero, it repeatedly appends empty tokens without progress until the minion exhausts memory. This is an abnormal outcome, not equivalence or an assertion kill. |
| `ShellHighlighter.stringEnd:118` | `IncrementsMutator`, 29 | TIMED_OUT | Changing the escape skip from `++` to `--` undoes the preceding character-read increment. An escaped character leaves the cursor on the same backslash indefinitely. The minion timeout detects nontermination. |
| `ShellHighlighter.stringEnd:121` | `IncrementsMutator`, 50 | TIMED_OUT | Changing the doubled-delimiter skip from `++` to `--` undoes the character-read increment. The same doubled delimiter is revisited indefinitely, causing minion timeout. |
| `ShellHighlighter.stringEnd:126` | `PrimitiveReturnsMutator`, 59 | MEMORY_ERROR | Returning zero resets the tokenizer to an opening quote at offset zero, repeatedly appending empty tokens without progress until memory exhaustion. This is an abnormal outcome, not equivalence or an assertion kill. |
