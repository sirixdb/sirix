# Bug: XML patch strings inherited JSON escaping

- Fix / issue: review R1 on `fm/sirix-shell-polish`.
- Symptom: a replacement ending in a backslash kept the following delete operation green.
- Trigger: change `<a>old<b/></a>` text to `C:\`, remove `<b/>`, and render
  `xn:diff('shop','resource1',1,2)` in a coloured terminal.
- Root cause: output inference reduced JSON and XML diffs to one boolean. The renderer
  consequently interpreted XQuery patch strings as JSON strings: backslashes escaped
  double quotes, and single-quoted header names did not enter string state. The broken
  invariant affected replacement values, rename strings, inserted values and both names.
- Why tests missed it: the actual XML diff check asserted the first replacement keyword;
  the handcrafted patch had no trailing backslash and checked no quoted-value styles.
- Regression test: `ShellOutputTest.actualXmlDiffBackslashReplacementRestoresFollowingDeleteColour`
  executes the real diff and asserts the independently worked patch, string style and next
  delete colour for all palettes and plain output. `ShellResultWriterTest` covers both
  delimiters, doubled delimiters, Unicode, markup-like string contents, every split/overload,
  flush, unterminated input, finish/reuse, JSON controls and existing allocation limits.
  Focused command: `./gradlew :sirix-query:test --tests 'io.sirix.query.shell.ShellResultWriterTest'
  --tests 'io.sirix.query.shell.ShellOutputTest'`. Verification results belong to the review
  phase evidence below.
- Lesson: preserve the serialized language through shared presentation boundaries;
  [lexical language boundaries](../LESSONS.md#lexical-language-boundaries).

## Focused review evidence

The regression ran against the original HEAD renderer (with only a constructor/enum
compatibility shim in ignored build scratch). It failed at the following delete-colour
assertion, after its independently expected patch bytes had matched. The fixed renderer
passed all 25 tests in `ShellOutputTest` and `ShellResultWriterTest`, including the
unchanged 512-byte allocation bound for JSON and XML patch modes. No query engine or
load/read algorithm changed; repository-wide test and lint gates remain with the executor.

A single focused Gradle invocation ran the baseline control, those two test classes and
unsampled PIT for `ShellOutput` and `ShellResultWriter`. JVM commands used the supplied
`heavy.sh`; `/` had 89 GiB available before building. Review scratch and reports stayed
inside the worktree under ignored `build/review-xml-quotes/`. Evidence paths:

- `build/review-xml-quotes/verification.log`
- `bundles/sirix-query/build/test-results/xmlQuoteBaselineRegression/TEST-io.sirix.query.shell.ShellOutputTest.xml`
- `bundles/sirix-query/build/test-results/test/TEST-io.sirix.query.shell.ShellOutputTest.xml`
- `bundles/sirix-query/build/test-results/test/TEST-io.sirix.query.shell.ShellResultWriterTest.xml`
- `build/review-xml-quotes/pitest/mutations.xml`

PIT generated 211 mutants: 210 killed, one equivalent survivor, zero uncovered or abnormal.
The survivor is `ShellOutput.diffMode`, line 77,
`ConditionalsBoundaryMutator`, bytecode index 136 (`changed conditional boundary`).
It changes the loop test `i < size - 1` to `i <= size - 1`. The preceding guard requires
even `size`, so `size - 1` is odd; `i` starts at 2 and increases by 2, and can never equal
that bound. Both loop conditions therefore visit exactly the same argument tokens for
every reachable input. No acceptance threshold or work budget was changed.
