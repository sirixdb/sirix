# Bug: XML operand names received diff operation colours

- Fix / issue: review R1 on `fm/sirix-shell-polish`.
- Symptom: an inserted attribute named `delete` was red; operation words within qualified
  or hyphenated names could also receive insertion, modification or deletion colours.
- Trigger: insert the attribute into an existing element, commit, and render `xn:diff`
  between the two revisions in a coloured shell.
- Root cause: the streaming renderer matched every unquoted identifier outside markup
  and value constructors. Matching spelling alone violated the invariant that only words
  at operation starts receive operation colours. Splitting names into identifiers made
  qualified/hyphenated names additional instances of the same defect. Numeric matching
  also treated hyphens/digits within these names as number starts. Both classifiers now
  require XML patch item boundaries rather than accepting arbitrary operand characters.
- Why tests missed it: handcrafted patches used ordinary attribute names and asserted
  operation colours without asserting operand styles. Some used JSON diff mode for XML
  patch words, masking the missing language boundary.
- Regression test: `ShellXmlDiffColourTest` executes real attribute insertion, name/value
  update and deletion diffs for operation-like bare, qualified, hyphenated and Unicode
  names. Worked patch goldens independently assert serialized bytes and terminal styles
  across palettes. A streaming golden covers all split positions, writer overloads,
  flushes, finish/reuse, paths, markup, strings and non-XML controls.
- Lesson: identify lexical position before classifying a matching word; assert both
  operation colours and neutral operand names. See [lexical language boundaries](../LESSONS.md#lexical-language-boundaries).

## Focused review evidence

The original HEAD renderer, compiled into an isolated baseline classpath, failed the
new real-diff regression at the neutral attribute-name style assertion after matching
the independently worked patch-byte golden. The fixed renderer passed all 30 tests:
15 renderer, 11 output and 4 XML operation-position tests. The real-diff test covers
13 names through insertion, combined rename/value update and deletion, across every
palette and plain output. The unchanged 512-byte allocation bound passed for both modes.

The first focused run exposed the numeric classifier's hyphenated-name sibling, which
was corrected at the same item boundary. The first unsampled PIT run then found one
surviving word-buffer boundary mutant; the long XML token/recovery assertion kills it.
Final unsampled PIT for `ShellResultWriter` generated 170 mutants: all 170 KILLED,
zero survivors, zero uncovered and zero abnormal outcomes.

All JVM work used the supplied `heavy.sh` and JDK 25. `/` had 90 GiB available before
each run. Scratch and logs stayed inside this worktree; prior public PR evidence was
not changed. No query engine/read/load algorithm or budget changed. Full tests, lint,
push, PR and CI remain with the outer executor.

Final command (through `heavy.sh`, with the ignored `focused.gradle` registering only
the baseline and focused test tasks):

```sh
./gradlew --no-daemon --max-workers=2 -Dorg.gradle.jvmargs=-Xmx2g \
  -I build/review-xml-operations/focused.gradle \
  :sirix-query:xmlOperationBaselineRegression :sirix-query:xmlOperationFocusedTest \
  :sirix-query:pitest -PprMutationModule=sirix-query \
  -PprMutationTargetsFile=build/review-xml-operations/targets.txt \
  -PprMutationTests=io.sirix.query.shell.ShellResultWriterTest,io.sirix.query.shell.ShellOutputTest,io.sirix.query.shell.ShellXmlDiffColourTest \
  -PprMutationReportDir=build/review-xml-operations/pitest
```

Evidence:

- `build/review-xml-operations/verification-complete.log`
- `build/review-xml-operations/pitest/mutations.xml`
- `bundles/sirix-query/build/test-results/xmlOperationBaselineRegression/TEST-io.sirix.query.shell.ShellXmlDiffColourTest.xml`
- The three shell XML reports under `bundles/sirix-query/build/test-results/xmlOperationFocusedTest/`.
