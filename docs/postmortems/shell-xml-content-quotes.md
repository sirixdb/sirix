# Bug in XML content quote handling

- Fix / issue is review finding R6 for the shell presentation change.
- Symptom was green string styling leaking from XML comments, text or processing
  instructions into following tags.
- Trigger was `<root><!-- " --><child/></root>`. XML text and PI content containing
  a quote caused the same failure. Embedded `>` characters also exposed premature
  exits from special markup.
- Root cause was the shared serialized-output lexer treating any double quote as a
  string opener and any markup `>` as a tag terminator. The tag boolean did not
  distinguish XML attributes, text, comments, processing instructions and CDATA.
- Earlier tests checked payload preservation on arbitrary strings and XML with plain
  comments. They checked attribute escaping, with no style oracle for quoted content
  followed by another tag.
- Regression tests are `xmlContentQuotesAndMarkupTerminatorsPreserveFollowingTagStyles`
  and `mixedXmlJsonAndPatchContextsRecoverAcrossEveryWriterOverloadAndFinish` in
  `ShellResultWriterTest`. Independently specified blue tag/markup, green attribute
  and default text styles accompany exact ANSI-stripped payload assertions. Tests
  enumerate every split and flush position, all writer overloads and incomplete
  finish/reuse cases. The original lexer leaves subsequent tags green on these inputs.
- The correction replaces the tag boolean with bounded markup context and element depth.
  Attribute quoting and JSON quoting retain their own escape rules. Comment, PI and
  CDATA terminators use two preceding characters across writes. All scratch remains
  fixed, with the existing allocation test extended to repeated mixed JSON/XML output.
- The prevention rule and regression evidence appear in the XML context entry in
  [Lessons](../LESSONS.md).

## Focused verification

The final `heavy.sh` / JDK 25 Gradle run completed successfully. Scoped Spotless formatting
covered the five touched Java files. `:sirix-query:test` selected `io.sirix.query.MainTest`
and `io.sirix.query.shell.*`, excluding the unrelated temporal-reader dependency task.
All 37 tests passed, including the mixed XML/JSON allocation test with its existing
512-byte bound. This run also executed the preceding review's final prompt assertions.

Scoped PIT targeted `ShellHighlighter.tokens` and the result writer's changed lexical,
key-colour and finish paths. Unchanged writer plumbing was excluded. The local init script
removed comma-containing JVM properties for PIT compatibility and bounded test heaps.
The final XML report contains 194 mutants with 191 `KILLED`, one `SURVIVED`, two
`TIMED_OUT` and zero `NO_COVERAGE`. These are the XML statuses, with timeouts counted
separately from ordinary kills. The report is `build/review-r2-r5-r7/pitest/mutations.xml`
and the invocation/output is `build/review-r2-r5-r7/verification.log` in this worktree.

The first report exposed an undetected premature PI terminator and uncovered EOF key
drain. Checking every content character's style kills the PI terminator mutant. The
incomplete JSON key golden covers and kills removal of `finish()`'s deferred key drain.

Individual remaining mutation outcomes follow.

- `ShellResultWriter.acceptXml`, line 190, `ConditionalsBoundaryMutator`, changes
  `xmlDepth > 0` to `xmlDepth >= 0`. The differing zero-depth case requires an unmatched
  closing tag. Every closing tag in well-formed serialized XML consumes an open element
  with positive depth. This case is unreachable for the production XML contract.
  Malformed-input tests specify payload preservation, which both variants satisfy.
- `ShellHighlighter.tokens`, line 95, `IncrementsMutator`, changes the increment after
  a backslash to a decrement. The next iteration revisits the same backslash forever.
  `incompleteEscapedAndUnicodeInputRoundTripsWhileTyping` supplies an escaped quote
  that exercises this mutation. The timeout detects its failure to advance.
- `ShellHighlighter.tokens`, line 98, `IncrementsMutator`, reverses the increment over
  a doubled quote. The scanner returns to the first quote and repeats that branch
  forever on the same test's `'it''s'` input. This is the second non-progress timeout.
