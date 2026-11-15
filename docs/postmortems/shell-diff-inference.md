# Bug: diff colours inferred from unexecuted or mixed source

- Fix / issue: review finding R8 for the shell presentation change.
- Symptom: ordinary JSON keys and atomic words received diff operation colours when a
  skipped, deferred or unrelated diff call occurred elsewhere in the source.
- Trigger: `if (false()) then jn:diff('shop','products',1,2) else {"delete":1}`
  displayed its ordinary delete key in red. XML calls and mixed sequences shared the defect.
- Root cause: `ShellOutput.isDiff` scanned for any diff function token and applied one
  result-wide mode. Presence in source established neither execution nor ownership of
  every result item. Both JSON key and XML patch-word consumers trusted that mode.
- Why tests missed it: direct real diffs and a comment-only false positive were covered.
  Executable conditionals, deferred functions and mixed sequences had no style oracle.
- Regression tests: `skippedDeferredAndMixedDiffCallsKeepOrdinaryResultStyles` executes
  both stores' skipped/deferred calls and both sequence orders, asserts blue ordinary
  keys and default ordinary words, and compares payloads with the existing serializer.
  `onlySupportedDirectDiffFormsSelectOperationStyles` covers direct JSON arities 4/5/6,
  comments/whitespace, and neutral wrappers, computed arguments and projections.
  The existing real XML control includes inter-token comments. The old scan enables
  operation colours on these ordinary-output cases and fails the new style assertions.
- Fix: match the entire significant token sequence against supported direct literal
  diff signatures at the single output boundary. This removes arbitrary-source inference.
  Revision arguments and optional JSON subtree arguments use numeric literals. Dynamic,
  nested and compound forms stay neutral. Query execution and resource access are unchanged.
- Lesson: infer presentation context only from supported result-producing forms, with
  skipped, deferred and mixed-result controls. See [Lessons](../LESSONS.md).

## Focused verification

The JDK 25 Gradle run through `heavy.sh` completed successfully. Spotless formatting
was scoped to `ShellOutput.java` and `ShellOutputTest.java`. The test task selected
`io.sirix.query.MainTest` and `io.sirix.query.shell.*`, excluding the unrelated
`temporalNodeReaderLifetimeGenericTest` dependency. All 39 tests passed, including
plain output/encoding, prompt progression, XML contexts, replacement colours and allocation
checks from the previous rounds. The new tests compare complete ANSI-stripped output
against the existing serializer with a deterministic timing footer.

PIT targeted `ShellOutput.isDiff`, excluding the unchanged `result` and `error` methods,
and selected `ShellOutputTest`. Its XML report contains 28 mutants with 27 `KILLED`,
one `SURVIVED`, zero `NO_COVERAGE` and zero timeouts. The report is
`build/review-r8/pitest/mutations.xml`; the command output is
`build/review-r8/verification.log` in this worktree. The local init script bounds test
heaps and removes comma-containing JVM properties for PIT compatibility.

The individual survivor is `ShellOutput.isDiff`, line 74,
`ConditionalsBoundaryMutator`, changing `i < size - 1` to `i <= size - 1`.
It is equivalent for every input. Odd token counts return before this loop. For every
remaining even size, `size - 1` is odd, while `i` starts at 2 and advances by 2.
The loop index can therefore never equal the differing boundary. Both versions visit
exactly the same argument tokens and return the same mode.
