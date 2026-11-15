# Bug: a complete shell query appeared to require continuation

- Fix / issue: shell presentation polish on `fm/sirix-shell-polish`, found in the real ANSI capture.
- Symptom: after a complete one-line query, `...>` and an extra blank row appeared before `Query result`.
- Trigger: enter `jn:doc('shop','products',1)` followed by the existing blank-line execution mark.
- Root cause: the shell's physical-line accumulator labelled every later read as syntactic
  continuation, even though it does not parse completeness. The result writer then added a
  leading newline on top of the blank execution line echoed by JLine.
- Why tests missed it: original prompt tests covered context/revisions, while terminal spacing
  tests accepted the leading newline inherited from plain CLI output.
- Regression tests: `MainTest.completeQueriesDoNotShowAMisleadingContinuationPrompt` and
  `ShellOutputTest.deterministicTimingAndBufferedOutputAreFlushed` assert the prompt and exact
  result spacing. Multiline, EOF and interrupt regression cases preserve the input protocol;
  the independent plain-output oracle preserves piped bytes.
- Lesson: distinguish a physical-line accumulator from a parser's continuation state, and
  verify the complete terminal transcript rather than isolated result tokens; see [Lessons](../LESSONS.md).

The evidence renderer also now decodes raw ANSI bytes without universal-newline
translation. JLine emits CR/CR/LF when accepting a line; treating each CR as a
newline exaggerated blank rows in the PNG. The asciinema stream always retained
the original control bytes. Refreshed captures preserve the single intentional
blank-line execution mark without an added result spacer.
