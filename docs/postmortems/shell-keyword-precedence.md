# Bug: expression keywords received function styling

- Fix / issue: review finding R2 for the shell presentation change.
- Symptom: `if (true())` and `return ($x+1)` styled the expression keyword blue instead of orange.
- Trigger: an expression keyword followed by whitespace and an opening parenthesis, including
  incomplete interactive input or a subsequent physical line.
- Root cause: the shared lexer classified opening-parenthesis lookahead before keyword membership.
- Why tests missed it: existing samples used keywords followed by variables or constructors and
  checked actual functions, without placing expression keywords before parentheses.
- Regression test: `ShellHighlighterTest.expressionKeywordsBeforeParenthesesKeepKeywordStyle`
  checks the actual rendered palette styles on conditional/FLWOR/switch input, incomplete input,
  multiline prefixes and built-in/qualified calls. Its orange keyword oracle rejects the old blue
  function classification; blue built-in controls reject an overbroad keyword-first correction.
- Fix: classify expression keywords before generic function calls, preserving callable keyword
  built-ins and bare keyword forms at the shared lexical boundary.
- Lesson: [test lexical precedence at overlapping forms](../LESSONS.md).

Review finding R5 exposed an incomplete callable overlap set in the first correction.
`count($v[])` in the repository demo and `last()` were still classified as keywords.
The same style regression test now checks all seven callable keyword overlaps, their bare
forms, the demo's count expression, and a multiline prefix. `count`, `last` and the
standard `replace(...)` string function join the existing callable overlap set.
The repository's `GroupTopKDifferentialTest` and `SegmentLengthLaneQueryTest` contain
unprefixed `replace(...)` calls, so its function form receives the same regression
coverage. The update expression `replace json value of ...` retains keyword styling. Expression-keyword precedence continues to satisfy R2.

The final focused run passed the extended seven-overlap style test and all 37 Main/shell
tests. Scoped mutation outcomes are recorded in the
[XML content verification record](shell-xml-content-quotes.md).
