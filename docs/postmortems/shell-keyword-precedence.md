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
