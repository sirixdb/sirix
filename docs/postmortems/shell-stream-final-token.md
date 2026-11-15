# Bug: terminal diff renderer lost the last unquoted token

- Fix / issue: shell presentation change on `fm/sirix-shell-polish` (caught before delivery).
- Symptom: an XML diff patch ending in `$doc/b` was displayed as `$doc/`.
- Trigger: `ShellResultWriterTest.xmlDiffPatchKeywordsHaveDistinctColours`, with one-character writes and no trailing newline.
- Root cause: the streaming diff highlighter deferred an unquoted token to decide its colour.
  End-of-result drained pending JSON keys and buffered output but omitted that deferred word.
  Closing the stream therefore violated the invariant that removing ANSI reproduces every input character.
- Why tests missed it: earlier golden JSON cases terminated tokens with punctuation; the seeded
  malformed-input exploration did not guarantee a trailing unquoted word in diff mode.
- Regression test: `ShellResultWriterTest.xmlDiffPatchKeywordsHaveDistinctColours`; the focused
  Gradle run failed with the missing `b` before `finish()` drained pending words. The test compares
  the entire patch independently of colour and checks all three operation colours.
- Lesson: streaming adapters must drain every lexical state at EOF, not only at delimiters;
  see [Lessons](../LESSONS.md).

A second adversarial boundary check covers `<a x='a\'/>`. XML attribute values do
not use JSON's backslash escapes: applying that state transition to XML left the
closing quote and following tag in string colour. The renderer now limits backslash
escaping to JSON strings. `xmlAttributeAndTagStylesAndIncompleteTokensResetPrecisely`
checks the exact tag/attribute styles as well as EOF resets for strings, numbers and tags.
