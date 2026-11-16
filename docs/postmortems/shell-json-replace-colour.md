# Bug in structural JSON replacement diff colour

- Fix / issue is review finding R7 for the shell presentation change.
- Symptom was blue styling for JSON structural replacement operations while scalar
  updates and XML replacements received orange modification styling.
- Trigger was a serialized diff containing `{"replace":{...}}`.
- Root cause was an incomplete mapping of the serializer's operation keys. The JSON
  mapping covered insert, update and delete, omitting the replace key emitted for
  `REPLACEDNEW` by `JsonDiffSerializer`.
- Earlier tests exercised real scalar JSON updates and XML replacements, with JSON
  golden coverage limited to insert/update/delete.
- Regression coverage extends `ShellResultWriterTest.goldenJsonAndDiffColours` with
  the exact orange replacement key, checks ordinary JSON replacement keys remain blue,
  and executes a real container-to-scalar replacement and `jn:diff` through
  `ShellOutputTest.structuralJsonReplacementDiffUsesModificationColour`.
- The correction maps the JSON replace key to the same orange palette entry as update.
  The pre-fix golden and real-diff assertions expect orange and receive blue.
- The prevention rule and test evidence appear in the diff operation entry in
  [Lessons](../LESSONS.md).

The final focused run passed the exact replacement-key golden, ordinary JSON control,
XML patch control and real structural `jn:diff` test. Mutation outcomes are recorded
in the [XML content verification record](shell-xml-content-quotes.md).
