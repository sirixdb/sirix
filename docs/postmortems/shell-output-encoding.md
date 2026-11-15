# Bug: shell output adapter replaced the stream's declared encoding

- Fix / issue: shell presentation change on `fm/sirix-shell-polish` (caught before delivery).
- Symptom: the new PrintWriter adapter could change redirected bytes when a PrintStream used
  an encoding different from the JVM default, including Windows console code pages.
- Trigger: adapt a Windows-1252 or UTF-16BE PrintStream and print `é 商店`.
- Root cause: `PrintWriter(OutputStream, boolean)` chooses the JVM default charset, whereas
  the original SirixStringSerializer adapter explicitly used `PrintStream.charset()`.
  Wrapping the same stream did not imply preserving its encoding.
- Why tests missed it: subprocess and Unicode golden tests ran with UTF-8 for both the default
  charset and stdout; they never separated those two settings.
- Regression test: `MainTest.outputAdapterPreservesExplicitStreamEncodings` independently
  derives the expected bytes with each stream's declared charset. An adapter using the
  JVM default fails the UTF-16BE and Windows-1252 cases; the explicit adapter passes.
- Lesson: preserve the encoding contract at an output adapter boundary; see [Lessons](../LESSONS.md).
