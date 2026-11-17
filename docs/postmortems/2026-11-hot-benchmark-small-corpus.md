# Bug: absent-probe setup disabled unrelated HOT benchmarks

- Fix / issue: review finding R4.
- Symptom: even serializeKey and pointGet failed setup for a one-title corpus.
- Trigger: run an existing stored-key benchmark stage with a corpus containing one distinct title.
- Root cause: shared setup required every absent-probe geometry, although a one-entry leaf has no insertion point strictly inside it. The requirement belongs to the selected absent-probe stage.
- Why tests missed it: benchmark validation used the full movies corpus, which supplies all three geometries.
- Regression test: `:sirix-benchmarks:verifyHotSmallCorpus` runs all six stored-key stages through JMH with a one-title corpus and asserts every stage completed; it makes no timing assertions.
- Lesson: validate optional benchmark inputs only for the stage that consumes them; see [Lessons](../LESSONS.md).
