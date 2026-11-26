---
kind: note
---
- **Bind resource proofs to filesystem identity.** A verified path and its canonical alias must
  authorize the same resource, while a different resource must still be refused. Test I/O
  observers must also match filesystem identity; retain a dot-segment fixture so Linux exercises
  the spelling mismatch seen with macOS temporary-directory aliases. When opening
  begins rejecting corruption earlier, keep direct-reader and loader regressions behind that
  boundary by opening before fault injection. Associate contract-focused behavior tests with
  changed mutation targets; avoid adding a whole maintenance suite when an existing focused
  fixture proves the same contract, since PIT repeats parameterized discovery in each worker. Evidence: [format baseline CI](postmortems/format-baseline-ci.md).
