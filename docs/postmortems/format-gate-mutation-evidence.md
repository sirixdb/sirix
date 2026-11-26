# Bug: format-gate refusal evidence omitted executable boundaries

- Fix / issue: PR #1480, FMT-401; authoritative local PIT report for
  `7c9603baa5131f7aa51dd637aa9e537fa23c226e` to
  `c2f49677dba6e139f7f48303953259318d012565` (results SHA-256 prefix `800be0d203008f3f`).
- Symptom: the complete changed-line analysis reported 31 surviving and five uncovered mutants;
  ordinary-open refusal tests did not establish the direct repair, SPI and ownership contracts.
- Trigger: select the PR's changed sources and mapped tests, then perform the complete changed-line
  analysis without a per-class sample. It generated 174 mutants: 134 killed, 31 surviving,
  five uncovered and four otherwise detected.
- Root cause: test selection followed class names while requirements crossed class boundaries.
  `LocalDatabase` did not select the retry-and-removal regression; `FileChannelWriter` did not
  select direct repair regressions; provider dispatch had no behavior test. Exact two-byte envelopes,
  partial reads, UUID agreement without settings, pruned latest roots, the first and last window
  slots, truncated slots after buffer reuse, and cleanup failures also lacked independent assertions.
  Redundant private proof rechecks also obscured the validation boundary: proofs were constructed
  and immediately checked against the identical arguments, while the latest live root was checked
  again after the window scan had already validated it. Those duplicates and their unused repair
  proof wrapper are removed. External proof-taking storage boundaries still check ownership; direct
  repairs still verify both descriptors, writable features and the relevant windows before writing.
  The directory scratch request is now its constant window maximum (still below the allocator's
  existing minimum), and partial frame lengths use the existing incomplete-input handling.
  The first complete follow-up run measured 164 changed-line mutants: 158 killed, six surviving,
  none uncovered. It exposed a second oracle gap: a wrong positional offset can still assemble
  version zero from zero-filled padding, so a compatible-only short-read test cannot establish
  foreign-version refusal. The tests now pair V0 and foreign version 1 through both byte-handler
  representations, preserve foreign-version priority over a malformed kind byte, and assert that
  recoverable input returns non-null resource-bound evidence before any recovery. Direct feature
  admission also separates readable read-only requirements from incompatible requirements; its
  redundant reader policy setter was removed because discovery never heals records.
- Why tests missed it: executing a higher-level open lets an earlier gate mask a missing later gate.
  An arbitrary malformed fixture also cannot establish which side of an exact format boundary is
  admitted, or whether a short directory slot is mistaken for a previously buffered complete slot.
- Regression test: `FormatRepairBoundaryTest`, `StorageTypeTest` and
  `InterruptedFirstCommitProofTest`, associated with their changed production boundaries in
  `.github/mutation-test-associations.toml`. Oracles are FMT-401's refusal/recovery table,
  FMT-104's complete-record and retention contracts, and CONV-301's bounded I/O contract.
  Assertions observe exceptions, retained ownership, returned storage, directory-read counts and
  persisted resource bytes, without inspecting implementation source. The supplied complete PIT
  report is the counterfactual evidence for the missing assertions; the follow-up local evidence
  lives under `build/format-ci-pit/` and does not change the CI lane.
- Lesson: map requirement tests to every executable boundary they establish, and exercise a later
  gate directly when earlier refusal would conceal its absence; see [LESSONS.md](../LESSONS.md).

## Final local evidence

The complete follow-up used the unchanged lane command generator, changed-line restriction and
PIT timeouts, with no per-class sample or enclosing deadline. It selected the same 24 changed
sources, 82 classes (the redundant `RepairProof` class was removed), 44 test patterns and 607
changed lines. All 163 changed-line mutants were resolved: 162 killed, one reviewed exact-EOF
boundary equivalent, zero uncovered and zero failing. No refusal-path or uncovered mutant was
dispositioned. Of the original 36 findings, 27 are detected by behavior tests and nine redundant
operations were removed; the retained local ledger records every original six-field identity and
its current killed counterfactuals or removal reason.

The final `results.json` SHA-256 is
`d88c44b59688c3c5a8466a8c11e3f4b3e0009c6633ec56b678d6d535f33d0406`.
The full report, command, selection, source hashes and original-mutant ledger are retained under
`build/format-ci-pit-final/`. The exact EOF identity and its CONV-201 cost-boundary argument are in
`.github/mutation-dispositions.toml`, with the requested provenance commit
`c2f49677dba6e139f7f48303953259318d012565`.

Final normal verification passed 438 core tests (including all standard core work budgets),
322 query tests and three MCP tests. Five query fixtures explicitly gated by
`SIRIX_VALID_TIME_LARGE_BUDGET` remained skipped under the documented standard command.
The full work-budget commands remain those in [VERIFICATION.md](../VERIFICATION.md#running-the-layers).
The focused refusal/admission suites can be reproduced with:

```bash
./gradlew :sirix-core:test -PtestHeapMin=128m -PtestHeapMax=3g \
  --tests io.sirix.access.FormatRepairBoundaryTest \
  --tests io.sirix.io.StorageTypeTest \
  --tests io.sirix.io.InterruptedFirstCommitProofTest \
  --tests io.sirix.budget.FormatDiscoveryWorkBudgetTest
```

The PR provenance addendum is prepared at `build/format-ci-pit-final/pr-body-section.md` for the
outer executor, which owns the PR phase. This CI phase did not push, edit the PR, waive a check or
control another pipeline.
