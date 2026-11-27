# Bug: Unified-pool acceptance oracles

- Fix / issue: Review findings R1–R8 on `fm/sirix-unified-pool-tests`.
- Symptom: The independent MEM tests could reject permitted platform and scratch behavior, accept slab shrinking or allocation regressions, miss OS-release coverage and use-after-unmap calls, or run with ambiguous bindings and a contaminated allocation worker.
- Trigger: Repeated release after writing a new pattern; returning a reusable scratch lease; closing the last lease in several slabs; allocating inside the frame-handle constructor; releasing 4 KiB slots on a 16/64 KiB-page OS; touching the first unmapped region while other regions remain live; enabling the planned allocation suites; supplying incompatible pressure-kind encodings.
- Root cause: R1 is a stale expected-value defect. R2 confuses mapped capacity with outstanding ownership. R3 samples the oracle after the destructive transition. R4 derives an allowance from the measured constructor. R5 fails to establish release eligibility or require an actual return. R6 observes global teardown completion rather than target-region lifetime and lacks an in-flight interleaving. R7 omits the required clean worker. R8 duplicates an unspecified API through discovery alternatives.
- Why tests missed it: Most MEM requirements are planned and their implementation does not exist yet; the acceptance harness itself needed review against the specification.
- Regression tests: `MemTestOracleRegressionTest` exercises the MEM-106 content oracle with successive patterns and foreign bytes, and the MEM-107 recording boundary with a stale target while another region is live. The existing acceptance tests now observe scratch lent-buffer/byte deltas, mappings and charges before every close, a fixed 40-byte handle allowance plus an independent JDK slice control, page-sized stress slots with positive return counts before cleanup, and shutdown overlapping a gated release. Both allocation suites use `dictionaryReadViewAllocationTest`. Planned step-1/2/3 behavior remains conditional on the corresponding implementation; no platform or pool conformance pass is claimed.
- Lesson: [Lessons](../LESSONS.md) via [the fragment](../lessons.d/unified-pool-test-oracles.md): establish independent pre-transition observations and explicit witness conditions, and give every seam exactly one contract.

The fixed handle allowance uses an explicitly configured clean-JVM layout: compressed references
and class pointers, ordinary 12-byte headers and 8-byte alignment. The historical handle's payload
is two references, two ints, a long and a close flag; its 37 bytes round to 40. The allowance never
executes that constructor. Extra constructor allocation therefore increases only the measured side.

Before verification, the failing sequences and ordinary callers were traced across repeated and
single platform releases, all lease/decode closes, both allocation controls, stress allocation and
version validation, every recording VM segment operation, pressure production/decoding, and both
constructor seams. Released-region state is checked again after the gated run observer, preventing
the synchronization itself from hiding an unmap that happened while the transfer was paused.

Vision check: these changes protect the specified bytes, reader safety and counted allocation cost;
they add no runtime behavior, persisted setting or default and preserve the independent test contract.
