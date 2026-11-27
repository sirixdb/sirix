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


## Follow-up: simulated geometry, shutdown and initial pool capacity

- Fix / issue: Follow-up review R9–R11; the R2 scratch-lease gauges, R3 pre-close retention snapshots
  and R6 per-region lifetime checks were already present and remain intact.
- Symptom: The fake could fail valid simulated-page releases on a host with larger pages; a shutdown
  that spins while draining transfers could deadlock the harness; a pool with prepaid slabs or metadata
  could fail the refused-reservation test despite obeying MEM-301.
- Trigger: Release a 16 KiB run at a 16 KiB offset on a 64 KiB-page host; pause a transfer and wait
  for a spinning shutdown to stop being RUNNABLE; exhaust frame budget then request an available warm
  pool slot while assuming its reservation must fail.
- Root cause: R9 mixed simulated geometry with host primitives. R10 used scheduler state as a
  synchronization contract and rejected the allocator's permitted transient refusal. R11 confused
  construction with an empty account and warm capacity with a new reservation.
- Why tests missed it: Planned allocator and compact-pool APIs are absent from the baseline, and the
  existing oracle regressions did not execute release/recharge simulation or these planned interleavings.
- Regression tests: `MemTestOracleRegressionTest.simulatedPrimitivesPreserveBytesAndHonorInjectedFailures`
  executes the recording invocation boundary for the fixed release/recharge contract with 16/64 KiB
  geometry, patterned real memory, returned and thrown failures, and stale targets. A test-local interface
  supplies those fixed method descriptors while the baseline VirtualMemory interface lacks them; it
  supplies no alternate production binding. `FrameReleaseFailureTest` now bounds its gate and joins,
  always opens the gate after shutdown starts, accepts an overlapping refusal, and retries after the
  transfer finishes. `CompactImageAccountTest.refusedReservationMakesThePoolRefuseTheLease` holds all
  admitted warm leases until refusal, compares against pre-request snapshots, and frees both frame
  owners and warm leases before checking readmission. These last two acceptance scenarios remain planned
  until their implementation lands; no runtime conformance result for them is claimed.
- Lesson: Simulated geometry must have simulated effects; use protocol events rather than scheduler
  states for race coordination; exhaust warm capacity before attributing refusal to a new reservation.

The shared fake has one release/recharge mode: success without touching bytes, or an injected failure.
The unused OS-effect toggle and counter are removed. Real OS effects remain the responsibility of
`PlatformReleaseSemanticsTest` and `OptimisticReaderReleaseStressTest`, which use the real backend.
The existing clean allocation fork and exact reflective contracts are unchanged.


## Follow-up: pre-admission baselines and completed concurrency witnesses

- Fix / issue: R12–R14. The earlier R2, R3, R10 and R11 corrections remain present.
- Symptom: A fallback decode could wrongly retain scratch or evict guarded images before the test
  sampled its baseline; shutdown could be delayed until after the transfer finished; readers could
  reject every recharged version while stable-owner reads kept the stress test green.
- Trigger: Mutate account/cache/eviction state during fallback admission; deschedule the shutdown
  thread before its call; return false for validation of every recharged owner.
- Root cause: These are missing observation boundaries: post-decode values cannot constrain decode,
  a thread-start signal cannot prove shutdown entry, and stable reads cannot witness recharged reads.
- Why tests missed it: Planned acceptance scenarios had assertions on terminal state but lacked the
  prerequisite events that made those assertions relevant to the required execution.
- Regression tests: The fallback test now captures all four account/cache/eviction values before
  each decode and requires equality both immediately on fallback admission and after closing it.
  The shutdown test keeps the transfer gated until `isTerminated()` observes admission closed or
  the overlapping call has actually refused; the wait is bounded and the existing final retry is
  preserved. The reader stress test pauses the first published recharged owner, requires a known
  valid read, and waits for a reader that selected that checkpoint before reading to validate it;
  it also requires a positive churn-read count. The checkpoint is cleared before churn resumes.
- Lesson: Snapshot before the transition being constrained, observe entry inside the operation's
  protocol, and require each reader population to produce successful validated reads.

Pre-verification traces cover fallback admission, normal close and failed assertions (which now
also return the fallback lease); shutdown that waits, spins, refuses or fails to enter; and readers
scheduled before publication, at the paused checkpoint, or after churn resumes. The shutdown witness
uses the existing admission query; no new runtime API, reflective alternative or scheduler-state
requirement is introduced. These scenarios remain planned until the implementation lands; compiling
and selecting them is not evidence of their runtime conformance.
