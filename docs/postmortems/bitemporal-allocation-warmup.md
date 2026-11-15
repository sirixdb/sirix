# Bug: interval allocation budget measures transient JVM work

- Fix / issue: correctness-gate test finding T1 for PR #1352.
- Symptom: `scratchAndNativeSlotsAreReusedAndFailureClosesOwner` intermittently reports
  nonzero allocation in its checked N or 2N window, despite other identical windows allocating zero.
- Trigger: a fresh JVM with `-XX:ActiveProcessorCount=2`, `-Xms256m`, and `-Xmx2g`.
  The initial focused Gradle replay passed, but eight of twenty direct executions of the exact
  test method failed with 1,808 bytes in the checked N window. The reported gate failure was
  1,512 bytes. A Flight Recorder replay also failed with 1,808 bytes in the checked 2N window.
- Behaviour / invariants (specified before repair): measure steady-state interval read,
  replacement and write work. N and 2N must each allocate exactly zero bytes. Keep the
  escaping-object positive control, insertion counts, native-slab bound and owner-close checks.
  Do not add allocation tolerances, timing assertions or retries of failed measurements.
- Regression checks (derived before repair): execute the existing test in fresh constrained
  JVMs; trace the nonzero windows; rerun the focused class after repair. Its
  `replacementAllocationBudgetRequiresZeroInBothWindows` control must still reject constant
  allocations and either nonzero window. Inject an escaping allocation into the ordinary
  replacement path in a disposable test copy and require failure.
- Root cause: a few long helper calls warm on-stack replacement (OSR), without warming the
  helper's normal entry. Late OSR activation can materialize VM strings on the measured thread.
  With TLABs disabled to record every allocation, Flight Recorder attributed all 1,808 bytes
  (23 strings and 23 backing byte arrays) to the helper's loop backedge, without interval-store
  frames. Disabling OSR on the unmodified test eliminated failures in twenty fresh JVMs.
  Increasing a byte allowance would hide this harness problem and weaken the product budget.
- Why tests missed it: the last two of six long calls were assumed to be warmed, but compilation
  progressed asynchronously. Passing replays merely moved the one-time burst to another window.
- Fix: make 40,000 short, two-operation calls before the existing windows, warming the normal
  entry as well as interval operations. Assert their insertion count. Keep all measured windows,
  their zero-byte assertions, positive allocation slope and failure/cleanup controls unchanged.
- Verification: twenty fresh constrained JVMs pass with the fix, as do ten with
  `-XX:CompileThresholdScaling=5`. Both checked windows allocate zero throughout; some earlier,
  unasserted warmup windows still include VM setup. The focused Gradle class passes all five tests
  with the constrained JVM configuration, including its strict-budget and parser controls.
  For comparison, the unchanged baseline fails eight of twenty fresh constrained JVMs.
- Mutation evidence: in a disposable copy, force the helper's escaping `new Object()` branch
  for ordinary operations too. The same fixed fixture fails its N-window assertion at 640,000
  bytes (2N: 1,280,000), so warmup does not hide per-operation allocations. No production logic
  changes; PIT does not mutate test warmup code.
- Reproduction: use a same-package runner calling
  `new BitemporalLoaderBudgetTest().scratchAndNativeSlotsAreReusedAndFailureClosesOwner()`
  on the query test runtime classpath, with `--enable-preview`,
  `--enable-native-access=ALL-UNNAMED`, `-XX:ActiveProcessorCount=2`, `-Xms256m`, `-Xmx2g`.
  Repeat with `-XX:-UseOnStackReplacement` for the baseline diagnostic, or
  `-XX:CompileThresholdScaling=5` for the fixed adversarial compilation case.
  The focused Gradle selector is `:sirix-query:test --rerun --tests
  io.sirix.query.bench.bitemporal.BitemporalLoaderBudgetTest`; no repository-wide suite,
  formatter, linter or static-analysis gate was run in this test repair phase.
- Lesson: warm the normal entry of an allocation-measurement helper through repeated short
  calls, rather than assuming long OSR loops establish steady-state entry behaviour.
  See [Lessons](../LESSONS.md).
