# Bug: lifecycle performance acceptance had no counted-work boundary

- Fix / issue: PR #1353, Firstmate097; CONV-301 and TXN-503/504/802.
- Symptom: lifecycle correctness tests could pass while commit or teardown revisited an entire
  unchanged document or repeated a completed durability barrier. The separate recovery-cleanup
  predicate survivor also lacked an accepted behavioral disposition.
- Trigger: load a document, commit twice without changing it, then close the writer, enclosing
  session or database, including repeated close and multiple owned readers.
- Root cause: the acceptance mapping covered ownership and errors but omitted work and
  allocation bounds. Recovery deletion additionally searched every process recovery key for a
  prefix although the resource owns exactly one marker.
- Why tests missed it: correctness alone does not distinguish constant owner work from a scan.
  The historical recovery test's private-map assertion was withdrawn; deletion/recreation
  does not independently prove absence of retained bookkeeping.
- Regression tests: `LifecycleWorkBudgetTest` derives a JSON/XML, size and owner matrix from
  the CONV-301 refinement, checks record visits and device forces, asserts every document
  value after commit and reopen, and supplies a positive traversal control.
  `LifecycleLockAllocationBudgetTest` checks zero uncontended admission allocations in the
  clean allocation JVM. These are new acceptance coverage, not a claim that existing code
  violated the newly specified ceilings.
- Recovery correction: remove the prefix scan and its predicate, using direct removal of the
  one marker path shared with writer admission. The public resource-removal regression is
  retained; it is not described as proof of private retention or as a kill of the old predicate.
  `ResourceConfiguration.canonicalResourceName` admits one path component, map entries are
  established only through `getCommitFile`, and `LocalDatabase.removeResource` is the sole
  cleanup caller. No supported nested-resource or database-prefix deletion contract is removed.
- Verification: see `docs/lifecycle-mutation-accounting.md` for qualified evidence and gaps.
  The first draft incorrectly compared an all-index record total against a document-only
  ceiling (XML name dictionaries contributed 4,096-dependent work), and included the terminal
  storage-pool safety flush in owner retirement. The corrected measurement keeps the ceilings,
  isolates document events under the existing diagnostic gate, and retains the pool with a
  peer owner. It makes no claim about total XML dictionary work or final pool shutdown.
- Lesson: [budget lifecycle work independently of lifecycle correctness](../LESSONS.md#budget-lifecycle-work-independently-of-lifecycle-correctness).
