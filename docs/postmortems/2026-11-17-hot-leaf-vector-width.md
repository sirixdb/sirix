# Bug: cached HOT lookup allocates on narrower-vector runtimes

- Fix / issue: PR #1454, macOS core check `134404960770` in run `44185722435`.
- Symptom: cached `HOTLeafPage.findEntry` allocates 983,040 bytes per 4,096 calls
  (240 bytes/call), equally for raw and decoded leaves. The failure is the absolute
  raw-zero bound, not an encoded-minus-raw regression. Earlier codec operations
  also have equal allocation. Native LZ4 is unavailable on that runner.
- Trigger: a 32-entry leaf with byte partial keys on a runtime whose preferred
  vectors are narrower than 256 bits. HotSpot C2 with `-XX:MaxVectorSize=16`
  reproduces exactly the same byte counts locally, with both available codecs.
- Root cause: all three partial-key equality searches test the lane count of their
  fixed `SPECIES_256`, a compile-time tautology, rather than runtime vector width.
  They therefore enter the allocating Vector API software fallback on 128-bit
  runtimes. JFR allocation stacks show `ByteVector.broadcast`, `Byte256Vector.compare`
  and mask construction beneath `simdEqualitySearchBytes`. The same HOT leaf source
  exists unchanged at the pre-PR merge base; the new codec gate exposed this
  pre-existing warm-path defect. The invariant is zero allocation for prepared
  cached lookups through both overloads, across byte, short and int partial keys.
- Why tests missed it: the workstation's wider-vector compiler removes these
  objects. Disabling escape analysis or changing compilation timing alone does
  not reproduce an unsupported vector shape. The original species-length checks
  also leave the scalar alternatives unreachable.
- Regression test: `HOTLeafLookupAllocationTest.cachedLookupsAllocateNothing`
  compares both overloads with JDK unsigned lexicographic binary search, for hits,
  misses and insertion points, including padded probe arrays. Sequential keys and
  zero plus powers of two cover byte/short/int keys and 8/9, 16/17, 32-entry
  boundaries. Exact zero allocation is measured after routing-index warmup.
  `:sirix-core:hotLeafNarrowVectorAllocationTest` runs it and the unchanged strict
  codec allocation gate in a clean C2 JVM at 128 bits; the existing clean fork
  also runs the new test at the runtime's normal width.
- Correction: retain fixed 256-bit vectors on capable runtimes; use the existing
  allocation-free scalar implementations otherwise. One runtime-width decision
  controls all three equality searches. No allocation bound or codec policy changes.
- Retrace: raw and decoded cached pages build their partial-key index once, then
  both `findEntry` overloads select byte, short or int equality by discriminative
  bit count. The runtime-width decision picks either the original vector mask
  computation or the existing scalar mask computation. Candidate suffix verification
  and binary-search insertion points remain shared. Lookup/value access, insert,
  update, removal and posting callers retain the same index/result contract;
  fragment-fill callers that explicitly disable routing still use binary search.
  The scalar branches become reachable; no helper, parameter or caller becomes dead.
- Rebase: all seven submitted commits were replayed onto `f4e081c7`. The only
  conflict joins the independently appended `docs/LESSONS.md` entries. Range-diff
  confirms the six prior fix commits are unchanged and no submitted fix was dropped.
- Lesson: [test runtime vector width, not a fixed species' lane count](../LESSONS.md#runtime-vector-width-is-an-allocation-boundary).

Reproduction/profiling evidence is under `build/ci-repair/round5/`. All Gradle and
JVM commands use the prescribed `heavy.sh` wrapper. This repair intentionally
changes warm lookup bytecode to fix the baseline allocation defect; it cannot
claim bytecode identity or prove the deferred native paired-latency campaign.
The measured codec-margin follow-up remains pending; this is an interim repair.

## Executed evidence

- Before correction, the original codec test fails at exactly 983,040 bytes for
  operation 10 in the 128-bit C2 fork. The new unsigned-order/lookup test fails
  seven of eight cases in that fork, covering every partial-key representation.
  The HOT leaf source blob is `b3d7e8eed6d914cdbf3ef17bf8de8d6837952198` both at
  the pre-PR merge base and before this repair. JFR stacks are retained in
  `before-vector16.jfr` and `allocation-stacks.txt` under the evidence directory.
- After correction, `:sirix-core:dictionaryReadViewAllocationTest` and
  `:sirix-core:hotLeafNarrowVectorAllocationTest` pass all 25 cases. Each of the
  eight new lookup cases allocates zero bytes in both forks. Both codec arms
  retain their strict relative bounds and all original absolute zero bounds;
  operation 11 completes too.
- Blocking native LZ4 loading with the existing local `dlopen` interposer and
  repeating the narrow-vector task passes nine cases and skips only the LZ4 arm.
  The zero-run arm again reaches operation 11, with zero allocation at operation 10.
- Four clean-fork source mutations are killed: forcing just the byte, short or int
  vector branch restores two, three or two allocation failures respectively;
  reversing the shared runtime-width comparison restores seven failures. These are
  executable allocation regressions, not source-text assertions. The fixed source
  is restored and recompiled after mutation. Exact failures and commands are in
  `manual-mutations.json`, `mutate-width.py` and the four `mutation-*.xml` reports.
  This targeted evidence does not claim a fresh full-PR PIT verdict.
- Final combined verification passes 522 core leaf/codec/work-budget cases,
  327 query cases (five expected skips), five query allocation cases and three
  MCP budget cases. It uses the complete work-budget selections from
  `docs/VERIFICATION.md`, plus `io.sirix.index.hot.page.*`, `io.sirix.page.HOTLeaf*`,
  `ZeroRunByteCodecTest` and `PageKindTest`. Dedicated core startup, epoch and
  allocation forks also complete successfully. `:sirix-core:spotlessJavaCheck`
  and `git diff --check` pass. The log is `work-budgets-and-regressions.log`;
  normalized counts are in `final-results.json`. Free disk remains about 82 GB,
  above the required 60 GB floor. Actual macOS CI and post-rebase pipeline
  revalidation remain with the outer executor.
