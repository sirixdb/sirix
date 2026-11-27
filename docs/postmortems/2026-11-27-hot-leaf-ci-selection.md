# Bug: HOT leaf mutation selection omitted existing format contracts

- Fix / issue: PR #1454, CI run `45918887829`.
- Symptom: the changed-source PIT lane exceeded its 1,320-second execution share. Its partial
  report contained uncovered and surviving directory, packed-access and retained-memory mutations;
  the Sonar analysis separately failed the new-code reliability gate (rating D).
- Trigger: selecting the FMT-210..217 change by production class name. Format tests live in
  `io.sirix.page`, cache tests in `io.sirix.io.filechannel`, and split tests in `io.sirix.index.hot`.
- Root cause: name-based selection does not express those cross-package contracts. The writer's
  package fallback happened to select the key/stamp suites, but neither that fallback nor the
  page's exact-name match selected the directory, native-wire and production cache-read contracts.
  The same selection also admitted three LZ77 timing methods, each running 105,000 or 210,000
  decodes of 32 KiB. Under instrumentation these consume the deadline without providing
  deterministic behavioral or cost evidence. Method-level `benchmark` tags now exclude just those
  three methods from PR mutation runs while retaining their three correctness/dispatch siblings.
  Sonar also reported implicit int arithmetic at long byte-offset boundaries, reusable thread-local
  scratch, and the intentionally thrown exception for an unreadable value reference.
- Why tests missed it: the missing tests existed and ran in ordinary CI; their selection for PIT
  depended on names rather than explicit associations. Passing ordinary tests does not certify
  static-analysis findings or completion of a mutation run.
- Regression checks: run the real `pr_mutation.test_scope` consumer with the prior and corrected
  association models. `HOTLeafDirectoryEncodingTest` for `HOTLeafBody`, `HOTLeafRawWriterTest` for
  `HOTLeafBodyWriter`, `HOTCompactFragmentReadTest` for `HOTLeafPage`, and
  `HOTLeafPageNativeWireTest` for `PageKind` are absent before and selected after. The existing
  Python mutation-helper tests validate the consumer and association schema. Execute the selected
  Java behavior suites and focused changed-line PIT; an unfinished report remains incomplete.
  `HOTLeafKeyContractTest` additionally derives key-word and distinguishing-bit expectations from
  logical key bytes, JDK ByteBuffer and one-bit probes (FMT-212/213), across every 0..8 byte page
  prefix and raw, packed, compact and writable-copy representations. It rejects incorrect prefix
  assembly or a distinguishing bit based on physical layout rather than the logical key.
- Lesson: record format and ownership contracts against every production entry point, even when
  another changed class happens to bring their tests into the union. See [Lessons](../LESSONS.md)
  and the fragment `hot-leaf-ci-selection.md`.

## Invariants and scope

CONV-204 requires the lane to select the existing behavior tests that observe the changed contract.
Explicit associations now cover the body reader and writer, the mutable/packed page, and PageKind's
ordinary, compact, native-wire and projection-overflow readers. The guarded native LZ77
entry point is explicitly associated with the persisted-overflow contracts as well. They add no waiver, mutation filter,
execution-budget increase or work-budget relaxation. They preserve all original name-based selections.

MemorySegment byte offsets must be evaluated in their long coordinate type before addition or
subtraction. Explicit widening covers posting encoding/expansion headers, writer key/value reads,
group/shared-value spans, hash reads, key writes and ordinary-slot value-length writes. HOT leaf
bounds already restrict these operands to the 64 KiB region; the widening preserves valid bytes,
refusals, branch structure and allocation behavior.

The three reported ThreadLocals are intentional bounded scratch caches. The writer owns only
fixed-size primitive buffers and a heap-backed posting segment. The compactor owns one 512-element
char array. The borrowed selector clears both caller-backed fields in its existing finally block on
success and failure. Field-local S5164 explanations preserve reuse without retaining caller storage.
The S6416 explanation is local to `copyStoredValue`: its public contract explicitly requires
IllegalStateException when `valueRef` returns the unreadable sentinel. Removing this refusal would
publish a malformed value instead of fixing a defect.

## Caller and failure trace

The persister still prepares the writer, selects the same widths/groups/sharing, and writes the
same directory and payload. Group and sharing comparisons and hash reads keep their existing
validated spans. Posting writers keep the same header values, counts and ordinals. Packed promotion
still expands each stored value, writes its u16 length before the payload, and returns its slot size.
`getValue` and `copyStoredValue` still copy readable values; the latter still refuses an unreadable
reference. Borrowed selection clears its caller references even when validation or lookup throws.
No parameter, helper or branch becomes unreachable from the arithmetic or annotation changes.

Vision check: preserves one format, exact results, malformed-storage refusal and allocation-free
scratch reuse. No runtime default or alternate read route is introduced. The mutation-only timing-test
selection change is explicit in the CI configuration and CONV-204 inventory for maintainer review. Remote Sonar quality-gate
confirmation belongs to the next outer CI run; local compilation cannot certify the hosted gate.

## Local verification and remaining blocker

- Mutation-helper consumer tests: 266 passed.
- The real Gradle configuration exposes `excludedGroups = [benchmark]`; JUnit discovery with that
  configuration retains exactly `nativeProducesSameOutputAsJava`, `dispatcherRoutesToNative` and
  `anUnpaddedInputUsesTheCheckedJavaFallback`, versus all six methods without the exclusion. This
  exercises the actual consumer of the compiled annotations rather than checking source text.
- The remaining-source run using the old, untagged baseline was stopped as superseded after the
  timing-loop fix. Its partial results are retained and are not counted as complete evidence.
- Key-contract and official core work-budget tests: 370 passed, zero failures, without changing
  any bound; startup/allocation prerequisite tasks also passed.
- Selected HOT leaf, posting, compact-read and overflow Java tests: 307 passed, zero failures;
  the standard task also ran its startup/allocation prerequisites.
- Native-wrapper changed-line PIT after the tag/association correction: completed in 33s,
  with 3 KILLED and 2 SURVIVED mutations and no uncovered mutations. The remaining availability
  survivors are reported, not waived.
- Focused changed-line PIT for HOTLeafBody, HOTLeafBodyWriter, HOTLeafPage and HOTPostingFrame:
  completed in 5m45s, with 1,068 mutations: 986 KILLED, 10 TIMED_OUT, 65 SURVIVED and 7 NO_COVERAGE.
  The four existing HOTLeafBody dispositions are preserved. The completed report is **not clean**:
  undispositioned survivors and uncovered paths remain, including writer hash quality/cost,
  mutation refusal/promotion, posting materialization and key-prefix boundary paths.

The association correction and logical-key test do not resolve that remaining mutation debt.
In particular, changing a hash can preserve every output while worsening collision work; ordinary
output assertions cannot establish equivalence, and CONV-201 does not permit a blanket cost waiver.
No new mutation disposition, widened work bound, sampling filter or timeout increase is included.
Reports and commands are retained locally under `build/ci-analysis/hot-leaf-ci/` for the outer
executor. This repair must not be reported as passing the blocking PIT criterion.
