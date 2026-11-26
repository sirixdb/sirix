# Bug: encoded ordinary leaves retained writable frames

Historical evidence for the retired kind-local leaf compression experiment. Its codec classes,
test names and settings below do not describe the current
[kind-12 format](../../spec/FMT-disk-format.md#fmt-210--hot-leaf-envelope-and-page-local-modes).

- Fix / issue: review R1, S1 HOT leaf payload codec.
- Symptom: a 4,256-byte slot payload retained a 64 KiB decoded frame after an encoded read, while raw production reads retained one 8 KiB allocation; cache charging increased and fewer leaves fit.
- Trigger: 32 entries with 128-byte values, FULL versioning, empty byte-handler pipeline, ordinary native read carrying a DecompressionResult. Scalar, batched and mapped ordinary reads converge at this decoder.
- Root cause: compressed input cannot transfer its outer storage as decoded slots, but the copying branch conflated that ownership constraint with the need for immediate writable capacity. The existing leaf already promotes small slot memory on mutation. The empty pipeline also made a redundant owned copy of encoded input, which can never transfer into the decoded page; the first production allocation comparison exposed its extra 72 B/page. Removing that unnecessary copy in the shared reader preserves the existing raw ownership path. The encoded leaf exposes its exact payload length and retains its allocation base for ownership and its allocation-era identity for stamps, so its cache charge also matches raw.
- Why tests missed it: full-materialization allocation and timing fixtures omitted DecompressionResult, so their raw arm also allocated 64 KiB; neither checked retained memory or cache charge.
- Regression test: HOTLeafPayloadCodecTest.ordinaryOwnedReadsRetainSmallFramesAndPromoteOnlyOnMutation executes both the public reader and the empty pipeline with real ownership, checks allocator bytes, canonical cache charge, input independence, side references, stamps, promotion and close for raw/encoded, zero/repeated payloads and all versioning types. Truncated side trailers must leave no live allocation. The pre-fix encoded path retains 65,536 bytes instead of the required 8,192. emptyEncodedZeroRunPayloadRemainsReadable covers the minimum positive allocator request. Run the focused codec test and clean-fork allocation task; the review verification result records execution separately. The initial production ownership allocation comparison failed by 72 B/page before removal of the unused identity-pipeline copy; that failure is retained in build/review-leaf-codec/focused.log.
- Lesson: compare production ownership routes and retained native/cache bytes, not only decoder allocation. See [Production read ownership and retention](../LESSONS.md#production-read-ownership-and-retention).

## Focused review verification

The final heavy-wrapper Gradle run passed 20 codec tests, both clean-fork
allocation cases, benchmark compilation and scoped PIT. The public-reader full
materialization comparison measured 3,520 B/page raw and 3,432 B/page encoded
for both zero and repeated fixtures; this is the unit comparison, not the
standing five-fork or EA-off gate. Both arms retain one 8 KiB allocation and
expose 4,256 slot bytes for equal cache charging. The nonempty-pipeline
adversarial regression also passes.

Local evidence is under `build/review-leaf-codec/`: `confirmed.log`,
`result.json`, `pit/mutations.xml`, and `mutant-dispositions.json`. The last
file records every non-killed mutant individually. PIT killed 75 of 208
mutants; 20 survived and 113 lacked coverage. Those totals include unchanged
initializers, raw copying, overflow and record-read behavior from the broader
method/class scope; they do not establish full-class mutation coverage.
Changed allocation-size and payload-view condition mutants were killed.

The remaining new reader-shortcut mutants are:

| Method, line/index | Mutation | Disposition |
| --- | --- | --- |
| deserializeFromSegment, 358/67 | conditional boundary | At two input bytes the flags byte is absent; either envelope parsing or the shortcut rejects the frame before publication. |
| deserializeFromSegment, 358/55 | negated conditional | Disables borrowing for pipelines and restores redundant input allocation. The separate clean-fork public-reader allocation assertion rejects that cost; it is intentionally outside PIT. |
| deserializeFromSegment, 358/67 | negated conditional | Disables borrowing for complete frames; the allocation assertion rejects the cost. Short headers still reject, potentially at a different check. |
| deserializeFromSegment, 359/76 | negated conditional | Disables HOT borrowing; the allocation assertion rejects that cost. It also admits other kinds with flag 0x02; those kinds are outside this focused codec suite, so no global correctness equivalence is claimed. |

The outer executor must renew the affected census, cold and warm measurements,
EA-off sites, five-fork comparison and work budgets. Prior footprint/timing
numbers describe the earlier implementation and are not reused as proof here.
The existing discard-only allocator diagnostic and its prior acceptance remain
in the benchmark gate notes; allocation-era lifetimes were not changed.
