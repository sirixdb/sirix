# Bug: HOT leaf codec CI fixtures assumed raw frames and native LZ4

Historical evidence for the retired kind-local leaf compression experiment. Its codec classes,
test names and settings below do not describe the current
[kind-12 format](../../spec/FMT-disk-format.md#fmt-210--hot-leaf-envelope-and-page-local-modes).

- Fix / issue: PR #1454; CI run `44147298174` at `0929d0b59`.
- Symptom: all four `ProjectionSortedFullImagePackingTest` versioning cases failed on Linux
  and Windows. Windows additionally failed the retained-memory codec fixture. CodeFactor
  reported complexity in the raw writer, leaf reader and mixed-format regression. PIT
  completed with eight surviving and two uncovered changed-line mutants.
- Trigger: serialize a packed sorted-index leaf with the new codec enabled, or run the
  repeated-value ownership fixture without native LZ4.
- Root cause: the packing oracle compared the encoded disk length with the independent
  pre-codec logical size (20,190 / 27,933 bytes). The ownership fixture used nonzero
  repeating bytes, compressible by LZ4 but not zero-run, yet required an encoded frame
  on every platform. Native extraction tests checked loaded symbols but not the staged
  filename, empty-root fallback or losing-publisher return. The identity-reader test
  checked retained ownership but not whether it allocated a temporary decompression frame.
- Why tests missed it: targeted codec suites omitted the existing sorted-directory packing
  regression; Linux supplied native LZ4. Byte equality alone cannot observe redundant
  temporary decompression or filesystem staging contracts.
- Invariants: packing sizes describe logical frames; hashes describe wire frames. An
  encoded ownership fixture must compress with every supported codec set. Refactoring
  must preserve sparse/full trailer emission and every copying/ownership read route.
- Regression tests: `ProjectionSortedFullImagePackingTest` checks the hash before expansion
  and checks the same exact logical sizes afterward, at both historical checkpoints and
  for all versioning modes. `HOTLeafPayloadCodecTest` keeps the 4,256-byte payload / 8 KiB
  allocation oracle, uses a zero-run-compressible repeated fixture, and repeats the owned,
  reader, truncated-frame and mutation-promotion checks with LZ4 availability disabled.
  Its real identity pipeline must not receive an encoded input for temporary decompression.
  `NativeCodecLibraryTest` executes extraction against temporary directories, independently
  computes SHA-256 via the JDK, and models a losing atomic rename at the filesystem boundary
  while checking the winning file's bytes and absence of staging debris.
- CodeFactor correction: extract full-entry packing and the common slot decoder, consolidate
  identical side-trailer emission, and split mixed-format assertions into focused helpers.
  No format, allocation ownership or codec preference changes are needed.
- Lesson: see [codec CI contracts](../LESSONS.md#codec-ci-contracts).

## Mutation evidence and justified survivors

The original gate used 7 mutations per class. Its report is the source of the line numbers
below; focused reruns must use the actual JVM test names and preserve the gate's scope.

- `HOTLeafPayloadCodec.expandImage`, line 224, conditional-boundary mutation of the
  nonpositive stored-length test: admitting zero still reaches `decodeHeap`, which rejects
  zero before decoding. This is an equivalent refusal, not a missing accepted input.
- `HOTProjectionImage.copyVerifiedFrame`, line 139, AND to OR: the mutant always chooses
  encoded scratch for frames longer than two bytes. `expandImage` independently reads the
  flag and copies a raw frame to its exact logical length. Values, ownership, checksums,
  position and retained size are identical; only transient allocation routing changes.
- `FFILz4Compressor.decompressSegmentRange`, line 328, negated native-availability branch:
  with native LZ4 available it selects the existing Java decoder. The literal-only LZ4
  block regression checks the decoded bytes independently and verifies that an available
  bundled native adapter performs the work without entering the Java fallback.
- `NativeCodecLibrary`'s five originally undetected mutants (lines 57, 76, 88, 136,
  151) are killed by the extraction/publication checks: Linux `x86_64` alias dispatch,
  the exact content-addressed filename, root fallback, and the winning publication path.
- `PageKind$11.serializePage`, line 5869, removed raw writer: unreachable with the default
  process-lifetime `sirix.page.hotLeaf.compress=true`. The diagnostic false setting needs
  a separate fresh PIT JVM running `HOTLeafRawWriterTest`; changing the default gate's
  property would remove coverage of the encoded writer instead.

## Verification

- Before the fix, the focused core invocation reproduced all four Linux packing failures
  (`expected 20190, actual 10641`). The Windows artifact records those same four failures
  (`actual 17553`) and the ownership fixture's compression-flag assertion at line 391.
- After the fix, the following passed, including their dedicated allocation/startup dependencies:

  ```sh
  ./gradlew :sirix-core:test \
    --tests io.sirix.page.HOTLeafPayloadCodecTest \
    --tests io.sirix.index.projection.ProjectionSortedFullImagePackingTest \
    --tests io.sirix.io.bytepipe.NativeCodecLibraryTest \
    --tests io.sirix.io.bytepipe.FFILz4BoundedSegmentTest \
    --tests io.sirix.index.hot.page.HOTLeafFullImagePackingTest \
    --tests io.sirix.index.hot.page.HOTProjectionImageTest \
    -PtestHeapMin=512m -PtestHeapMax=3g
  ```

- The same 38-class / 7-mutant CI sample completed (182 mutants), killing the original
  reader and all five native-library mutants. A focused seven-mutant FFI rerun with the
  added literal-block test killed the native-routing mutant. A fresh PIT JVM with
  `JAVA_TOOL_OPTIONS=-Dsirix.page.hotLeaf.compress=false`, target `io.sirix.page.PageKind$11`
  and `HOTLeafRawWriterTest*` killed the removed raw-writer call. Both raw and encoded
  process settings are therefore needed to interpret coverage; the default reporter still
  reports that raw-only branch uncovered. The two functionally equivalent mutants above
  remain reported, without changing thresholds, selection or the reporter's blocking policy.
- CodeFactor's three flagged methods were reduced below their original complexities by
  the focused extractions. Provider reanalysis and Windows execution remain the outer
  executor's responsibility; they were not triggered from this phase.
- The focused suite also passed with `-Dsirix.lz77Codec.native.disable=true` (43 tests,
  no failures or skips). This includes the explicit no-LZ4 ownership scenario and all
  four versioning modes. The unchanged work-budget commands from `docs/VERIFICATION.md`
  passed for core, query and MCP, including the dedicated allocation tasks. Spotless and
  `git diff --check` passed. Local logs and mutation reports are retained under the ignored
  `build/ci-repair/` directory.

## Follow-up: HotSpot allocation, writer-mode coverage and Sonar lifetime

- Symptom: run `44156426448` failed the macOS allocation fork; its artifact upload
  omitted nonstandard test-task reports, so the original per-operation values are
  unavailable. Sonar rejected only `java:S5164` on the two reusable codec thread locals
  (new-code coverage was 88.6%). PIT reported the two equivalent survivors above and
  the raw-writer branch without coverage.
- Root cause reproduced locally: HotSpot without escape analysis allocates an
  `Optional` from `MemorySegment.heapBase()` on every encoded serialization. The
  unchanged allocation regression measured operation 7 at 0 raw versus 65,536 encoded
  bytes per 4,096 pages, for both payload patterns. Graal eliminated this object and
  hid the defect. This reproduces the allocation invariant violation, although the
  unavailable macOS detail cannot establish that runner's exact allocating operation.
- Invariants and scope: both process-lifetime writer modes preserve the V0 wire image,
  codec flag, payload and complete side-reference trailer; encoded serialization adds
  no per-page object. This covers full and sparse leaves, projection and other index
  types, both codec choices and raw fallback at the shared kind-12 writer boundary.
  Every scalar, batched, fragment and mapped read continues to receive the same bytes.
  Thread-local codec scratch is reused until thread exit, native leases are closed in
  `finally`, and published projection images own their decoded bytes.
- Fix: expose the array through the writer's existing cached heap `ByteBuffer`, which
  allocates a view only when backing storage changes. Move the unchanged mode dispatch
  to `serializeHOTLeaf` so the ordinary `PageKindTest` selection executes both settings
  without changing the process default or creating a second writer implementation.
  Annotate the two intentional thread-lifetime scratch fields locally, following the
  existing decoder-scratch lifetime convention; do not remove scratch on every page.
- Regression tests and independent oracles: `HOTLeafCodecAllocationTest` retains its
  zero-byte comparisons and all twelve operations. `PageKindTest` exercises both writer
  settings against the independent one-entry V0 hexadecimal vector, then uses a
  256-byte zero value (compressible by zero-run alone), flag bit 1 and a side reference
  with disk key 123. These reject an omitted raw write, an ignored setting, lost payload
  and omitted/duplicated trailer emission.
- Retrace: disabled mode writes directly to the caller with side references; enabled
  mode clears heap scratch, writes the logical frame without its trailer, offers its
  array to the codec, then emits the trailer once. The cached buffer exposes the current
  backing array even after growth and bounds its limit to the written prefix. No reader,
  ownership-transfer or lazy-promotion branch changes; both existing callers and the
  new test call use the same encoder.
- Verification: the same HotSpot/no-escape-analysis command passes after the array
  accessor correction, including operation 7 at zero bytes in both arms. Focused PIT
  mutates all seven instructions/conditions in `serializeHOTLeaf`: all seven are
  killed, including the formerly uncovered raw-writer call and trailer emission:

  ```sh
  JAVA_TOOL_OPTIONS='-XX:-UseJVMCICompiler -XX:-DoEscapeAnalysis' \
    ./gradlew :sirix-core:dictionaryReadViewAllocationTest \
    --tests io.sirix.page.HOTLeafCodecAllocationTest -PtestHeapMin=512m -PtestHeapMax=3g
  ```

The unchanged 7-per-class CI sample also completes (177 mutants). Against the
current changed lines it kills 15 of 17 mutants, has no uncovered changed-line
mutants, and retains only the two equivalent survivors already justified above:
`expandImage` at line 227 and `copyVerifiedFrame` at line 141. The blocking reporter
will still flag those two; neither its thresholds nor its scope are weakened.

Final local checks pass: 370 combined core codec/packing/writer/work-budget tests,
seven dedicated allocation tests, the query and MCP work-budget blocks from
`docs/VERIFICATION.md`, `spotlessJavaCheck`, and `git diff --check`. The earlier
separate core test invocations overlapped Gradle's report directory and produced
EOF/missing-report-file errors; the 370-test combined run replaces those invalid
results. Logs, downloaded provider reports and both PIT reports are retained under
ignored `build/ci-repair/round3/`. Remote Sonar reanalysis and OS CI reruns belong
to the outer executor and were not triggered here.

### Windows query failure remains unconfirmed locally

The downloaded Windows report identifies `I5-leaf-constancy` in
`AbstractHOTIndexWriter.validatePublishedStructuralScopeBounded` during a pressured
SH1 commit (`BitemporalStorageEpochEvictionTest`, unbounded case). Both eviction tests
pass locally with the default codec settings, with native decoding disabled, and with
the legacy raw writer selected in a fresh JVM. These controls do not prove the Windows
failure is external to this PR. No structural algorithm or test assertion is changed
without a reproducer; the original Windows failure still requires platform validation.

## Follow-up: redundant expansion route and missing macOS diagnostics

- Run `44169043569` again reports the two PIT survivors above. The macOS core lane
  fails `HOTLeafCodecAllocationTest.encodedPathsAddNoAllocation(false)` at the final
  relative-allocation assertion. Its log omits the operation and byte counts, and the
  artifact uploader matches only `reports/tests/test`, so it uploads no allocation report.
- Invariants and scope: raw image publication owns one payload array, including through
  both `copyIfFits` and `copyVerifiedFrame`. Encoded publication owns one expanded payload
  array; only encoded frames enter the expander. Expansion respects its logical input
  length before sizing output, for both exact arrays and longer reusable scratch.
  All matrix lanes must retain reports
  from their actual failing test task, including allocation and startup forks.
- Root cause of the routing survivor: the AND-to-OR mutant makes raw images take an
  unnecessary scratch-and-expand copy route. Expansion's raw acceptance makes its result
  indistinguishable from the direct raw route, including allocation: it copies thread-local
  scratch into one owned array. The raw expansion branch has no production caller other
  than this erroneous routing. Remove that acceptance instead of adding work counters or
  weakening mutation reporting. Raw image factories keep their existing direct copy.
- Regression: `projectionScratchRejectsEveryShortFrameAndDoesNotAliasRetainedImages`
  now requires raw frames to bypass the encoded-frame expander, both for exact arrays
  and bounded scratch. Existing image-factory tests continue to require raw publication,
  so the erroneous routing becomes observable. Test-only packing/differential oracles
  normalize their generated wire frames according to the compression flag.
  `encodedImagesRejectInvalidStoredLengths` exercises negative, zero, truncated and
  overflowing lengths against exact arrays and reusable scratch, using malformed wire
  frames as the independent rejection oracle.
- Remove the duplicate positive/MAX_ENCODED check in `expandImage`; the shared
  `decodeHeap` boundary already enforces it. JDK range checks bound both the five-byte
  codec header and the stored payload to the caller's logical frame before allocating
  the expanded image. The former handwritten header arithmetic also had an equivalent
  mutant because the later payload check independently refused it. Negative or
  truncated ranges now throw `IndexOutOfBoundsException`, matching the other argument
  range checks; zero-length payloads still reach the decoder's explicit refusal.
- Retrace: valid encoded inputs produce the same output size and tail offset, then
  decode into their owned array. Negative and oversized ranges fail before allocation;
  zero stored length fails in `decodeHeap`. Both image factories keep their existing
  raw and encoded allocation/ownership paths. Ordinary reads, fragments, serializer modes,
  side references and lazy mutation promotion are unchanged.
- A further sampled survivor moved the writer's `used < 6` shortcut by one byte;
  the final savings-margin check still wrote raw bytes. Keep only the empty-payload
  shortcut required by positive-size native scratch borrows, and let the shared savings
  check decide every positive payload. Extend the independent framing golden to lengths
  1, 5 and 6. Both codec sets retain raw bytes for every tested payload of at most 8 bytes.
- macOS remains unconfirmed locally: HotSpot runs with escape analysis disabled, with
  native LZ4 loading blocked, and with slower compilation/OSR disabled all pass. These
  controls do not establish that the macOS failure is external. Keep its assertion and
  every allocation/work bound intact. The cross-platform uploader now includes all HTML
  and XML test-task reports, and the allocation task prints full assertion messages.
  The next actual macOS execution belongs to the outer executor.
- Reproduction evidence under `build/ci-repair/round4/`: the independent V0 golden-frame
  probe fails against the original expander and passes against the strict expander.
  Twelve fresh HotSpot JVMs, each restricted to two processors with LZ4 loading blocked,
  pass the unchanged allocation method. An escaping 32-byte-array-per-encoded-operation
  control fails it, including every previously zero-allocation operation at 196,608 bytes
  per 4,096 calls. No tolerance, retry, work budget or codec-default change is introduced.
- Final verification: the unchanged 38-class / 7-per-class bounded PIT configuration
  completes with all 17 changed-line mutants killed, zero survivors and zero uncovered
  mutants (177 total; unchanged-line debt remains advisory). Both original survivors
  are eliminated without exclusions or reporter changes. The blocking local command is
  `python3 .github/scripts/pr_mutation.py run --output build/ci-repair/round4/mutation-complete --seconds 1320 --blocking`;
  its selection uses the supplied base commit and current working-tree source.
- The final combined core codec/packing/work-budget invocation passes 362 tests plus
  seven dedicated allocation tests. The query and MCP work-budget commands from
  `docs/VERIFICATION.md` pass (327 query cases, five intentionally skipped; five query
  allocation cases; three MCP cases). Six additional boundary contracts pass with native
  LZ4 loading blocked. `spotlessJavaCheck`, `actionlint .github/workflows/gradle.yml` and
  `git diff --check` pass. The macOS assertion is still unresolved, not classified as
  infrastructure or claimed fixed by these local results.
