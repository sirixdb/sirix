# Bug: HOT leaf codec CI fixtures assumed raw frames and native LZ4

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
