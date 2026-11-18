# HOT leaf codec allocation gate

`io.sirix.page.HOTLeafCodecAllocationBenchmark` keeps full and compact results
in an escaping sink: production readers publish decoded pages. `fullMaterialize`
calls the public ordinary reader with an empty byte-handler pipeline, including
raw `DecompressionResult` transfer, borrowed encoded input and decoded-frame
release. `HOTLeafCodecBenchmark.coldMaterialize` uses the same ownership route
and reports retained decoded bytes during setup. The direct codec and compact
fixtures remain separate diagnostics. The RAW fork
sets the diagnostic writer property before loading PageKind, and uses only
pre-existing APIs on its measured paths. The same benchmark class therefore
runs over unchanged-main production classes without a fixture rewrite.

Run every JVM/Gradle command through the project's authorized heavy-job wrapper.
Build `:sirix-benchmarks:jmhJar`. To compare with the pre-codec base, compile the
unchanged base versions of AbstractReader, PageKind, HOTProjectionImage,
ZeroRunByteCodec and FFILz4Compressor into an external class directory using the JMH JAR classpath.
Place that directory ahead of the JAR for the RAW baseline; use the JAR alone
for ENCODED. Pin the baseline commit, source hashes, classpaths and JVM version
in the retained artifacts. Never substitute the branch's RAW arm for main.

Run both `HOTLeafCodecAllocationBenchmark.fullMaterialize` and
`HOTLeafCodecAllocationBenchmark.uncachedSelection` with ZERO and
REPEATED content, GC profiler, one thread, at least five fresh forks per arm,
five one-second warmup iterations and five one-second measurement iterations.
Write RAW results to `main-owned.json`, ENCODED to `encoded-owned.json`, then:

```sh
python3 /absolute/repo/bundles/sirix-benchmarks/scripts/check-hot-leaf-allocation.py \
  /absolute/artifacts/main-owned.json /absolute/artifacts/encoded-owned.json
```

The standing assertion requires **every** encoded fork mean to be at most the
maximum unchanged-main fork mean plus 1 B/op of profiler background noise
(less than one object), within the same method/content fixture. Preserve all
forks; a failing fork cannot be excluded or hidden by averaging. Do not widen the allowance after a failure. The clean-fork
`HOTLeafCodecAllocationTest` separately asserts exactly zero extra bytes on
12 paths, including zero bytes for codec selection and reusable destinations.
The comparator also accepts a full benchmark suite; use `--minimum-forks 1`
only for supplemental per-path profiles, not the standing owned-result gate.

The discard-only full-read fixture can retain an additional 40-byte
NativeMemorySegmentImpl in the unchanged allocator's `asSlice/reinterpret`
chain when escape analysis differs. It is diagnostic evidence, not the
production allocation gate: the accepted comparison publishes the owned result
and compares with unchanged main. Keep the discard finding in change notes;
changing the allocator's allocation-era lifetime is a separate task.

For both methods and both contents, additionally capture matched JFR allocation
sites at stack depth 128 with escape analysis disabled
(`-Djdk.graal.PartialEscapeAnalysis=false` on Graal, `-XX:-DoEscapeAnalysis` on C2).
Enable ObjectAllocationSample, ObjectAllocationInNewTLAB and
ObjectAllocationOutsideTLAB. Compare steady-state sites against unchanged main;
there must be no candidate-only per-call object. Scope and thread ownership
must remain enforced when removing view allocations. The native range adapter
passes both base segments as FFM ADDRESS arguments and adjusts offsets in C.

The owned-read regression in `HOTLeafPayloadCodecTest` pins the 32-entry,
128-byte-value fixture to 4,256 slot bytes: raw input retains one 8 KiB allocator
slot and exposes its 4,256-byte payload view; encoded input retains one 8 KiB
decoded slot. Both expose 4,256 slot bytes and receive the same cache charge
including canonical retained heap, never a speculative 64 KiB writable frame. Both promote to 64 KiB on
first insertion. Empty payloads, malformed trailers, side references, allocation
stamps, close and all four versioning types are covered.

After the ownership fix and any change to the writer rule, regenerate the
five-fork owned-result comparisons, EA-off site captures, work budgets, store
census, cold I/O/decode probe and affected warm evidence in their pipeline phases.
Earlier numbers used a different reader fixture or codec policy and are historical,
not evidence for the revised implementation. Preserve the discard-only diagnostic
and its prior disposition. Keep evidence inside the authorized worktree/evidence
boundary; do not overwrite earlier artifacts or external PR notes from this review.

## Decode cost behind the writer rule

The writer keeps LZ4 only when it saves a third of the payload
([rule and derivation](../../../docs/DISK_FORMAT.md#hot-leaf-payload-codecs-kind-12-v0)). The
constant comes from two measured costs, and both must be measured again before it is changed.

Decode cost per codec: take raw kind-12 frames from a real store (the derivation used the 8,192
leaves of the t100k comparison store's cold probe), re-frame each one under a forced codec so that
the writer rule plays no part, check that every arm decodes to the raw slot bytes, and time
`AbstractReader.deserializeFromSegment` with an empty byte-handler pipeline on in-memory frames:
all leaves, and per payload size class. One arm per fresh JVM, ten profiler-free JVMs per arm,
arm order rotated, each in an exclusive timing window on one core. Arms: raw, LZ4 with the native
range decoder, LZ4 with the Java decoder (bundled library absent), zero-run. Report every fork.

Storage cost per byte: the paired cold probe of the raw and the encoded store (`cold I/O/decode
probe` above) gives the read time and the bytes read of each; the difference in read time over
the difference in bytes is what a byte not read is worth.

A codec pays for itself on a cold read when `saved bytes × storage cost per byte` is at least
`payload bytes × (decode cost per byte − raw copy cost per byte)`.

## The writer is opt-in

Leaf compression is off by default and switched on with `-Dsirix.page.hotLeaf.compress=true`, read
once per process. Every measurement in this file that compares raw with encoded leaves therefore
needs that property on the encoded arm. The reason for the default, with its numbers, is in
[`docs/DISK_FORMAT.md`](../../../docs/DISK_FORMAT.md) under "Why it is off by default": compression
pays where leaf bytes come from the device and costs a fresh process that reads them from the page
cache.
