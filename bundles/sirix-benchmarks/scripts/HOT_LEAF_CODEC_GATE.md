# HOT leaf codec allocation gate

`io.sirix.page.HOTLeafCodecAllocationBenchmark` keeps full and compact results
in an escaping sink: production readers publish decoded pages. The RAW fork
sets the diagnostic writer property before loading PageKind, and uses only
pre-existing APIs on its measured paths. The same benchmark class therefore
runs over unchanged-main production classes without a fixture rewrite.

Run every JVM/Gradle command through the project's authorized heavy-job wrapper.
Build `:sirix-benchmarks:jmhJar`. To compare with the pre-codec base, compile the
unchanged base versions of PageKind, HOTProjectionImage, ZeroRunByteCodec and
FFILz4Compressor into an external class directory using the JMH JAR classpath.
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
