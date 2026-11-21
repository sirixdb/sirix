# HOT leaf read and allocation gate

Use the same paired raw and packed fixtures for
`HOTLeafCodecBenchmark` and `HOTLeafCodecAllocationBenchmark`. Each arm owns
the same key/value results and uses the same reader and configuration.
The raw control is constructed without candidate-only production helpers,
so it can run against the baseline with the same benchmark classes. The
packed frame is emitted by the current kind-12 writer.

Build `:sirix-benchmarks:jmhJar`. Run paired `encoding=RAW,ENCODED` forks
for ZERO and REPEATED slot contents. Run `HOTLeafCodecBenchmark` with `keyBytes=1,8`
to cover the contiguous projection word layout. Measure cold full and compact lookup,
uncached selection, warm cached leaf key and value lookup, warm projection-image lookup, and
allocation with JMH's GC profiler. Include an eight-byte key on the cache
routing path, in addition to variable-key cases. No fork with foreign
benchmark or busy JVM work enters the scored population.

The broader paired native gate compares warm lookups, fresh processes
with a resident OS cache and age-controlled device-cold reads against
current main. The paired lower 95% bound must be at most zero, the paired
point estimate at most +1%, and median allocation delta at most zero.
After reads pass, compare the same datasets' total allocated and per-kind
logical bytes. Record the raw measurements, the image heads, and the
compiled shape for key comparison, leaf value lookup and posting ordinal reads.

## Single-byte lookup fast-path gate

`hot-leaf-fastpath-gate.sh` guards `HOTLeafBody.findValueRef`'s single-byte fast path, which only
latency can observe: with it disabled, every result, error and probe count is unchanged. The script
builds the working tree (candidate) and the committed `HEAD` (reference), runs
`HOTLeafCodecBenchmark.uncachedImageLookup` (keyBytes=1, ZERO and REPEATED, packed leaves) in five
alternating fork pairs with the fork settings above, and exits 1 when the candidate's paired mean is
more than **10 %** slower than the reference for either content. It measures wall-clock time, so it
never runs in CI or in a test suite (CONV-301); run it only on a quiet machine with nothing else
running. It is the detecting command of the fast path's `killed-out-of-lane` disposition.

Margin derivation (written before first use). A timed counterfactual on 2026-11-21 (five alternating
pairs, these settings, every pair admitted by the foreign-load protocol) compared the code with the
fast path disabled against the code with it:

| content | per-pair slowdown without the fast path | mean | 95 % interval |
| --- | --- | ---: | --- |
| ZERO | +30.9, +22.8, +27.5, +22.0, +17.5 % | +24.0 % | [+20.3, +28.1] % |
| REPEATED | +22.5, +24.2, +25.4, +24.7, +22.7 % | +23.9 % | [+22.9, +24.9] % |

The per-pair spread is at most 0.042 in log ratio (ZERO), so a five-pair mean has a standard error of
about 0.019. A 10 % margin (0.095 in log ratio) is about five standard errors above zero, so two
builds of the same code essentially never fail it, and about six standard errors below the smallest
mean effect (+23.9 %, 0.214), so a build without the fast path essentially always fails it.
