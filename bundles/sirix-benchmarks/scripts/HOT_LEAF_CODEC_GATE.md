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
