# Bug: cold leaf codec setup changed measured warm lookup latency

Historical evidence for the retired kind-local leaf compression experiment. Its codec classes,
test names and settings below do not describe the current
[kind-12 format](../../spec/FMT-disk-format.md#fmt-210--hot-leaf-envelope-and-page-local-modes).

- Fix / issue: S1 HOT leaf payload codec, found before delivery by the latency gate.
- Symptom: unchanged warm leaf lookup grew from 54–56 ns to 66–70 ns in the
  bounded raw/encoded JMH A/B; allocation stayed effectively zero.
- Trigger: serialize and decode an encoded leaf before repeatedly looking up its
  entries. Initial codec code parsed heap serialization scratch through
  MemorySegmentBytesIn and expanded cached images through heap-backed segments.
- Suspected mechanism: foreign-memory scalar accessors share receiver-type profiles
  across the process. The initial codec introduced heap-backed receivers where
  warm leaf callers supplied native segments. Removing those receivers eliminated
  the observed warm-leaf loss. The original timing runs were not CPU-affinity
  controlled, so this does not establish receiver pollution as the sole cause.
- Why tests missed it: value, ownership and allocation assertions all passed.
  They do not measure native/heap specialization or generated lookup code.
- Regression test: `HOTLeafCodecBenchmark` runs raw and encoded full leaf
  materialization, warm leaf lookup and warm projection-image lookup in fresh
  JVM forks with identical logical data. Preserve bounded A/B results and
  compiled-method evidence; do not introduce a wall-clock unit-test threshold.
  `HOTLeafPayloadCodecTest` also pins byte-identical expanded cache images.
- Fix: parse heap scratch using byte-array accessors and decode to native
  scratch before copying into retained heap images. Native scalar accessors
  never see a heap segment from the new codec paths.
- Follow-up: keep expansion in the cold image factories and preserve the original
  constructor and warm methods. A fixed-P-core, three-fork comparison measured
  warm projection lookup at 48.07 ns raw versus 47.83 ns encoded, both 144 B/op.
  JVMCI inlines the selector in both arms without codec calls. Earlier unpinned
  projection results varied across forks; do not attribute their improvement to
  the structural change alone.
- Lesson: [Cold paths can change warm compilation](../LESSONS.md#cold-paths-can-change-warm-compilation).
