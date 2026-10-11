# Column-backed record answers

`ColumnarRecordSequence` retains owned long, presence, and string columns plus an ordered row
selection. The joined row route returns it after sorting row indexes with Brackit's `Ordering`,
including the existing document-key tie breakers. The returned column data retains no projection
lease or database transaction.

Use the Sirix serializer to write these results in batches:

```java
try (final SirixStringSerializer serializer = new SirixStringSerializer(writer)) {
  new Query(chain, text).serialize(context, serializer);
}
```

`SirixStringSerializer` accepts a `PrintWriter` or `PrintStream` and inherits Brackit's format and
indent settings. Sirix's shell, benchmark runners, and `xml:serialize` use this adapter.
`JsonDBSerializer` uses the same writer inside its REST result envelope. The standalone Brackit
serializer remains a compatible ordinary-item consumer; callers using its default query overload
can select the adapter explicitly to obtain batching.

The writer uses one reusable 32 KiB character buffer. It writes signed integral values directly,
including `Long.MIN_VALUE`, and preserves Brackit's field order and JSON escaping. It also batches
computed `ArrayObject` records whose field values have exactly the classes `Int32`, `Int64`, `Str`,
`Bool`, or `Null`, or are absent (`null`). This covers grouped row answers such as SH1 Q8 without
changing their query routes.
`SirixStringSerializer` delegates other values and pretty printing to Brackit's serializer.
REST computed records remain compact, including when `JsonDBSerializer`'s pretty-print flag is
enabled; that flag applies to database-backed items. REST fallback records use a character writer
so the fallback does not add an encoding round trip.

When an unsupported item follows batched records, the adapter drains the buffer and delegates
the remaining suffix to one Brackit call. The original iterator is consumed and closed once,
and fallback printer allocations and flushes stay constant regardless of the suffix length.

Any route producing columns can use the public column factories or `ColumnarRecordSequence.Builder`.
Factory and constructor arrays are copied. A row selection may reorder, repeat, or select a subset
of the column rows.

Iteration and positional access return normal JDM object items. Reading field values materializes
them, and mutation retains normal object behavior across repeated iteration. Serialization can
read untouched rows directly from the columns. Mutated primitive records can still use the batch
writer. Neither execution-scoped sequence wrappers nor ordinary consumers require an unwrapping
API.

Parity tests are in `ColumnarRecordSerializationTest`; the
[work-budget inventory](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md#what-is-here)
describes `ColumnarSerializationWorkBudgetTest`. The serializer-only JMH fixture
is `ColumnarRecordSerializationBenchmark` in `sirix-benchmarks`, with 5,412 and 8,000 rows and
numeric or mixed string columns. Benchmark timing is separate from the deterministic budgets.
