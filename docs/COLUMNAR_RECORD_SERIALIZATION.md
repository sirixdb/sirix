# Column-backed record answers

`ColumnarRecordSequence` retains owned long, presence, and string columns plus an ordered row
selection. The joined row route returns it after sorting row indexes with Brackit's `Ordering`,
including the existing document-key tie breakers. Projection leases and database transactions
are released before the result is returned.

Use the Sirix serializer to write these results in batches:

```java
try (final SirixStringSerializer serializer = new SirixStringSerializer(writer)) {
  new Query(chain, text).serialize(context, serializer);
}
```

`SirixStringSerializer` accepts a `PrintWriter` or `PrintStream` and inherits Brackit's format and
indent settings. Sirix's shell, benchmark runners, and serialization functions use this adapter.
`JsonDBSerializer` uses the same writer inside its REST result envelope. The standalone Brackit
serializer remains a compatible ordinary-item consumer; callers using its default query overload
can select the adapter explicitly to obtain batching.

The writer uses one reusable 32 KiB character buffer. It writes signed integral values directly,
including `Long.MIN_VALUE`, and preserves Brackit's field order and JSON escaping. It also batches
computed `ArrayObject` records with integral, string, boolean, or null fields. This covers grouped
row answers such as SH1 Q8 without changing their query routes. Other values and pretty printing
use Brackit's serializer. REST fallback records use a character writer so the fallback does not
add an encoding round trip.

Any route producing columns can use the public column factories or `ColumnarRecordSequence.Builder`.
Factory and constructor arrays are copied. A row selection may reorder, repeat, or select a subset
of the column rows. Generic `Sequence` columns preserve other JDM types through the ordinary path. Their values are
retained by reference, and the caller owns the lifetime of those values.

Iteration and positional access return normal JDM object items. Field access materializes their
values, and mutation retains normal object behavior across repeated iteration. Serialization can
read untouched rows directly from the columns. Mutated primitive records can still use the batch
writer. Neither execution-scoped sequence wrappers nor ordinary consumers require an unwrapping
API.

Parity tests are in `ColumnarRecordSerializationTest`; the query work-budget suite includes
`ColumnarSerializationWorkBudgetTest`, which bounds writer calls for both columnar and grouped
answers and checks the unbatched serializer as a positive control. The serializer-only JMH fixture
is `ColumnarRecordSerializationBenchmark` in `sirix-benchmarks`, with 5,412 and 8,000 rows and
numeric or mixed string columns. Benchmark timing is separate from the deterministic budgets.
