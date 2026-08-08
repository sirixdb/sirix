/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Bisects what a single cursor hop costs, with no query engine on top.
 *
 * <p>Exists because "trie navigation should be faster than a heap scan" is a claim that needs a
 * number, and because a whole-query profile cannot separate the bind from the pointer chase from the
 * locality. The three benchmarks below visit <em>exactly the same number of nodes</em> and differ
 * only in where the next node key comes from, so the deltas between them are attributable:
 *
 * <ul>
 *   <li>{@link #denseMoveTo} — {@code moveTo} over {@code elementCount} <em>consecutive</em> node
 *       keys. Same bind work per call, perfect locality, next key already in a register. This is the
 *       bind floor.
 *   <li>{@link #stridedMoveTo} — {@code moveTo} over the array's element keys, collected into a
 *       {@code long[]} up front. Same bind work, but now at the real element stride (each element's
 *       field nodes sit between it and the next), while the next key still comes from a sequential
 *       array the hardware prefetcher runs ahead of. Minus {@link #denseMoveTo}, this is the pure
 *       locality penalty.
 *   <li>{@link #siblingWalk} — the ordinary walk: {@code moveToRightSibling()} from each element.
 *       Identical keys in identical order to {@link #stridedMoveTo}; the only difference is that the
 *       next key is a field of the record just bound, so every hop depends on a scattered load that
 *       cannot be overlapped. Minus {@link #stridedMoveTo}, this is the cost of pointer chasing.
 * </ul>
 *
 * <p>Measured on the 2.11 GB / 3,482,208-record corpus of {@code docs/COMPARISON_POSTGRES_BULK.md},
 * warm. All four figures below are from ONE run — differencing rows across runs produced a published
 * decomposition that did not sum, so don't:
 *
 * <pre>
 *   denseMoveTo              97.5 ms/op    28.0 ns/element   the bind floor
 *   stridedMoveTo           204.0 ms/op    58.6 ns/element   + locality at the ~15-key stride
 *   stridedReadingSibling   458.2 ms/op   131.6 ns/element   + reading the sibling key   (+73.0 ns)
 *   siblingWalk             758.8 ms/op   217.9 ns/element   + depending on it           (+86.3 ns)
 * </pre>
 *
 * <p>(A 2-fork x 15-iteration run gives 126.6 / 184.1 / 466.2 / 716.0 ms: same ordering, +-10-30 %.)
 *
 * <p>The read and the dependency are roughly equal, ~73-86 ns each, and the data cannot reliably
 * order them — about 11.5 ns/element of the last term is kernel page-zeroing present only in that
 * {@link #siblingWalk} run. A CPU profile puts {@code ObjectNode.readDeltaField} at 48.2 % of
 * {@link #siblingWalk} self time, more than every page-resolution, slot-lookup and bind frame
 * combined. That is NOT varint decode: {@code readDeltaField}'s own body contains one memory access,
 * the field-offset byte, and 99.986 % of the sibling deltas on this corpus encode in a single byte.
 * The bind itself never touches the record heap at all, because the node kind comes out of the slot
 * directory — which is why {@link #stridedMoveTo} is a third of the cost.
 *
 * <p>716-759 ms for the bare walk against 798.1 ms for {@link BulkQueryScanBenchmark#countAll} on the
 * same store means the walk is ~90 % of the warm scan.
 *
 * <p>This is a warm steady-state question, so it is a JMH benchmark rather than a timing loop — see
 * {@code docs/BENCHMARK_DESIGN.md} R4. {@link #setUp} collects the element keys once, which also
 * faults the whole resource in; without that the first measured iteration would carry the cold page
 * load. Divide the reported ms/op by the {@code elements=} count printed by {@link #setUp} to get
 * ns/element; all three benchmarks share that divisor by construction.
 *
 * <p>Point it at a store built by {@code PostgresBulkBench ingest} in {@code single} mode:
 *
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh \
 *       -Pjmh.includes=CursorWalkBisectBenchmark \
 *       -Pjmh.jvmArgs="-Dsirix.bench.store=/path/to/store -Dsirix.bench.db=db-name"
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xmx8g", "--add-modules", "jdk.incubator.vector",
    "--enable-native-access=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED"})
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 3)
public class CursorWalkBisectBenchmark {

  /** Initial capacity of the element-key buffer; it doubles from here. */
  private static final int INITIAL_KEY_CAPACITY = 1 << 20;

  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private JsonNodeReadOnlyTrx rtx;

  /** Node keys of the top-level array's elements, in document order. */
  private long[] elementKeys;

  /** Number of valid entries in {@link #elementKeys}; the shared divisor for all three walks. */
  private int elementCount;

  /** First element key — also the start of {@link #denseMoveTo}'s consecutive range. */
  private long firstKey;

  @Setup(Level.Trial)
  public void setUp() {
    final Path location = Paths.get(System.getProperty("sirix.bench.store",
                                                       System.getProperty("java.io.tmpdir")));
    final String dbName = System.getProperty("sirix.bench.db", "db-bulk");
    final String resource = System.getProperty("sirix.bench.resource", "movies");

    database = Databases.openJsonDatabase(location.resolve(dbName));
    session = database.beginResourceSession(resource);
    rtx = session.beginNodeReadOnlyTrx();

    // Collect the element keys once. This also faults every page of the resource in, so the
    // measured iterations are warm; a cold first iteration would be measuring the storage layer.
    long[] keys = new long[INITIAL_KEY_CAPACITY];
    int count = 0;
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      do {
        if (count == keys.length) {
          keys = Arrays.copyOf(keys, keys.length << 1);
        }
        keys[count++] = rtx.getNodeKey();
      } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
    }
    if (count < 2) {
      throw new IllegalStateException("need at least two array elements to walk, found " + count);
    }

    elementKeys = Arrays.copyOf(keys, count);
    elementCount = count;
    firstKey = elementKeys[0];

    System.out.printf("%n# elements=%,d  firstKey=%,d  lastKey=%,d  meanStride=%.1f"
                          + "  (divide ms/op by elements for ns/element)%n",
                      count, firstKey, elementKeys[count - 1],
                      (elementKeys[count - 1] - firstKey) / (double) (count - 1));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    rtx.close();
    session.close();
    database.close();
  }

  /**
   * The bind floor: {@code elementCount} binds with perfect locality.
   *
   * @return the number of keys that resolved to a node, so JMH cannot eliminate the loop
   */
  @Benchmark
  public long denseMoveTo() {
    final JsonNodeReadOnlyTrx cursor = rtx;
    final long start = firstKey;
    final int n = elementCount;
    long visited = 0;
    for (int i = 0; i < n; i++) {
      if (cursor.moveTo(start + i)) {
        visited++;
      }
    }
    return visited;
  }

  /**
   * Bind floor plus the locality penalty: same binds, real element stride, prefetchable key source.
   *
   * @return the number of keys that resolved to a node, so JMH cannot eliminate the loop
   */
  @Benchmark
  public long stridedMoveTo() {
    final JsonNodeReadOnlyTrx cursor = rtx;
    final long[] keys = elementKeys;
    final int n = elementCount;
    long visited = 0;
    for (int i = 0; i < n; i++) {
      if (cursor.moveTo(keys[i])) {
        visited++;
      }
    }
    return visited;
  }

  /**
   * The discriminator between "reading the sibling pointer is expensive" and "<em>depending</em> on
   * it is expensive". Identical to {@link #stridedMoveTo} plus a read of each bound record's
   * right-sibling key — the same scattered heap load {@link #siblingWalk} performs — except that the
   * next address does not come from it, so the load can retire lazily instead of gating the next
   * bind.
   *
   * <p>If this lands near {@link #stridedMoveTo}, the cost in {@link #siblingWalk} is the dependency
   * chain. If it lands near {@link #siblingWalk}, the cost is the load itself and prefetching the
   * chain would not help.
   *
   * @return the XOR of every sibling key read, so neither the loop nor the reads can be eliminated
   */
  @Benchmark
  public long stridedReadingSibling() {
    final JsonNodeReadOnlyTrx cursor = rtx;
    final long[] keys = elementKeys;
    final int n = elementCount;
    long mixed = 0;
    for (int i = 0; i < n; i++) {
      if (cursor.moveTo(keys[i])) {
        mixed ^= cursor.getRightSiblingKey();
      }
    }
    return mixed;
  }

  /**
   * The whole cost: the same nodes in the same order, but each next key read from the record just
   * bound, so the loads serialize into a dependency chain.
   *
   * @return the number of elements visited, so JMH cannot eliminate the loop
   */
  @Benchmark
  public long siblingWalk() {
    final JsonNodeReadOnlyTrx cursor = rtx;
    long visited = 0;
    cursor.moveToDocumentRoot();
    cursor.moveToFirstChild();
    if (cursor.hasFirstChild()) {
      cursor.moveToFirstChild();
      do {
        visited++;
      } while (cursor.hasRightSibling() && cursor.moveToRightSibling());
    }
    return visited;
  }
}
