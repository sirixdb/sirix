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
 * warm, 2 forks x (5 warm-up + 10 measured) iterations:
 *
 * <pre>
 *   denseMoveTo      98.0 +- 10.0 ms/op    28.1 ns/element   the bind floor
 *   stridedMoveTo   200.1 +- 17.2 ms/op    57.5 ns/element   + locality        (x2.0)
 *   siblingWalk     726.8 +- 25.1 ms/op   208.7 ns/element   + pointer chase   (x3.6)
 * </pre>
 *
 * <p>72 % of the per-element cost is the dependency chain, not the bind — and 726.8 ms for the bare
 * walk against 798.1 ms for {@link BulkQueryScanBenchmark#countAll} on the same store means the walk
 * is 91 % of the warm scan.
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
