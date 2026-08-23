package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The async snapshot flush serializes a {@link PathNode} on the background snapshot-append thread
 * while the ingest thread keeps merging leaf page keys into the very same node: the serialization
 * copy taken by {@code KeyValueLeafPage.deepCopy()} copies the {@code records[]} array but not the
 * records, so both pages hold one {@link PathStats} and one {@code RoaringBitmap}.
 *
 * <p>This test reproduces that handoff directly against the real {@link NodeKind#PATH} serializer —
 * the same entry point {@code KeyValueLeafPage.processEntries()} uses — and asserts the only thing
 * a flush is allowed to produce: a self-consistent record. It fails loudly when the bitmap is
 * serialized while being mutated, in any of the three ways that can happen:
 *
 * <ul>
 *   <li>{@code runOptimize()} overruns its fill and throws
 *       {@code ArrayIndexOutOfBoundsException: Index N out of bounds for length N};</li>
 *   <li>{@code runOptimize()} under-fills instead, yielding a run container whose unwritten tail
 *       decodes as page key {@code 0} — a key this fixture never records, so its presence in the
 *       decoded bitmap is proof of silent corruption;</li>
 *   <li>{@code serializedSizeInBytes()} and {@code serialize()} disagree, so the length prefix does
 *       not match the payload and the record no longer decodes.</li>
 * </ul>
 */
final class PathStatsConcurrentFlushTest {

  /** Contiguous seed: one long run, so {@code runOptimize} converts the container. */
  private static final int SEED_LO = 1_000;
  private static final int SEED_HI = 3_000;

  /**
   * Isolated keys added during the flush, each its own new run. Kept in the same 16-bit chunk as
   * the seed and well under the point where the run encoding stops being the smaller one, so every
   * add grows the run count of a container {@code runOptimize} still rewrites.
   */
  private static final int SPARSE_BASE = 5_000;
  private static final int SPARSE_STRIDE = 4;
  private static final int SPARSE_COUNT = 1_200;

  private static final int ROUNDS = 60;

  private static PathNode freshNode() {
    return new PathNode(new QNm("age"), NodeKind.OBJECT_NAMED_OBJECT, 1, 1,
        1L, -1L, -1, 0, (SirixDeweyID) null,
        -1L, -1L, -1L, -1L, 0L, 0L,
        -1, -1, 42, 0L);
  }

  private static ResourceConfiguration config() {
    return new ResourceConfiguration.Builder("test-path-stats-concurrent-flush")
        .buildPathSummary(true)
        .buildPathStatistics(true)
        .build();
  }

  private static boolean isLegalPageKey(final int key) {
    if (key >= SEED_LO && key < SEED_HI) {
      return true;
    }
    final int offset = key - SPARSE_BASE;
    return offset >= 0 && offset % SPARSE_STRIDE == 0 && offset / SPARSE_STRIDE < SPARSE_COUNT;
  }

  @Test
  void pageKeysStaySerializableWhileIngestKeepsMergingIntoThem() throws InterruptedException {
    final ResourceConfiguration config = config();
    final AtomicReference<String> anomaly = new AtomicReference<>();
    long flushes = 0L;
    int lastCardinality = 0;

    for (int round = 0; round < ROUNDS && anomaly.get() == null; round++) {
      final PathNode node = freshNode();
      final IntOpenHashSet seed = new IntOpenHashSet(SEED_HI - SEED_LO);
      for (int key = SEED_LO; key < SEED_HI; key++) {
        seed.add(key);
      }
      node.mergePageKeys(seed);

      final AtomicInteger merged = new AtomicInteger();
      final Thread ingest = new Thread(() -> {
        final IntOpenHashSet batch = new IntOpenHashSet(1);
        for (int i = 0; i < SPARSE_COUNT; i++) {
          batch.clear();
          batch.add(SPARSE_BASE + i * SPARSE_STRIDE);
          node.mergePageKeys(batch);
          merged.incrementAndGet();
          // Stretch the merge sequence across many flushes instead of racing through it in one
          // burst; without this the window closes before the flush thread can overlap it.
          Thread.onSpinWait();
        }
      }, "ingest-merge-" + round);

      final AtomicInteger flushCount = new AtomicInteger();
      final Thread flush = new Thread(() -> {
        while (merged.get() < SPARSE_COUNT && anomaly.get() == null) {
          try {
            final BytesOut<?> sink = Bytes.elasticHeapByteBuffer();
            NodeKind.PATH.serialize(sink, node, config);
            final PathNode restored =
                (PathNode) NodeKind.PATH.deserialize(sink.asBytesIn(), node.getNodeKey(), null, config);
            final int[] decoded = restored.getPageKeysArray();
            if (decoded == null) {
              anomaly.compareAndSet(null, "flushed record lost the page-key bitmap entirely");
              return;
            }
            int seedSeen = 0;
            for (final int key : decoded) {
              if (!isLegalPageKey(key)) {
                anomaly.compareAndSet(null,
                    "flushed record decoded page key " + key + ", which was never recorded");
                return;
              }
              if (key >= SEED_LO && key < SEED_HI) {
                seedSeen++;
              }
            }
            if (seedSeen != SEED_HI - SEED_LO) {
              anomaly.compareAndSet(null,
                  "flushed record decoded " + seedSeen + " of " + (SEED_HI - SEED_LO)
                      + " seed page keys — a flush may not drop keys committed before it started");
              return;
            }
            flushCount.incrementAndGet();
          } catch (final RuntimeException | Error e) {
            anomaly.compareAndSet(null, "flush threw " + e);
            return;
          }
        }
      }, "snapshot-flush-" + round);

      ingest.start();
      flush.start();
      ingest.join();
      flush.join();

      flushes += flushCount.get();
      final int[] finalKeys = node.getPageKeysArray();
      assertNotNull(finalKeys);
      lastCardinality = finalKeys.length;
    }

    assertNull(anomaly.get(), anomaly.get());
    // Guard against a vacuous pass: the two threads must actually have overlapped.
    assertTrue(flushes > ROUNDS,
        "expected the flush thread to serialize repeatedly while ingest merged, but it completed "
            + flushes + " flushes over " + ROUNDS + " rounds");
    assertTrue(lastCardinality == (SEED_HI - SEED_LO) + SPARSE_COUNT,
        "every merged page key must survive the concurrent flushes, expected "
            + ((SEED_HI - SEED_LO) + SPARSE_COUNT) + " but the bitmap held " + lastCardinality);
  }
}
