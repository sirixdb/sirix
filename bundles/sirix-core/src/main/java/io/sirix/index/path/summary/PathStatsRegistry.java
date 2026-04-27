package io.sirix.index.path.summary;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.jspecify.annotations.Nullable;

/**
 * Per-revision off-page registry mapping {@code pathNodeKey} to its
 * {@link PathStats}. Held on the owning {@link io.sirix.page.PathSummaryPage}.
 *
 * <p>Lazily allocated — a resource configured without
 * {@code withPathStatistics} never materialises an entry, paying zero bytes on
 * disk and zero per-page memory.
 *
 * <p>Wire format (when present):
 * <pre>
 *   int n                    // number of entries
 *   for n entries:
 *     long pathNodeKey
 *     int  payloadLen
 *     byte[payloadLen] payload  // {@link PathStats#toBytes}
 * </pre>
 */
public final class PathStatsRegistry {

  private final Long2ObjectOpenHashMap<PathStats> map;

  public PathStatsRegistry() {
    this.map = new Long2ObjectOpenHashMap<>();
  }

  private PathStatsRegistry(final Long2ObjectOpenHashMap<PathStats> map) {
    this.map = map;
  }

  /** Returns the stats for {@code pathNodeKey} or {@code null} if absent. */
  public @Nullable PathStats get(final long pathNodeKey) {
    return map.get(pathNodeKey);
  }

  /**
   * Returns the existing stats for {@code pathNodeKey}, creating an empty record
   * lazily if absent. Used by the writer to lazily upgrade a path that just saw
   * its first value.
   */
  public PathStats getOrCreate(final long pathNodeKey) {
    PathStats s = map.get(pathNodeKey);
    if (s == null) {
      s = new PathStats();
      map.put(pathNodeKey, s);
    }
    return s;
  }

  public void put(final long pathNodeKey, final PathStats stats) {
    map.put(pathNodeKey, stats);
  }

  public void remove(final long pathNodeKey) {
    map.remove(pathNodeKey);
  }

  public int size() {
    return map.size();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public LongIterator keyIterator() {
    return map.keySet().iterator();
  }

  public void serialize(final BytesOut<?> sink) {
    sink.writeInt(map.size());
    final LongIterator it = map.keySet().iterator();
    while (it.hasNext()) {
      final long pnk = it.nextLong();
      final PathStats stats = map.get(pnk);
      final byte[] payload = stats.toBytes();
      sink.writeLong(pnk);
      sink.writeInt(payload.length);
      sink.write(payload);
    }
  }

  public static PathStatsRegistry deserialize(final BytesIn<?> source) {
    final int n = source.readInt();
    final Long2ObjectOpenHashMap<PathStats> map = new Long2ObjectOpenHashMap<>(Math.max(16, n * 2));
    for (int i = 0; i < n; i++) {
      final long pnk = source.readLong();
      final int payloadLen = source.readInt();
      final byte[] payload = new byte[payloadLen];
      source.read(payload, 0, payloadLen);
      map.put(pnk, PathStats.fromBytes(payload));
    }
    return new PathStatsRegistry(map);
  }
}
