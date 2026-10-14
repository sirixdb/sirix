package io.sirix.index.aggregate;

import io.sirix.index.hot.reader.HOTRangeCursor;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.page.PageReference;
import io.sirix.page.TemporalAggregateIndexPage;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Small raw HOT slots; all mutation and revision ownership use the standard secondary-index path.
 */
public final class TemporalAggregateStorage extends AbstractHOTIndexWriter<byte[]> implements AutoCloseable {
  private static final boolean WORK_DIAGNOSTICS = Boolean.getBoolean("sirix.hot.mergeDiag");
  private static final LongAdder GROUP_HEADS_VISITED = new LongAdder();
  private static final LongAdder GROUP_HEADS_DECODED = new LongAdder();
  private static final LongAdder PART_CHUNKS_READ = new LongAdder();
  private static final LongAdder SLOTS_WRITTEN = new LongAdder();
  private static final LongAdder HEADS_READ = new LongAdder();

  public static long slotsWritten() {
    return SLOTS_WRITTEN.sum();
  }

  public static long headsRead() {
    return HEADS_READ.sum();
  }

  static final byte[] HEADER = {0};
  private byte[] keyBuffer = new byte[32];
  private final Reader reader;
  private long nodesWritten;

  public static boolean workDiagnosticsEnabled() {
    return WORK_DIAGNOSTICS;
  }

  public static long groupHeadsVisited() {
    return GROUP_HEADS_VISITED.sum();
  }

  public static long groupHeadsDecoded() {
    return GROUP_HEADS_DECODED.sum();
  }

  public static long partChunksRead() {
    return PART_CHUNKS_READ.sum();
  }

  public TemporalAggregateStorage(final StorageEngineWriter writer, final int index) {
    super(writer, IndexType.TEMPORAL_AGGREGATE, index);
    initializeTemporalAggregateIndex();
    reader = new Reader(writer, () -> rootReference);
  }

  static void resetForBuild(final StorageEngineWriter writer, final int index) {
    final TemporalAggregateIndexPage page = writer.prepareSecondaryIndexPage(IndexType.TEMPORAL_AGGREGATE);
    if (!page.isIndexInitialized(index)) {
      return;
    }
    final List<DerivedRevisionMapping> mappings = index == 0
        ? DerivedRevisionMapping.readAll(writer)
        : List.of();
    page.resetTemporalAggregateIndexTree(writer, index, writer.getLog());
    for (final DerivedRevisionMapping mapping : mappings) {
      mapping.write(writer);
    }
  }

  public long nodesWritten() {
    return nodesWritten;
  }

  public Reader reader() {
    return reader;
  }

  public void put(final byte[] key, final byte[] value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    if (key.length == 0 || key.length > 2048 || value.length > 2048) {
      throw new IllegalArgumentException("Temporal aggregate slot exceeds inline bound");
    }
    doIndex(key, key.length, value, value.length);
    if (WORK_DIAGNOSTICS) {
      SLOTS_WRITTEN.increment();
    }
    if (key[0] == 3) {
      nodesWritten++;
    }
  }

  public void putPart(final long id, final byte[] bytes) {
    for (int offset = 0, chunk = 0; offset < bytes.length; offset += TemporalAggregateParts.CHUNK_BYTES, chunk++) {
      put(partKey(id, chunk),
          Arrays.copyOfRange(bytes, offset, Math.min(bytes.length, offset + TemporalAggregateParts.CHUNK_BYTES)));
    }
  }

  // Defensive accessors own the primitive arrays; this carrier is not a value-equality key.
  @SuppressWarnings("ArrayRecordComponent")
  public record GroupHead(long rows, int checkpointTier, long[] parts, byte[] tiers, long checkpointBytes,
      long deltaBytes) {
    public GroupHead(final long rows, final int checkpointTier, final long[] parts, final byte[] tiers) {
      this(rows, checkpointTier, parts, tiers, 1, 0);
    }

    public GroupHead {
      Objects.requireNonNull(parts);
      Objects.requireNonNull(tiers);
      if (checkpointBytes < 1 || deltaBytes < 0 || rows < 0 || checkpointTier < 1
          || checkpointTier > TemporalAggregateParts.MAX_TIERS || parts.length == 0
          || parts.length > TemporalAggregateParts.MAX_PARTS || tiers.length != parts.length
          || tiers[0] != checkpointTier) {
        throw new IllegalArgumentException("Invalid temporal aggregate group head");
      }
      parts = parts.clone();
      tiers = tiers.clone();
      final int[] counts = new int[checkpointTier];
      for (int index = 0; index < parts.length; index++) {
        if (parts[index] < 1 || (index > 0 && (tiers[index] < 0 || tiers[index] >= checkpointTier
            || ++counts[tiers[index]] >= TemporalAggregateParts.FAN_IN))) {
          throw new IllegalArgumentException("Invalid temporal aggregate part id/tier");
        }
      }
    }

    @Override
    public long[] parts() {
      return parts.clone();
    }

    @Override
    public byte[] tiers() {
      return tiers.clone();
    }

    public byte[] encode() {
      final ByteBuffer bytes = ByteBuffer.allocate(32 + parts.length * (Long.BYTES + 1));
      bytes.putLong(rows).putInt(checkpointTier).putInt(parts.length).putLong(checkpointBytes).putLong(deltaBytes);
      for (int index = 0; index < parts.length; index++) {
        bytes.putLong(parts[index]).put(tiers[index]);
      }
      return bytes.array();
    }

    public static GroupHead decode(final byte[] value) {
      if (value.length < 41 || value.length > 32 + TemporalAggregateParts.MAX_PARTS * (Long.BYTES + 1)) {
        throw new IllegalStateException("Invalid temporal aggregate group head length");
      }
      final ByteBuffer bytes = ByteBuffer.wrap(value);
      final long rows = bytes.getLong();
      final int checkpointTier = bytes.getInt();
      final int count = bytes.getInt();
      if (count < 1 || count > TemporalAggregateParts.MAX_PARTS || value.length != 32 + count * (Long.BYTES + 1)) {
        throw new IllegalStateException("Invalid temporal aggregate group part count");
      }
      final long checkpointBytes = bytes.getLong();
      final long deltaBytes = bytes.getLong();
      final long[] parts = new long[count];
      final byte[] tiers = new byte[count];
      for (int index = 0; index < count; index++) {
        parts[index] = bytes.getLong();
        tiers[index] = bytes.get();
      }
      return new GroupHead(rows, checkpointTier, parts, tiers, checkpointBytes, deltaBytes);
    }
  }

  @Override
  protected byte[] getKeyBuffer() {
    return keyBuffer;
  }

  @Override
  protected void setKeyBuffer(final byte[] buffer) {
    keyBuffer = buffer;
  }

  @Override
  protected int serializeKey(final byte[] key, final byte[] buffer, final int offset) {
    System.arraycopy(key, 0, buffer, offset, key.length);
    return key.length;
  }

  @Override
  public void close() {
    reader.close();
  }

  @FunctionalInterface
  public interface GroupConsumer {
    void accept(byte[] key, GroupHead group);
  }

  public static final class Reader implements AutoCloseable {
    private final StorageEngineReader storage;
    private final HOTTrieReader trie;
    private long nodesRead;
    private final Supplier<@Nullable PageReference> root;

    public Reader(final StorageEngineReader storage, final int index) {
      this(storage,
          () -> storage.getTemporalAggregateIndexPage(storage.getActualRevisionRootPage()).getIndexReference(index));
    }

    private Reader(final StorageEngineReader storage, final Supplier<@Nullable PageReference> root) {
      this.storage = storage;
      trie = new HOTTrieReader(storage);
      this.root = root;
    }

    public long nodesRead() {
      return nodesRead;
    }

    public byte @Nullable [] get(final byte[] key) {
      if (WORK_DIAGNOSTICS && key[0] == 1) {
        HEADS_READ.increment();
      }
      if (key[0] == 3) {
        nodesRead++;
        if (WORK_DIAGNOSTICS) {
          PART_CHUNKS_READ.increment();
        }
      }
      final PageReference reference = root.get();
      if (reference == null) {
        return null;
      }
      for (int attempt = 0; attempt < 16; attempt++) {
        final MemorySegment value = trie.get(reference, key);
        if (value == null) {
          return null;
        }
        try {
          final byte[] copy = value.toArray(ValueLayout.JAVA_BYTE);
          if (trie.validateCurrentLeaf()) {
            return copy.length == 0
                ? null
                : copy;
          }
        } catch (final RuntimeException failure) {
          if (trie.validateCurrentLeaf()) {
            throw failure;
          }
        }
      }
      throw new IllegalStateException("Temporal aggregate leaf did not stabilize");
    }

    public boolean groups(final long maximumHeads, final GroupConsumer consumer) {
      if (maximumHeads < 0) {
        throw new IllegalArgumentException("Negative temporal aggregate dictionary bound");
      }
      Objects.requireNonNull(consumer);
      final PageReference reference = root.get();
      if (reference == null) {
        return true;
      }
      try (HOTTrieReader groupReader = new HOTTrieReader(storage);
          HOTRangeCursor cursor = groupReader.range(reference, new byte[] {1}, new byte[] {2})) {
        long heads = 0;
        while (cursor.hasNext()) {
          if (WORK_DIAGNOSTICS) {
            GROUP_HEADS_VISITED.increment();
          }
          if (heads == maximumHeads) {
            return false;
          }
          heads++;
          cursor.advance();
        }
      }
      try (HOTTrieReader groupReader = new HOTTrieReader(storage);
          HOTRangeCursor cursor = groupReader.range(reference, new byte[] {1}, new byte[] {2})) {
        while (cursor.hasNext()) {
          final HOTRangeCursor.Entry entry = cursor.next();
          if (WORK_DIAGNOSTICS) {
            GROUP_HEADS_DECODED.increment();
          }
          consumer.accept(entry.keyBytes(), GroupHead.decode(entry.valueBytes()));
        }
      }
      return true;
    }

    public TemporalAggregateParts.Store parts() {
      return (part, chunk) -> Objects.requireNonNull(get(partKey(part, chunk)),
          "Missing temporal aggregate part chunk");
    }

    @Override
    public void close() {
      trie.close();
    }
  }

  private static byte[] partKey(final long part, final int chunk) {
    return ByteBuffer.allocate(13).put((byte) 3).putLong(part).putInt(chunk).array();
  }
}
