package io.sirix.index.aggregate;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.index.IndexType;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Prefix 4 is independent of the group and part key spaces, in physical aggregate tree zero. */
record DerivedRevisionMapping(int sourceRevision, int indexRevision, int projection, long timestamp, long identityHigh,
    long identityLow) {
  static final int CACHE_CAPACITY = 64;

  private static byte[] key(final int revision) {
    return ByteBuffer.allocate(5).put((byte) 4).putInt(revision).array();
  }

  static DerivedRevisionMapping of(final StorageEngineReader source, final int indexRevision, final int projection) {
    final byte[] identity = projection < 0
        ? new byte[16]
        : ChangeLogStorage.identity(source, projection, source.getRevisionNumber());
    if (identity == null) {
      throw new IllegalStateException("Source revision has no durable change-log identity");
    }
    final ByteBuffer bytes = ByteBuffer.wrap(identity);
    return new DerivedRevisionMapping(source.getRevisionNumber(), indexRevision, projection,
        source.getActualRevisionRootPage().getRevisionTimestamp(), bytes.getLong(), bytes.getLong());
  }

  boolean matches(final StorageEngineReader source) {
    if (source.getRevisionNumber() != sourceRevision
        || source.getActualRevisionRootPage().getRevisionTimestamp() != timestamp) {
      return false;
    }
    if (projection < 0) {
      return source.getResourceSession()
                   .getRtxIndexController(sourceRevision)
                   .getIndexes()
                   .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE) == 0;
    }
    final byte[] identity = ChangeLogStorage.identity(source, projection, sourceRevision);
    return identity != null && ByteBuffer.wrap(identity).getLong() == identityHigh
        && ByteBuffer.wrap(identity).getLong(8) == identityLow;
  }

  void write(final StorageEngineWriter writer) {
    try (TemporalAggregateStorage storage = new TemporalAggregateStorage(writer, 0)) {
      storage.put(key(sourceRevision),
          ByteBuffer.allocate(40)
                    .putInt(sourceRevision)
                    .putInt(indexRevision)
                    .putInt(projection)
                    .putInt(1)
                    .putLong(timestamp)
                    .putLong(identityHigh)
                    .putLong(identityLow)
                    .array());
      storage.put(key(-1), ByteBuffer.allocate(4).putInt(sourceRevision).array());
    }
  }

  static int frontier(final StorageEngineReader reader) {
    try (TemporalAggregateStorage.Reader storage = new TemporalAggregateStorage.Reader(reader, 0)) {
      final byte[] value = storage.get(key(-1));
      if (value == null) {
        return 0;
      }
      if (value.length != Integer.BYTES || ByteBuffer.wrap(value).getInt() < 0) {
        throw new IllegalStateException("Invalid derived maintenance frontier");
      }
      return ByteBuffer.wrap(value).getInt();
    }
  }

  static @Nullable DerivedRevisionMapping read(final StorageEngineReader reader, final int revision) {
    try (TemporalAggregateStorage.Reader storage = new TemporalAggregateStorage.Reader(reader, 0)) {
      final byte[] value = storage.get(key(revision));
      return value == null
          ? null
          : decode(value, revision, reader.getRevisionNumber());
    }
  }

  private static DerivedRevisionMapping decode(final byte[] value, final int sourceRevision,
      final int latestIndexRevision) {
    if (value.length != 40) {
      throw new IllegalStateException("Invalid derived revision mapping");
    }
    final ByteBuffer bytes = ByteBuffer.wrap(value);
    final int source = bytes.getInt();
    final int index = bytes.getInt();
    final int projection = bytes.getInt();
    if (bytes.getInt() != 1 || source < 1 || index < 1 || projection < -1 || source != sourceRevision
        || index > latestIndexRevision) {
      throw new IllegalStateException("Invalid derived revision mapping identity");
    }
    return new DerivedRevisionMapping(source, index, projection, bytes.getLong(), bytes.getLong(), bytes.getLong());
  }

  static List<DerivedRevisionMapping> readAll(final StorageEngineReader reader) {
    return readRange(reader, new byte[] {4}, new byte[] {5}, 10);
  }

  /** One bounded metadata read serves adjacent historical snapshots after a cold open. */
  static List<DerivedRevisionMapping> readWindow(final StorageEngineReader reader, final int firstRevision) {
    if (firstRevision < 1) {
      throw new IllegalArgumentException("A mapping window starts at a positive source revision");
    }
    // The cursor includes both bounds. Widen before adding so the last window cannot wrap into
    // negative revision keys (which include the frontier marker).
    final int lastRevision = (int) Math.min(Integer.MAX_VALUE, (long) firstRevision + CACHE_CAPACITY - 1);
    return readRange(reader, key(firstRevision), key(lastRevision), CACHE_CAPACITY);
  }

  private static List<DerivedRevisionMapping> readRange(final StorageEngineReader reader, final byte[] lower,
      final byte[] upper, final int capacity) {
    final List<DerivedRevisionMapping> mappings = new ArrayList<>(capacity);
    final PageReference root =
        reader.getTemporalAggregateIndexPage(reader.getActualRevisionRootPage()).getIndexReference(0);
    if (root == null) {
      return mappings;
    }
    try (HOTTrieReader trie = new HOTTrieReader(reader); HOTRangeCursor cursor = trie.range(root, lower, upper)) {
      while (cursor.hasNext()) {
        final HOTRangeCursor.Entry entry = cursor.next();
        final byte[] key = entry.keyBytes();
        if (key.length != 5) {
          throw new IllegalStateException("Invalid derived revision mapping key");
        }
        if (ByteBuffer.wrap(key).getInt(1) == -1) {
          continue;
        }
        final byte[] value = entry.value().toArray(ValueLayout.JAVA_BYTE);
        mappings.add(decode(value, ByteBuffer.wrap(key).getInt(1), reader.getRevisionNumber()));
      }
    }
    return mappings;
  }
}
