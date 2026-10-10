package io.sirix.index.aggregate;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Arrays;
import java.util.function.Supplier;

/** Small raw HOT slots; all mutation and revision ownership use the standard secondary-index path. */
public final class TemporalAggregateStorage extends AbstractHOTIndexWriter<byte[]> implements AutoCloseable {
  static final byte[] HEADER = {0};
  private byte[] keyBuffer = new byte[32];
  private final Reader reader;
  private long nodesWritten;

  public TemporalAggregateStorage(final StorageEngineWriter writer, final int index) {
    super(writer, IndexType.TEMPORAL_AGGREGATE, index);
    initializeTemporalAggregateIndex();
    reader = new Reader(writer, () -> rootReference);
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
    if (key[0] == 3) {
      nodesWritten++;
    }
  }

  public void putPart(final long id, final byte[] bytes) {
    for (int offset = 0, chunk = 0; offset < bytes.length; offset += TemporalAggregateParts.CHUNK_BYTES, chunk++) {
      put(partKey(id, chunk), Arrays.copyOfRange(bytes, offset,
          Math.min(bytes.length, offset + TemporalAggregateParts.CHUNK_BYTES)));
    }
  }

  /** Revision-owned pointers to the bounded immutable parts of one typed group. */
  public record GroupHead(long rows, int revision, long checkpointBytes, long deltaBytes, long[] parts) {
    public GroupHead {
      Objects.requireNonNull(parts);
      if (rows < 0 || revision < 0 || checkpointBytes < 1 || deltaBytes < 0 || parts.length == 0 || parts.length > TemporalAggregateParts.MAX_PARTS) {
        throw new IllegalArgumentException("Invalid temporal aggregate group head");
      }
      parts = parts.clone();
      for (final long part : parts) {
        if (part < 1) {
          throw new IllegalArgumentException("Invalid temporal aggregate part id");
        }
      }
    }

    @Override
    public long[] parts() {
      return parts.clone();
    }

    public byte[] encode() {
      final ByteBuffer bytes = ByteBuffer.allocate(32 + parts.length * Long.BYTES);
      bytes.putLong(rows).putInt(revision).putInt(parts.length).putLong(checkpointBytes).putLong(deltaBytes);
      for (final long part : parts) {
        bytes.putLong(part);
      }
      return bytes.array();
    }

    public static GroupHead decode(final byte[] value) {
      if (value.length < 40 || value.length > 32 + TemporalAggregateParts.MAX_PARTS * Long.BYTES) {
        throw new IllegalStateException("Invalid temporal aggregate group head length");
      }
      final ByteBuffer bytes = ByteBuffer.wrap(value);
      final long rows = bytes.getLong();
      final int revision = bytes.getInt();
      final int count = bytes.getInt();
      if (count < 1 || value.length != 32 + count * Long.BYTES) {
        throw new IllegalStateException("Invalid temporal aggregate group part count");
      }
      final long checkpointBytes = bytes.getLong();
      final long deltaBytes = bytes.getLong();
      final long[] parts = new long[count];
      for (int i = 0; i < count; i++) {
        parts[i] = bytes.getLong();
      }
      return new GroupHead(rows, revision, checkpointBytes, deltaBytes, parts);
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
      this(storage, () -> storage.getTemporalAggregateIndexPage(storage.getActualRevisionRootPage())
          .getIndexReference(index));
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
      if (key[0] == 3) {
        nodesRead++;
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
            return copy.length == 0 ? null : copy;
          }
        } catch (final RuntimeException failure) {
          if (trie.validateCurrentLeaf()) {
            throw failure;
          }
        }
      }
      throw new IllegalStateException("Temporal aggregate leaf did not stabilize");
    }

    public void groups(final GroupConsumer consumer) {
      final PageReference reference = root.get();
      if (reference == null) {
        return;
      }
      try (HOTTrieReader groupReader = new HOTTrieReader(storage);
          HOTRangeCursor cursor = groupReader.range(reference, new byte[] {1}, new byte[] {2})) {
        while (cursor.hasNext()) {
          final HOTRangeCursor.Entry entry = cursor.next();
          consumer.accept(entry.keyBytes(), GroupHead.decode(entry.valueBytes()));
        }
      }
    }

    public TemporalAggregateParts.Store parts() {
      return (part, chunk) -> Objects.requireNonNull(get(partKey(part, chunk)), "Missing temporal aggregate part chunk");
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
