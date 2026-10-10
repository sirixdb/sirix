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

  public TemporalAggregateTree.Store tree(final long group) {
    return new TemporalAggregateTree.Store() {
      private final byte[] key = new byte[29];

      @Override
      public byte @Nullable [] read(final long segment, final long prefix, final int bit) {
        treeKey(key, group, segment, prefix, bit);
        return reader.get(key);
      }

      @Override
      public void write(final long segment, final long prefix, final int bit, final byte[] value) {
        treeKey(key, group, segment, prefix, bit);
        put(key, value);
      }
    };
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
    void accept(byte[] key, long group);
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
          consumer.accept(entry.keyBytes(), ByteBuffer.wrap(entry.valueBytes()).getLong());
        }
      }
    }

    public TemporalAggregateTree.Store tree(final long group) {
      return new TemporalAggregateTree.Store() {
        private final byte[] key = new byte[29];

        @Override
        public byte @Nullable [] read(final long segment, final long prefix, final int bit) {
          treeKey(key, group, segment, prefix, bit);
          return get(key);
        }

        @Override
        public void write(final long segment, final long prefix, final int bit, final byte[] value) {
          throw new UnsupportedOperationException("Read-only temporal aggregate tree");
        }
      };
    }

    @Override
    public void close() {
      trie.close();
    }
  }

  public static byte[] recordKey(final long record) {
    return ByteBuffer.allocate(9).put((byte) 2).putLong(record).array();
  }

  private static void treeKey(final byte[] target, final long group, final long segment, final long prefix,
      final int bit) {
    ByteBuffer.wrap(target).put((byte) 3).putLong(group).putLong(segment).putLong(prefix).putInt(bit);
  }
}
