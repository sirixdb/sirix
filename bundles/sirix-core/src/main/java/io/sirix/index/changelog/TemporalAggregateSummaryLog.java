package io.sirix.index.changelog;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.hot.reader.HOTRangeCursor;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Revision-owned packed group operands, scanned as a complete tail during query catch-up. */
public final class TemporalAggregateSummaryLog {
  private static final int MAGIC = 0x54415331;
  private static final int HEADER_BYTES = 60;
  private static final int BLOCK_HEADER_BYTES = 6;
  private static final int MAX_BLOCK_BYTES = 2 * ChangeLogStorage.CHUNK_BYTES;
  private static volatile WriteWork lastWrite = new WriteWork(0, 0, 0);

  public record WriteWork(int groups, int bytes, long slots) {
  }

  public static WriteWork lastWrite() {
    return lastWrite;
  }

  private TemporalAggregateSummaryLog() {}

  /** Maximum operand bytes, including the group record's framing within a two-chunk block. */
  public static int operandLimit(final byte[] group) {
    if (group.length < 1 || group.length > ChangeLogStorage.CHUNK_BYTES) {
      throw new IllegalArgumentException("Invalid temporal summary group key");
    }
    return MAX_BLOCK_BYTES - BLOCK_HEADER_BYTES - 4 - group.length;
  }

  /** Null operands are explicit oversized/unservable markers; empty operands must be omitted. */
  public static WriteWork write(final ChangeLogStorage storage, final ChangeLogSlab slab, final int index,
      final NavigableMap<byte[], byte[]> groups, final long unsupportedDelta, final boolean unservable) {
    if (index < 0) {
      throw new IllegalArgumentException("Negative aggregate declaration");
    }
    final byte[] identity = storage.publicationIdentity();
    final byte[] prefix = prefix(slab.revision(), index, identity);
    final long before = storage.slotsWritten();
    final ByteBuffer block = ByteBuffer.allocate(MAX_BLOCK_BYTES);
    block.position(BLOCK_HEADER_BYTES);
    int count = 0;
    int blocks = 0;
    int chunks = 0;
    int bytes = HEADER_BYTES;
    byte[] last = null;
    for (final Map.Entry<byte[], byte[]> group : groups.entrySet()) {
      final byte[] key = group.getKey();
      final byte[] operand = group.getValue();
      final int length = operand == null
          ? 0
          : operand.length;
      if (key.length == 0 || key.length > ChangeLogStorage.CHUNK_BYTES || (operand != null && length == 0)
          || length > operandLimit(key) || (last != null && Arrays.compareUnsigned(last, key) >= 0)) {
        throw new IllegalArgumentException("Invalid or unsorted temporal group operand");
      }
      final int size = 4 + key.length + length;
      if (count != 0 && block.position() + size > ChangeLogStorage.CHUNK_BYTES) {
        final int written = flush(storage, prefix, Objects.requireNonNull(last), block, count);
        bytes = Math.addExact(bytes, written);
        chunks += (written + 2047) / 2048;
        blocks++;
        block.clear().position(BLOCK_HEADER_BYTES);
        count = 0;
      }
      block.putShort((short) key.length).putShort((short) length).put(key);
      if (operand != null) {
        block.put(operand);
      }
      count++;
      last = key;
    }
    if (count != 0) {
      final int written = flush(storage, prefix, Objects.requireNonNull(last), block, count);
      bytes = Math.addExact(bytes, written);
      chunks += (written + 2047) / 2048;
      blocks++;
    }
    storage.put(headerKey(slab.revision(), index),
        ByteBuffer.allocate(HEADER_BYTES)
                  .putInt(MAGIC)
                  .putInt(slab.revision())
                  .putInt(slab.baseRevision())
                  .putInt(index)
                  .putInt(groups.size())
                  .putInt(blocks)
                  .putInt(chunks)
                  .putInt(bytes)
                  .putLong(unsupportedDelta)
                  .putInt(unservable
                      ? 1
                      : 0)
                  .put(identity)
                  .array());
    final WriteWork work = new WriteWork(groups.size(), bytes, storage.slotsWritten() - before);
    lastWrite = work;
    return work;
  }

  @SuppressWarnings("ByteBufferBackingArray") // write() supplies a zero-offset buffer from allocate().
  private static int flush(final ChangeLogStorage storage, final byte[] prefix, final byte[] last,
      final ByteBuffer block, final int groups) {
    final int length = block.position();
    block.putInt(0, length).putShort(4, (short) groups);
    for (int offset = 0, chunk = 0; offset < length; offset += ChangeLogStorage.CHUNK_BYTES, chunk++) {
      storage.put(blockKey(prefix, last, chunk),
          Arrays.copyOfRange(block.array(), offset, Math.min(length, offset + ChangeLogStorage.CHUNK_BYTES)));
    }
    return length;
  }

  private static byte[] headerKey(final int revision, final int index) {
    return ByteBuffer.allocate(9).put((byte) 7).putInt(revision).putInt(index).array();
  }

  private static byte[] prefix(final int revision, final int index, final byte[] identity) {
    return ByteBuffer.allocate(25).put((byte) 6).putInt(revision).putInt(index).put(identity).array();
  }

  private static byte[] blockKey(final byte[] prefix, final byte[] group, final int chunk) {
    return ByteBuffer.allocate(prefix.length + group.length + 2).put(prefix).put(group).putShort((short) chunk).array();
  }

  private static byte[] successor(final byte[] prefix) {
    final byte[] upper = prefix.clone();
    for (int at = upper.length - 1; at >= 0; at--) {
      if (++upper[at] != 0) {
        return Arrays.copyOf(upper, at + 1);
      }
    }
    throw new IllegalArgumentException("No successor for prefix");
  }

  /** A reader is pinned to the caller's source revision; no later publication can enter the fold. */
  public static final class Reader implements AutoCloseable {
    private final HOTTrieReader trie;
    private final PageReference root;
    private final TemporalAggregateCatchUpBudget.Budget budget;
    private final byte[] prefix;
    private final int groups;
    private final int blocks;
    private final int chunks;
    private final int bytes;
    private int readChunks;
    private int readBytes;

    public Reader(final StorageEngineReader source, final int projection, final int revision, final int index,
        final TemporalAggregateCatchUpBudget.Budget budget) {
      if (projection < 0 || index < 0 || revision < 1 || revision > source.getRevisionNumber()) {
        throw new IllegalArgumentException("Invalid temporal summary view");
      }
      this.budget = Objects.requireNonNull(budget);
      root = Objects.requireNonNull(
          source.getChangeLogIndexPage(source.getActualRevisionRootPage()).getIndexReference(projection),
          "Missing source log");
      trie = new HOTTrieReader(source);
      try {
        final byte[] header = ChangeLogStorage.read(trie, root, headerKey(revision, index));
        if (header == null) {
          throw new TemporalAggregateCatchUpBudget.LimitExceeded();
        }
        if (header.length != HEADER_BYTES) {
          throw new IllegalStateException("Invalid temporal summary header length");
        }
        final ByteBuffer in = ByteBuffer.wrap(header);
        if (in.getInt() != MAGIC || in.getInt() != revision || in.getInt() != revision - 1 || in.getInt() != index) {
          throw new TemporalAggregateCatchUpBudget.LimitExceeded();
        }
        groups = in.getInt();
        blocks = in.getInt();
        chunks = in.getInt();
        bytes = in.getInt();
        final long unsupported = in.getLong();
        final int flags = in.getInt();
        if (groups < 0 || blocks < 0 || blocks > groups || chunks < blocks || (long) chunks > 2L * blocks
            || bytes < HEADER_BYTES || (long) bytes > HEADER_BYTES + (long) chunks * ChangeLogStorage.CHUNK_BYTES
            || (blocks == 0) != (groups == 0) || flags < 0 || flags > 1) {
          throw new IllegalStateException("Invalid temporal summary coverage");
        }
        if (unsupported != 0 || flags != 0 || groups > 4096) {
          throw new TemporalAggregateCatchUpBudget.LimitExceeded();
        }
        final byte[] identity = new byte[16];
        in.get(identity);
        prefix = prefix(revision, index, identity);
      } catch (final RuntimeException | Error failure) {
        trie.close();
        throw failure;
      }
    }

    public void visit(final BiConsumer<byte[], byte[]> consumer) {
      Objects.requireNonNull(consumer);
      final int firstChunk = readChunks;
      final int firstByte = readBytes;
      int readGroups = 0;
      int readBlocks = 0;
      byte[] previous = null;
      try (HOTRangeCursor cursor = trie.range(root, prefix, successor(prefix))) {
        while (cursor.hasNext()) {
          if (++readBlocks > blocks) {
            throw new IllegalStateException("Extra temporal summary block");
          }
          final Block block = readBlock(cursor);
          previous = visitBlock(block, previous, consumer);
          readGroups = Math.addExact(readGroups, block.groups());
        }
      }
      if (readGroups != groups || readBlocks != blocks || readChunks - firstChunk != chunks
          || readBytes - firstByte + HEADER_BYTES != bytes) {
        throw new IllegalStateException("Incomplete temporal summary coverage");
      }
    }

    @SuppressWarnings("ArrayRecordComponent") // Internal decoding carrier; fence bytes are not equality keys.
    private record Block(byte[] fence, ByteBuffer bytes, int groups) {
    }

    private Block readBlock(final HOTRangeCursor cursor) {
      final HOTRangeCursor.Entry first = cursor.next();
      final byte[] key = first.keyBytes();
      if (key.length <= prefix.length + 2 || !Arrays.equals(prefix, 0, prefix.length, key, 0, prefix.length)
          || ByteBuffer.wrap(key).getShort(key.length - 2) != 0) {
        throw new IllegalStateException("Invalid temporal summary block key");
      }
      final byte[] firstBytes = readChunk(first);
      if (firstBytes.length < BLOCK_HEADER_BYTES) {
        throw new IllegalStateException("Truncated temporal summary block");
      }
      final ByteBuffer header = ByteBuffer.wrap(firstBytes);
      final int length = header.getInt();
      final int count = Short.toUnsignedInt(header.getShort());
      if (length < BLOCK_HEADER_BYTES || length > MAX_BLOCK_BYTES || count < 1
          || firstBytes.length != Math.min(length, ChangeLogStorage.CHUNK_BYTES)
          || (length > ChangeLogStorage.CHUNK_BYTES && count != 1)) {
        throw new IllegalStateException("Invalid temporal summary block extent");
      }
      final byte[] value;
      if (length > firstBytes.length) {
        if (!cursor.hasNext()) {
          throw new IllegalStateException("Missing temporal summary continuation");
        }
        final HOTRangeCursor.Entry continuation = cursor.next();
        final byte[] nextKey = key.clone();
        nextKey[nextKey.length - 1] = 1;
        if (!Arrays.equals(continuation.keyBytes(), nextKey)) {
          throw new IllegalStateException("Invalid temporal summary continuation");
        }
        final byte[] rest = readChunk(continuation);
        if (rest.length != length - firstBytes.length) {
          throw new IllegalStateException("Invalid temporal summary continuation length");
        }
        value = Arrays.copyOf(firstBytes, length);
        System.arraycopy(rest, 0, value, firstBytes.length, rest.length);
      } else {
        value = firstBytes;
      }
      final ByteBuffer in = ByteBuffer.wrap(value);
      in.position(BLOCK_HEADER_BYTES);
      return new Block(Arrays.copyOfRange(key, prefix.length, key.length - 2), in, count);
    }

    private byte[] readChunk(final HOTRangeCursor.Entry entry) {
      budget.chunk();
      ChangeLogStorage.recordChunkRead();
      final long length = entry.value().byteSize();
      if (length < 1 || length > ChangeLogStorage.CHUNK_BYTES) {
        throw new IllegalStateException("Invalid temporal summary chunk");
      }
      final byte[] value = entry.value().toArray(ValueLayout.JAVA_BYTE);
      readChunks++;
      readBytes = Math.addExact(readBytes, value.length);
      return value;
    }

    private static byte[] visitBlock(final Block block, final byte @Nullable [] previous,
        final BiConsumer<byte[], byte[]> consumer) {
      final ByteBuffer in = block.bytes();
      byte[] last = previous;
      for (int at = 0; at < block.groups(); at++) {
        final int keyLength = Short.toUnsignedInt(in.getShort());
        final int size = Short.toUnsignedInt(in.getShort());
        if (keyLength < 1 || keyLength > ChangeLogStorage.CHUNK_BYTES || keyLength + size > in.remaining()) {
          throw new IllegalStateException("Invalid temporal summary group length");
        }
        final byte[] key = new byte[keyLength];
        in.get(key);
        if (last != null && Arrays.compareUnsigned(last, key) >= 0) {
          throw new IllegalStateException("Unsorted temporal summary groups");
        }
        last = key;
        if (size == 0) {
          throw new TemporalAggregateCatchUpBudget.LimitExceeded();
        }
        final byte[] operand = new byte[size];
        in.get(operand);
        consumer.accept(key, operand);
      }
      if (in.hasRemaining() || !Arrays.equals(last, block.fence())) {
        throw new IllegalStateException("Invalid temporal summary upper fence");
      }
      return Objects.requireNonNull(last);
    }

    @Override
    public void close() {
      trie.close();
    }
  }
}
