package io.sirix.index.changelog;

import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.index.hot.reader.HOTRangeCursor;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * One revision-owned change-log tree per source projection. Values are bounded inline HOT slots.
 */
public final class ChangeLogStorage extends AbstractHOTIndexWriter<byte[]> implements AutoCloseable {
  public static final int CHUNK_BYTES = 2048;
  private static final boolean WORK_DIAGNOSTICS = Boolean.getBoolean("sirix.changelog.workDiag");
  private static final LongAdder CHUNKS_READ = new LongAdder();

  public static long chunksRead() {
    return CHUNKS_READ.sum();
  }

  private static final byte[] HEADER = {0};
  private static final byte[] NEXT_AGGREGATE_ID = {5, -1, -1, -1, -1};
  private byte[] keyBuffer = new byte[16];
  private final HOTTrieReader reader;
  private long slotsWritten;
  private byte @Nullable [] header;
  private long exceptionalRows;
  private boolean headerDirty;
  private static volatile WriteWork lastWrite = new WriteWork(0, 0, 0);

  public record WriteWork(int records, int bytes, long slots) {
  }

  public static WriteWork lastWrite() {
    return lastWrite;
  }

  public ChangeLogStorage(final StorageEngineWriter writer, final int projection) {
    super(writer, IndexType.CHANGE_LOG, projection);
    initializeChangeLogIndex();
    reader = new HOTTrieReader(writer);
    header = get(HEADER);
    if (header != null) {
      if (header.length != 20 || ByteBuffer.wrap(header).getInt() != ChangeLogSlab.FORMAT) {
        throw new IllegalStateException("Invalid change-log header");
      }
      final int columns = ByteBuffer.wrap(header).getInt(8);
      if (columns < 1 || columns > 65535 || ByteBuffer.wrap(header).getInt(4) < 1) {
        throw new IllegalStateException("Invalid change-log coverage header");
      }
      exceptionalRows = ByteBuffer.wrap(header).getLong(12);
      if (exceptionalRows < 0) {
        throw new IllegalStateException("Negative change-log exceptional row count");
      }
    }
  }

  public long slotsWritten() {
    return slotsWritten;
  }

  public void initialize(final int firstRevision, final byte[] kinds) {
    if (header == null) {
      if (firstRevision < 1 || kinds.length < 1 || kinds.length > 65535) {
        throw new IllegalArgumentException("Invalid change-log schema");
      }
      header = ByteBuffer.allocate(20)
                         .putInt(ChangeLogSlab.FORMAT)
                         .putInt(firstRevision)
                         .putInt(kinds.length)
                         .putLong(0)
                         .array();
      put(HEADER, header);
    }
  }

  public boolean initialized() {
    return header != null;
  }

  /** Reset sparse row flags before resuming capture after an interval without consumers. */
  public void clearProvenance() {
    if (exceptionalRows == 0) {
      return;
    }
    // Collect before mutating: a cursor must not walk a tree it is simultaneously rewriting.
    final List<byte[]> extents = new ArrayList<>();
    try (HOTRangeCursor cursor = reader.range(rootReference, new byte[] {1}, new byte[] {2})) {
      while (cursor.hasNext()) {
        final HOTRangeCursor.Entry entry = cursor.next();
        final byte[] key = entry.keyBytes();
        if (key.length == 13 && key[0] == 1 && ByteBuffer.wrap(key).getInt(9) == -1 && entry.value().byteSize() != 0) {
          extents.add(key);
        }
      }
    }
    for (final byte[] extent : extents) {
      put(extent, new byte[0]);
    }
    exceptionalRows = 0;
    headerDirty = true;
  }

  public void write(final ChangeLogSlab slab) {
    final long before = slotsWritten;
    final byte[] bytes = slab.encode();
    final int chunks = (bytes.length + CHUNK_BYTES - 1) / CHUNK_BYTES;
    for (int chunk = 0; chunk < chunks; chunk++) {
      put(slabKey(slab.revision(), chunk),
          Arrays.copyOfRange(bytes, chunk * CHUNK_BYTES, Math.min(bytes.length, (chunk + 1) * CHUNK_BYTES)));
    }
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    put(slabKey(slab.revision(), -1),
        ByteBuffer.allocate(24)
                  .putInt(bytes.length)
                  .putInt(chunks)
                  .putLong(random.nextLong())
                  .putLong(random.nextLong())
                  .array());
    lastWrite = new WriteWork(slab.entries().size(), bytes.length, slotsWritten - before);
  }

  /** Sparse flags for rows whose leaf-wide provenance cannot identify the exceptional cells. */
  public void provenance(final long record, final ChangeLogSlab.@Nullable Row row) {
    final byte[] key = provenanceKey(record, -1);
    if (row != null && row.flags.length != ByteBuffer.wrap(Objects.requireNonNull(header)).getInt(8)) {
      throw new IllegalArgumentException("Change-log provenance schema mismatch");
    }
    if (row != null && row.exceptional()) {
      final byte[] value = new byte[1 + row.flags.length];
      value[0] = (byte) (row.object
          ? 1
          : 2);
      for (int column = 0; column < row.flags.length; column++) {
        value[column + 1] = (byte) (row.flags[column] & ~ChangeLogSlab.PRESENT);
      }
      final byte[] previous = provenance(record);
      if (!Arrays.equals(value, previous)) {
        for (int offset = 0, chunk = 0; offset < value.length; offset += CHUNK_BYTES, chunk++) {
          put(provenanceKey(record, chunk),
              Arrays.copyOfRange(value, offset, Math.min(value.length, offset + CHUNK_BYTES)));
        }
        put(key, ByteBuffer.allocate(4).putInt(value.length).array());
        if (previous == null) {
          exceptionalRows = Math.incrementExact(exceptionalRows);
          headerDirty = true;
        }
      }
    } else if (exceptionalRows != 0 && get(key) != null) {
      put(key, new byte[0]);
      exceptionalRows--;
      headerDirty = true;
    }
  }

  public byte @Nullable [] provenance(final long record) {
    if (exceptionalRows == 0) {
      return null;
    }
    final byte[] extent = get(provenanceKey(record, -1));
    if (extent == null) {
      return null;
    }
    final int columns = ByteBuffer.wrap(Objects.requireNonNull(header)).getInt(8);
    if (extent.length != 4 || ByteBuffer.wrap(extent).getInt() != columns + 1) {
      throw new IllegalStateException("Invalid change-log provenance extent");
    }
    final byte[] value = new byte[columns + 1];
    for (int offset = 0, chunk = 0; offset < value.length; offset += CHUNK_BYTES, chunk++) {
      final byte[] part = Objects.requireNonNull(get(provenanceKey(record, chunk)), "Missing change-log provenance");
      if (part.length != Math.min(CHUNK_BYTES, value.length - offset)) {
        throw new IllegalStateException("Invalid change-log provenance chunk");
      }
      System.arraycopy(part, 0, value, offset, part.length);
    }
    return value;
  }

  private static byte[] provenanceKey(final long record, final int chunk) {
    return ByteBuffer.allocate(13).put((byte) 1).putLong(record).putInt(chunk).array();
  }

  private static byte[] slabKey(final int revision, final int chunk) {
    return ByteBuffer.allocate(9).put((byte) 2).putInt(revision).putInt(chunk).array();
  }

  public static ChangeLogSlab read(final StorageEngineReader storage, final int projection, final int revision) {
    final PageReference root =
        storage.getChangeLogIndexPage(storage.getActualRevisionRootPage()).getIndexReference(projection);
    if (root == null) {
      throw new IllegalStateException("Missing change log");
    }
    try (HOTTrieReader reader = new HOTTrieReader(storage)) {
      final byte[] header = Objects.requireNonNull(read(reader, root, slabKey(revision, -1)), "Missing change slab");
      if (header.length != 24) {
        throw new IllegalStateException("Invalid change-log chunk directory");
      }
      final ByteBuffer buffer = ByteBuffer.wrap(header);
      final int length = buffer.getInt();
      final int chunks = buffer.getInt();
      if (length < 30 || chunks != (length + (long) CHUNK_BYTES - 1) / CHUNK_BYTES) {
        throw new IllegalStateException("Invalid change-log chunk count");
      }
      final byte[] bytes = new byte[length];
      for (int chunk = 0; chunk < chunks; chunk++) {
        if (WORK_DIAGNOSTICS) {
          CHUNKS_READ.increment();
        }
        final byte[] part =
            Objects.requireNonNull(read(reader, root, slabKey(revision, chunk)), "Missing change-log chunk");
        final int expected = Math.min(CHUNK_BYTES, length - chunk * CHUNK_BYTES);
        if (part.length != expected) {
          throw new IllegalStateException("Invalid change-log chunk length");
        }
        System.arraycopy(part, 0, bytes, chunk * CHUNK_BYTES, part.length);
      }
      final ChangeLogSlab slab = ChangeLogSlab.decode(bytes);
      if (slab.projection() != projection || slab.revision() != revision) {
        throw new IllegalStateException("Change-log identity mismatch");
      }
      return slab;
    }
  }

  /** A slab directory identifies a publication even if truncate reuses its revision and timestamp. */
  public static byte @Nullable [] identity(final StorageEngineReader storage, final int projection,
      final int revision) {
    final PageReference root =
        storage.getChangeLogIndexPage(storage.getActualRevisionRootPage()).getIndexReference(projection);
    if (root == null) {
      return null;
    }
    try (HOTTrieReader reader = new HOTTrieReader(storage)) {
      final byte[] directory = read(reader, root, slabKey(revision, -1));
      if (directory == null) {
        return null;
      }
      if (directory.length != 24) {
        throw new IllegalStateException("Invalid change-log identity");
      }
      return Arrays.copyOfRange(directory, 8, 24);
    }
  }

  /** Aggregate declaration reservations live with the log, never in source aggregate pages. */
  public static int nextAggregateId(final StorageEngineReader storage) {
    final PageReference root = storage.getChangeLogIndexPage(storage.getActualRevisionRootPage()).getIndexReference(0);
    if (root == null) {
      return 0;
    }
    try (HOTTrieReader reader = new HOTTrieReader(storage)) {
      final byte[] next = read(reader, root, NEXT_AGGREGATE_ID);
      return next == null
          ? 0
          : ByteBuffer.wrap(next).getInt();
    }
  }

  public static boolean aggregateIdReserved(final StorageEngineReader storage, final int index) {
    final PageReference root = storage.getChangeLogIndexPage(storage.getActualRevisionRootPage()).getIndexReference(0);
    if (root == null) {
      return false;
    }
    try (HOTTrieReader reader = new HOTTrieReader(storage)) {
      return read(reader, root, ByteBuffer.allocate(5).put((byte) 5).putInt(index).array()) != null;
    }
  }

  public static void reserveAggregateId(final StorageEngineWriter writer, final int index) {
    if (index < 0 || index == Integer.MAX_VALUE || aggregateIdReserved(writer, index)) {
      throw new IllegalArgumentException("Temporal aggregate id has already been reserved or is invalid: " + index);
    }
    final int next = Math.max(nextAggregateId(writer), index + 1);
    try (ChangeLogStorage storage = new ChangeLogStorage(writer, 0)) {
      storage.put(ByteBuffer.allocate(5).put((byte) 5).putInt(index).array(), new byte[] {1});
      storage.put(NEXT_AGGREGATE_ID, ByteBuffer.allocate(4).putInt(next).array());
    }
  }

  private byte @Nullable [] get(final byte[] key) {
    return read(reader, rootReference, key);
  }

  private static byte @Nullable [] read(final HOTTrieReader reader, final PageReference root, final byte[] key) {
    for (int attempt = 0; attempt < 16; attempt++) {
      final MemorySegment value = reader.get(root, key);
      if (value == null) {
        return null;
      }
      try {
        final byte[] copy = value.toArray(ValueLayout.JAVA_BYTE);
        if (reader.validateCurrentLeaf()) {
          return copy.length == 0
              ? null
              : copy;
        }
      } catch (final RuntimeException failure) {
        if (reader.validateCurrentLeaf()) {
          throw failure;
        }
      }
    }
    throw new IllegalStateException("Change-log leaf did not stabilize");
  }

  private void put(final byte[] key, final byte[] value) {
    if (value.length > CHUNK_BYTES) {
      throw new IllegalArgumentException("Change-log slot exceeds inline bound");
    }
    doIndex(key, key.length, value, value.length);
    slotsWritten++;
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
    try {
      if (headerDirty) {
        ByteBuffer.wrap(Objects.requireNonNull(header)).putLong(12, exceptionalRows);
        put(HEADER, header);
      }
    } finally {
      reader.close();
    }
  }
}
