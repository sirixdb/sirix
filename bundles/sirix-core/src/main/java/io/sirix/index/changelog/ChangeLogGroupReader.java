package io.sirix.index.changelog;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.page.PageReference;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Bounded, group-addressable reads of one immutable publication. Shared chunks are read once. */
public final class ChangeLogGroupReader implements AutoCloseable {
  public static final int GROUP_CHUNK_LIMIT = 2;
  public static final int METADATA_BYTE_LIMIT = 64 * 1024;
  public static final int ENTRY_LIMIT = 4096;

  /** Query-owned budget, shared across revisions and groupings. */
  public static final class Budget {
    private int chunks = 512;
    private int entries = 16_384;
    private int cells = 262_144;

    private void chunk() {
      if (chunks-- <= 0) {
        throw new LimitExceeded();
      }
    }

    private void entries(final int count, final int columns) {
      final long requiredCells = 2L * count * columns;
      if (count > entries || requiredCells > cells) {
        throw new LimitExceeded();
      }
      entries -= count;
      cells -= (int) requiredCells;
    }
  }

  /** Expected route decline; do not log a stack trace or return a partial result. */
  public static final class LimitExceeded extends RuntimeException {
    public LimitExceeded() {
      super("Temporal aggregate catch-up work budget exhausted", null, false, false);
    }
  }

  private final HOTTrieReader reader;
  private final PageReference root;
  private final int revision;
  private final int length;
  private final Budget budget;
  private final Int2ObjectOpenHashMap<byte[]> chunks = new Int2ObjectOpenHashMap<>();
  private final IntOpenHashSet metadataChunks = new IntOpenHashSet(GROUP_CHUNK_LIMIT);
  private final byte[] kinds;
  private final String[] dictionary;
  private final int metadataLength;
  private final int directoryOffset;
  private final byte[] inlineDirectory;
  private final Int2ObjectOpenHashMap<ChangeLogDirectory.Groups> groupings = new Int2ObjectOpenHashMap<>();
  private @Nullable List<ChangeLogDirectory.Section> sections;
  private final int baseRevision;

  public ChangeLogGroupReader(final StorageEngineReader source, final int projection, final int revision,
      final Budget budget) {
    if (projection < 0 || revision < 1 || revision > source.getRevisionNumber()) {
      throw new IllegalArgumentException("Invalid source change-log view");
    }
    this.budget = Objects.requireNonNull(budget);
    this.revision = revision;
    root = Objects.requireNonNull(
        source.getChangeLogIndexPage(source.getActualRevisionRootPage()).getIndexReference(projection),
        "Missing change log");
    reader = new HOTTrieReader(source);
    try {
      final byte[] extent = Objects.requireNonNull(
          ChangeLogStorage.read(reader, root, ChangeLogStorage.slabKey(revision, -1)), "Missing change log extent");
      if (extent.length < 28 || extent.length > ChangeLogStorage.CHUNK_BYTES) {
        throw new IllegalStateException("Invalid change-log extent");
      }
      final ByteBuffer header = ByteBuffer.wrap(extent);
      length = header.getInt();
      if (length < 42 || header.getInt() != (length + 2047L) / 2048) {
        throw new IllegalStateException("Invalid change-log extent length");
      }
      final Sections selected = readSections(header, extent);
      metadataLength = selected.metadata().length;
      directoryOffset = ByteBuffer.wrap(selected.metadata()).getInt(28);
      inlineDirectory = selected.directory();
      final Schema schema = readSchema(selected.metadata(), projection);
      baseRevision = schema.baseRevision();
      kinds = schema.kinds();
      dictionary = schema.dictionary();
    } catch (final IOException exception) {
      reader.close();
      throw new IllegalStateException("Malformed change-log metadata", exception);
    } catch (final RuntimeException | Error exception) {
      reader.close();
      throw exception;
    }
  }

  private record Sections(byte[] metadata, byte[] directory) {
  }

  private record Schema(int baseRevision, byte[] kinds, String[] dictionary) {
  }

  private Sections readSections(final ByteBuffer header, final byte[] extent) {
    final int inline = header.getInt(24);
    final byte[] metadata;
    final byte[] directoryBytes;
    if (inline > 0) {
      if (inline < 38 || inline > extent.length - 32) {
        throw new IllegalStateException("Invalid inline log metadata");
      }
      metadata = new byte[inline];
      System.arraycopy(extent, 28, metadata, 0, inline);
      directoryBytes = new byte[extent.length - 28 - inline];
      System.arraycopy(extent, 28 + inline, directoryBytes, 0, directoryBytes.length);
    } else {
      if (inline != 0 || extent.length != 28) {
        throw new IllegalStateException("Invalid external log metadata");
      }
      final ByteBuffer first = ByteBuffer.wrap(chunk(0));
      final int metadataLength = first.getInt(24);
      final int directoryOffset = first.getInt(28);
      if (metadataLength < 38 || directoryOffset < metadataLength || directoryOffset > length) {
        throw new IllegalStateException("Invalid log section offsets");
      }
      if (metadataLength > METADATA_BYTE_LIMIT) {
        throw new LimitExceeded();
      }
      requireChunks(metadataChunks, 0, metadataLength);
      metadata = bytes(0, metadataLength);
      directoryBytes = new byte[0];
    }
    return new Sections(metadata, directoryBytes);
  }

  private Schema readSchema(final byte[] metadata, final int projection) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(metadata))) {
      if (in.readInt() != ChangeLogSlab.MAGIC || in.readInt() != ChangeLogSlab.FORMAT || in.readInt() != projection
          || in.readInt() != revision) {
        throw new IllegalStateException("Invalid log metadata identity");
      }
      final int baseRevision = in.readInt();
      if (baseRevision < 0 || baseRevision >= revision || in.readInt() < 0 || in.readInt() != metadata.length
          || in.readInt() != directoryOffset || directoryOffset < metadataLength || directoryOffset > length - 4
          || inlineDirectory.length > length - directoryOffset) {
        throw new IllegalStateException("Invalid log section identity");
      }
      final int columns = ChangeLogSlab.bounded(in.readUnsignedShort(), in.available());
      final byte[] kinds = in.readNBytes(columns);
      if (columns == 0) {
        throw new IllegalStateException("Empty change-log schema");
      }
      for (final byte kind : kinds) {
        if (kind < ChangeLogSlab.LONG || kind > ChangeLogSlab.DATE) {
          throw new IllegalStateException("Invalid change-log cell kind");
        }
      }
      final String[] dictionary = new String[ChangeLogSlab.bounded(in.readInt(), in.available())];
      for (int at = 0; at < dictionary.length; at++) {
        final int size = ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), in.available());
        dictionary[at] = new String(in.readNBytes(size), StandardCharsets.UTF_8);
      }
      if (in.available() != 0) {
        throw new IllegalStateException("Trailing change-log metadata");
      }
      return new Schema(baseRevision, kinds, dictionary);
    }
  }

  public int baseRevision() {
    return baseRevision;
  }

  public byte[] kinds() {
    return kinds.clone();
  }

  public ChangeLogDirectory.@Nullable Groups groups(final int index) {
    final ChangeLogDirectory.Groups cached = groupings.get(index);
    if (cached != null) {
      return cached;
    }
    try {
      if (sections == null) {
        final int count = ByteBuffer.wrap(directoryBytes(0, 4)).getInt();
        final int tableLength = 4 + 12 * ChangeLogSlab.bounded(count, (length - directoryOffset - 4) / 12);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(directoryBytes(0, tableLength)))) {
          sections = ChangeLogDirectory.readSections(in, length - directoryOffset);
        }
      }
      for (final ChangeLogDirectory.Section section : sections) {
        if (section.index() == index) {
          try (DataInputStream in =
              new DataInputStream(new ByteArrayInputStream(directoryBytes(section.offset(), section.length())))) {
            final ChangeLogDirectory.Groups groups =
                ChangeLogDirectory.readGroups(in, index, kinds.length, metadataLength, directoryOffset);
            groupings.put(index, groups);
            return groups;
          }
        }
      }
      return null;
    } catch (final IOException exception) {
      throw new IllegalStateException("Malformed change-log grouping", exception);
    }
  }

  private byte[] directoryBytes(final int offset, final int size) {
    if (offset < 0 || size < 0 || size > length - directoryOffset - offset) {
      throw new IllegalStateException("Invalid change-log directory range");
    }
    if (offset + size <= inlineDirectory.length) {
      final byte[] result = new byte[size];
      System.arraycopy(inlineDirectory, offset, result, 0, size);
      return result;
    }
    // Preflight the whole external span before allocation or reads. Inline prefixes are free
    // of payload-chunk I/O, but a partially inlined group still charges its external suffix.
    final int external = Math.max(offset, inlineDirectory.length);
    requireChunks(metadataChunks, directoryOffset + external, offset + size - external);
    final byte[] result = new byte[size];
    final int prefix = external - offset;
    System.arraycopy(inlineDirectory, Math.min(offset, inlineDirectory.length), result, 0, prefix);
    final byte[] suffix = bytes(directoryOffset + external, size - prefix);
    System.arraycopy(suffix, 0, result, prefix, suffix.length);
    return result;
  }

  public void visit(final ChangeLogDirectory.Groups groups, final byte[] key,
      final Consumer<ChangeLogSlab.Entry> consumer) {
    final List<ChangeLogDirectory.Range> ranges = groups.ranges(key);
    if (ranges == null) {
      return;
    }
    final IntOpenHashSet required = new IntOpenHashSet(metadataChunks);
    int entries = 0;
    for (final ChangeLogDirectory.Range range : ranges) {
      entries = Math.addExact(entries, range.count());
      if (entries > ENTRY_LIMIT) {
        throw new LimitExceeded();
      }
      requireChunks(required, range.offset(), range.length());
    }
    budget.entries(entries, kinds.length);
    for (final ChangeLogDirectory.Range range : ranges) {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes(range.offset(), range.length())))) {
        long previous = 0;
        for (int at = 0; at < range.count(); at++) {
          final long record = ChangeLogSlab.readRecord(in, previous, at == 0);
          previous = record;
          if (at == 0 && record != range.firstRecord()) {
            throw new IllegalStateException("Invalid change-log range start");
          }
          final long[] changed = new long[(kinds.length + 63) / 64];
          for (int word = 0; word < changed.length; word++) {
            changed[word] = in.readLong();
          }
          consumer.accept(new ChangeLogSlab.Entry(record, changed, ChangeLogSlab.readRow(in, kinds, dictionary),
              ChangeLogSlab.readRow(in, kinds, dictionary)));
        }
        if (in.available() != 0) {
          throw new IllegalStateException("Invalid change-log range length");
        }
      } catch (final IOException exception) {
        throw new IllegalStateException("Malformed change-log range", exception);
      }
    }
  }

  /** Metadata is part of the cold group-read bound, even when another group cached it. */
  private static void requireChunks(final IntOpenHashSet required, final int offset, final int size) {
    for (int chunk = offset / ChangeLogStorage.CHUNK_BYTES; chunk <= (offset + size - 1)
        / ChangeLogStorage.CHUNK_BYTES; chunk++) {
      required.add(chunk);
      if (required.size() > GROUP_CHUNK_LIMIT) {
        throw new LimitExceeded();
      }
    }
  }

  private byte[] bytes(final int offset, final int size) {
    if (offset < 0 || size < 0 || size > length - offset) {
      throw new IllegalStateException("Invalid change-log byte range");
    }
    final byte[] result = new byte[size];
    int copied = 0;
    while (copied < size) {
      final int position = offset + copied;
      final byte[] chunk = chunk(position / 2048);
      final int from = position % 2048;
      final int count = Math.min(size - copied, chunk.length - from);
      System.arraycopy(chunk, from, result, copied, count);
      copied += count;
    }
    return result;
  }

  private byte[] chunk(final int number) {
    byte[] bytes = chunks.get(number);
    if (bytes == null) {
      budget.chunk();
      ChangeLogStorage.recordChunkRead();
      bytes = Objects.requireNonNull(ChangeLogStorage.read(reader, root, ChangeLogStorage.slabKey(revision, number)),
          "Missing change-log chunk");
      if (bytes.length != Math.min(2048, length - number * 2048)) {
        throw new IllegalStateException("Invalid change-log chunk length");
      }
      chunks.put(number, bytes);
    }
    return bytes;
  }

  @Override
  public void close() {
    reader.close();
  }
}
