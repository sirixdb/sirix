package io.sirix.index.changelog;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Format-1 record changes. Slabs are self-contained; dictionary IDs refer only to this slab. */
public final class ChangeLogSlab {
  public static final int MAGIC = 0x53434c47;
  public static final int FORMAT = 1;
  public static final byte LONG = 1;
  public static final byte STRING = 2;
  public static final byte BOOLEAN = 3;
  public static final byte DOUBLE = 4;
  public static final byte STRING_SET = 5;
  public static final byte TIMESTAMP = 6;
  public static final byte DATE = 7;
  public static final byte PRESENT = 1;
  public static final byte UNREPRESENTABLE = 2;
  public static final byte NON_INTEGRAL = 4;
  public static final byte NON_DOUBLE = 8;

  /** Primitive lanes and row-local provenance. Owned by the capture, never borrowed from a leaf. */
  public static final class Row {
    public final boolean object;
    public final byte[] flags;
    public final long[] numbers;
    public final Object[] text;

    public Row(final boolean object, final int columns) {
      if (columns < 1 || columns > 65535) {
        throw new IllegalArgumentException("Invalid change-log column count");
      }
      this.object = object;
      flags = new byte[columns];
      numbers = new long[columns];
      text = new Object[columns];
    }

    public Row copy() {
      final Row copy = new Row(object, flags.length);
      System.arraycopy(flags, 0, copy.flags, 0, flags.length);
      System.arraycopy(numbers, 0, copy.numbers, 0, flags.length);
      System.arraycopy(text, 0, copy.text, 0, flags.length);
      return copy;
    }

    public boolean present(final int column) {
      return (flags[column] & PRESENT) != 0;
    }

    public boolean supported(final int column) {
      return (flags[column] & (UNREPRESENTABLE | NON_INTEGRAL)) == 0;
    }

    public boolean exceptional() {
      if (!object) {
        return true;
      }
      for (final byte flag : flags) {
        if ((flag & ~PRESENT) != 0) {
          return true;
        }
      }
      return false;
    }
  }

  // Internal serialization carrier; masks are primitive lanes rather than value-equality keys.
  @SuppressWarnings("ArrayRecordComponent")
  public record Entry(long record, long[] changed, @Nullable Row before, @Nullable Row after) {
    public Entry {
      if (record < 0) {
        throw new IllegalArgumentException("Negative change-log record");
      }
      Objects.requireNonNull(changed);
    }
  }

  private final int projection;
  private final int revision;
  private final int baseRevision;
  private final byte[] kinds;
  private final List<Entry> entries;

  public ChangeLogSlab(final int projection, final int revision, final int baseRevision, final byte[] kinds,
      final List<Entry> entries) {
    if (projection < 0 || revision < 1 || baseRevision < 0 || baseRevision >= revision || kinds.length < 1
        || kinds.length > 65535) {
      throw new IllegalArgumentException("Invalid change-log header");
    }
    for (final byte kind : kinds) {
      if (kind < LONG || kind > DATE) {
        throw new IllegalArgumentException("Unknown change-log cell kind");
      }
    }
    this.projection = projection;
    this.revision = revision;
    this.baseRevision = baseRevision;
    this.kinds = kinds.clone();
    this.entries = List.copyOf(entries);
    long previous = -1;
    for (final Entry entry : entries) {
      if (entry.record <= previous || entry.changed.length != (kinds.length + 63) / 64
          || (entry.before != null && entry.before.flags.length != kinds.length)
          || (entry.after != null && entry.after.flags.length != kinds.length)) {
        throw new IllegalArgumentException("Invalid change-log entry shape or ordering");
      }
      previous = entry.record;
    }
  }

  public int projection() {
    return projection;
  }

  public int revision() {
    return revision;
  }

  public int baseRevision() {
    return baseRevision;
  }

  public byte[] kinds() {
    return kinds.clone();
  }

  public List<Entry> entries() {
    return entries;
  }

  public byte[] encode() {
    final Map<String, Integer> dictionary = new LinkedHashMap<>();
    for (final Entry entry : entries) {
      collect(dictionary, entry.before);
      collect(dictionary, entry.after);
    }
    try {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final DataOutputStream out = new DataOutputStream(bytes);
      out.writeInt(MAGIC);
      out.writeInt(FORMAT);
      out.writeInt(projection);
      out.writeInt(revision);
      out.writeInt(baseRevision);
      out.writeInt(entries.size());
      out.writeShort(kinds.length);
      out.write(kinds);
      out.writeInt(dictionary.size());
      for (final String value : dictionary.keySet()) {
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        unsigned(out, utf8.length);
        out.write(utf8);
      }
      long previous = 0;
      for (final Entry entry : entries) {
        unsigned(out, entry.record - previous);
        previous = entry.record;
        for (final long word : entry.changed) {
          out.writeLong(word);
        }
        writeRow(out, entry.before, dictionary);
        writeRow(out, entry.after, dictionary);
      }
      return bytes.toByteArray();
    } catch (final IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private void collect(final Map<String, Integer> dictionary, final @Nullable Row row) {
    if (row == null) {
      return;
    }
    for (int column = 0; column < kinds.length; column++) {
      if (!row.present(column) || (row.flags[column] & UNREPRESENTABLE) != 0) {
        continue;
      }
      if (kinds[column] == STRING) {
        dictionary.computeIfAbsent((String) row.text[column], ignored -> dictionary.size());
      } else if (kinds[column] == STRING_SET) {
        for (final String value : (String[]) row.text[column]) {
          dictionary.computeIfAbsent(value, ignored -> dictionary.size());
        }
      }
    }
  }

  private void writeRow(final DataOutputStream out, final @Nullable Row row, final Map<String, Integer> dictionary)
      throws IOException {
    out.writeByte(row == null
        ? 0
        : row.object
            ? 1
            : 2);
    if (row == null) {
      return;
    }
    for (int column = 0; column < kinds.length; column++) {
      final byte flag = row.flags[column];
      if ((flag & ~15) != 0) {
        throw new IllegalArgumentException("Unknown change-log cell flags");
      }
      out.writeByte(flag);
      if ((flag & PRESENT) == 0 || ((flag & UNREPRESENTABLE) != 0 && kinds[column] != TIMESTAMP)) {
        continue;
      }
      switch (kinds[column]) {
        case STRING -> unsigned(out, Objects.requireNonNull(dictionary.get((String) row.text[column])));
        case STRING_SET -> {
          final String[] values = (String[]) row.text[column];
          unsigned(out, values.length);
          for (final String value : values) {
            unsigned(out, Objects.requireNonNull(dictionary.get(value)));
          }
        }
        default -> {
          final long value = row.numbers[column];
          unsigned(out, (value << 1) ^ (value >> 63));
        }
      }
    }
  }

  public static ChangeLogSlab decode(final byte[] bytes) {
    try {
      final DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
      if (in.readInt() != MAGIC || in.readInt() != FORMAT) {
        throw new IllegalStateException("Unsupported change-log slab format");
      }
      final int projection = in.readInt();
      final int revision = in.readInt();
      final int base = in.readInt();
      final int count = bounded(in.readInt(), in.available());
      final int columns = in.readUnsignedShort();
      final byte[] kinds = in.readNBytes(columns);
      if (kinds.length != columns) {
        throw new IllegalStateException("Truncated change-log schema");
      }
      final int dictionarySize = bounded(in.readInt(), in.available());
      final String[] dictionary = new String[dictionarySize];
      for (int i = 0; i < dictionary.length; i++) {
        final int length = bounded(readUnsigned(in), in.available());
        dictionary[i] = new String(in.readNBytes(length), StandardCharsets.UTF_8);
      }
      final List<Entry> entries = new ArrayList<>(count);
      long record = 0;
      for (int i = 0; i < count; i++) {
        record = Math.addExact(record, readUnsigned(in));
        final long[] changed = new long[(columns + 63) / 64];
        for (int word = 0; word < changed.length; word++) {
          changed[word] = in.readLong();
        }
        entries.add(new Entry(record, changed, readRow(in, kinds, dictionary), readRow(in, kinds, dictionary)));
      }
      if (in.available() != 0) {
        throw new IllegalStateException("Trailing change-log bytes");
      }
      return new ChangeLogSlab(projection, revision, base, kinds, entries);
    } catch (final IOException | IllegalArgumentException | ArithmeticException malformed) {
      throw new IllegalStateException("Malformed change-log slab", malformed);
    }
  }

  private static @Nullable Row readRow(final DataInputStream in, final byte[] kinds, final String[] dictionary)
      throws IOException {
    final int kind = in.readUnsignedByte();
    if (kind == 0) {
      return null;
    }
    if (kind > 2) {
      throw new IllegalStateException("Unknown change-log record kind");
    }
    final Row row = new Row(kind == 1, kinds.length);
    for (int column = 0; column < kinds.length; column++) {
      final byte flags = in.readByte();
      if ((flags & ~15) != 0) {
        throw new IllegalStateException("Unknown change-log cell flags");
      }
      row.flags[column] = flags;
      if ((flags & PRESENT) == 0 || ((flags & UNREPRESENTABLE) != 0 && kinds[column] != TIMESTAMP)) {
        continue;
      }
      switch (kinds[column]) {
        case STRING -> row.text[column] = dictionary[bounded(readUnsigned(in), dictionary.length - 1)];
        case STRING_SET -> {
          final String[] values = new String[bounded(readUnsigned(in), in.available())];
          for (int i = 0; i < values.length; i++) {
            values[i] = dictionary[bounded(readUnsigned(in), dictionary.length - 1)];
          }
          row.text[column] = values;
        }
        default -> {
          final long value = readUnsigned(in);
          row.numbers[column] = (value >>> 1) ^ -(value & 1);
        }
      }
    }
    return row;
  }

  private static int bounded(final long value, final int limit) {
    if (value < 0 || value > limit) {
      throw new IllegalStateException("Invalid change-log length or reference");
    }
    return (int) value;
  }

  private static void unsigned(final DataOutputStream out, long value) throws IOException {
    while ((value & ~127L) != 0) {
      out.writeByte(((int) value & 127) | 128);
      value >>>= 7;
    }
    out.writeByte((int) value);
  }

  private static long readUnsigned(final DataInputStream in) throws IOException {
    long value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      final int next = in.readUnsignedByte();
      if (shift == 63 && (next & ~1) != 0) {
        throw new IllegalStateException("Change-log varint overflow");
      }
      value |= (long) (next & 127) << shift;
      if ((next & 128) == 0) {
        if (shift != 0 && next == 0) {
          throw new IllegalStateException("Noncanonical change-log varint");
        }
        return value;
      }
    }
    throw new IllegalStateException("Truncated change-log varint");
  }
}
