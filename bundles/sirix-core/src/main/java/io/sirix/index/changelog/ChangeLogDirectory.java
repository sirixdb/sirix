package io.sirix.index.changelog;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Sorted per-declaration group directory. Ranges share the slab's existing row payloads. */
public final class ChangeLogDirectory {
  public record Grouping(int index, List<Integer> columns) {
    public Grouping {
      columns = List.copyOf(columns);
      if (index < 0 || columns.isEmpty() || columns.size() > 65535) {
        throw new IllegalArgumentException("Invalid change-log grouping");
      }
    }
  }

  public record Range(int offset, int length, int count, long firstRecord) {
    public Range {
      if (offset < 0 || length < 1 || count < 1 || firstRecord < 0) {
        throw new IllegalArgumentException("Invalid change-log range");
      }
    }
  }

  public static final class Groups {
    private final Grouping grouping;
    private final TreeMap<byte[], List<Range>> groups = new TreeMap<>(Arrays::compareUnsigned);
    private boolean supported = true;

    private Groups(final Grouping grouping) {
      this.grouping = grouping;
    }

    public Grouping grouping() {
      return grouping;
    }

    public boolean supported() {
      return supported;
    }

    public Iterable<byte[]> keys() {
      return groups.navigableKeySet();
    }

    public @Nullable List<Range> ranges(final byte[] key) {
      return groups.get(key);
    }

    private void add(final byte[] key, final Range range) {
      final List<Range> ranges = groups.computeIfAbsent(key, ignored -> new ArrayList<>(1));
      if (!ranges.isEmpty()) {
        final Range previous = ranges.getLast();
        if (previous.offset() + previous.length() == range.offset()) {
          ranges.set(ranges.size() - 1, new Range(previous.offset(), previous.length() + range.length(),
              previous.count() + range.count(), previous.firstRecord()));
          return;
        }
      }
      ranges.add(range);
    }
  }

  private final List<Groups> indexes;

  ChangeLogDirectory(final List<Grouping> groupings) {
    indexes = new ArrayList<>(groupings.size());
    for (final Grouping grouping : groupings) {
      indexes.add(new Groups(grouping));
    }
  }

  public @Nullable Groups groups(final int index) {
    for (final Groups groups : indexes) {
      if (groups.grouping.index() == index) {
        return groups;
      }
    }
    return null;
  }

  List<Grouping> groupings() {
    return indexes.stream().map(groups -> groups.grouping).toList();
  }

  void add(final ChangeLogSlab.AddressedEntry addressed, final int offset, final int length) {
    final ChangeLogSlab.Entry entry = addressed.entry();
    final Range range = new Range(offset, length, 1, entry.record());
    for (int index = 0; index < indexes.size(); index++) {
      final Groups groups = indexes.get(index);
      if (!groups.supported) {
        continue;
      }
      final byte[] before = addressed.before()[index];
      final byte[] after = addressed.after()[index];
      if ((entry.before() != null && before == null) || (entry.after() != null && after == null)) {
        groups.supported = false;
        groups.groups.clear();
        continue;
      }
      if (before != null) {
        groups.add(before, range);
      }
      if (after != null && !Arrays.equals(before, after)) {
        groups.add(after, range);
      }
    }
  }

  record Section(int index, int offset, int length) {
  }

  void write(final DataOutputStream out) throws IOException {
    final List<byte[]> encoded = new ArrayList<>(indexes.size());
    for (final Groups groups : indexes) {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      writeGroups(new DataOutputStream(bytes), groups);
      encoded.add(bytes.toByteArray());
    }
    out.writeInt(indexes.size());
    int offset = Math.addExact(4, Math.multiplyExact(12, indexes.size()));
    for (int index = 0; index < indexes.size(); index++) {
      final byte[] bytes = encoded.get(index);
      out.writeInt(indexes.get(index).grouping.index());
      out.writeInt(offset);
      out.writeInt(bytes.length);
      offset = Math.addExact(offset, bytes.length);
    }
    for (final byte[] bytes : encoded) {
      out.write(bytes);
    }
  }

  private static void writeGroups(final DataOutputStream out, final Groups groups) throws IOException {
    out.writeShort(groups.grouping.columns().size());
    for (final int column : groups.grouping.columns()) {
      out.writeShort(column);
    }
    out.writeBoolean(groups.supported);
    out.writeInt(groups.groups.size());
    byte[] previousKey = new byte[0];
    for (final Map.Entry<byte[], List<Range>> group : groups.groups.entrySet()) {
      final byte[] key = group.getKey();
      int prefix = Arrays.mismatch(previousKey, key);
      if (prefix < 0) {
        prefix = key.length;
      }
      ChangeLogSlab.unsigned(out, prefix);
      ChangeLogSlab.unsigned(out, key.length - prefix);
      out.write(key, prefix, key.length - prefix);
      previousKey = key;
      ChangeLogSlab.unsigned(out, group.getValue().size());
      int previous = 0;
      for (final Range range : group.getValue()) {
        ChangeLogSlab.unsigned(out, range.offset() - previous);
        ChangeLogSlab.unsigned(out, range.length());
        ChangeLogSlab.unsigned(out, range.count());
        ChangeLogSlab.unsigned(out, range.firstRecord());
        previous = range.offset() + range.length();
      }
    }
  }

  static List<Section> readSections(final DataInputStream in, final int length) throws IOException {
    final int count = ChangeLogSlab.bounded(in.readInt(), in.available() / 12);
    final List<Section> sections = new ArrayList<>(count);
    int previousIndex = -1;
    int expectedOffset = 4 + 12 * count;
    for (int index = 0; index < count; index++) {
      final int id = in.readInt();
      final int offset = in.readInt();
      final int size = ChangeLogSlab.bounded(in.readInt(), length - expectedOffset);
      if (id <= previousIndex || offset != expectedOffset || size < 9) {
        throw new IllegalStateException("Invalid change-log grouping section");
      }
      sections.add(new Section(id, offset, size));
      expectedOffset += size;
      previousIndex = id;
    }
    if (expectedOffset != length) {
      throw new IllegalStateException("Invalid change-log directory length");
    }
    return sections;
  }

  static ChangeLogDirectory read(final DataInputStream in, final int columns, final int firstEntry,
      final int directoryOffset) throws IOException {
    final List<Section> sections = readSections(in, in.available());
    final ChangeLogDirectory result = new ChangeLogDirectory(List.of());
    for (final Section section : sections) {
      try (DataInputStream group = new DataInputStream(new ByteArrayInputStream(in.readNBytes(section.length())))) {
        result.indexes.add(readGroups(group, section.index(), columns, firstEntry, directoryOffset));
      }
    }
    return result;
  }

  static Groups readGroups(final DataInputStream in, final int index, final int columns, final int firstEntry,
      final int directoryOffset) throws IOException {
    final int count = ChangeLogSlab.bounded(in.readUnsignedShort(), columns);
    final List<Integer> selected = new ArrayList<>(count);
    for (int column = 0; column < count; column++) {
      selected.add(ChangeLogSlab.bounded(in.readUnsignedShort(), columns - 1));
    }
    final Groups groups = new Groups(new Grouping(index, selected));
    groups.supported = in.readBoolean();
    final int keys = ChangeLogSlab.bounded(in.readInt(), in.available() / 3);
    byte[] previousKey = new byte[0];
    for (int key = 0; key < keys; key++) {
      final int prefix = ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), previousKey.length);
      final int suffix = ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), Math.min(2048 - prefix, in.available()));
      final byte[] bytes = Arrays.copyOf(previousKey, prefix + suffix);
      in.readFully(bytes, prefix, suffix);
      if (!groups.supported || bytes.length < 1 || bytes[0] != 1
          || (key != 0 && Arrays.compareUnsigned(previousKey, bytes) >= 0)) {
        throw new IllegalStateException("Invalid change-log group directory order");
      }
      previousKey = bytes;
      final int ranges = ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), in.available() / 4);
      if (ranges == 0) {
        throw new IllegalStateException("Empty change-log group ranges");
      }
      final List<Range> values = new ArrayList<>(ranges);
      int previous = 0;
      for (int range = 0; range < ranges; range++) {
        final int start =
            Math.addExact(previous, ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), directoryOffset));
        final int bytesInRange = ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), directoryOffset - start);
        final int entries = ChangeLogSlab.bounded(ChangeLogSlab.readUnsigned(in), bytesInRange);
        final long firstRecord = ChangeLogSlab.readUnsigned(in);
        if (start < firstEntry) {
          throw new IllegalStateException("Change-log range overlaps metadata");
        }
        values.add(new Range(start, bytesInRange, entries, firstRecord));
        previous = start + bytesInRange;
      }
      groups.groups.put(bytes, List.copyOf(values));
    }
    if (in.available() != 0) {
      throw new IllegalStateException("Trailing change-log grouping bytes");
    }
    return groups;
  }
}
