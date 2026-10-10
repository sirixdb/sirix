package io.sirix.query;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.AbstractObject;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Owned output columns plus a row permutation. Iteration exposes ordinary mutable JDM objects;
 * fields are materialized only when a consumer reads or modifies them. Serializers can instead
 * append unmaterialized rows directly. Primitive columns retain no transaction or projection lease.
 */
public final class ColumnarRecordSequence extends AbstractSequence {
  private final QNm[] names;
  private final String[] prefixes;
  private final Column[] columns;
  private final int[] rows;
  private final Record[] records;
  private final boolean primitiveColumns;
  private final IntNumeric count;
  private final IntNumeric width;

  /** A column owns its values. Factory methods copy caller arrays. */
  public sealed interface Column permits LongColumn, StringColumn, SequenceColumn {
    int size();

    Sequence value(int row);
  }

  private record LongColumn(long[] values, boolean[] present) implements Column {
    public int size() {
      return values.length;
    }

    public Sequence value(final int row) {
      return present[row]
          ? new Int64(values[row])
          : null;
    }
  }

  private record StringColumn(String[] values) implements Column {
    public int size() {
      return values.length;
    }

    public Sequence value(final int row) {
      return values[row] == null
          ? null
          : new Str(values[row]);
    }
  }

  private record SequenceColumn(Sequence[] values) implements Column {
    public int size() {
      return values.length;
    }

    public Sequence value(final int row) {
      return values[row];
    }
  }

  public static Column longs(final long[] values, final boolean[] present) {
    Objects.requireNonNull(values);
    Objects.requireNonNull(present);
    if (values.length != present.length) {
      throw new IllegalArgumentException("Value and presence column lengths differ");
    }
    return new LongColumn(values.clone(), present.clone());
  }

  public static Column strings(final String[] values) {
    return new StringColumn(Objects.requireNonNull(values).clone());
  }

  /** Other JDM values are retained by reference; the caller owns their lifetime. */
  public static Column sequences(final Sequence[] values) {
    return new SequenceColumn(Objects.requireNonNull(values).clone());
  }

  public ColumnarRecordSequence(final QNm[] names, final Column[] columns, final int[] rows) {
    this.names = Objects.requireNonNull(names).clone();
    this.columns = Objects.requireNonNull(columns).clone();
    this.rows = Objects.requireNonNull(rows).clone();
    if (names.length != columns.length) {
      throw new IllegalArgumentException("Field and column counts differ");
    }
    final int length = columns.length == 0
        ? 0
        : Objects.requireNonNull(columns[0]).size();
    prefixes = new String[names.length];
    boolean primitive = true;
    for (int i = 0; i < names.length; i++) {
      Objects.requireNonNull(names[i]);
      if (Objects.requireNonNull(columns[i]).size() != length) {
        throw new IllegalArgumentException("Column lengths differ");
      }
      primitive &= !(columns[i] instanceof SequenceColumn);
      final StringBuilder prefix = new StringBuilder();
      appendString(prefix, names[i].stringValue());
      prefixes[i] = prefix.append(':').toString();
    }
    for (final int row : rows) {
      if (row < 0 || (columns.length != 0 && row >= length)) {
        throw new IllegalArgumentException("Row outside columns: " + row);
      }
    }
    records = new Record[rows.length];
    primitiveColumns = primitive;
    count = new Int32(rows.length);
    width = new Int32(names.length);
  }

  @Override
  public IntNumeric size() {
    return count;
  }

  @Override
  public IntNumeric knownSize() {
    return size();
  }

  @Override
  public boolean isRepeatable() {
    return true;
  }

  @Override
  public Item get(final IntNumeric position) {
    Objects.requireNonNull(position);
    return position.cmp(Int32.ZERO) <= 0 || position.cmp(size()) > 0
        ? null
        : record(position.intValue() - 1);
  }

  @Override
  public Iter iterate() {
    return new BaseIter() {
      private int position;

      public Item next() {
        return position < rows.length
            ? record(position++)
            : null;
      }

      public void close() {}
    };
  }

  private Record record(final int position) {
    Record record = records[position];
    if (record == null) {
      record = new Record(rows[position]);
      records[position] = record;
    }
    return record;
  }

  private boolean appendRow(final ColumnarJsonWriter buffer, final int row) throws IOException {
    if (!primitiveColumns) {
      return false;
    }
    buffer.append('{');
    for (int field = 0; field < columns.length; field++) {
      if (field != 0) {
        buffer.append(',');
      }
      buffer.append(prefixes[field]);
      if (columns[field] instanceof LongColumn column) {
        if (column.present[row]) {
          buffer.append(column.values[row]);
        } else {
          buffer.append("null");
        }
      } else {
        buffer.string(((StringColumn) columns[field]).values[row]);
      }
    }
    buffer.append('}');
    return true;
  }

  /** Appends exactly Brackit's JSON string spelling, including short control escapes. */
  private static void appendString(final StringBuilder buffer, final String value) {
    if (value == null) {
      buffer.append("null");
      return;
    }
    buffer.append('"');
    int start = 0;
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c == '"' || c == '\\' || c < 0x20) {
        buffer.append(value, start, i).append('\\');
        switch (c) {
          case '"', '\\' -> buffer.append(c);
          case '\b' -> buffer.append('b');
          case '\f' -> buffer.append('f');
          case '\n' -> buffer.append('n');
          case '\r' -> buffer.append('r');
          case '\t' -> buffer.append('t');
          default ->
            buffer.append("u00").append("0123456789abcdef".charAt(c >>> 4)).append("0123456789abcdef".charAt(c & 15));
        }
        start = i + 1;
      }
    }
    buffer.append(value, start, value.length()).append('"');
  }

  /** A normal JDM record, with copy-on-access materialization and persistent mutation semantics. */
  final class Record extends AbstractObject {
    private final int row;
    private ArrayObject materialized;

    private Record(final int row) {
      this.row = row;
    }

    boolean isBatchWritable() {
      return materialized == null
          ? primitiveColumns
          : ColumnarJsonWriter.isBatchRecord(materialized);
    }

    boolean append(final ColumnarJsonWriter buffer) throws IOException {
      return materialized == null
          ? appendRow(buffer, row)
          : buffer.record(materialized);
    }

    private ArrayObject materialize() {
      if (materialized == null) {
        final Sequence[] values = new Sequence[columns.length];
        for (int i = 0; i < columns.length; i++) {
          values[i] = columns[i].value(row);
        }
        materialized = new ArrayObject(names, values);
      }
      return materialized;
    }

    public Object replace(final QNm field, final Sequence value) {
      materialize().replace(field, value);
      return this;
    }

    public Object rename(final QNm field, final QNm name) {
      materialize().rename(field, name);
      return this;
    }

    public Object insert(final QNm field, final Sequence value) {
      materialize().insert(field, value);
      return this;
    }

    public Object remove(final QNm field) {
      materialize().remove(field);
      return this;
    }

    public Object remove(final IntNumeric index) {
      materialize().remove(index);
      return this;
    }

    public Object remove(final int index) {
      materialize().remove(index);
      return this;
    }

    public Sequence get(final QNm field) {
      return materialize().get(field);
    }

    public Sequence value(final IntNumeric index) {
      return materialize().value(index);
    }

    public Sequence value(final int index) {
      return materialize().value(index);
    }

    public Array names() {
      return materialize().names();
    }

    public Array values() {
      return materialize().values();
    }

    public QNm name(final IntNumeric index) {
      return materialize().name(index);
    }

    public QNm name(final int index) {
      return materialized == null && index >= 0 && index < names.length
          ? names[index]
          : materialize().name(index);
    }

    public IntNumeric length() {
      return materialized == null
          ? width
          : materialized.length();
    }

    public int len() {
      return materialized == null
          ? names.length
          : materialized.len();
    }
  }

  /** Accumulates primitive joined output without allocating field items or record objects. */
  public static final class Builder {
    private final QNm[] names;
    private final boolean[] stringColumns;
    private final long[][] longs;
    private final boolean[][] present;
    private final String[][] strings;
    private int capacity;
    private int size;

    public Builder(final QNm[] names, final boolean[] stringColumns, final int initialCapacity) {
      this.names = Objects.requireNonNull(names).clone();
      this.stringColumns = Objects.requireNonNull(stringColumns).clone();
      if (names.length != stringColumns.length || initialCapacity < 0) {
        throw new IllegalArgumentException("Invalid column builder dimensions");
      }
      capacity = Math.max(16, initialCapacity);
      longs = new long[names.length][];
      present = new boolean[names.length][];
      strings = new String[names.length][];
      for (int i = 0; i < names.length; i++) {
        Objects.requireNonNull(names[i]);
        if (stringColumns[i]) {
          strings[i] = new String[capacity];
        } else {
          longs[i] = new long[capacity];
          present[i] = new boolean[capacity];
        }
      }
    }

    public int addRow() {
      if (size == capacity) {
        capacity = Math.addExact(capacity, Math.max(1, capacity >>> 1));
        for (int i = 0; i < names.length; i++) {
          if (stringColumns[i]) {
            strings[i] = Arrays.copyOf(strings[i], capacity);
          } else {
            longs[i] = Arrays.copyOf(longs[i], capacity);
            present[i] = Arrays.copyOf(present[i], capacity);
          }
        }
      }
      return size++;
    }

    public void setLong(final int field, final int row, final long value) {
      Objects.checkIndex(row, size);
      Objects.checkIndex(field, names.length);
      if (stringColumns[field]) {
        throw new IllegalArgumentException("Field is a string column");
      }
      longs[field][row] = value;
      present[field][row] = true;
    }

    public void setString(final int field, final int row, final String value) {
      Objects.checkIndex(row, size);
      Objects.checkIndex(field, names.length);
      if (!stringColumns[field]) {
        throw new IllegalArgumentException("Field is a long column");
      }
      strings[field][row] = value;
    }

    /** Brackit's Ordering still owns comparisons; materialize only requested sort keys. */
    public Sequence value(final int field, final int row) {
      Objects.checkIndex(row, size);
      Objects.checkIndex(field, names.length);
      return stringColumns[field]
          ? (strings[field][row] == null
              ? null
              : new Str(strings[field][row]))
          : (present[field][row]
              ? new Int64(longs[field][row])
              : null);
    }

    public ColumnarRecordSequence build(final int[] rowOrder) {
      final Column[] columns = new Column[names.length];
      for (int i = 0; i < columns.length; i++) {
        columns[i] = stringColumns[i]
            ? new StringColumn(Arrays.copyOf(strings[i], size))
            : new LongColumn(Arrays.copyOf(longs[i], size), Arrays.copyOf(present[i], size));
      }
      return new ColumnarRecordSequence(names, columns, rowOrder);
    }
  }
}
