package io.sirix.query;

import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

/** One bounded buffer for columnar JSON, with allocation-free integral conversion. */
final class ColumnarJsonWriter {
  private static final int CAPACITY = 32 * 1024;
  private static final String HEX = "0123456789abcdef";
  private final Appendable out;
  private final char[] buffer = new char[CAPACITY];
  private int position;

  ColumnarJsonWriter(final Appendable out) {
    this.out = Objects.requireNonNull(out);
  }

  static boolean isBatchRecord(final Item item) {
    if (item instanceof ColumnarRecordSequence.Record record) {
      return record.isBatchWritable();
    }
    if (!(item instanceof ArrayObject record)) {
      return false;
    }
    for (int field = 0; field < record.len(); field++) {
      final Sequence value = record.value(field);
      if (value != null && value.getClass() != Int64.class && value.getClass() != Int32.class
          && value.getClass() != Str.class && value.getClass() != Bool.class && value.getClass() != Null.class) {
        return false;
      }
    }
    return true;
  }

  boolean record(final Item item) throws IOException {
    if (item instanceof ColumnarRecordSequence.Record record) {
      return record.append(this);
    }
    if (!isBatchRecord(item)) {
      return false;
    }
    final ArrayObject record = (ArrayObject) item;
    append('{');
    for (int field = 0; field < record.len(); field++) {
      if (field != 0) {
        append(',');
      }
      string(record.name(field).stringValue());
      append(':');
      final Sequence value = record.value(field);
      if (value instanceof Int64 number) {
        append(number.longValue());
      } else if (value instanceof Int32 number) {
        append(number.longValue());
      } else if (value instanceof Str string) {
        string(string.toString());
      } else if (value instanceof Bool bool) {
        append(bool.booleanValue()
            ? "true"
            : "false");
      } else {
        append("null");
      }
    }
    append('}');
    return true;
  }

  void append(final char value) throws IOException {
    if (position == buffer.length) {
      drain();
    }
    buffer[position++] = value;
  }

  void append(final String value) throws IOException {
    append(value, 0, value.length());
  }

  private void append(final String value, final int start, final int end) throws IOException {
    int from = start;
    while (from < end) {
      if (position == buffer.length) {
        drain();
      }
      final int count = Math.min(end - from, buffer.length - position);
      value.getChars(from, from + count, buffer, position);
      position += count;
      from += count;
    }
  }

  void append(final long value) throws IOException {
    if (buffer.length - position < 20) {
      drain();
    }
    final int end = position + 20;
    int at = end;
    // The negative domain includes MIN_VALUE; no overflow or temporary numeric string.
    long rest = value > 0
        ? -value
        : value;
    do {
      final long quotient = rest / 10;
      buffer[--at] = (char) ('0' - (rest - quotient * 10));
      rest = quotient;
    } while (rest != 0);
    if (value < 0) {
      buffer[--at] = '-';
    }
    final int length = end - at;
    if (at != position) {
      System.arraycopy(buffer, at, buffer, position, length);
    }
    position += length;
  }

  void string(final String value) throws IOException {
    if (value == null) {
      append("null");
      return;
    }
    append('"');
    int plainFrom = 0;
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c == '"' || c == '\\' || c < 0x20) {
        append(value, plainFrom, i);
        append('\\');
        switch (c) {
          case '"', '\\' -> append(c);
          case '\b' -> append('b');
          case '\f' -> append('f');
          case '\n' -> append('n');
          case '\r' -> append('r');
          case '\t' -> append('t');
          default -> {
            append("u00");
            append(HEX.charAt(c >>> 4));
            append(HEX.charAt(c & 15));
          }
        }
        plainFrom = i + 1;
      }
    }
    append(value, plainFrom, value.length());
    append('"');
  }

  void drain() throws IOException {
    if (position == 0) {
      return;
    }
    final int count = position;
    position = 0;
    if (out instanceof Writer writer) {
      writer.write(buffer, 0, count);
    } else if (out instanceof StringBuilder builder) {
      builder.append(buffer, 0, count);
    } else {
      out.append(new String(buffer, 0, count));
    }
  }
}
