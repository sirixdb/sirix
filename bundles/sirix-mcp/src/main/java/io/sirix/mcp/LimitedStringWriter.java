package io.sirix.mcp;

import java.io.Writer;

/**
 * A bounded serialization buffer; overflow aborts the value instead of exposing a partial value.
 */
final class LimitedStringWriter extends Writer {
  private final StringBuilder buffer;
  private int limit;

  LimitedStringWriter(final int limit) {
    if (limit < 0)
      throw new IllegalArgumentException("limit must not be negative");
    this.limit = limit;
    buffer = new StringBuilder(Math.min(limit, 4096));
  }

  void reset(final int limit) {
    if (limit < 0)
      throw new IllegalArgumentException("limit must not be negative");
    this.limit = limit;
    buffer.setLength(0);
  }

  int length() {
    return buffer.length();
  }

  @Override
  public void write(final char[] chars, final int offset, final int length) {
    if (length > limit - buffer.length())
      throw new LimitExceededException();
    buffer.append(chars, offset, length);
  }

  @Override
  public void write(final String value, final int offset, final int length) {
    if (length > limit - buffer.length())
      throw new LimitExceededException();
    buffer.append(value, offset, offset + length);
  }

  @Override
  public void write(final int character) {
    if (buffer.length() == limit)
      throw new LimitExceededException();
    buffer.append((char) character);
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}

  @Override
  public String toString() {
    return buffer.toString();
  }

  static final class LimitExceededException extends RuntimeException {
    private LimitExceededException() {
      super(null, null, false, false);
    }
  }
}
