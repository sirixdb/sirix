package io.sirix.service.json.serialize;

import java.io.IOException;

/**
 * Unsynchronized chunking buffer in front of an arbitrary {@link Appendable}.
 *
 * <p>The serializers emit millions of tiny appends (single brackets, quotes, commas, short
 * keys). Pushing each through the target — typically a {@link java.io.StringWriter}, whose
 * backing {@link StringBuffer} takes a monitor on EVERY call — dominated serialization profiles.
 * This buffer batches them into one downstream {@code append} per {@value #CAPACITY}-char chunk.
 *
 * <p>Single-threaded by design (one serializer instance per call); {@link #flush()} must run
 * once after the final emit.
 */
public final class BufferedAppendable implements Appendable {

  private static final int CAPACITY = 1 << 13; // 8 KiB chars per downstream append

  private final Appendable target;
  private final char[] buffer = new char[CAPACITY];
  private int position;

  public BufferedAppendable(final Appendable target) {
    this.target = target;
  }

  @Override
  public Appendable append(final CharSequence csq) throws IOException {
    return append(csq, 0, csq.length());
  }

  @Override
  public Appendable append(final CharSequence csq, final int start, final int end) throws IOException {
    int from = start;
    while (from < end) {
      if (position == CAPACITY) {
        flushBuffer();
      }
      final int n = Math.min(end - from, CAPACITY - position);
      if (csq instanceof String s) {
        // Bulk copy — String.getChars beats a char-by-char loop for the common String case.
        s.getChars(from, from + n, buffer, position);
        position += n;
        from += n;
      } else {
        for (int i = 0; i < n; i++) {
          buffer[position++] = csq.charAt(from++);
        }
      }
    }
    return this;
  }

  @Override
  public Appendable append(final char c) throws IOException {
    if (position == CAPACITY) {
      flushBuffer();
    }
    buffer[position++] = c;
    return this;
  }

  /** Flushes the buffered tail to the target. MUST be called once after the final emit. */
  public void flush() throws IOException {
    flushBuffer();
    if (target instanceof java.io.Writer writer) {
      writer.flush();
    }
  }

  private void flushBuffer() throws IOException {
    if (position > 0) {
      if (target instanceof java.io.Writer writer) {
        writer.write(buffer, 0, position); // no intermediate String for Writer targets
      } else {
        target.append(java.nio.CharBuffer.wrap(buffer, 0, position));
      }
      position = 0;
    }
  }
}
