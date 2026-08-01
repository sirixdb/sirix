package io.sirix.service.json.serialize;

import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;
import java.util.Arrays;

/**
 * Unsynchronized chunking buffer in front of an arbitrary {@link Appendable}.
 *
 * <p>The serializers emit millions of tiny appends (single brackets, quotes, commas, short
 * keys). Pushing each through the target — typically a {@link java.io.StringWriter}, whose
 * backing {@link StringBuffer} takes a monitor on EVERY call — dominated serialization profiles.
 * This buffer batches them into one downstream {@code append} per chunk of at most
 * {@value #MAX_CAPACITY} chars.
 *
 * <p>The chunk starts at {@value #INITIAL_CAPACITY} chars and doubles up to that ceiling as
 * output accumulates, because a serializer is constructed per serialize call: a fixed
 * max-size chunk made every call — however short its document — allocate the ceiling, and on the
 * small-document read path that single array was the dominant allocation of the whole operation
 * (measured: 94 % of all bytes allocated while serializing a 1.2 KB document). Growth is
 * amortized O(1) and stops at the ceiling, so large documents reach the same chunk size as before
 * after a handful of doublings.
 *
 * <p>Single-threaded by design (one serializer instance per call); {@link #flush()} must run
 * once after the final emit.
 */
public final class BufferedAppendable implements Appendable {

  private static final int INITIAL_CAPACITY = 1 << 8;  // 256 chars — covers a short document whole
  private static final int MAX_CAPACITY = 1 << 13;     // 8 KiB chars per downstream append

  private final Appendable target;
  private char[] buffer = new char[INITIAL_CAPACITY];
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
      if (position == buffer.length) {
        growOrFlush();
      }
      final int n = Math.min(end - from, buffer.length - position);
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

  /**
   * Append bytes that the caller has proven to be plain ASCII, widening them straight into the
   * chunk. For ASCII the widening IS the UTF-8 decode, so this is exactly the result of
   * {@code append(new String(bytes, UTF_8))} — without the intermediate String.
   *
   * @param bytes ASCII bytes to append
   * @param offset index of the first byte to append
   * @param length number of bytes to append
   */
  public void appendAscii(final byte[] bytes, final int offset, final int length) throws IOException {
    int from = offset;
    final int end = offset + length;
    while (from < end) {
      if (position == buffer.length) {
        growOrFlush();
      }
      final int n = Math.min(end - from, buffer.length - position);
      final char[] buf = buffer;
      final int pos = position;
      // Masking makes this a zero-extending widen rather than a sign-extending one, which is both
      // the correct decode for ASCII and the shape C2 turns into a single vector widen.
      for (int i = 0; i < n; i++) {
        buf[pos + i] = (char) (bytes[from + i] & 0xFF);
      }
      position = pos + n;
      from += n;
    }
  }

  @Override
  public Appendable append(final char c) throws IOException {
    if (position == buffer.length) {
      growOrFlush();
    }
    buffer[position++] = c;
    return this;
  }

  /** Double the chunk while it is below the ceiling, else hand it downstream and start over. */
  private void growOrFlush() throws IOException {
    final int capacity = buffer.length;
    if (capacity < MAX_CAPACITY) {
      buffer = Arrays.copyOf(buffer, capacity << 1);
    } else {
      flushBuffer();
    }
  }

  /** Flushes the buffered tail to the target. MUST be called once after the final emit. */
  public void flush() throws IOException {
    flushBuffer();
    if (target instanceof Writer writer) {
      writer.flush();
    }
  }

  private void flushBuffer() throws IOException {
    if (position > 0) {
      if (target instanceof Writer writer) {
        writer.write(buffer, 0, position); // no intermediate String for Writer targets
      } else {
        target.append(CharBuffer.wrap(buffer, 0, position));
      }
      position = 0;
    }
  }
}
