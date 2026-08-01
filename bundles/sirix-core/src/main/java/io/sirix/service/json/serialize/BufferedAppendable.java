package io.sirix.service.json.serialize;

import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Unsynchronized chunking buffer in front of an arbitrary {@link Appendable}.
 *
 * <p>The serializers emit millions of tiny appends (single brackets, quotes, commas, short
 * keys). Pushing each through the target — typically a {@link java.io.StringWriter}, whose
 * backing {@link StringBuffer} takes a monitor on EVERY call — dominated serialization profiles.
 * This buffer batches them into one downstream {@code append} per chunk of at most 8 KiB chars.
 *
 * <p>The chunk starts small and doubles up to that ceiling as output accumulates
 * (the shared policy in {@link JsonOutputSink#INITIAL_CAPACITY}/{@link JsonOutputSink#MAX_CAPACITY}),
 * because a serializer is constructed per serialize call: a fixed max-size chunk made every call —
 * however short its document — allocate the ceiling, and on the small-document read path that
 * single array was the dominant allocation of the whole operation (measured: 94 % of all bytes
 * allocated while serializing a 1.2 KB document). Growth is amortized O(1) and stops at the
 * ceiling, so large documents reach the same chunk size as before after a handful of doublings.
 *
 * <p>Single-threaded by design (one serializer instance per call); {@link #flush()} must run
 * once after the final emit.
 */
public final class BufferedAppendable implements Appendable {

  private final Appendable target;
  private char[] buffer = new char[JsonOutputSink.INITIAL_CAPACITY];
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
      final int n = ensureRoom(end - from);
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
    Objects.requireNonNull(bytes, "bytes");
    Objects.checkFromIndexSize(offset, length, bytes.length);
    int from = offset;
    final int end = offset + length;
    while (from < end) {
      final int n = ensureRoom(end - from);
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
    ensureRoom(1);
    buffer[position++] = c;
    return this;
  }

  /**
   * Make at least one char of room (growing while below the ceiling, flushing at it) and return
   * how much of {@code remaining} fits in the chunk right now — the shared prologue of every
   * append loop.
   */
  private int ensureRoom(final int remaining) throws IOException {
    if (position == buffer.length) {
      final int capacity = buffer.length;
      if (capacity < JsonOutputSink.MAX_CAPACITY) {
        buffer = Arrays.copyOf(buffer, capacity << 1);
      } else {
        flushBuffer();
      }
    }
    return Math.min(remaining, buffer.length - position);
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
