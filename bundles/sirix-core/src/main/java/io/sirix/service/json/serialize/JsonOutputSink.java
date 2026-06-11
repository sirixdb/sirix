package io.sirix.service.json.serialize;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Output abstraction for the JSON serializer with two implementations:
 *
 * <ul>
 *   <li>{@link CharOutputSink} — the classic {@link Appendable}/Writer pipeline (chars all the
 *       way; the REST layer then encodes the resulting String to UTF-8 for the wire).</li>
 *   <li>{@link Utf8OutputSink} — a byte-oriented pipeline over an {@link OutputStream}: stored
 *       string values are ALREADY UTF-8 bytes, so escape-free values are bulk-copied straight to
 *       the output with no String construction and no char→byte re-encoding at all.</li>
 * </ul>
 */
interface JsonOutputSink extends Appendable {

  // Appendable bridge: lets the sink stand in wherever the legacy char pipeline expects an
  // Appendable (e.g. the JsonLimitedSerializer delegation) — everything still funnels through
  // one buffered output. Per the Appendable contract a null CharSequence appends "null".
  @Override
  default Appendable append(final CharSequence csq) throws IOException {
    text(csq == null ? "null" : csq.toString());
    return this;
  }

  @Override
  default Appendable append(final CharSequence csq, final int start, final int end) throws IOException {
    text((csq == null ? "null" : csq).subSequence(start, end).toString());
    return this;
  }

  @Override
  default Appendable append(final char c) throws IOException {
    if (c < 0x80) {
      ascii(c);
    } else {
      text(String.valueOf(c));
    }
    return this;
  }

  /** Append a single ASCII character (brackets, quotes, separators, digits). */
  void ascii(char c) throws IOException;

  /** Append a string (keys, escaped values, number/boolean lexical forms). */
  void text(String s) throws IOException;

  /**
   * Append verbatim UTF-8 bytes. Only called with bytes proven escape-free; the char sink decodes
   * (same result as the equivalent {@link #text(String)}), the byte sink bulk-copies.
   */
  void utf8(byte[] bytes) throws IOException;

  /** {@code true} when {@link #utf8(byte[])} is a zero-conversion fast path worth gating for. */
  boolean prefersRawUtf8();

  /** Flush any buffered output to the target. MUST run once after the final emit. */
  void flush() throws IOException;

  /** Classic char pipeline — buffered {@link Appendable}. */
  final class CharOutputSink implements JsonOutputSink {

    private final BufferedAppendable out;

    CharOutputSink(final Appendable target) {
      this.out = new BufferedAppendable(target);
    }

    @Override
    public void ascii(final char c) throws IOException {
      out.append(c);
    }

    @Override
    public void text(final String s) throws IOException {
      out.append(s);
    }

    @Override
    public void utf8(final byte[] bytes) throws IOException {
      out.append(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public boolean prefersRawUtf8() {
      return false;
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }
  }

  /** Byte pipeline — UTF-8 straight to an {@link OutputStream}, 8 KiB chunks. */
  final class Utf8OutputSink implements JsonOutputSink {

    private static final int CAPACITY = 1 << 13;

    private final OutputStream target;
    private final byte[] buffer = new byte[CAPACITY];
    private int position;

    /**
     * High surrogate buffered by {@link #append(char)} until its low half arrives. The default
     * bridge encoded each half separately via {@code String.valueOf(char)}, turning a surrogate
     * PAIR split across two {@code append(char)} calls into {@code ??} — the char pipeline emits
     * the correct 4-byte sequence for the same call pattern.
     */
    private char pendingHighSurrogate;

    Utf8OutputSink(final OutputStream target) {
      this.target = target;
    }

    @Override
    public Appendable append(final char c) throws IOException {
      if (pendingHighSurrogate != 0) {
        final char high = pendingHighSurrogate;
        pendingHighSurrogate = 0;
        if (Character.isLowSurrogate(c)) {
          text(String.valueOf(new char[] { high, c }));
          return this;
        }
        // Lone high surrogate — degrade exactly like the char pipeline's eventual UTF-8 encode.
        text(String.valueOf(high));
      }
      if (Character.isHighSurrogate(c)) {
        pendingHighSurrogate = c;
      } else if (c < 0x80) {
        ascii(c);
      } else {
        text(String.valueOf(c));
      }
      return this;
    }

    @Override
    public Appendable append(final CharSequence csq) throws IOException {
      drainPendingSurrogate();
      text(csq == null ? "null" : csq.toString());
      return this;
    }

    @Override
    public Appendable append(final CharSequence csq, final int start, final int end) throws IOException {
      drainPendingSurrogate();
      text((csq == null ? "null" : csq).subSequence(start, end).toString());
      return this;
    }

    private void drainPendingSurrogate() throws IOException {
      if (pendingHighSurrogate != 0) {
        final char high = pendingHighSurrogate;
        pendingHighSurrogate = 0;
        text(String.valueOf(high));
      }
    }

    @Override
    public void ascii(final char c) throws IOException {
      if (position == CAPACITY) {
        flushBuffer();
      }
      buffer[position++] = (byte) c;
    }

    @Override
    public void text(final String s) throws IOException {
      final int len = s.length();
      // Fast path: ASCII strings (separators, keys, number lexical forms) copy char→byte
      // directly into the buffer. The first non-ASCII char falls back to a one-shot UTF-8
      // encode of the remainder.
      int i = 0;
      while (i < len) {
        if (position == CAPACITY) {
          flushBuffer();
        }
        final int n = Math.min(len - i, CAPACITY - position);
        for (int k = 0; k < n; k++) {
          final char c = s.charAt(i + k);
          if (c >= 0x80) {
            position += k;
            utf8(s.substring(i + k).getBytes(StandardCharsets.UTF_8));
            return;
          }
          buffer[position + k] = (byte) c;
        }
        position += n;
        i += n;
      }
    }

    @Override
    public void utf8(final byte[] bytes) throws IOException {
      int from = 0;
      final int len = bytes.length;
      while (from < len) {
        if (position == CAPACITY) {
          flushBuffer();
        }
        final int n = Math.min(len - from, CAPACITY - position);
        System.arraycopy(bytes, from, buffer, position, n);
        position += n;
        from += n;
      }
    }

    @Override
    public boolean prefersRawUtf8() {
      return true;
    }

    @Override
    public void flush() throws IOException {
      drainPendingSurrogate();
      flushBuffer();
      target.flush();
    }

    private void flushBuffer() throws IOException {
      if (position > 0) {
        target.write(buffer, 0, position);
        position = 0;
      }
    }
  }
}
