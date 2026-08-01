package io.sirix.service.json.serialize;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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

  /**
   * Chunk-growth policy shared by BOTH pipelines' buffers ({@link BufferedAppendable} chars,
   * {@link Utf8OutputSink} bytes): start at {@link #INITIAL_CAPACITY}, double up to
   * {@link #MAX_CAPACITY}, then flush downstream instead of growing. One tuning decision, one
   * place — 256 was measured against 1 KiB / 2 KiB / 4 KiB starts and won (see
   * {@code docs/HANDOFF_COMMIT_READ_PATH.md}); a retune must move both pipelines together.
   */
  int INITIAL_CAPACITY = 1 << 8;

  /** Ceiling of the doubling chunk — 8 KiB units per downstream write; see {@link #INITIAL_CAPACITY}. */
  int MAX_CAPACITY = 1 << 13;

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

  /**
   * Emit a stored string — an object key or a string value — as a quoted JSON string directly from
   * its UTF-8 bytes, when THIS sink can prove that safe; returns {@code false} (emitting nothing)
   * when it cannot, in which case the caller must take the escaping path.
   *
   * <p>The predicate is the sink's own, because what "safe" means differs per pipeline and putting
   * the choice at call sites let them drift apart (keys and values briefly gated on different
   * predicates in the same file). The byte sink accepts any escape-free UTF-8 run
   * ({@code !mayNeedJsonEscape}) and bulk-copies it — UTF-8 is already its wire form. The char sink
   * accepts plain ASCII ({@code isPlainAscii}, strictly stronger) and widens byte→char — for ASCII
   * the widening IS the UTF-8 decode, so this is a zero-allocation fast path on the char pipeline
   * too, where the escaping path costs a decoded String plus an escape-scan copy.
   *
   * @param utf8 the stored UTF-8 bytes of the string, without quotes
   * @return {@code true} if the quoted string was emitted, {@code false} if nothing was written
   */
  boolean tryEmitQuoted(byte[] utf8) throws IOException;

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
    public boolean tryEmitQuoted(final byte[] utf8) throws IOException {
      if (!JsonValueScan.isPlainAscii(utf8)) {
        return false;
      }
      out.append('"');
      out.appendAscii(utf8, 0, utf8.length);
      out.append('"');
      return true;
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }
  }

  /**
   * Byte pipeline — UTF-8 straight to an {@link OutputStream}, in chunks that grow per the shared
   * policy ({@link #INITIAL_CAPACITY}/{@link #MAX_CAPACITY}). A serializer is built per serialize
   * call, so a chunk allocated at its ceiling every time is pure waste on short documents; see
   * {@link BufferedAppendable} for the measurement that motivated growing instead.
   */
  final class Utf8OutputSink implements JsonOutputSink {

    private final OutputStream target;
    private byte[] buffer = new byte[INITIAL_CAPACITY];
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
      if (position == buffer.length) {
        growOrFlush();
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
        if (position == buffer.length) {
          growOrFlush();
        }
        final int n = Math.min(len - i, buffer.length - position);
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
        if (position == buffer.length) {
          growOrFlush();
        }
        final int n = Math.min(len - from, buffer.length - position);
        System.arraycopy(bytes, from, buffer, position, n);
        position += n;
        from += n;
      }
    }

    @Override
    public boolean tryEmitQuoted(final byte[] utf8) throws IOException {
      // Wider acceptance than the char sink: any escape-free UTF-8 run is already this sink's
      // wire form, multi-byte sequences included.
      if (JsonValueScan.mayNeedJsonEscape(utf8)) {
        return false;
      }
      ascii('"');
      utf8(utf8);
      ascii('"');
      return true;
    }

    @Override
    public void flush() throws IOException {
      drainPendingSurrogate();
      flushBuffer();
      target.flush();
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

    private void flushBuffer() throws IOException {
      if (position > 0) {
        target.write(buffer, 0, position);
        position = 0;
      }
    }
  }
}
