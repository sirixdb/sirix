package io.sirix.query.bench.clickbench;

import java.io.BufferedReader;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

/**
 * Opens a ClickBench {@code hits} record stream as a {@link Reader} over a single JSON array, which
 * is what SirixDB's shredder consumes.
 *
 * <p>
 * Three source spellings are accepted:
 * <ul>
 * <li>{@code generate:<rows>[:seed]} — the offline synthetic generator, so the port runs without
 * the 14 GB download;</li>
 * <li>a path to a {@code .json} file holding a JSON array (what {@code prepare-data.sh} produces
 * from {@code hits.parquet});</li>
 * <li>a path to a JSON-lines file — one object per line, the shape of the official
 * {@code hits.json.gz} — which is adapted to an array on the fly.</li>
 * </ul>
 * Any path ending in {@code .gz} is decompressed transparently.
 */
public final class ClickBenchSource {

  /** Read buffer for file sources; large because the shredder pulls small chunks. */
  private static final int BUFFER_BYTES = 1 << 20;

  private ClickBenchSource() {
    throw new AssertionError("no instances");
  }

  /**
   * @param spec one of the three source spellings documented on this class
   * @return a reader over a JSON array of hit objects; the caller closes it
   * @throws IOException if the source cannot be opened
   */
  public static Reader open(final String spec) throws IOException {
    if (spec == null || spec.isBlank()) {
      throw new IllegalArgumentException("source spec must not be blank");
    }
    if (spec.startsWith("generate:")) {
      final String[] parts = spec.split(":");
      if (parts.length < 2 || parts.length > 3) {
        throw new IllegalArgumentException("expected generate:<rows>[:seed], got: " + spec);
      }
      final long rows = Long.parseLong(parts[1]);
      if (rows <= 0) {
        throw new IllegalArgumentException("row count must be positive: " + rows);
      }
      final long seed = parts.length == 3
          ? Long.parseLong(parts[2])
          : 42L;
      return new ClickBenchHitsGenerator(0L, rows, seed);
    }
    final Path path = Path.of(spec);
    if (!Files.isReadable(path)) {
      throw new IOException("hits source is not readable: " + path.toAbsolutePath());
    }
    final InputStream in = Files.newInputStream(path);
    final InputStream decoded = spec.toLowerCase(Locale.ROOT).endsWith(".gz")
        ? new GZIPInputStream(in, BUFFER_BYTES)
        : in;
    final PushbackReader reader = new PushbackReader(
        new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), BUFFER_BYTES), PEEK_CHARS);
    return isJsonArray(reader)
        ? reader
        : new JsonLinesAsArrayReader(reader);
  }

  /** How far {@link #isJsonArray} may look ahead for the first non-whitespace character. */
  private static final int PEEK_CHARS = 16;

  /**
   * Peeks past leading whitespace for the {@code '['} that distinguishes a JSON array file from a
   * JSON-lines file, pushing everything it consumed back so the returned reader is positioned at the
   * start of the stream either way.
   */
  private static boolean isJsonArray(final PushbackReader pushback) throws IOException {
    final char[] seen = new char[PEEK_CHARS];
    int consumed = 0;
    int c = -1;
    while (consumed < seen.length && (c = pushback.read()) != -1) {
      seen[consumed++] = (char) c;
      if (!Character.isWhitespace(c)) {
        break;
      }
    }
    if (consumed > 0) {
      pushback.unread(seen, 0, consumed);
    }
    return c == '[';
  }

  /**
   * Adapts a JSON-lines stream ({@code {...}\n{...}\n}) to a single JSON array by injecting the
   * brackets and the separating commas, without buffering the whole file.
   *
   * <p>
   * Records are emitted verbatim, so this is byte-transparent for the record bodies; only the framing
   * changes. Blank lines are skipped, which is what a trailing newline produces.
   */
  private static final class JsonLinesAsArrayReader extends FilterReader {

    private static final int STATE_OPEN = 0;
    private static final int STATE_BODY = 1;
    private static final int STATE_CLOSE = 2;
    private static final int STATE_DONE = 3;

    private int state = STATE_OPEN;
    /** True once at least one record has been emitted, i.e. the next record needs a comma. */
    private boolean needComma;
    /** True while we are positioned at the start of a line and have emitted nothing from it yet. */
    private boolean atLineStart = true;
    /** A character pulled from the delegate that did not fit into the caller's buffer. */
    private int pending = -1;

    private JsonLinesAsArrayReader(final Reader in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      final char[] one = new char[1];
      final int n = read(one, 0, 1);
      return n == -1
          ? -1
          : one[0];
    }

    @Override
    public int read(final char[] cbuf, final int off, final int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      switch (state) {
        case STATE_OPEN -> {
          state = STATE_BODY;
          cbuf[off] = '[';
          return 1;
        }
        case STATE_CLOSE -> {
          state = STATE_DONE;
          cbuf[off] = ']';
          return 1;
        }
        case STATE_DONE -> {
          return -1;
        }
        default -> {
          // body: fall through
        }
      }
      int written = 0;
      while (written < len) {
        final int c;
        if (pending != -1) {
          c = pending;
          pending = -1;
        } else {
          c = in.read();
        }
        if (c == -1) {
          if (written == 0) {
            state = STATE_DONE;
            cbuf[off] = ']';
            return 1;
          }
          state = STATE_CLOSE;
          return written;
        }
        if (c == '\n' || c == '\r') {
          atLineStart = true;
          continue;
        }
        if (atLineStart) {
          atLineStart = false;
          final boolean comma = needComma;
          needComma = true;
          if (comma) {
            cbuf[off + written++] = ',';
            if (written == len) {
              // The record's first character has to wait for the next call.
              pending = c;
              return written;
            }
          }
        }
        cbuf[off + written++] = (char) c;
      }
      return written;
    }
  }
}
