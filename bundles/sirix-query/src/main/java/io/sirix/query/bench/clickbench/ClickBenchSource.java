package io.sirix.query.bench.clickbench;

import com.fasterxml.jackson.core.JsonParser;
import io.sirix.service.json.shredder.JacksonJsonShredder;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
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

  /**
   * Read buffer for file sources. Keep both the byte-stream buffer and the legacy reader's
   * two-byte {@code char[]} representation below G1's smallest humongous-object threshold.
   */
  private static final int BUFFER_BYTES = 128 * 1024;

  private ClickBenchSource() {
    throw new AssertionError("no instances");
  }

  /** Jackson parser plus the framing mode required by the public transaction ingest API. */
  public record JacksonSource(JsonParser parser, boolean ldjson) implements AutoCloseable {

    public JacksonSource {
      if (parser == null) {
        throw new IllegalArgumentException("parser must not be null");
      }
    }

    @Override
    public void close() throws IOException {
      parser.close();
    }
  }

  /**
   * Open the source in Jackson's lowest-copy form. File sources stay as bytes so the UTF-8 parser
   * reads the gzip stream directly; generated sources remain readers. JSON-lines files are left
   * unwrapped and use the transaction's native LDJSON mode instead of injecting brackets/commas
   * through a character adapter.
   */
  public static JacksonSource openJackson(final String spec) throws IOException {
    validateSpec(spec);
    if (spec.startsWith("generate:")) {
      return new JacksonSource(JacksonJsonShredder.createReaderParser(openGenerated(spec)), false);
    }

    final Path path = Path.of(spec);
    if (!Files.isReadable(path)) {
      throw new IOException("hits source is not readable: " + path.toAbsolutePath());
    }

    final InputStream file = Files.newInputStream(path);
    try {
      final InputStream decoded = spec.toLowerCase(Locale.ROOT).endsWith(".gz")
          ? new GZIPInputStream(file, BUFFER_BYTES)
          : file;
      final PushbackInputStream input = new PushbackInputStream(new BufferedInputStream(decoded, BUFFER_BYTES), 3);
      final boolean ldjson = !isJsonArray(input);
      return new JacksonSource(JacksonJsonShredder.createInputStreamParser(input), ldjson);
    } catch (final IOException | RuntimeException exception) {
      file.close();
      throw exception;
    }
  }

  /**
   * @param spec one of the three source spellings documented on this class
   * @return a reader over a JSON array of hit objects; the caller closes it
   * @throws IOException if the source cannot be opened
   */
  public static Reader open(final String spec) throws IOException {
    validateSpec(spec);
    if (spec.startsWith("generate:")) {
      return openGenerated(spec);
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
        new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), BUFFER_BYTES), 1);
    return isJsonArray(reader)
        ? reader
        : new JsonLinesAsArrayReader(reader);
  }

  private static void validateSpec(final String spec) {
    if (spec == null || spec.isBlank()) {
      throw new IllegalArgumentException("source spec must not be blank");
    }
  }

  private static Reader openGenerated(final String spec) {
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

  /** Byte-stream equivalent of {@link #isJsonArray(PushbackReader)}. */
  private static boolean isJsonArray(final PushbackInputStream pushback) throws IOException {
    int value = pushback.read();
    if (value == 0xEF) {
      final int second = pushback.read();
      final int third = pushback.read();
      if (second == 0xBB && third == 0xBF) {
        value = pushback.read();
      } else {
        if (third != -1) {
          pushback.unread(third);
        }
        if (second != -1) {
          pushback.unread(second);
        }
      }
    }
    while (value == ' ' || value == '\t' || value == '\r' || value == '\n') {
      value = pushback.read();
    }
    if (value != -1) {
      pushback.unread(value);
    }
    return value == '[';
  }

  /**
   * Peeks past leading whitespace for the {@code '['} that distinguishes a JSON array file from a
   * JSON-lines file, retaining the first significant character for the returned reader.
   */
  private static boolean isJsonArray(final PushbackReader pushback) throws IOException {
    int c = pushback.read();
    if (c == '\uFEFF') {
      c = pushback.read();
    }
    while (c != -1 && Character.isWhitespace(c)) {
      c = pushback.read();
    }
    if (c != -1) {
      pushback.unread(c);
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
