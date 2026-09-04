package io.sirix.query.bench.clickbench;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import io.sirix.access.trx.node.json.NdjsonAsArrayInputStream;
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
import java.math.BigDecimal;
import java.math.BigInteger;
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
   * Read buffer for file sources. Keep both the byte-stream buffer and the legacy reader's two-byte
   * {@code char[]} representation below G1's smallest humongous-object threshold.
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
    return openJackson(spec, false);
  }

  /**
   * Open the source, optionally normalising the official ClickHouse JSONEachRow representation to
   * this benchmark port's documented JSON schema while tokens are pulled.
   *
   * <p>
   * The official source quotes its six signed {@code BIGINT} columns and spells timestamps with a
   * space. The SirixDB port requires numeric JSON tokens and ISO-8601 {@code T}; materialising a
   * second 100M-row file merely to change those delimiters is both unnecessary and operationally
   * unsafe. The normaliser below changes only the parser's exposed token/value view. It neither
   * buffers rows nor rewrites the source.
   * </p>
   */
  public static JacksonSource openJackson(final String spec, final boolean normalizeSchema) throws IOException {
    validateSpec(spec);
    if (spec.startsWith("generate:")) {
      final JsonParser parser = JacksonJsonShredder.createReaderParser(openGenerated(spec));
      return new JacksonSource(normalizeSchema
          ? new SchemaNormalizingParser(parser)
          : parser, false);
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
      final JsonParser parser = JacksonJsonShredder.createInputStreamParser(input);
      return new JacksonSource(normalizeSchema
          ? new SchemaNormalizingParser(parser)
          : parser, ldjson);
    } catch (final IOException | RuntimeException exception) {
      file.close();
      throw exception;
    }
  }

  /**
   * Open a file source as one JSON-array byte stream for the parallel bulk importer.
   *
   * <p>
   * Gzip decoding and NDJSON framing remain streaming. When {@code normalizeSchema} is true, the
   * returned stream also applies {@link ClickBenchSchemaInputStream}'s allocation-stable schema
   * normalization and every-record validation. Generated sources use {@link #open(String)} and the
   * importer's reader entry point instead.
   * </p>
   */
  public static InputStream openParallelInput(final String spec, final boolean normalizeSchema) throws IOException {
    validateSpec(spec);
    if (spec.startsWith("generate:")) {
      throw new IllegalArgumentException("generated ClickBench sources use the parallel importer's Reader entry point");
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
      final InputStream arrayInput = isJsonArray(input)
          ? input
          : new NdjsonAsArrayInputStream(input);
      return normalizeSchema
          ? new ClickBenchSchemaInputStream(arrayInput)
          : arrayInput;
    } catch (final IOException | RuntimeException exception) {
      file.close();
      throw exception;
    }
  }

  /**
   * Allocation-stable schema view over the canonical, flat ClickBench record shape.
   *
   * <p>
   * Jackson canonicalises field-name strings. The first record pins those instances and validates all
   * 105 names; later records verify the fixed schema order by reference comparison (with an equality
   * fallback for a parser that does not canonicalise). Only six LONG and three DATETIME ordinals
   * enter conversion code. Integer parsing uses a negative accumulator so {@link Long#MIN_VALUE} is
   * accepted without a temporary {@link String}.
   * </p>
   */
  private static final class SchemaNormalizingParser extends JsonParserDelegate {

    private static final byte INT = 1;
    private static final byte LONG = 2;
    private static final byte STRING = 3;
    private static final byte DATE = 4;
    private static final byte DATETIME = 5;

    private final String[] canonicalNames = new String[ClickBenchSchema.COLUMNS.size()];
    private final byte[] kinds = new byte[ClickBenchSchema.COLUMNS.size()];
    private final char[] normalizedTimestamp = new char[19];

    private JsonToken exposedToken;
    private int objectDepth;
    private int fieldOrdinal = -1;
    private byte currentKind;
    private boolean expectingValue;
    private long currentLong;
    private boolean longNormalized;
    private boolean timestampNormalized;
    private long normalizedLongValues;
    private long normalizedTimestamps;

    private SchemaNormalizingParser(final JsonParser delegate) {
      super(delegate);
      for (int i = 0; i < kinds.length; i++) {
        kinds[i] = switch (ClickBenchSchema.typeOf(ClickBenchSchema.COLUMNS.get(i))) {
          case INT -> INT;
          case LONG -> LONG;
          case STRING -> STRING;
          case DATE -> DATE;
          case DATETIME -> DATETIME;
        };
      }
    }

    @Override
    public JsonToken nextToken() throws IOException {
      final JsonToken raw = delegate.nextToken();
      longNormalized = false;
      timestampNormalized = false;
      exposedToken = raw;
      if (raw == null) {
        return null;
      }
      if (expectingValue) {
        validateValueToken(raw);
        expectingValue = false;
      }
      switch (raw) {
        case START_OBJECT -> {
          objectDepth++;
          if (objectDepth == 1) {
            fieldOrdinal = -1;
          }
        }
        case END_OBJECT -> {
          if (objectDepth == 1 && fieldOrdinal != kinds.length - 1) {
            throw new IOException("ClickBench record has " + (fieldOrdinal + 1) + " fields; expected " + kinds.length);
          }
          objectDepth--;
        }
        case FIELD_NAME -> selectFieldOrdinal();
        case VALUE_STRING -> normalizeStringValue();
        default -> {
          // No schema adaptation for structural, boolean, null or already-numeric tokens.
        }
      }
      return exposedToken;
    }

    private void selectFieldOrdinal() throws IOException {
      if (objectDepth != 1) {
        throw new IOException("ClickBench records must be flat objects");
      }
      final int ordinal = ++fieldOrdinal;
      if (ordinal >= kinds.length) {
        throw new IOException("ClickBench record has more than " + kinds.length + " fields");
      }
      final String actual = delegate.currentName();
      final String canonical = canonicalNames[ordinal];
      if (canonical == null) {
        final String expected = ClickBenchSchema.COLUMNS.get(ordinal);
        if (!expected.equals(actual)) {
          throw new IOException("ClickBench field " + ordinal + " is '" + actual + "'; expected '" + expected + "'");
        }
        canonicalNames[ordinal] = actual;
      } else if (canonical != actual && !canonical.equals(actual)) {
        throw new IOException("ClickBench field " + ordinal + " is '" + actual + "'; expected '" + canonical + "'");
      }
      currentKind = kinds[ordinal];
      expectingValue = true;
    }

    private void validateValueToken(final JsonToken token) throws IOException {
      final boolean valid = switch (currentKind) {
        case INT -> token == JsonToken.VALUE_NUMBER_INT;
        case LONG -> token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_STRING;
        case STRING, DATE, DATETIME -> token == JsonToken.VALUE_STRING;
        default -> false;
      };
      if (!valid) {
        throw new IOException("ClickBench field '" + ClickBenchSchema.COLUMNS.get(fieldOrdinal) + "' has token " + token
            + "; expected " + expectedTokenDescription(currentKind));
      }
    }

    private static String expectedTokenDescription(final byte kind) {
      return switch (kind) {
        case INT -> "an integral JSON number";
        case LONG -> "an integral JSON number or a normalisable quoted BIGINT";
        case STRING, DATE, DATETIME -> "a JSON string";
        default -> "the declared schema type";
      };
    }

    private void normalizeStringValue() throws IOException {
      if (currentKind == LONG) {
        currentLong = parseSignedLong(delegate.getTextCharacters(), delegate.getTextOffset(), delegate.getTextLength());
        longNormalized = true;
        exposedToken = JsonToken.VALUE_NUMBER_INT;
        normalizedLongValues++;
      } else if (currentKind == DATETIME) {
        final char[] source = delegate.getTextCharacters();
        final int offset = delegate.getTextOffset();
        final int length = delegate.getTextLength();
        if (length != normalizedTimestamp.length) {
          throw new IOException("ClickBench timestamp in " + ClickBenchSchema.COLUMNS.get(fieldOrdinal) + " has length "
              + length + "; expected 19");
        }
        final char separator = source[offset + 10];
        if (separator == ' ') {
          System.arraycopy(source, offset, normalizedTimestamp, 0, length);
          normalizedTimestamp[10] = 'T';
          timestampNormalized = true;
          normalizedTimestamps++;
        } else if (separator != 'T') {
          throw new IOException("ClickBench timestamp in " + ClickBenchSchema.COLUMNS.get(fieldOrdinal)
              + " must use 'T' or a normalisable space at offset 10");
        }
      }
    }

    private static long parseSignedLong(final char[] chars, final int offset, final int length) throws IOException {
      if (length == 0) {
        throw new IOException("empty quoted BIGINT");
      }
      int index = offset;
      final int end = offset + length;
      final boolean negative = chars[index] == '-';
      if (negative && ++index == end) {
        throw new IOException("quoted BIGINT contains only a sign");
      }
      final long limit = negative
          ? Long.MIN_VALUE
          : -Long.MAX_VALUE;
      final long multiplyLimit = limit / 10L;
      long result = 0L;
      while (index < end) {
        final int digit = chars[index++] - '0';
        if (digit < 0 || digit > 9 || result < multiplyLimit) {
          throw invalidLong(chars, offset, length);
        }
        result *= 10L;
        if (result < limit + digit) {
          throw invalidLong(chars, offset, length);
        }
        result -= digit;
      }
      return negative
          ? result
          : -result;
    }

    private static IOException invalidLong(final char[] chars, final int offset, final int length) {
      return new IOException("invalid or out-of-range quoted BIGINT: " + new String(chars, offset, length));
    }

    @Override
    public JsonToken currentToken() {
      return exposedToken;
    }

    @Override
    @SuppressWarnings("deprecation")
    public JsonToken getCurrentToken() {
      return exposedToken;
    }

    @Override
    public NumberType getNumberType() throws IOException {
      return longNormalized
          ? NumberType.LONG
          : super.getNumberType();
    }

    @Override
    public Number getNumberValue() throws IOException {
      return longNormalized
          ? Long.valueOf(currentLong)
          : super.getNumberValue();
    }

    @Override
    public long getLongValue() throws IOException {
      return longNormalized
          ? currentLong
          : super.getLongValue();
    }

    @Override
    public int getIntValue() throws IOException {
      if (longNormalized) {
        return Math.toIntExact(currentLong);
      }
      return super.getIntValue();
    }

    @Override
    public BigInteger getBigIntegerValue() throws IOException {
      return longNormalized
          ? BigInteger.valueOf(currentLong)
          : super.getBigIntegerValue();
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
      return longNormalized
          ? BigDecimal.valueOf(currentLong)
          : super.getDecimalValue();
    }

    @Override
    public double getDoubleValue() throws IOException {
      return longNormalized
          ? (double) currentLong
          : super.getDoubleValue();
    }

    @Override
    public float getFloatValue() throws IOException {
      return longNormalized
          ? (float) currentLong
          : super.getFloatValue();
    }

    @Override
    public char[] getTextCharacters() throws IOException {
      return timestampNormalized
          ? normalizedTimestamp
          : super.getTextCharacters();
    }

    @Override
    public int getTextOffset() throws IOException {
      return timestampNormalized
          ? 0
          : super.getTextOffset();
    }

    @Override
    public int getTextLength() throws IOException {
      return timestampNormalized
          ? normalizedTimestamp.length
          : super.getTextLength();
    }

    @Override
    public String getText() throws IOException {
      return timestampNormalized
          ? new String(normalizedTimestamp)
          : super.getText();
    }

    private long normalizedLongValues() {
      return normalizedLongValues;
    }

    private long normalizedTimestamps() {
      return normalizedTimestamps;
    }
  }

  /** Result of the mutation-free first-record source preflight. */
  public record SourceValidation(int columns, long normalizedLongValues, long normalizedTimestamps) {
  }

  /**
   * Validate the first source record before the loader opens (and therefore replaces) its target.
   */
  public static SourceValidation validateFirstRecord(final String spec, final boolean normalizeSchema)
      throws IOException {
    try (JacksonSource source = openJackson(spec, normalizeSchema)) {
      final JsonParser parser = source.parser();
      JsonToken token = parser.nextToken();
      if (token == JsonToken.START_ARRAY) {
        token = parser.nextToken();
      }
      if (token != JsonToken.START_OBJECT) {
        throw new IOException("ClickBench source must start with a record object, got " + token);
      }
      int columns = 0;
      while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
        if (token != JsonToken.FIELD_NAME) {
          throw new IOException("expected field " + columns + ", got " + token);
        }
        final String name = parser.currentName();
        if (columns >= ClickBenchSchema.COLUMNS.size() || !ClickBenchSchema.COLUMNS.get(columns).equals(name)) {
          throw new IOException("unexpected ClickBench field at ordinal " + columns + ": " + name);
        }
        final JsonToken value = parser.nextToken();
        final ClickBenchSchema.ColumnType type = ClickBenchSchema.typeOf(name);
        if ((type == ClickBenchSchema.ColumnType.INT || type == ClickBenchSchema.ColumnType.LONG)
            && value != JsonToken.VALUE_NUMBER_INT) {
          throw new IOException(name + " must be an integral JSON number, got " + value);
        }
        if ((type == ClickBenchSchema.ColumnType.STRING || type == ClickBenchSchema.ColumnType.DATE
            || type == ClickBenchSchema.ColumnType.DATETIME) && value != JsonToken.VALUE_STRING) {
          throw new IOException(name + " must be a JSON string, got " + value);
        }
        if (type == ClickBenchSchema.ColumnType.INT || type == ClickBenchSchema.ColumnType.LONG) {
          parser.getLongValue();
        } else if (type == ClickBenchSchema.ColumnType.DATE) {
          validateDateText(parser, name);
        } else if (type == ClickBenchSchema.ColumnType.DATETIME) {
          validateDateTimeText(parser, name);
        }
        columns++;
      }
      if (columns != ClickBenchSchema.COLUMNS.size()) {
        throw new IOException(
            "ClickBench record has " + columns + " columns; expected " + ClickBenchSchema.COLUMNS.size());
      }
      final SchemaNormalizingParser normalizer = parser instanceof SchemaNormalizingParser normalized
          ? normalized
          : null;
      return new SourceValidation(columns, normalizer == null
          ? 0L
          : normalizer.normalizedLongValues(),
          normalizer == null
              ? 0L
              : normalizer.normalizedTimestamps());
    }
  }

  private static void validateDateText(final JsonParser parser, final String field) throws IOException {
    final char[] text = parser.getTextCharacters();
    final int offset = parser.getTextOffset();
    final int length = parser.getTextLength();
    if (length != 10) {
      throw new IOException(field + " must use YYYY-MM-DD, got length " + length);
    }
    validateDateDigits(text, offset, field);
  }

  private static void validateDateTimeText(final JsonParser parser, final String field) throws IOException {
    final char[] text = parser.getTextCharacters();
    final int offset = parser.getTextOffset();
    final int length = parser.getTextLength();
    if (length != 19) {
      throw new IOException(field + " must use YYYY-MM-DDTHH:MM:SS, got length " + length);
    }
    validateDateDigits(text, offset, field);
    if (text[offset + 10] != 'T' || text[offset + 13] != ':' || text[offset + 16] != ':') {
      throw new IOException(field + " must use YYYY-MM-DDTHH:MM:SS");
    }
    final int hour = twoDigits(text, offset + 11, field);
    final int minute = twoDigits(text, offset + 14, field);
    final int second = twoDigits(text, offset + 17, field);
    if (hour > 23 || minute > 59 || second > 59) {
      throw new IOException(field + " contains an out-of-range time");
    }
  }

  private static void validateDateDigits(final char[] text, final int offset, final String field) throws IOException {
    if (text[offset + 4] != '-' || text[offset + 7] != '-') {
      throw new IOException(field + " must use YYYY-MM-DD");
    }
    final int year = fourDigits(text, offset, field);
    final int month = twoDigits(text, offset + 5, field);
    final int day = twoDigits(text, offset + 8, field);
    if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
      throw new IOException(field + " contains an out-of-range date");
    }
  }

  private static int fourDigits(final char[] text, final int offset, final String field) throws IOException {
    return digit(text[offset], field) * 1_000 + digit(text[offset + 1], field) * 100
        + digit(text[offset + 2], field) * 10 + digit(text[offset + 3], field);
  }

  private static int twoDigits(final char[] text, final int offset, final String field) throws IOException {
    return digit(text[offset], field) * 10 + digit(text[offset + 1], field);
  }

  private static int digit(final char value, final String field) throws IOException {
    final int digit = value - '0';
    if (digit < 0 || digit > 9) {
      throw new IOException(field + " contains a non-digit in its date/time value");
    }
    return digit;
  }

  private static int daysInMonth(final int year, final int month) {
    return switch (month) {
      case 2 -> year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
          ? 29
          : 28;
      case 4, 6, 9, 11 -> 30;
      default -> 31;
    };
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
    final PushbackReader reader =
        new PushbackReader(new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), BUFFER_BYTES), 1);
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
        if (atLineStart && (c == ' ' || c == '\t')) {
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
