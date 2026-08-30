/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Allocation-stable byte-stream normalizer for the canonical ClickBench record schema.
 *
 * <p>
 * The official JSONEachRow corpus quotes six signed {@code BIGINT} values and uses a space in its
 * three timestamp values. SirixDB's JSON model must instead see integral tokens and ISO-8601
 * timestamps. This filter removes only those twelve numeric quote bytes per record and changes the
 * three timestamp separators to {@code T}. It validates every field name, field order and value
 * token kind while streaming; records and the complete input are never buffered.
 * </p>
 *
 * <p>
 * The input must already be framed as one top-level JSON array. Strings are copied byte for byte,
 * including their escape spelling, so normalization cannot change an unrelated value.
 * </p>
 */
final class ClickBenchSchemaInputStream extends InputStream {

  private static final int INPUT_BUFFER_BYTES = 128 * 1024;

  private static final byte INT = 1;
  private static final byte LONG = 2;
  private static final byte STRING = 3;
  private static final byte DATE = 4;
  private static final byte DATETIME = 5;

  private static final int ROOT_START = 0;
  private static final int RECORD_START = 1;
  private static final int FIELD_START = 2;
  private static final int FIELD_NAME = 3;
  private static final int COLON = 4;
  private static final int VALUE_START = 5;
  private static final int STRING_VALUE = 6;
  private static final int NUMBER_VALUE = 7;
  private static final int VALUE_END = 8;
  private static final int RECORD_END = 9;
  private static final int TRAILING = 10;

  private final InputStream input;
  private final byte[] inputBuffer = new byte[INPUT_BUFFER_BYTES];
  private final byte[] fieldKinds = new byte[ClickBenchSchema.COLUMNS.size()];
  private final byte[] dateTime = new byte[19];
  private final byte[] oneByte = new byte[1];

  private int inputOffset;
  private int inputLimit;
  private int state = ROOT_START;
  private int fieldOrdinal;
  private int fieldNameOffset;
  private int stringOffset;
  private byte currentKind;
  private boolean escaped;
  private boolean quotedNumber;
  private boolean numberNegative;
  private boolean numberHasDigit;
  private long numberValue;
  private long numberLimit;
  private long numberMultiplyLimit;
  /** Bytes in input buffers that were completely consumed before the current one. */
  private long inputBufferBaseOffset;
  private boolean closed;

  ClickBenchSchemaInputStream(final InputStream input) {
    this.input = Objects.requireNonNull(input);
    for (int i = 0; i < fieldKinds.length; i++) {
      fieldKinds[i] = switch (ClickBenchSchema.typeOf(ClickBenchSchema.COLUMNS.get(i))) {
        case INT -> INT;
        case LONG -> LONG;
        case STRING -> STRING;
        case DATE -> DATE;
        case DATETIME -> DATETIME;
      };
    }
  }

  @Override
  public int read() throws IOException {
    final int count = read(oneByte, 0, 1);
    return count < 0
        ? -1
        : oneByte[0] & 0xFF;
  }

  @Override
  public int read(final byte[] target, final int offset, final int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, target.length);
    if (length == 0) {
      return 0;
    }
    if (closed) {
      throw new IOException("ClickBench source is closed");
    }

    int written = 0;
    while (written < length) {
      final int source = readSource();
      if (source < 0) {
        if (state != TRAILING) {
          throw malformed("unexpected end of input");
        }
        return written == 0
            ? -1
            : written;
      }
      final int normalized = normalize(source);
      if (normalized >= 0) {
        target[offset + written++] = (byte) normalized;
      }
    }
    return written;
  }

  private int normalize(final int source) throws IOException {
    return switch (state) {
      case ROOT_START -> rootStart(source);
      case RECORD_START -> recordStart(source);
      case FIELD_START -> fieldStart(source);
      case FIELD_NAME -> fieldName(source);
      case COLON -> colon(source);
      case VALUE_START -> valueStart(source);
      case STRING_VALUE -> stringValue(source);
      case NUMBER_VALUE -> numberValue(source);
      case VALUE_END -> valueEnd(source);
      case RECORD_END -> recordEnd(source);
      case TRAILING -> trailing(source);
      default -> throw new AssertionError("unknown ClickBench stream state " + state);
    };
  }

  private int rootStart(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (source != '[') {
      throw malformed("expected a top-level JSON array");
    }
    state = RECORD_START;
    return source;
  }

  private int recordStart(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (source == ']') {
      state = TRAILING;
      return source;
    }
    if (source != '{') {
      throw malformed("expected a record object");
    }
    fieldOrdinal = 0;
    state = FIELD_START;
    return source;
  }

  private int fieldStart(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (source != '"') {
      throw malformed("expected field " + fieldOrdinal + " ('" + expectedField() + "')");
    }
    fieldNameOffset = 0;
    state = FIELD_NAME;
    return source;
  }

  private int fieldName(final int source) throws IOException {
    if (source == '"') {
      if (fieldNameOffset != expectedField().length()) {
        throw malformed("field " + fieldOrdinal + " is shorter than expected '" + expectedField() + "'");
      }
      currentKind = fieldKinds[fieldOrdinal];
      state = COLON;
      return source;
    }
    final String expected = expectedField();
    if (source == '\\' || source > 0x7F || fieldNameOffset >= expected.length()
        || source != expected.charAt(fieldNameOffset)) {
      throw malformed("field " + fieldOrdinal + " does not match expected '" + expected + "'");
    }
    fieldNameOffset++;
    return source;
  }

  private int colon(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (source != ':') {
      throw malformed("expected ':' after field '" + expectedField() + "'");
    }
    state = VALUE_START;
    return source;
  }

  private int valueStart(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (currentKind == INT) {
      if (source != '-' && !isDigit(source)) {
        throw wrongToken("an integral JSON number");
      }
      startNumber(source, false);
      state = NUMBER_VALUE;
      return source;
    }
    if (currentKind == LONG) {
      if (source == '"') {
        startNumber(-1, true);
        state = NUMBER_VALUE;
        return -1; // Suppress the official JSONEachRow opening quote.
      }
      if (source != '-' && !isDigit(source)) {
        throw wrongToken("an integral JSON number or quoted signed BIGINT");
      }
      startNumber(source, false);
      state = NUMBER_VALUE;
      return source;
    }
    if (source != '"') {
      throw wrongToken("a JSON string");
    }
    escaped = false;
    stringOffset = 0;
    state = STRING_VALUE;
    return source;
  }

  private int stringValue(final int source) throws IOException {
    if (currentKind == STRING) {
      if (escaped) {
        escaped = false;
        return source;
      }
      if (source == '\\') {
        escaped = true;
        return source;
      }
      if (source == '"') {
        state = VALUE_END;
      } else if (source < 0x20) {
        throw malformed("unescaped control byte in field '" + expectedField() + "'");
      }
      return source;
    }

    if (source == '\\') {
      throw malformed("date/time field '" + expectedField() + "' must use unescaped ASCII");
    }
    if (source == '"') {
      final int expectedLength = currentKind == DATE
          ? 10
          : 19;
      if (stringOffset != expectedLength) {
        throw malformed("field '" + expectedField() + "' has length " + stringOffset + "; expected " + expectedLength);
      }
      validateDateTime();
      state = VALUE_END;
      return source;
    }
    if (source > 0x7F || stringOffset >= dateTime.length) {
      throw malformed("date/time field '" + expectedField() + "' must use unescaped ASCII");
    }
    int normalized = source;
    if (currentKind == DATETIME && stringOffset == 10 && source == ' ') {
      normalized = 'T';
    }
    dateTime[stringOffset++] = (byte) normalized;
    return normalized;
  }

  private int numberValue(final int source) throws IOException {
    if (quotedNumber) {
      if (source == '"') {
        finishNumber();
        state = VALUE_END;
        return -1; // Suppress the official JSONEachRow closing quote.
      }
      if (!isDigit(source) && !(source == '-' && !numberHasDigit && numberValue == 0L)) {
        throw wrongToken("a quoted signed BIGINT");
      }
      acceptNumberByte(source);
      return source;
    }

    if (isDigit(source)) {
      acceptDigit(source - '0');
      return source;
    }
    finishNumber();
    state = VALUE_END;
    return valueEnd(source);
  }

  private int valueEnd(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (fieldOrdinal + 1 < fieldKinds.length) {
      if (source != ',') {
        throw malformed("expected ',' after field '" + expectedField() + "'");
      }
      fieldOrdinal++;
      state = FIELD_START;
      return source;
    }
    if (source != '}') {
      throw malformed("expected record end after final field '" + expectedField() + "'");
    }
    state = RECORD_END;
    return source;
  }

  private int recordEnd(final int source) throws IOException {
    if (isWhitespace(source)) {
      return source;
    }
    if (source == ',') {
      state = RECORD_START;
      return source;
    }
    if (source == ']') {
      state = TRAILING;
      return source;
    }
    throw malformed("expected ',' or the end of the top-level array");
  }

  private int trailing(final int source) throws IOException {
    if (!isWhitespace(source)) {
      throw malformed("non-whitespace byte " + source + " after the top-level array");
    }
    return source;
  }

  private void startNumber(final int firstByte, final boolean quoted) throws IOException {
    quotedNumber = quoted;
    numberNegative = false;
    numberHasDigit = false;
    numberValue = 0L;
    numberLimit = -Long.MAX_VALUE;
    numberMultiplyLimit = numberLimit / 10L;
    if (firstByte >= 0) {
      acceptNumberByte(firstByte);
    }
  }

  private void acceptNumberByte(final int source) throws IOException {
    if (source == '-' && !numberHasDigit && numberValue == 0L && !numberNegative) {
      numberNegative = true;
      numberLimit = Long.MIN_VALUE;
      numberMultiplyLimit = numberLimit / 10L;
      return;
    }
    if (!isDigit(source)) {
      throw wrongToken("a signed integral value");
    }
    acceptDigit(source - '0');
  }

  private void acceptDigit(final int digit) throws IOException {
    if (numberValue < numberMultiplyLimit) {
      throw wrongToken("a signed 64-bit integral value");
    }
    numberValue *= 10L;
    if (numberValue < numberLimit + digit) {
      throw wrongToken("a signed 64-bit integral value");
    }
    numberValue -= digit;
    numberHasDigit = true;
  }

  private void finishNumber() throws IOException {
    if (!numberHasDigit) {
      throw wrongToken("a signed integral value");
    }
  }

  private void validateDateTime() throws IOException {
    if (dateTime[4] != '-' || dateTime[7] != '-') {
      throw malformed("field '" + expectedField() + "' must use YYYY-MM-DD");
    }
    final int year = fourDigits(0);
    final int month = twoDigits(5);
    final int day = twoDigits(8);
    if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
      throw malformed("field '" + expectedField() + "' contains an out-of-range date");
    }
    if (currentKind == DATETIME) {
      if (dateTime[10] != 'T' || dateTime[13] != ':' || dateTime[16] != ':') {
        throw malformed("field '" + expectedField() + "' must use YYYY-MM-DDTHH:MM:SS");
      }
      final int hour = twoDigits(11);
      final int minute = twoDigits(14);
      final int second = twoDigits(17);
      if (hour > 23 || minute > 59 || second > 59) {
        throw malformed("field '" + expectedField() + "' contains an out-of-range time");
      }
    }
  }

  private int fourDigits(final int offset) throws IOException {
    return digit(offset) * 1_000 + digit(offset + 1) * 100 + digit(offset + 2) * 10 + digit(offset + 3);
  }

  private int twoDigits(final int offset) throws IOException {
    return digit(offset) * 10 + digit(offset + 1);
  }

  private int digit(final int offset) throws IOException {
    final int digit = dateTime[offset] - '0';
    if (digit < 0 || digit > 9) {
      throw malformed("field '" + expectedField() + "' contains a non-digit in its date/time value");
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

  private String expectedField() {
    return ClickBenchSchema.COLUMNS.get(fieldOrdinal);
  }

  private IOException wrongToken(final String expected) {
    return malformed("field '" + expectedField() + "' must be " + expected);
  }

  private IOException malformed(final String message) {
    // readSource() has already advanced inputOffset past the offending byte. Computing the absolute
    // position from the buffer cursor keeps the per-byte normalization loop free of a long add.
    return new IOException(
        "invalid ClickBench source at byte " + (inputBufferBaseOffset + inputOffset) + ": " + message);
  }

  private int readSource() throws IOException {
    while (true) {
      if (inputLimit < 0) {
        return -1;
      }
      if (inputOffset < inputLimit) {
        return inputBuffer[inputOffset++] & 0xFF;
      }
      inputBufferBaseOffset = Math.addExact(inputBufferBaseOffset, inputLimit);
      inputLimit = input.read(inputBuffer);
      inputOffset = 0;
      if (inputLimit < 0) {
        return -1;
      }
      // InputStream permits a zero-byte read. Retry iteratively so a pathological wrapper cannot
      // grow the Java stack while the normal hot path remains one predictable loop iteration.
    }
  }

  private static boolean isWhitespace(final int value) {
    return value == ' ' || value == '\t' || value == '\r' || value == '\n';
  }

  private static boolean isDigit(final int value) {
    return value >= '0' && value <= '9';
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      input.close();
    }
  }
}
