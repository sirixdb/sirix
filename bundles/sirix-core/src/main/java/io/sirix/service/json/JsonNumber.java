package io.sirix.service.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public final class JsonNumber {

  private JsonNumber() {}

  private static Number stringDecimal(final String stringValue) {
    try {
      final BigDecimal exact = new BigDecimal(stringValue);
      return decimalNumber(exact, containsExponent(stringValue));
    } catch (final NumberFormatException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Number decimalNumber(final BigDecimal exact, final boolean exponentNotation) {
    if (!exponentNotation) {
      return exact;
    }

    // Keep the compact double only when Double.toString() round-trips the literal exactly
    // (the double faithfully represents it); otherwise keep the BigDecimal so the value
    // round-trips faithfully. This covers overflow (doubleValue() -> +/-Infinity, which is
    // NOT valid JSON), subnormal rounding (e.g. -5e-324 -> -4.9e-324) and excess precision.
    // Float narrowing is intentionally dropped: a value can be exact as a float yet its
    // shorter Float.toString() form re-parses to a different double, which silently corrupted
    // values such as 2.2e-308 (underflowed to 0.0f) and 2^-52.
    final double asDouble = exact.doubleValue();
    if (Double.isFinite(asDouble)
        && new BigDecimal(Double.toString(asDouble)).compareTo(exact) == 0) {
      return Double.valueOf(asDouble);
    }
    return exact;
  }

  private static boolean containsExponent(final CharSequence value) {
    for (int index = 0, length = value.length(); index < length; index++) {
      final char character = value.charAt(index);
      if (character == 'e' || character == 'E') {
        return true;
      }
    }
    return false;
  }

  /**
   * Parse an integral JSON literal without using exceptions for ordinary type widening.
   *
   * <p>The JDK's parse-and-catch sequence allocates a {@link NumberFormatException} and its stack
   * trace for every value outside the {@code int} range, even though such values are routine in
   * analytical data. Accumulating negatively (the same overflow-safe technique used by the JDK)
   * also represents {@link Long#MIN_VALUE}, whose magnitude has no positive {@code long}
   * representation.</p>
   */
  private static Number stringInteger(final String stringValue) {
    final int length = stringValue.length();
    if (length == 0) {
      return bigIntegerOrThrow(stringValue);
    }

    int index = 0;
    boolean negative = false;
    final char first = stringValue.charAt(0);
    if (first == '-' || first == '+') {
      negative = first == '-';
      if (++index == length) {
        return bigIntegerOrThrow(stringValue);
      }
    }

    final long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
    final long multiplyLimit = limit / 10;
    long result = 0;
    while (index < length) {
      final int digit = stringValue.charAt(index++) - '0';
      if (digit < 0 || digit > 9) {
        return bigIntegerOrThrow(stringValue);
      }
      if (result < multiplyLimit) {
        return bigIntegerOrThrow(stringValue);
      }
      result *= 10;
      if (result < limit + digit) {
        return bigIntegerOrThrow(stringValue);
      }
      result -= digit;
    }

    final long value = negative ? result : -result;
    // Do not express this as an Integer/Long conditional expression: binary numeric promotion
    // unboxes both branches, widens the int to long, and boxes every result as Long.
    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
      return Integer.valueOf((int) value);
    }
    return Long.valueOf(value);
  }

  private static BigInteger bigIntegerOrThrow(final String stringValue) {
    try {
      return new BigInteger(stringValue);
    } catch (final NumberFormatException exception) {
      throw new IllegalStateException(exception);
    }
  }

  public static Number stringToNumber(final String stringValue) {
    Number number;

    // Route fractional AND exponent literals (e.g. 1e5, 6E23, 0e0) to stringDecimal. JSON
    // numbers may use scientific notation with no decimal point; the old `contains(".")`-only
    // check sent them to the integer path (Integer/Long/BigInteger), which rejects `e`/`E` and
    // aborted the whole shred with an IllegalStateException — i.e. valid JSON failed to store.
    if (stringValue.contains(".") || stringValue.contains("e") || stringValue.contains("E")) {
      number = stringDecimal(stringValue);
    } else {
      number = stringInteger(stringValue);
    }

    return number;
  }

  /**
   * Reads the current Jackson numeric token without materializing its lexical form as a
   * {@link String}.
   *
   * <p>Jackson already classifies integral tokens while scanning them, so using its typed accessors
   * preserves Sirix's narrowest exact {@link Integer}/{@link Long}/{@link BigInteger} representation.
   * Decimal tokens are requested as {@link BigDecimal} to avoid Jackson's default eager narrowing
   * to {@code double}. The parser-owned character buffer is inspected only to retain the existing
   * distinction between ordinary decimal and exponent notation.</p>
   *
   * @param parser parser positioned on an integer or floating-point token
   * @return the exact Sirix numeric representation
   * @throws IOException if Jackson cannot decode the token
   * @throws IllegalStateException if the parser is not positioned on a numeric token
   */
  public static Number fromJsonParser(final JsonParser parser) throws IOException {
    final JsonToken token = parser.currentToken();
    if (token == JsonToken.VALUE_NUMBER_INT) {
      return switch (parser.getNumberType()) {
        case INT -> Integer.valueOf(parser.getIntValue());
        case LONG -> Long.valueOf(parser.getLongValue());
        case BIG_INTEGER -> parser.getBigIntegerValue();
        default -> throw new IllegalStateException("Unexpected integral number type: " + parser.getNumberType());
      };
    }
    if (token == JsonToken.VALUE_NUMBER_FLOAT) {
      final char[] characters = parser.getTextCharacters();
      final int offset = parser.getTextOffset();
      final int end = offset + parser.getTextLength();
      boolean exponentNotation = false;
      for (int index = offset; index < end; index++) {
        final char character = characters[index];
        if (character == 'e' || character == 'E') {
          exponentNotation = true;
          break;
        }
      }
      return decimalNumber(parser.getDecimalValue(), exponentNotation);
    }
    throw new IllegalStateException("Parser is not positioned on a numeric token: " + token);
  }
}
