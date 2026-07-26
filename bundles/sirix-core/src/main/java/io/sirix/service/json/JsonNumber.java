package io.sirix.service.json;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class JsonNumber {

  private JsonNumber() {}

  private static Number stringDecimal(String stringValue) {
    Number number;
    try {
      final BigDecimal exact = new BigDecimal(stringValue);
      if (stringValue.contains("E") || stringValue.contains("e")) {
        // Keep the compact double only when Double.toString() round-trips the literal exactly
        // (the double faithfully represents it); otherwise keep the BigDecimal so the value
        // round-trips faithfully. This covers overflow (doubleValue() -> +/-Infinity, which is
        // NOT valid JSON), subnormal rounding (e.g. -5e-324 -> -4.9e-324) and excess precision.
        // Float narrowing is intentionally dropped: a value can be exact as a float yet its
        // shorter Float.toString() form re-parses to a different double, which silently corrupted
        // values such as 2.2e-308 (underflowed to 0.0f) and 2^-52.
        final double asDouble = exact.doubleValue();
        number = (Double.isFinite(asDouble) && new BigDecimal(Double.toString(asDouble)).compareTo(exact) == 0)
            ? (Number) asDouble
            : (Number) exact;
      } else {
        number = exact;
      }
    } catch (final NumberFormatException e) {
      throw new IllegalStateException(e);
    }

    return number;
  }

  public static Number stringToNumber(String stringValue) {
    Number number;

    // Route fractional AND exponent literals (e.g. 1e5, 6E23, 0e0) to stringDecimal. JSON
    // numbers may use scientific notation with no decimal point; the old `contains(".")`-only
    // check sent them to the integer path (Integer/Long/BigInteger), which rejects `e`/`E` and
    // aborted the whole shred with an IllegalStateException — i.e. valid JSON failed to store.
    if (stringValue.contains(".") || stringValue.contains("e") || stringValue.contains("E")) {
      number = stringDecimal(stringValue);
    } else {
      try {
        number = Integer.parseInt(stringValue);
      } catch (final NumberFormatException e) {
        try {
          number = Long.parseLong(stringValue);
        } catch (final NumberFormatException ee) {
          try {
            number = new BigInteger(stringValue);
          } catch (final NumberFormatException eee) {
            throw new IllegalStateException(eee);
          }
        }
      }
    }

    return number;
  }
}
