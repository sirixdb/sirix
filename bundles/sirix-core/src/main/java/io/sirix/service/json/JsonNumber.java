package io.sirix.service.json;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class JsonNumber {

  private JsonNumber() {}

  private static Number stringDecimal(String stringValue) {
    Number number;
    try {
      if (stringValue.contains("E") || stringValue.contains("e")) {
        final double parsed = Double.parseDouble(stringValue);

        if (parsed <= Float.MAX_VALUE) {
          number = Float.parseFloat(stringValue);
        } else {
          number = parsed;
        }
      } else {
        number = new BigDecimal(stringValue);
      }
    } catch (final NumberFormatException e) {
      throw new IllegalStateException(e);
    }

    return number;
  }

  public static Number stringToNumber(String stringValue) {
    Number number;

    if (stringValue.contains(".")) {
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
