package org.sirix.service.json;

import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonNumber {

  static Number stringDecimal(String stringValue) {
    Number number;
    Number n1 = Float.MAX_VALUE;
    try{
      if (stringValue.contains("E") || stringValue.contains("e")) {
        number = Double.valueOf(stringValue);

        if (number.doubleValue() <= n1.doubleValue()) {
          number = Float.valueOf(stringValue);
        }
      } else {
          number = new BigDecimal(stringValue);
      }
    }catch (final NumberFormatException eeeeee) {
      throw new IllegalStateException(eeeeee);
    }

    return number;
  }


  public static Number stringToNumber(String stringValue) {
    Number number;

    if (stringValue.contains(".")) {
      number = stringDecimal(stringValue);
    } else {
      try {
        number = Integer.valueOf(stringValue);
      } catch (final NumberFormatException e) {
        try {
          number = Long.valueOf(stringValue);
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
