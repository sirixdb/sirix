package org.sirix.service.json;

import java.math.BigDecimal;
import java.math.BigInteger;

public class JsonNumber {
    public static Number stringToNumber(String stringValue) {
        Number number;

        if (stringValue.contains(".")) {
            if (stringValue.contains("E") || stringValue.contains("e")) {
                try {
                    number = Float.valueOf(stringValue);
                } catch (final NumberFormatException eeee) {
                    try {
                        number = Double.valueOf(stringValue);
                    } catch (final NumberFormatException eeeee) {
                        throw new IllegalStateException(eeeee);
                    }
                }
            } else {
                try {
                    number = new BigDecimal(stringValue);
                } catch (final NumberFormatException eeeeee) {
                    throw new IllegalStateException(eeeeee);
                }
            }
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
