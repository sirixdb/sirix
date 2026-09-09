/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.sirix.service.json.JsonNumber;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

final class BulkJsonScannerNumberTest {

  @Test
  void integralLanePreservesNarrowestExactTypeAcrossEveryBoundary() throws IOException {
    final String[] literals = {"0", "-0", "2147483647", "-2147483648", "2147483648", "-2147483649",
        "9223372036854775807", "-9223372036854775808"};
    final long[] values = {0, 0, Integer.MAX_VALUE, Integer.MIN_VALUE, (long) Integer.MAX_VALUE + 1,
        (long) Integer.MIN_VALUE - 1, Long.MAX_VALUE, Long.MIN_VALUE};
    final int[] types = {BulkJsonScanner.NUMBER_TYPE_INT, BulkJsonScanner.NUMBER_TYPE_INT,
        BulkJsonScanner.NUMBER_TYPE_INT, BulkJsonScanner.NUMBER_TYPE_INT, BulkJsonScanner.NUMBER_TYPE_LONG,
        BulkJsonScanner.NUMBER_TYPE_LONG, BulkJsonScanner.NUMBER_TYPE_LONG, BulkJsonScanner.NUMBER_TYPE_LONG};
    final BulkJsonScanner scanner = scannerFor(literals);

    assertEquals(BulkJsonScanner.EVENT_BEGIN_ARRAY, scanner.next());
    for (int i = 0; i < literals.length; i++) {
      assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.next(), literals[i]);
      assertEquals(types[i], scanner.numberType(), literals[i]);
      assertEquals(values[i], scanner.integralNumberValue(), literals[i]);

      final Number compatible = scanner.number();
      if (types[i] == BulkJsonScanner.NUMBER_TYPE_INT) {
        assertInstanceOf(Integer.class, compatible, literals[i]);
      } else {
        assertInstanceOf(Long.class, compatible, literals[i]);
      }
      assertEquals(values[i], compatible.longValue(), literals[i]);
      assertSame(compatible, scanner.number(), "the compatibility wrapper is materialized at most once per event");
    }
    assertEquals(BulkJsonScanner.EVENT_END_ARRAY, scanner.next());
    assertEquals(BulkJsonScanner.EVENT_END_DOCUMENT, scanner.next());
  }

  @Test
  void overflowDecimalAndExponentFormsUseTheExactExistingFallback() throws IOException {
    final String[] literals =
        {"9223372036854775808", "-9223372036854775809", "1.5", "-0.5", "1e3", "1E-3", "1.25e2", "3.0e0", "1e400"};
    final BulkJsonScanner scanner = scannerFor(literals);

    assertEquals(BulkJsonScanner.EVENT_BEGIN_ARRAY, scanner.next());
    for (final String literal : literals) {
      assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.next(), literal);
      assertEquals(BulkJsonScanner.NUMBER_TYPE_FALLBACK, scanner.numberType(), literal);
      assertThrows(IllegalStateException.class, scanner::integralNumberValue, literal);

      final Number expected = JsonNumber.stringToNumber(literal);
      final Number actual = scanner.number();
      assertEquals(expected.getClass(), actual.getClass(), literal);
      assertEquals(expected, actual, literal);
      assertSame(actual, scanner.number(), "fallback objects remain stable for the exposed event");
    }
    assertEquals(BulkJsonScanner.EVENT_END_ARRAY, scanner.next());

    assertInstanceOf(BigInteger.class, JsonNumber.stringToNumber(literals[0]));
    assertInstanceOf(BigInteger.class, JsonNumber.stringToNumber(literals[1]));
  }

  @Test
  void peekPopulatesOnlyScratchNumericStateUntilNextPromotesIt() throws IOException {
    // The deliberately tiny scanner buffer also puts the first long across a refill boundary.
    final BulkJsonScanner scanner = new BulkJsonScanner(new StringReader("[9223372036854775807,1.5,-7]"), 16);
    assertEquals(BulkJsonScanner.EVENT_BEGIN_ARRAY, scanner.next());
    assertEquals(BulkJsonScanner.NUMBER_TYPE_NONE, scanner.numberType());
    assertNull(scanner.number());

    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.peek());
    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.peek(), "repeated peek must not scan a second token");
    assertEquals(BulkJsonScanner.NUMBER_TYPE_NONE, scanner.numberType(), "peek must not expose scratch state");
    assertNull(scanner.number());

    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.next());
    assertEquals(BulkJsonScanner.NUMBER_TYPE_LONG, scanner.numberType());
    assertEquals(Long.MAX_VALUE, scanner.integralNumberValue());
    final Number exposedLong = scanner.number();

    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.peek());
    assertEquals(BulkJsonScanner.NUMBER_TYPE_LONG, scanner.numberType(), "peek must preserve the current event");
    assertEquals(Long.MAX_VALUE, scanner.integralNumberValue());
    assertSame(exposedLong, scanner.number());

    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.next());
    assertEquals(BulkJsonScanner.NUMBER_TYPE_FALLBACK, scanner.numberType());
    assertEquals(JsonNumber.stringToNumber("1.5"), scanner.number());

    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.peek());
    assertEquals(BulkJsonScanner.NUMBER_TYPE_FALLBACK, scanner.numberType());
    assertEquals(BulkJsonScanner.EVENT_NUMBER, scanner.next());
    assertEquals(BulkJsonScanner.NUMBER_TYPE_INT, scanner.numberType());
    assertEquals(-7, scanner.integralNumberValue());

    assertEquals(BulkJsonScanner.EVENT_END_ARRAY, scanner.next());
  }

  @Test
  void malformedIntegralLookingTokensStillTakeTheExistingRefusalPath() throws IOException {
    final String[] malformed = {"-", "--1", "1e", "1e+", "1..2"};
    for (final String literal : malformed) {
      final BulkJsonScanner scanner = new BulkJsonScanner(new StringReader('[' + literal + ']'), 16);
      assertEquals(BulkJsonScanner.EVENT_BEGIN_ARRAY, scanner.next(), literal);
      assertThrows(IllegalStateException.class, scanner::next, literal);
    }
  }

  private static BulkJsonScanner scannerFor(final String[] literals) {
    return new BulkJsonScanner(new StringReader('[' + String.join(",", literals) + ']'), 16);
  }
}
