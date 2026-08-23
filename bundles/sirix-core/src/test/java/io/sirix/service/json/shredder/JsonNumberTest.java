package io.sirix.service.json.shredder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.sirix.service.json.JsonNumber;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public final class JsonNumberTest {

  @Test
  public void testFloat() {
    // Float narrowing was removed because it silently lost round-trip precision (e.g. 2.2e-308
    // underflowed to 0.0f, and 2^-52 emitted a shorter Float.toString that re-parses to a
    // different double). A value in float range is now kept as the faithful double instead.
    double f = (double) Float.MAX_VALUE - 1;
    String s = Double.toString(f);
    Number n = JsonNumber.stringToNumber(s);

    assertTrue("float-range exponent literal is now kept as a faithful Double, not a lossy Float", n instanceof Double);
  }

  @Test
  public void testDouble() {
    Double d = Double.MAX_VALUE - 1;
    String s = Double.toString(d);
    Number n = JsonNumber.stringToNumber(s);

    assertTrue("Expected type is Double", n instanceof Double);
  }


  @Test
  public void testLong() {
    Long l = Long.MAX_VALUE - 1;
    String s = Long.toString(l);
    Number n = JsonNumber.stringToNumber(s);

    assertTrue("Expected type is Long", n instanceof Long);
  }


  @Test
  public void testInteger() {
    Integer i = Integer.MAX_VALUE - 1;
    String s = Integer.toString(i);
    Number n = JsonNumber.stringToNumber(s);

    assertTrue("Expected type is Integer", n instanceof Integer);
  }


  @Test
  public void testBigInteger() {
    BigInteger b = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    String s = b.toString();
    Number n = JsonNumber.stringToNumber(s);

    assertTrue("Expected type is BigInteger", n instanceof BigInteger);
  }

  @Test
  public void testIntegralTypeBoundaries() {
    assertIntegral(Integer.MIN_VALUE, Integer.class);
    assertIntegral(Integer.MAX_VALUE, Integer.class);
    assertIntegral((long) Integer.MIN_VALUE - 1, Long.class);
    assertIntegral((long) Integer.MAX_VALUE + 1, Long.class);
    assertIntegral(Long.MIN_VALUE, Long.class);
    assertIntegral(Long.MAX_VALUE, Long.class);

    final BigInteger aboveLong = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    final BigInteger belowLong = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
    assertIntegral(aboveLong, BigInteger.class);
    assertIntegral(belowLong, BigInteger.class);
  }

  @Test
  public void jacksonParserPathMatchesStringParserExactly() throws IOException {
    final String[] literals = {"0", "-1", Integer.toString(Integer.MIN_VALUE), Integer.toString(Integer.MAX_VALUE),
        Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE),
        BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toString(),
        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toString(), "1.0", "-0.0000000000000000000000001",
        "6.022e23", "2.2e-308", "-5e-324", "1e309", "1.234567890123456789e42"};

    final JsonFactory factory = new JsonFactory();
    for (final String literal : literals) {
      final Number expected = JsonNumber.stringToNumber(literal);
      try (JsonParser parser = factory.createParser(literal)) {
        parser.nextToken();
        final Number actual = JsonNumber.fromJsonParser(parser);
        assertEquals("type differs for " + literal, expected.getClass(), actual.getClass());
        assertEquals("value differs for " + literal, expected.toString(), actual.toString());
      }
    }
  }

  private static void assertIntegral(final Number expected, final Class<? extends Number> expectedType) {
    final Number actual = JsonNumber.stringToNumber(expected.toString());
    assertEquals(expectedType, actual.getClass());
    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testException() {
    String s = ("1.0ae10");

    try {
      JsonNumber.stringToNumber(s);
      fail("Expected IllegalStateException to be thrown");
    } catch (IllegalStateException e) {
      assertTrue(true);
    }
  }

}
