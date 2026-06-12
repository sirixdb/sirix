package io.sirix.service.json.shredder;

import io.sirix.service.json.JsonNumber;
import org.junit.Test;

import java.math.BigInteger;

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

    assertTrue("float-range exponent literal is now kept as a faithful Double, not a lossy Float",
        n instanceof Double);
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
