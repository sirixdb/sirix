package io.sirix.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock("SYSTEM_PROPERTIES")
final class ConfigurationPropertiesTest {
  private static final String PROPERTY = "sirix.test.memory.cap";

  @Test
  void binaryUnitsAndWhitespace() {
    assertEquals(17, ConfigurationProperties.parsePositiveBytes(PROPERTY, "17"));
    assertEquals(2048, ConfigurationProperties.parsePositiveBytes(PROPERTY, " 2 kb "));
    assertEquals(3L << 20, ConfigurationProperties.parsePositiveBytes(PROPERTY, "3M"));
    assertEquals(16L << 30, ConfigurationProperties.parsePositiveBytes(PROPERTY, "16GB"));
    assertEquals(Long.MAX_VALUE, ConfigurationProperties.parsePositiveBytes(PROPERTY, "9223372036854775807"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", " ", "oops", "1T", "1.5G", "0", "-1", "-2G", "9223372036854775808", "8589934592G", "17179869185G"})
  void explicitInvalidCapsFailWithThePropertyAndValue(final String value) {
    final IllegalArgumentException failure =
        assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.parsePositiveBytes(PROPERTY, value));
    assertTrue(failure.getMessage().contains(PROPERTY));
    assertTrue(failure.getMessage().contains("=" + value));
  }

  @Test
  void binaryUnitBoundariesMatchAnArbitraryPrecisionOracle() {
    // Binary unit definitions and signed-long limits are the oracle, independent of checked-long
    // multiplication in the parser. Include the first rejected value for every unit.
    final String[] suffixes = {"", "K", "kb", "M", "mB", "G", "gb"};
    final int[] shifts = {0, 10, 10, 20, 20, 30, 30};
    final BigInteger maximum = BigInteger.valueOf(Long.MAX_VALUE);
    for (int unit = 0; unit < suffixes.length; unit++) {
      final BigInteger limit = maximum.shiftRight(shifts[unit]);
      final BigInteger[] numbers = {BigInteger.ZERO, BigInteger.ONE, BigInteger.TWO, limit.subtract(BigInteger.ONE),
          limit, limit.add(BigInteger.ONE), maximum, maximum.add(BigInteger.ONE)};
      for (final BigInteger number : numbers) {
        final String input = number + suffixes[unit];
        final BigInteger bytes = number.shiftLeft(shifts[unit]);
        if (bytes.signum() > 0 && bytes.compareTo(maximum) <= 0) {
          assertEquals(bytes.longValueExact(), ConfigurationProperties.parsePositiveBytes(PROPERTY, input), input);
        } else {
          final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
              () -> ConfigurationProperties.parsePositiveBytes(PROPERTY, input), input);
          assertTrue(failure.getMessage().contains(PROPERTY), input);
          assertTrue(failure.getMessage().contains(input), input);
        }
      }
    }
  }

  @Test
  void rejectsMissingInputAndInvalidDefaults() {
    assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.parsePositiveBytes(PROPERTY, null));
    assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveBytes(PROPERTY, 0));
    assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveBytes(PROPERTY, -1));
    assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveInt(PROPERTY, 0));
    assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveInt(PROPERTY, -1));
  }

  @Test
  void defaultsApplyOnlyWhenAbsent() {
    final String previous = System.getProperty(PROPERTY);
    try {
      System.clearProperty(PROPERTY);
      assertEquals(4096, ConfigurationProperties.positiveBytes(PROPERTY, 4096));
      assertEquals(20, ConfigurationProperties.positiveInt(PROPERTY, 20));
      System.setProperty(PROPERTY, "2K");
      assertEquals(2048, ConfigurationProperties.positiveBytes(PROPERTY, 4096));
      assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveInt(PROPERTY, 20));
      System.setProperty(PROPERTY, "2147483648");
      assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveInt(PROPERTY, 20));
      System.setProperty(PROPERTY, "");
      assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveBytes(PROPERTY, 4096));
      assertThrows(IllegalArgumentException.class, () -> ConfigurationProperties.positiveInt(PROPERTY, 20));
    } finally {
      if (previous == null) {
        System.clearProperty(PROPERTY);
      } else {
        System.setProperty(PROPERTY, previous);
      }
    }
  }
}
