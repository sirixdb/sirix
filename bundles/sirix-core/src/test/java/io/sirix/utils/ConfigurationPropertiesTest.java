package io.sirix.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
  @ValueSource(strings = {"", " ", "oops", "1T", "1.5G", "0", "-1", "-2G", "9223372036854775808", "8589934592G"})
  void explicitInvalidCapsFailWithThePropertyAndValue(final String value) {
    final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> ConfigurationProperties.parsePositiveBytes(PROPERTY, value));
    assertTrue(failure.getMessage().contains(PROPERTY));
    assertTrue(failure.getMessage().contains("=" + value));
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
