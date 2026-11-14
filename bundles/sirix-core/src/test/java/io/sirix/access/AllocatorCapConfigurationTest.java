package io.sirix.access;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AllocatorCapConfigurationTest {
  @Test
  void acceptsDocumentedUnitsAndMaximum() {
    assertEquals(16L << 30, DatabaseConfiguration.parseSegmentSize("16G"));
    assertEquals(1L << 40, DatabaseConfiguration.parseSegmentSize("1024GB"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "typo", "0", "-1", "1025G", "8589934592G", "9223372036854775808"})
  void refusesInvalidAllocatorRequests(final String value) {
    final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> DatabaseConfiguration.parseSegmentSize(value));
    assertTrue(failure.getMessage().contains(DatabaseConfiguration.SEGMENT_ALLOCATION_SIZE_PROPERTY));
    assertTrue(failure.getMessage().contains(value));
  }
}
