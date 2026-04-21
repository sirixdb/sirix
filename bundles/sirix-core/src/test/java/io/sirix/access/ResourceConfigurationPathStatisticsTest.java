package io.sirix.access;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Builder-level tests for the {@code buildPathStatistics} flag on
 * {@link ResourceConfiguration}. The full serialize/deserialize round-trip lives in
 * the integration tests that already set up a database directory.
 */
final class ResourceConfigurationPathStatisticsTest {

  @Test
  void defaultIsFalse() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("test-default").build();
    assertFalse(config.withPathStatistics);
  }

  @Test
  void enabledWithPathSummary() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("test-enabled")
        .buildPathSummary(true)
        .buildPathStatistics(true)
        .build();
    assertTrue(config.withPathStatistics);
    assertTrue(config.withPathSummary);
  }

  @Test
  void enablingWithoutPathSummaryThrows() {
    final ResourceConfiguration.Builder builder = new ResourceConfiguration.Builder("test-invalid")
        .buildPathSummary(false);
    final var ex = assertThrows(IllegalStateException.class,
        () -> builder.buildPathStatistics(true));
    assertTrue(ex.getMessage().contains("buildPathSummary"),
        "message should reference buildPathSummary dependency: " + ex.getMessage());
  }

  @Test
  void disablingIsAlwaysAllowed() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("test-disable")
        .buildPathSummary(false)
        .buildPathStatistics(false)
        .build();
    assertFalse(config.withPathStatistics);
  }
}
