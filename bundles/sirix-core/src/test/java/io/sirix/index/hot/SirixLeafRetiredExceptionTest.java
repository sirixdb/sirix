package io.sirix.index.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.exception.SirixLeafRetiredException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * FMT-304's interim retirement failure exposes the failed page and elapsed wait to callers. The
 * constructor's nonnegative-duration contract supplies the boundary oracle; literal page keys and
 * durations reject lost diagnostics independently of the writer's retry implementation.
 */
final class SirixLeafRetiredExceptionTest {

  @ParameterizedTest
  @ValueSource(longs = {0L, 1L, 2_000_000L, Long.MAX_VALUE})
  void preservesTheReportedPageAndEveryNonnegativeWait(final long waitedNanos) {
    final SirixLeafRetiredException failure = new SirixLeafRetiredException(73L, waitedNanos);

    assertEquals(73L, failure.getPageKey());
    assertEquals(waitedNanos, failure.getWaitedNanos());
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, Long.MIN_VALUE})
  void rejectsNegativeWaits(final long waitedNanos) {
    assertThrows(IllegalArgumentException.class, () -> new SirixLeafRetiredException(73L, waitedNanos));
  }

  @Test
  void describesTheRetiredPageAndWaitInMilliseconds() {
    final SirixLeafRetiredException failure = new SirixLeafRetiredException(73L, 2_500_000L);

    assertTrue(failure.getMessage().contains("HOT leaf 73 was retired by page-cache eviction"));
    assertTrue(failure.getMessage().contains("within 2 ms."));
  }
}
