package io.sirix.index.aggregate;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TemporalAggregateDomainTest {
  @Test
  void preservesMillisecondOrderAcrossDayBoundaries() {
    final TemporalAggregateDomain domain = new TemporalAggregateDomain();
    final Instant midnight = Instant.parse("2024-02-29T00:00:00Z");
    final long coordinate = domain.point(midnight);
    assertEquals(0, coordinate & ((1L << 27) - 1));
    assertTrue(domain.point(midnight.minusMillis(1)) < coordinate);
    assertEquals(coordinate + 1, domain.point(midnight.plusMillis(1)));
    assertEquals(coordinate + (1L << 27), domain.point(midnight.plusSeconds(86_400)));
    assertEquals(0, domain.lowerBound(null));
    assertEquals(0, domain.point(Instant.EPOCH));
    final Instant last = Instant.ofEpochMilli((1L << 48) - 2);
    assertTrue(domain.isExact(last));
    assertTrue(domain.point(last) < domain.maxValue());
    assertFalse(domain.isExact(last.plusMillis(1)));
    assertFalse(domain.isExact(Instant.EPOCH.minusMillis(1)));
    assertFalse(domain.isExact(midnight.plusNanos(1)));
    assertThrows(IllegalArgumentException.class, () -> domain.point(midnight.plusNanos(1)));
  }
}
