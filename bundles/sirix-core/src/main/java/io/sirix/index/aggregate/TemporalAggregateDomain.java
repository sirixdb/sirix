package io.sirix.index.aggregate;

import io.sirix.index.interval.IntervalDomain;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Exact millisecond coordinates with UTC days aligned to a binary radix. Unused coordinates at
 * the end of each day are never queried. This preserves order while avoiding fragmentation of
 * day-aligned intervals into millisecond-sized canonical covers.
 */
public final class TemporalAggregateDomain {
  private static final long MILLIS_PER_DAY = 86_400_000L;
  private static final int DAY_SHIFT = 27;
  private static final IntervalDomain EXACT_DOMAIN = new IntervalDomain();

  public int height() {
    return 49;
  }

  public long maxValue() {
    return (1L << height()) - 1;
  }

  public boolean isExact(final @Nullable Instant instant) {
    return EXACT_DOMAIN.isExact(instant);
  }

  public long lowerBound(final @Nullable Instant instant) {
    return instant == null ? 0 : point(instant);
  }

  public long point(final Instant instant) {
    if (!isExact(instant)) {
      throw new IllegalArgumentException("Temporal aggregate instant is outside the exact domain");
    }
    final long millis = instant.toEpochMilli();
    return ((millis / MILLIS_PER_DAY) << DAY_SHIFT) | (millis % MILLIS_PER_DAY);
  }
}
