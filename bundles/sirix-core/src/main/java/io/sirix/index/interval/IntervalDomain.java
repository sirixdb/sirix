/*
 * [New BSD License]
 * Copyright (c) 2024, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import java.time.Instant;

/**
 * Maps valid-time instants onto the {@link RelationalIntervalTree} integer domain {@code [1, 2^h-1]}.
 *
 * <p>The mapping is {@code domain = clamp(epochMilli - originMillis + 1, 1, maxValue)} at millisecond
 * resolution. It is <em>monotonic</em>, which is the only property the index needs: a true match
 * {@code lo <= x <= hi} (compared as exact instants) implies {@code map(lo) <= map(x) <= map(hi)}
 * (floor-to-milli and clamping both preserve order), so the index returns a SUPERSET of matches — no
 * false negatives. False positives (sub-millisecond ties, clamped out-of-range instants) are removed
 * by the caller's exact-instant re-verification, exactly as with the CAS-index narrowing path.
 *
 * <p>Open-ended intervals: a missing/over-range {@code validFrom} maps to {@code 1} ("valid since the
 * beginning of time"), a missing/over-range {@code validTo} to {@code maxValue} ("valid forever").
 *
 * @author Johannes Lichtenberger
 */
public final class IntervalDomain {

  /** Default height: domain {@code [1, 2^48-1]} ms &asymp; 8920 years from the origin. */
  public static final int DEFAULT_HEIGHT = 48;

  private final int height;
  private final long originMillis;
  private final long maxValue;

  /** Domain anchored at the Unix epoch (1970-01-01T00:00:00Z), height {@link #DEFAULT_HEIGHT}. */
  public IntervalDomain() {
    this(DEFAULT_HEIGHT, 0L);
  }

  /**
   * @param height       virtual-tree height (domain {@code [1, 2^height-1]}); {@code 2 <= height <= 62}
   * @param originMillis the epoch-millisecond mapped to domain value {@code 1}
   */
  public IntervalDomain(final int height, final long originMillis) {
    if (height < 2 || height > 62) {
      throw new IllegalArgumentException("height out of range [2,62]: " + height);
    }
    this.height = height;
    this.originMillis = originMillis;
    this.maxValue = (1L << height) - 1L;
  }

  public int height() {
    return height;
  }

  public long maxValue() {
    return maxValue;
  }

  /** Monotonic milli-resolution map into {@code [1, maxValue]}. */
  public long toDomain(final long epochMilli) {
    // careful with overflow: do the subtraction in a way that saturates rather than wraps.
    final long shifted = epochMilli - originMillis; // may overflow only for extreme inputs
    if (epochMilli < originMillis || shifted == Long.MIN_VALUE) {
      return 1L;
    }
    final long v = shifted + 1L;
    if (v < 1L) {
      return maxValue; // overflowed positive-large
    }
    return Math.min(v, maxValue);
  }

  /** Lower endpoint for a (possibly open) interval start; {@code null} =&gt; {@code 1}. */
  public long lowerBound(final Instant validFrom) {
    return validFrom == null ? RelationalIntervalTree.MIN_VALUE : toDomain(validFrom.toEpochMilli());
  }

  /** Upper endpoint for a (possibly open) interval end; {@code null} =&gt; {@code maxValue}. */
  public long upperBound(final Instant validTo) {
    return validTo == null ? maxValue : toDomain(validTo.toEpochMilli());
  }

  /** Query point for an instant. */
  public long point(final Instant at) {
    return toDomain(at.toEpochMilli());
  }
}
