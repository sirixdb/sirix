/*
 * [New BSD License]
 * Copyright (c) 2024, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import java.util.function.LongConsumer;

/**
 * Relational Interval Tree (Kriegel, Pötke, Seidl) — interval indexing realised on ordinary ordered
 * indexes, giving output-sensitive {@code O(h·log n + k)} stabbing queries with a hard {@code O(h)}
 * bound on index probes (the property that makes worst-case latency predictable).
 *
 * <p>A virtual perfect binary tree labels the domain {@code [1, 2^h - 1]} in-order: the root is
 * {@code 2^(h-1)} and a node's level equals the trailing-zero count of its value. An interval
 * {@code [lo, hi]} is <em>registered</em> at its fork node — the highest node value inside
 * {@code [lo, hi]} ({@link #forkNode}) — into two ordered stores: a {@code lower} store keyed
 * {@code (fork, lo)} and an {@code upper} store keyed {@code (fork, hi)}.
 *
 * <p>A stabbing query "which intervals contain x?" walks the root&rarr;x path (at most {@code h}
 * nodes). An interval forked at a path node {@code f} satisfies {@code lo <= f <= hi}, and since
 * {@code f} is an ancestor of {@code x}, exactly one endpoint test remains:
 * <ul>
 *   <li>{@code f > x} (turned left): {@code x <= f <= hi} already &rArr; stabs x iff {@code lo <= x}
 *       &rarr; scan {@code lower(f)} for {@code lo in [MIN, x]};</li>
 *   <li>{@code f < x} (turned right): {@code lo <= f <= x} already &rArr; stabs x iff {@code hi >= x}
 *       &rarr; scan {@code upper(f)} for {@code hi in [x, MAX]};</li>
 *   <li>{@code f == x}: every interval forked here stabs x &rarr; scan {@code lower(x)} fully.</li>
 * </ul>
 * Exact (no false positives) and complete (every stabbing interval's fork is on the path); proved by
 * randomized differential testing against a brute-force filter.
 *
 * <p>Domain mapping (timestamps &harr; {@code [1, 2^h - 1]}) is the caller's concern — see
 * {@link IntervalDomain}. Stateless apart from the two stores; concurrency is delegated to them.
 *
 * @author Johannes Lichtenberger
 */
public final class RelationalIntervalTree {

  /** Inclusive lower domain bound. Endpoints and query points must lie in {@code [1, maxValue]}. */
  public static final long MIN_VALUE = 1L;

  private final int height;
  private final long maxValue;
  private final OrderedStore lower;
  private final OrderedStore upper;

  /**
   * @param height the virtual-tree height {@code h}; domain is {@code [1, 2^h - 1]}. Must satisfy
   *               {@code 2 <= h <= 62} and {@code 2^h - 1 >=} every endpoint ever registered/queried.
   * @param lower  store keyed {@code (fork, lo)}
   * @param upper  store keyed {@code (fork, hi)}
   */
  public RelationalIntervalTree(final int height, final OrderedStore lower, final OrderedStore upper) {
    if (height < 2 || height > 62) {
      throw new IllegalArgumentException("height out of range [2,62]: " + height);
    }
    this.height = height;
    this.maxValue = (1L << height) - 1L;
    this.lower = lower;
    this.upper = upper;
  }

  public int height() {
    return height;
  }

  public long maxValue() {
    return maxValue;
  }

  /**
   * Fork node of {@code [lo, hi]} (with {@code 1 <= lo <= hi}): the highest node value (most trailing
   * zeros, closest to the root) lying inside {@code [lo, hi]}. {@code O(1)}, branchless.
   *
   * <p>With {@code a = lo - 1}, a multiple of {@code 2^p} lies in {@code (a, hi]} iff
   * {@code floor(hi/2^p) > floor(a/2^p)}; the largest such {@code p} is the index of the highest set
   * bit of {@code a ^ hi}. The witness {@code (hi >>> p) << p} has exactly {@code p} trailing zeros —
   * the unique level-{@code p} node in the interval.
   */
  public static long forkNode(final long lo, final long hi) {
    final int p = 63 - Long.numberOfLeadingZeros((lo - 1) ^ hi);
    return (hi >>> p) << p;
  }

  /** Register the interval {@code [lo, hi]} for record {@code ref}. */
  public void insert(final long ref, final long lo, final long hi) {
    checkInterval(lo, hi);
    final long fork = forkNode(lo, hi);
    lower.insert(fork, lo, ref);
    upper.insert(fork, hi, ref);
  }

  /** Remove the interval {@code [lo, hi]} for record {@code ref} (must match a prior insert). */
  public void delete(final long ref, final long lo, final long hi) {
    checkInterval(lo, hi);
    final long fork = forkNode(lo, hi);
    lower.remove(fork, lo, ref);
    upper.remove(fork, hi, ref);
  }

  /**
   * Stabbing query: stream every {@code ref} whose interval contains {@code x} ({@code lo <= x <= hi})
   * to {@code out}, in at most {@code h} store scans. Each match is delivered once (unless a ref was
   * registered more than once).
   *
   * @param x   query point; if outside {@code [1, maxValue]} the result is empty
   * @param out receiver for matching refs
   */
  public void stab(final long x, final LongConsumer out) {
    if (x < MIN_VALUE || x > maxValue) {
      return;
    }
    long node = 1L << (height - 1); // root value
    int level = height - 1;
    while (true) {
      if (node == x) {
        lower.scan(node, MIN_VALUE, maxValue, out); // every interval forked at x stabs x
        return;
      }
      final long half = 1L << (level - 1);
      if (x < node) {
        lower.scan(node, MIN_VALUE, x, out); // node > x: need lo <= x
        node -= half;
      } else {
        upper.scan(node, x, maxValue, out);  // node < x: need hi >= x
        node += half;
      }
      level--;
      if (level < 0) {
        return; // unreachable for x in [1, maxValue]; defensive
      }
    }
  }

  private void checkInterval(final long lo, final long hi) {
    if (lo < MIN_VALUE || hi > maxValue || lo > hi) {
      throw new IllegalArgumentException("interval [" + lo + "," + hi + "] outside [1," + maxValue + "]");
    }
  }
}
