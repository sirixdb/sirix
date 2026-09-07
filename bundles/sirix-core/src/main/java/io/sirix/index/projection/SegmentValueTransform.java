/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.SegmentGroupCanonicaliser.CellResolver;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.util.BitSet;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

/**
 * Query-local string transform with bounded retention of group representatives. Raw storage order
 * is deliberately NOT exposed as transformed value order: a transform can reorder arbitrary values.
 */
final class SegmentValueTransform implements CellResolver {
  static final long CACHE_BYTES = 64L << 20;
  /** At most 4 MiB of identity markers; larger group spaces keep the ordinary bounded fallback. */
  private static final int IDENTITY_LIMIT = 32 << 20;

  private final CellResolver source;
  private final UnaryOperator<String> transform;
  private final long budget;
  private final Long2ObjectOpenHashMap<String> representatives = new Long2ObjectOpenHashMap<>();
  private final BitSet unchanged = new BitSet();
  private long retainedBytes;

  SegmentValueTransform(final CellResolver source, final UnaryOperator<String> transform, final long budget) {
    this.source = requireNonNull(source, "source must not be null");
    this.transform = requireNonNull(transform, "transform must not be null");
    if (budget < 0) {
      throw new IllegalArgumentException("budget must not be negative: " + budget);
    }
    this.budget = budget;
  }

  @Override
  public @Nullable String valueOfCell(final long cell) {
    final String raw = source.valueOfCell(cell);
    return raw == null ? null : apply(raw);
  }

  @Nullable String apply(final String raw) {
    return transform.apply(raw);
  }

  /** Called only under the canonicaliser's monitor, like {@link #remember}. */
  @Nullable String representative(final int canonical, final long cell) {
    if (unchanged.get(canonical)) {
      return source.valueOfCell(cell); // an identity result already lives in the source dictionary
    }
    final String cached = representatives.get(cell);
    return cached != null ? cached : valueOfCell(cell);
  }

  /**
   * Cache by representative CELL, not every input cell. Above the budget equality rereads only the
   * representative; the incoming value is still evaluated once. Even with no cache this performs
   * fewer transforms than the old hash-then-transform-both-cells equality path.
   */
  void remember(final int canonical, final long cell, final String value, final boolean identity) {
    if (identity && canonical < IDENTITY_LIMIT) {
      unchanged.set(canonical);
      return;
    }
    // Allow two bytes per UTF-16 unit plus String/array headers, alignment and hash-table capacity
    // (including the old table during a growth). Large or expanding transforms cannot defeat this cap.
    final long bytes = 128L + 2L * value.length();
    if (bytes <= budget - retainedBytes) {
      representatives.put(cell, value);
      retainedBytes += bytes;
    }
  }

  long retainedBytes() {
    return retainedBytes;
  }
}
