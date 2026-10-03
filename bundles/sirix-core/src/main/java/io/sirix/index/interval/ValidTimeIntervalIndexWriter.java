/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

/**
 * Writer-side wiring of a valid-time interval index: a {@link RelationalIntervalTree} over a single
 * persistent HOT sub-tree, plus the logic that maps a record OBJECT's {@code (validFrom, validTo)}
 * fields onto an interval and registers/unregisters it.
 *
 * <p>
 * The two RI-tree stores (lower keyed {@code (fork, lo)}, upper keyed {@code (fork, hi)}) are both
 * realised on the same HOT sub-tree via a one-byte store discriminator — see
 * {@link HotOrderedStore} / {@link ValidTimeKey}. The record reference registered is the containing
 * OBJECT's node key (the same record identity the CAS-narrowing path resolves), so the query side
 * can re-read the exact {@code validFrom}/{@code validTo} instants off that object and re-verify.
 * </p>
 *
 * <p>
 * Open-ended intervals are supported: a missing/unparseable {@code validFrom} maps to "valid since
 * the beginning" and a missing/unparseable {@code validTo} to "valid forever" (via
 * {@link IntervalDomain}). A record is registered when <em>at least one</em> bound is present and
 * the resulting interval is non-inverted ({@code lo <= hi}) — exactly matching the exact predicate
 * ({@code ValidTimeIndexScan.isValidAtTime}), which treats an absent bound as unbounded on that
 * side. A record with neither bound, or an inverted interval, matches no {@code x} in the scan and
 * so is not registered.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeIntervalIndexWriter {

  private final RelationalIntervalTree tree;
  private final OrderedStore verificationStore;
  private final OrderedStore membershipStore;
  private final OrderedStore orderStore;
  private final IntervalDomain domain;
  private final String validFromField;
  private final String validToField;

  /**
   * @param tree the RI-tree (its two stores share one HOT sub-tree, writer-backed)
   * @param domain the instant&harr;[1,2^h-1] domain map (its height must equal the tree's)
   * @param validFromField the local field name of the valid-time start (e.g. {@code validFrom})
   * @param validToField the local field name of the valid-time end (e.g. {@code validTo})
   */
  public ValidTimeIntervalIndexWriter(final RelationalIntervalTree tree, final OrderedStore verificationStore,
      final OrderedStore membershipStore, final OrderedStore orderStore, final IntervalDomain domain,
      final String validFromField, final String validToField) {
    this.tree = tree;
    this.verificationStore = verificationStore;
    this.membershipStore = membershipStore;
    this.orderStore = orderStore;
    this.domain = domain;
    this.validFromField = validFromField;
    this.validToField = validToField;
  }

  /**
   * An interval extracted from a record's bounds. {@code present} controls registration in the
   * RI-tree. Ambiguous duplicates can instead require a verification-only posting even when the
   * selected pair is inverted; the query's original field lookup decides their answer. The parent
   * posting follows either form, independently of whether the interval is exact at millisecond
   * resolution.
   */
  public record Interval(boolean present, long lo, long hi, boolean exact, long parentKey, boolean verificationOnly) {
    static final Interval ABSENT = new Interval(false, 0L, 0L, false, -1L, false);

    public Interval withExactLexicalBounds(final boolean lexical) {
      return exact && !lexical
          ? new Interval(present, lo, hi, false, parentKey, verificationOnly)
          : this;
    }

    public boolean hasPostings() {
      return present || verificationOnly;
    }

    public Interval atParent(final long parent) {
      return hasPostings()
          ? new Interval(present, lo, hi, exact, parent, verificationOnly)
          : this;
    }
  }

  public IntervalDomain domain() {
    return domain;
  }

  /**
   * Read the valid-time interval of the OBJECT at the rtx's current position, mapping it into the
   * integer domain. Navigates the object's direct fused field children by name and restores the
   * cursor to the object on return.
   *
   * @param rtx a read cursor positioned at the record OBJECT (or fused {@code OBJECT_NAMED_OBJECT})
   * @return the extracted interval (possibly {@link Interval#ABSENT})
   */
  public Interval readIntervalAtCursor(final JsonNodeReadOnlyTrx rtx) {
    final long objectKey = rtx.getNodeKey();
    final long parentKey = rtx.getParentKey();
    Instant from = null;
    Instant to = null;
    boolean fromLexical = false;
    boolean toLexical = false;
    long fromCount = 0;
    long toCount = 0;
    try {
      if (rtx.moveToFirstChild()) {
        do {
          final String fieldName = fieldNameAtCursor(rtx);
          if (fieldName == null) {
            continue;
          }
          if (validFromField.equals(fieldName)) {
            fromCount++;
            if (from == null) {
              from = readInstantOfFieldAtCursor(rtx);
              fromLexical = from != null && isExactLexicalBound(rtx.getValue());
            }
          } else if (validToField.equals(fieldName)) {
            toCount++;
            if (to == null) {
              to = readInstantOfFieldAtCursor(rtx);
              toLexical = to != null && isExactLexicalBound(rtx.getValue());
            }
          }
        } while (rtx.moveToRightSibling());
      }
    } finally {
      rtx.moveTo(objectKey);
    }
    return toInterval(from, to, fromCount, toCount).withExactLexicalBounds(fromLexical && toLexical)
                                                   .atParent(parentKey);
  }

  /**
   * Map a {@code (validFrom, validTo)} instant pair into the integer domain. Mirrors
   * {@link #readIntervalAtCursor} so the listener (which already holds the parsed instants from the
   * change event) and the builder agree on registrability.
   */
  public Interval toInterval(final @Nullable Instant from, final @Nullable Instant to) {
    return toInterval(from, to, 1, 1);
  }

  /** Duplicate fields retain the exact-predicate fallback instead of trusting the selected bounds. */
  public Interval toInterval(final @Nullable Instant from, final @Nullable Instant to, final long fromCount,
      final long toCount) {
    // A record with no parseable validFrom AND no parseable validTo carries no interval at all —
    // the exact predicate never matches it, so don't register. One bound present => open-ended
    // interval.
    final boolean duplicates = fromCount > 1 || toCount > 1;
    if (from == null && to == null) {
      return duplicates
          ? new Interval(false, 0, 0, false, -1, true)
          : Interval.ABSENT;
    }
    final long lo = domain.lowerBound(from); // null -> 1 (open-ended start)
    final long hi = domain.upperBound(to); // null -> maxValue (open-ended end)
    if (lo > hi) {
      // Inverted interval: never stabbed by any x in the scan's exact predicate.
      // Duplicate lookup semantics need the original field predicate even if the builder's
      // first-parseable pair is inverted. Keep an exceptional posting without a tree interval.
      return duplicates
          ? new Interval(false, 0, 0, false, -1, true)
          : Interval.ABSENT;
    }
    return new Interval(true, lo, hi, fromCount == 1 && toCount == 1 && domain.isExact(from) && domain.isExact(to), -1L,
        false);
  }

  /** Register {@code [lo, hi]} for the record object {@code ref} in the RI-tree. */
  public void insert(final long ref, final Interval interval) {
    if (interval.present()) {
      tree.insert(ref, interval.lo(), interval.hi());
    }
    membershipStore.insert(interval.parentKey(), 0, ref);
    if (!interval.exact()) {
      verificationStore.insert(0, 0, ref);
    }
  }

  /** Remove {@code [lo, hi]} for the record object {@code ref} from the RI-tree. */
  public void delete(final long ref, final Interval interval) {
    if (interval.present()) {
      tree.delete(ref, interval.lo(), interval.hi());
    }
    membershipStore.remove(interval.parentKey(), 0, ref);
    if (!interval.exact()) {
      verificationStore.remove(0, 0, ref);
    }
  }

  /**
   * Convenience: register the interval of the OBJECT at the cursor; returns the registered interval.
   */
  public Interval indexObjectAtCursor(final JsonNodeReadOnlyTrx rtx) {
    final long objectKey = rtx.getNodeKey();
    final Interval interval = readIntervalAtCursor(rtx);
    if (interval.hasPostings()) {
      insert(objectKey, interval);
      checkOrder(objectKey, interval.parentKey(), rtx.getLeftSiblingKey(), rtx.getRightSiblingKey());
    }
    return interval;
  }

  /**
   * Once a sibling inversion is observed, retain this conservative guard in descendant revisions.
   * Rebuilding the index can establish orderedness again. No per-array mutable state is retained.
   */
  public void checkOrder(final long key, final long parent, final long left, final long right) {
    if (left > key || right >= 0 && right < key) {
      orderStore.insert(parent, 0, 0);
    }
  }

  /**
   * The local field name at the cursor — the field whose value this node carries. Works for the fused
   * named-primitive shape ({@code OBJECT_NAMED_*}, name on the node itself) and the legacy shape (an
   * {@code OBJECT_KEY} whose name is the field name).
   */
  private static @Nullable String fieldNameAtCursor(final JsonNodeReadOnlyTrx rtx) {
    final var name = rtx.getName();
    return name == null
        ? null
        : name.getLocalName();
  }

  /**
   * Read the string instant value of the field at the cursor.
   *
   * <p>
   * SirixDB is fusion-only (the legacy {@code OBJECT_KEY -> STRING_VALUE} shape was removed in Phase
   * 4): a valid-time string field is a fused {@code OBJECT_NAMED_STRING} whose value is inline, so
   * {@code rtx.getValue()} reads it directly. The {@code isStringValue()} guard also covers any
   * hypothetical string-value-bearing node generically. Returns {@code null} if the value is missing
   * or not a canonical ISO-8601 instant.
   */
  private static @Nullable Instant readInstantOfFieldAtCursor(final JsonNodeReadOnlyTrx rtx) {
    final NodeKind kind = rtx.getKind();
    String raw = null;
    if (kind == NodeKind.OBJECT_NAMED_STRING || rtx.isStringValue()) {
      raw = rtx.getValue();
    }
    return parseInstant(raw);
  }

  /**
   * Lexical subset shared by Instant and xs:dateTime. Parsing has already validated the value;
   * exclude leap seconds, extended/year-zero dates and relaxed ISO spellings which the two parsers do
   * not interpret identically. Fractional precision is checked by IntervalDomain.
   */
  public static boolean isExactLexicalBound(final @Nullable String raw) {
    if (raw == null || raw.length() < 20 || raw.charAt(4) != '-' || raw.charAt(10) != 'T' || raw.charAt(0) < '0'
        || raw.charAt(0) > '9' || raw.startsWith("0000") || raw.charAt(17) == '6') {
      return false;
    }
    final int length = raw.length();
    return raw.charAt(length - 1) == 'Z'
        || (raw.charAt(length - 6) == '+' || raw.charAt(length - 6) == '-') && raw.charAt(length - 3) == ':';
  }

  /** Parse a canonical ISO-8601 UTC instant string, or {@code null} on any failure. */
  public static @Nullable Instant parseInstant(final @Nullable String raw) {
    if (raw == null) {
      return null;
    }
    try {
      return Instant.parse(raw);
    } catch (final Exception e) {
      return null;
    }
  }
}
