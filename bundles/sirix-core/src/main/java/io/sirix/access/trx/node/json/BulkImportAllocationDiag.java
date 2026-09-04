/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import java.util.concurrent.atomic.LongAdder;

/**
 * Exact allocation counters for the bulk JSON import path, activated by
 * {@code -Dsirix.ingestAllocDiag=true}; off by default and folded away by the JIT when off.
 *
 * <p>
 * A sampling profiler tells you which frame allocates and roughly how much; on an aggressively
 * inlined scanner loop it cannot tell you the line, and its weights are estimates. These counters
 * exist to turn one such estimate into an exact number: how many transient names the scanner mints,
 * how long they are, and how many of those mints were pure waste because the canonical instance was
 * already in the table.
 * </p>
 *
 * <p>
 * The scanner interns one {@link String} per object-key OCCURRENCE so the canonical instance stays
 * stable for downstream identity-hashed memos. Every occurrence past the first allocates a String
 * and its Latin-1 {@code byte[]} purely to be used as a map key and dropped — which is the quantity
 * these counters price.
 * </p>
 */
public final class BulkImportAllocationDiag {

  /** Read once at class initialisation so every call site folds away when the property is absent. */
  public static final boolean ENABLED = Boolean.getBoolean("sirix.ingestAllocDiag");

  /** Object-key occurrences the scanner interned — one transient String each. */
  private static final LongAdder NAME_INTERN_CALLS = new LongAdder();

  /** Characters across those names; the Latin-1 payload the transient {@code byte[]} carries. */
  private static final LongAdder NAME_INTERN_CHARS = new LongAdder();

  /** Interns that found no canonical instance and therefore had to keep the String they built. */
  private static final LongAdder NAME_INTERN_MISSES = new LongAdder();

  /** Key occurrences resolved from the decode buffer with NO allocation at all. */
  private static final LongAdder CANONICAL_NAME_LOOKUPS = new LongAdder();

  /** Characters across those lookups; what the String-keyed lane would have copied. */
  private static final LongAdder CANONICAL_NAME_CHARS = new LongAdder();

  /** Object header + one reference field + the length field, rounded to the 8-byte object grain. */
  private static final long STRING_OBJECT_BYTES = 24L;

  /** Array header for the Latin-1 value array, before its rounded payload. */
  private static final long BYTE_ARRAY_HEADER_BYTES = 16L;

  static {
    if (ENABLED) {
      Runtime.getRuntime().addShutdownHook(new Thread(BulkImportAllocationDiag::dump, "bulk-import-alloc-diag-dump"));
    }
  }

  private BulkImportAllocationDiag() {
    throw new AssertionError("no instances");
  }

  /**
   * Record one key occurrence resolved from the decode buffer without building a String — the same
   * denominator {@link #recordNameIntern} counts, so the two lanes are directly comparable.
   *
   * @param chars length of the name in characters
   */
  public static void recordCanonicalNameLookup(final int chars) {
    CANONICAL_NAME_LOOKUPS.increment();
    CANONICAL_NAME_CHARS.add(chars);
  }

  /** Key occurrences that allocated nothing. */
  public static long canonicalNameLookups() {
    return CANONICAL_NAME_LOOKUPS.sum();
  }

  /** Bytes the String-keyed lane would have allocated for those same occurrences. */
  public static long canonicalNameLookupsAvoidedBytes() {
    final long calls = CANONICAL_NAME_LOOKUPS.sum();
    return calls * (STRING_OBJECT_BYTES + BYTE_ARRAY_HEADER_BYTES) + CANONICAL_NAME_CHARS.sum() + calls * 4L;
  }

  /**
   * Record one name intern.
   *
   * @param chars length of the interned name in characters
   * @param hit whether the canonical instance already existed, making the built String garbage
   */
  public static void recordNameIntern(final int chars, final boolean hit) {
    NAME_INTERN_CALLS.increment();
    NAME_INTERN_CHARS.add(chars);
    if (!hit) {
      NAME_INTERN_MISSES.increment();
    }
  }

  /** Interned object-key occurrences. */
  public static long nameInternCalls() {
    return NAME_INTERN_CALLS.sum();
  }

  /** Characters across every interned occurrence. */
  public static long nameInternChars() {
    return NAME_INTERN_CHARS.sum();
  }

  /** Interns whose String had to be kept because the name was new. */
  public static long nameInternMisses() {
    return NAME_INTERN_MISSES.sum();
  }

  /**
   * Bytes the interning allocated, counting a {@link String} plus a Latin-1 {@code byte[]} per
   * occurrence. A Latin-1 name of {@code n} characters occupies {@code n} bytes in the value array;
   * the estimate is exact for names that stay in the Latin-1 range, which every JSON object key in a
   * schema-shaped corpus does.
   */
  public static long nameInternBytes() {
    final long calls = NAME_INTERN_CALLS.sum();
    final long chars = NAME_INTERN_CHARS.sum();
    return calls * (STRING_OBJECT_BYTES + BYTE_ARRAY_HEADER_BYTES) + roundToObjectGrain(chars, calls);
  }

  /**
   * Sum of each value array's payload rounded up to the 8-byte grain, approximated over the total.
   */
  private static long roundToObjectGrain(final long chars, final long calls) {
    // Rounding each array individually would need the per-name distribution; over a schema-shaped
    // corpus every occurrence of a given key has the same length, so the mean rounding error is
    // under 4 bytes per occurrence and is added here as that mean.
    return chars + calls * 4L;
  }

  private static void dump() {
    final long lookups = CANONICAL_NAME_LOOKUPS.sum();
    if (lookups > 0L) {
      System.out.printf(
          "[BulkImportAllocDiag] canonical name lookups: occurrences=%,d  chars=%,d  allocated=0 B"
              + "  (avoided %,d B = %.2f GB the String-keyed lane would have built)%n",
          lookups, CANONICAL_NAME_CHARS.sum(), canonicalNameLookupsAvoidedBytes(),
          canonicalNameLookupsAvoidedBytes() / (double) (1L << 30));
      System.out.flush();
    }
    final long calls = NAME_INTERN_CALLS.sum();
    if (calls == 0L) {
      return;
    }
    final long chars = NAME_INTERN_CHARS.sum();
    final long misses = NAME_INTERN_MISSES.sum();
    System.out.printf(
        "[BulkImportAllocDiag] name interning: occurrences=%,d  distinctMints=%,d  chars=%,d (avg %.1f)"
            + "  allocated=%,d B (%.2f GB), of which %.2f GB is garbage on the spot%n",
        calls, misses, chars, calls == 0L
            ? 0.0d
            : (double) chars / calls,
        nameInternBytes(), nameInternBytes() / (double) (1L << 30),
        nameInternBytes() * (calls - misses) / (double) calls / (double) (1L << 30));
    System.out.flush();
  }
}
