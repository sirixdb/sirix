/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Canonical JSON object-key strings, looked up directly from the scanner's decode buffer.
 *
 * <p>
 * The scanner needs the canonical {@link String} for every key OCCURRENCE, and a map keyed by
 * {@code String} forces one to be BUILT before it can be looked up — a String header plus its
 * Latin-1 {@code byte[]}, thrown away the moment the map returns the instance it already held. On a
 * 1M-row ClickBench load that is 105,000,000 occurrences and 5.39 GB, 35 % of everything the load
 * allocates, of which 104,941,200 mints were pure waste. This table is keyed by the
 * {@code (char[], offset, length)} slice instead, so a repeat occurrence allocates nothing at all:
 * it hashes and compares against the stored instance in place.
 * </p>
 *
 * <p>
 * <b>Shared across the import, not per chunk.</b> The parallel importer builds one scanner per
 * chunk, so a per-scanner table canonicalises only within a chunk — the same 1M load minted the
 * same 105 names 560 times over. One table for the whole import makes the canonical instance
 * global, which is what the downstream memos want (they hash it, and a shared instance turns their
 * {@code equals} into a pointer comparison on the first test).
 * </p>
 *
 * <p>
 * <b>Concurrency.</b> Open addressing with linear probing over an {@link AtomicReferenceArray},
 * published by {@code compareAndExchange}. Reads — the overwhelming majority, since the key set is
 * fixed after the first few rows — are plain acquire loads with no lock and no allocation. Two
 * threads racing on the same name probe the same slot sequence and meet at the same empty slot; the
 * loser is handed the winner's instance and returns it, so exactly one canonical instance per name
 * survives. Threads racing on DIFFERENT names that collide simply probe on.
 * </p>
 *
 * <p>
 * <b>Bounded, and degrades instead of failing.</b> The table never resizes. A document whose object
 * keys are effectively unbounded (keys carrying data rather than schema) would otherwise turn a
 * scanner-local cache into an unbounded retainer; past {@link #MAX_PROBES} the table stops trying
 * and returns a fresh String — which is exactly today's cost and today's behaviour, since the
 * canonical instance is a performance property and never a correctness one (the PCR and name memos
 * are value-equality maps). Load stays under 1/2 for any key set that fits.
 * </p>
 */
final class NameInternTable {

  /** Distinct names a default table canonicalises before it starts handing out fresh instances. */
  static final int DEFAULT_CAPACITY = 1 << 14;

  /**
   * Probes before giving up on a name. Bounds the worst case of a full table to a fixed cost rather
   * than a scan, and is the only thing that decides whether an over-full table is slow or hostile.
   */
  private static final int MAX_PROBES = 8;

  /** Fibonacci mixer; the 31-based accumulation alone clusters short ASCII names badly. */
  private static final int MIX = 0x9E3779B9;

  private final AtomicReferenceArray<String> slots;

  private final int mask;

  /**
   * @param capacity slots; must be a positive power of two
   * @throws IllegalArgumentException if {@code capacity} is not a positive power of two
   */
  NameInternTable(final int capacity) {
    if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
      throw new IllegalArgumentException("capacity must be a positive power of two: " + capacity);
    }
    this.slots = new AtomicReferenceArray<>(capacity);
    this.mask = capacity - 1;
  }

  NameInternTable() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * The canonical instance for a name held in a char slice, allocating only the first time that name
   * is seen.
   *
   * @param chars buffer holding the decoded name
   * @param offset first character of the name
   * @param length characters in the name
   * @return the canonical instance, or a fresh equal instance when the table is full
   * @throws IndexOutOfBoundsException if the slice is not inside {@code chars}
   */
  String intern(final char[] chars, final int offset, final int length) {
    if (offset < 0 || length < 0 || offset + length > chars.length) {
      throw new IndexOutOfBoundsException("slice [" + offset + ", " + (offset + length) + ") of " + chars.length);
    }
    int index = hash(chars, offset, length) & mask;
    for (int probe = 0; probe < MAX_PROBES; probe++) {
      final String resident = slots.getAcquire(index);
      if (resident == null) {
        // First sighting: this is the ONE allocation this name is allowed over the whole import.
        final String minted = new String(chars, offset, length);
        final String winner = slots.compareAndExchange(index, null, minted);
        if (winner == null) {
          return minted;
        }
        // Lost the slot. If the winner is this same name, its instance is the canonical one and
        // the String just built is dropped; otherwise the slot belongs to another name, probe on.
        if (matches(winner, chars, offset, length)) {
          return winner;
        }
      } else if (matches(resident, chars, offset, length)) {
        return resident;
      }
      index = (index + 1) & mask;
    }
    // Table full along this probe path. Correct, just no longer free — see the class comment.
    return new String(chars, offset, length);
  }

  /** Hash of a char slice; must not depend on a String existing. */
  private static int hash(final char[] chars, final int offset, final int length) {
    int h = 0;
    for (int i = 0; i < length; i++) {
      h = 31 * h + chars[offset + i];
    }
    h *= MIX;
    return h ^ (h >>> 16);
  }

  /** Whether {@code resident} holds exactly the characters of the slice. */
  private static boolean matches(final String resident, final char[] chars, final int offset, final int length) {
    if (resident.length() != length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (resident.charAt(i) != chars[offset + i]) {
        return false;
      }
    }
    return true;
  }

  /** Distinct names currently canonicalised; diagnostics and tests only. */
  int size() {
    int size = 0;
    for (int i = 0; i < slots.length(); i++) {
      if (slots.getAcquire(i) != null) {
        size++;
      }
    }
    return size;
  }
}
