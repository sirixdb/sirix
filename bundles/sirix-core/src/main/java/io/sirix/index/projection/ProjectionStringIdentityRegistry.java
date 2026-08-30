/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;
import java.util.Objects;

/**
 * Proves that a composite group-by's per-leaf dictionary STRING components are identified EXACTLY,
 * not merely fingerprinted.
 *
 * <h2>Why a fingerprint pair is not an identity</h2>
 *
 * A composite group's string components are carried in
 * {@link CompositeGroupIdentity} lanes as an FNV-1a primary paired with an xxh3 secondary. That pair
 * is an excellent DISCRIMINATOR — it separates different strings essentially always — but it is not
 * an identity, and the failure mode is the nastiest kind: if two distinct strings share BOTH
 * fingerprints, their identity lanes are equal, so
 * {@link NumericGroupAggTable#acquireExact} finds a match, never walks on, and never sets
 * {@link NumericGroupAggTable#hasProbeKeyCollision()}. The two groups merge in total silence. A
 * probabilistic bound is not a database semantic.
 *
 * <h2>What this registry does</h2>
 *
 * It keeps ONE canonical byte copy per distinct fingerprint pair, per key component. Every
 * dictionary entry a scan hashes is checked against it:
 *
 * <ul>
 * <li>fingerprint unseen → the value's bytes are copied in and become canonical;</li>
 * <li>fingerprint seen, bytes EQUAL → the same value recurring in another leaf, which is the
 * overwhelmingly common case and the only one on the hot path;</li>
 * <li>fingerprint seen, bytes DIFFER → a real fingerprint collision. The scan cannot represent the
 * two values apart, so it latches {@link #collisionDetected()} and the serve DECLINES.</li>
 * </ul>
 *
 * <p>
 * The check runs in the per-leaf dictionary pass, where the bytes are already in hand and already
 * in cache from hashing them — never per row. Callers additionally memoize the fingerprints they
 * have already cleared, so a worker consults this shared structure once per distinct value rather
 * than once per dictionary entry per leaf.
 *
 * <h2>Conservative by construction</h2>
 *
 * Canonical bytes are bounded by {@link #DEFAULT_MAX_CANONICAL_BYTES}. Exceeding the budget does not
 * silently stop checking: it latches {@link #unproven()}, which declines exactly as a collision
 * does. Anything this class cannot PROVE, it refuses.
 *
 * <p>
 * Not needed at all when every component is numeric or substring-cast — those carry their raw value
 * in an exact lane, and the executor then builds no registry.
 */
public final class ProjectionStringIdentityRegistry {

  /**
   * Canonical-byte budget before the registry declines rather than stops proving:
   * {@code -Dsirix.projection.compositeIdentityMaxBytes}, else an eighth of the heap clamped to
   * [32 MiB, 1 GiB]. The registry retains one canonical copy of every distinct string a surviving
   * group key names — the answer's own vocabulary — so the bound scales with the JVM the query runs
   * in rather than being a fixed number a large corpus outgrows.
   */
  public static final long DEFAULT_MAX_CANONICAL_BYTES = defaultMaxCanonicalBytes();

  private static long defaultMaxCanonicalBytes() {
    final Long configured = Long.getLong("sirix.projection.compositeIdentityMaxBytes");
    if (configured != null) {
      if (configured <= 0L) {
        throw new IllegalArgumentException(
            "sirix.projection.compositeIdentityMaxBytes must be positive: " + configured);
      }
      return configured;
    }
    return Math.max(32L << 20, Math.min(1L << 30, Runtime.getRuntime().maxMemory() / 8L));
  }

  /**
   * Supplies the identity lanes for a value. Production uses {@link #DEFAULT_FINGERPRINT}; tests
   * substitute one that FORCES collisions, which is the only way to exercise the byte-equality
   * fallback — two strings colliding in both real functions cannot be constructed.
   */
  public interface Fingerprint {

    /**
     * @param utf8 the value's bytes
     * @param off offset into {@code utf8}
     * @param len length in {@code utf8}
     * @param fnv1a64 the FNV-1a hash the kernel already computed for the probe chain
     * @return identity lane A
     */
    long primary(byte[] utf8, int off, int len, long fnv1a64);

    /**
     * @param utf8 the value's bytes
     * @param off offset into {@code utf8}
     * @param len length in {@code utf8}
     * @return identity lane B
     */
    long secondary(byte[] utf8, int off, int len);
  }

  /** Reuses the kernel's FNV-1a for lane A and xxh3 for lane B. */
  public static final Fingerprint DEFAULT_FINGERPRINT = new Fingerprint() {

    @Override
    public long primary(final byte[] utf8, final int off, final int len, final long fnv1a64) {
      return fnv1a64;
    }

    @Override
    public long secondary(final byte[] utf8, final int off, final int len) {
      return GlobalValueDictionary.secondaryValueHash(utf8, off, len);
    }
  };

  private static final int INITIAL_CAPACITY = 1 << 10;

  /**
   * Bytes charged against the budget for each retained value ON TOP of its own length.
   *
   * <p>
   * A budget that counted only value bytes would not bound this structure at all: millions of SHORT
   * values fit inside it while the open-addressed lanes ({@code 2 x long}), the reference slot, the
   * liveness flag and every {@code byte[]}'s own object header dwarf the payload — hundreds of
   * megabytes of table and header for tens of megabytes of strings, which is major-GC territory
   * exactly when an adversarial query wants it to be. Charging a per-entry constant makes the budget
   * bound ENTRIES as well as bytes, so a query with millions of tiny distinct keys declines early
   * instead of allocating its way to a pause.
   *
   * <p>
   * 24 bytes of lanes and reference, plus a 16-byte array header, plus 8 bytes of load-factor
   * headroom for the slot the entry reserves in a table kept below 3/4 full.
   */
  static final int ENTRY_OVERHEAD_BYTES = 48;

  /** Largest table a component may grow to before the registry declines rather than doubling. */
  private static final int MAX_COMPONENT_CAPACITY = 1 << 26;

  private final int components;
  private final Fingerprint fingerprint;
  private final long maxCanonicalBytes;

  /** Open-addressed, one table per component. {@code slotUsed} marks live slots. */
  private long[][] lanesA;
  private long[][] lanesB;
  private byte[][][] canonical;
  private boolean[][] used;
  private int[] sizes;
  private int[] masks;

  private long canonicalBytes;
  private volatile boolean collision;
  private volatile boolean unproven;

  /**
   * @param components number of key components (only string ones are ever registered)
   * @param fingerprint lane supplier; {@link #DEFAULT_FINGERPRINT} in production
   * @param maxCanonicalBytes budget for retained canonical values
   */
  public ProjectionStringIdentityRegistry(final int components, final Fingerprint fingerprint,
      final long maxCanonicalBytes) {
    if (components <= 0) {
      throw new IllegalArgumentException("components must be > 0");
    }
    if (components > CompositeGroupIdentity.MAX_KEY_COMPONENTS) {
      throw new IllegalArgumentException(
          "components " + components + " exceeds " + CompositeGroupIdentity.MAX_KEY_COMPONENTS);
    }
    if (maxCanonicalBytes <= 0) {
      throw new IllegalArgumentException("maxCanonicalBytes must be > 0");
    }
    this.components = components;
    this.fingerprint = Objects.requireNonNull(fingerprint, "fingerprint must not be null");
    this.maxCanonicalBytes = maxCanonicalBytes;
    this.lanesA = new long[components][];
    this.lanesB = new long[components][];
    this.canonical = new byte[components][][];
    this.used = new boolean[components][];
    this.sizes = new int[components];
    this.masks = new int[components];
    for (int c = 0; c < components; c++) {
      allocate(c, INITIAL_CAPACITY);
    }
  }

  /**
   * The fingerprint every scan-built registry uses. Only a test ever replaces it: two strings that
   * collide in BOTH real functions cannot be constructed, so forcing the collision is the only way
   * to execute the byte-equality path and the decline that follows it. Production never calls the
   * setter, and the field is read once per registry rather than per value.
   */
  private static volatile Fingerprint installedFingerprint = DEFAULT_FINGERPRINT;

  /**
   * Replace the fingerprint for the duration of a test. Callers MUST restore it with
   * {@link #resetFingerprint()}.
   *
   * @param replacement the fingerprint to install
   */
  public static void installFingerprintForTesting(final Fingerprint replacement) {
    installedFingerprint = Objects.requireNonNull(replacement, "replacement must not be null");
  }

  /** Restore the production fingerprint. */
  public static void resetFingerprint() {
    installedFingerprint = DEFAULT_FINGERPRINT;
  }

  /** Production registry over the installed fingerprint functions and the default budget. */
  public ProjectionStringIdentityRegistry(final int components) {
    this(components, installedFingerprint, DEFAULT_MAX_CANONICAL_BYTES);
  }

  /** Production fingerprint functions with a caller-chosen budget (the executor's documented knob). */
  public ProjectionStringIdentityRegistry(final int components, final long maxCanonicalBytes) {
    this(components, installedFingerprint, maxCanonicalBytes);
  }

  private void allocate(final int component, final int capacity) {
    lanesA[component] = new long[capacity];
    lanesB[component] = new long[capacity];
    canonical[component] = new byte[capacity][];
    used[component] = new boolean[capacity];
    masks[component] = capacity - 1;
    sizes[component] = 0;
  }

  /** Identity lane A for a value whose FNV-1a the caller already has. */
  public long laneA(final byte[] utf8, final int off, final int len, final long fnv1a64) {
    return fingerprint.primary(utf8, off, len, fnv1a64);
  }

  /** Identity lane B for a value. */
  public long laneB(final byte[] utf8, final int off, final int len) {
    return fingerprint.secondary(utf8, off, len);
  }

  /**
   * Whether two distinct values were proven to share a fingerprint pair. A serve that has seen this
   * must decline: its identity lanes cannot tell the two groups apart.
   */
  public boolean collisionDetected() {
    return collision;
  }

  /** Whether the registry ran out of budget and therefore stopped being able to prove equality. */
  public boolean unproven() {
    return unproven;
  }

  /** Whether the scan may trust its string identity lanes. */
  public boolean identityProven() {
    return !collision && !unproven;
  }

  /**
   * Prove that {@code (laneA, laneB)} denotes exactly one value for {@code component}.
   *
   * @param component the key component ordinal
   * @param laneA identity lane A, from {@link #laneA}
   * @param laneB identity lane B, from {@link #laneB}
   * @param utf8 the value's bytes
   * @param off offset into {@code utf8}
   * @param len length in {@code utf8}
   * @return {@code true} when the fingerprint provably denotes this value, {@code false} when the
   *         scan must decline
   */
  public synchronized boolean prove(final int component, final long laneA, final long laneB, final byte[] utf8,
      final int off, final int len) {
    if (component < 0 || component >= components) {
      throw new IllegalArgumentException("component " + component + " out of range");
    }
    Objects.requireNonNull(utf8, "utf8 must not be null");
    if (off < 0 || len < 0 || off > utf8.length - len) {
      throw new IllegalArgumentException(
          "range [" + off + ", " + off + " + " + len + ") is outside a " + utf8.length + "-byte value");
    }
    if (collision || unproven) {
      return false;
    }
    final int mask = masks[component];
    final long[] a = lanesA[component];
    final long[] b = lanesB[component];
    final byte[][] values = canonical[component];
    final boolean[] live = used[component];
    int slot = (int) mix(laneA ^ mix(laneB)) & mask;
    while (live[slot]) {
      if (a[slot] == laneA && b[slot] == laneB) {
        final byte[] stored = values[slot];
        if (stored.length != len || !Arrays.equals(stored, 0, len, utf8, off, off + len)) {
          // Two different values, one fingerprint: the lanes cannot represent them apart, and
          // acquireExact would fold them WITHOUT ever setting hasProbeKeyCollision.
          collision = true;
          return false;
        }
        return true;
      }
      slot = slot + 1 & mask;
    }
    // Subtraction, not addition: an addition could overflow a long in principle, and an overflowed
    // comparison would read as "fits" and silently stop bounding the retained memory. The charge
    // includes ENTRY_OVERHEAD_BYTES so the budget bounds table and header footprint too, not just
    // the value bytes — see that constant for why counting bytes alone bounds nothing.
    final long charge = (long) len + ENTRY_OVERHEAD_BYTES;
    if (charge > maxCanonicalBytes - canonicalBytes) {
      unproven = true;
      return false;
    }
    live[slot] = true;
    a[slot] = laneA;
    b[slot] = laneB;
    values[slot] = Arrays.copyOfRange(utf8, off, off + len);
    canonicalBytes += charge;
    if (++sizes[component] > (mask + 1) * 3 / 4) {
      grow(component);
    }
    return true;
  }

  private void grow(final int component) {
    final long[] oldA = lanesA[component];
    final long[] oldB = lanesB[component];
    final byte[][] oldValues = canonical[component];
    final boolean[] oldLive = used[component];
    final int oldCapacity = oldLive.length;
    if (oldCapacity >= MAX_COMPONENT_CAPACITY) {
      // Refuse to double rather than allocate a table whose reference arrays alone are
      // major-GC-scale. Declining is always available; a pause is not.
      unproven = true;
      return;
    }
    allocate(component, oldCapacity << 1);
    final int mask = masks[component];
    for (int i = 0; i < oldCapacity; i++) {
      if (!oldLive[i]) {
        continue;
      }
      int slot = (int) mix(oldA[i] ^ mix(oldB[i])) & mask;
      while (used[component][slot]) {
        slot = slot + 1 & mask;
      }
      used[component][slot] = true;
      lanesA[component][slot] = oldA[i];
      lanesB[component][slot] = oldB[i];
      canonical[component][slot] = oldValues[i];
      sizes[component]++;
    }
  }

  /**
   * A worker's bounded, unsynchronised front cache for {@link #prove}.
   *
   * <p>
   * The same string recurs in most leaves' dictionaries, so without this the shared registry would
   * be entered once per dictionary entry per leaf — millions of monitor acquisitions across 20
   * workers. A hit here still compares the canonical BYTES, so the cache can only ever skip work
   * that was already proven; it can never turn an unproven pair into a proven one. Direct-mapped
   * with a fixed slot count, so it adds a constant, small footprint per worker.
   */
  public static final class LocalProofCache {

    private static final int SLOTS = 1 << 10;
    private static final int SLOT_MASK = SLOTS - 1;

    /**
     * Bytes each slot can hold INLINE. Group-key strings are overwhelmingly shorter than this; a
     * longer value simply is not cached and pays the shared registry every time, which costs a
     * monitor but never an allocation.
     */
    private static final int INLINE_CAPACITY = 64;

    private final long[] lanesA;
    private final long[] lanesB;
    /** Canonical length per slot; {@code -1} marks an empty slot. */
    private final int[] lengths;
    /** One flat arena, {@link #INLINE_CAPACITY} bytes per slot — never reallocated. */
    private final byte[] arena;

    /** @param components number of key components this worker may register */
    public LocalProofCache(final int components) {
      if (components <= 0) {
        throw new IllegalArgumentException("components must be > 0");
      }
      // Overflow-checked: `components * SLOTS * INLINE_CAPACITY` silently wraps negative for a large
      // component count, and a negative length throws deep inside array allocation instead of here.
      // MAX_KEY_COMPONENTS bounds this in practice; the check makes it true rather than assumed.
      if (components > CompositeGroupIdentity.MAX_KEY_COMPONENTS) {
        throw new IllegalArgumentException(
            "components " + components + " exceeds " + CompositeGroupIdentity.MAX_KEY_COMPONENTS);
      }
      final int slots = Math.multiplyExact(components, SLOTS);
      final int arenaBytes = Math.multiplyExact(slots, INLINE_CAPACITY);
      this.lanesA = new long[slots];
      this.lanesB = new long[slots];
      this.lengths = new int[slots];
      this.arena = new byte[arenaBytes];
      Arrays.fill(lengths, -1);
    }

    /**
     * Prove the value through the cache, falling through to {@code registry} on a miss.
     *
     * <p>
     * Allocation-free after construction: canonical bytes live in a fixed arena and an eviction
     * OVERWRITES its slot rather than allocating a replacement. A high-cardinality scan therefore
     * thrashes this cache without producing a single byte of garbage — it degrades into registry
     * calls, not into GC pressure.
     *
     * <p>
     * A hit still re-reads the registry's terminal state, so once any worker has disproved identity
     * every other worker stops at its very next value instead of running to the end of its leaf
     * range and relying on the post-join check.
     *
     * @return {@code false} when the scan must decline
     */
    public boolean prove(final ProjectionStringIdentityRegistry registry, final int component, final long laneA,
        final long laneB, final byte[] utf8, final int off, final int len) {
      if (len > INLINE_CAPACITY) {
        return registry.prove(component, laneA, laneB, utf8, off, len);
      }
      final int slot = component * SLOTS + ((int) mix(laneA ^ mix(laneB)) & SLOT_MASK);
      final int base = slot * INLINE_CAPACITY;
      if (lengths[slot] == len && lanesA[slot] == laneA && lanesB[slot] == laneB
          && Arrays.equals(arena, base, base + len, utf8, off, off + len)) {
        // Cheap volatile read: a collision another worker latched must stop THIS worker too.
        return registry.identityProven();
      }
      if (!registry.prove(component, laneA, laneB, utf8, off, len)) {
        return false;
      }
      System.arraycopy(utf8, off, arena, base, len);
      lanesA[slot] = laneA;
      lanesB[slot] = laneB;
      lengths[slot] = len;
      return true;
    }
  }

  private static long mix(final long value) {
    long h = value * 0x9E3779B97F4A7C15L;
    h ^= h >>> 32;
    return h ^ h >>> 16;
  }
}
