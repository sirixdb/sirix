/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
 * <h2>Lock-free on the hot path</h2>
 *
 * Twenty workers proving the same few million values would serialise on one monitor — a 100M-row
 * scan spent a third of its wall time parked here. So a slot is WRITE-ONCE and PUBLISHED by a
 * release store of its canonical bytes: a reader probes the current table without any lock and
 * returns {@code true} on a byte-equal hit, which is the only outcome the hot path ever produces.
 * Everything else — an unseen fingerprint, a byte mismatch — falls through to the synchronized path,
 * which re-probes under the monitor and is the sole writer (insert, collision latch, growth). A grow
 * builds a NEW table and publishes it; the old one is never mutated again, so a reader holding it can
 * at worst miss a later insert and take the locked path, never read a torn slot. Kill switch:
 * {@code -Dsirix.projection.compositeIdentity.lockFreeProbe=false} sends every proof through the
 * monitor; witness: {@link #lockedProves()} counts the proofs that took it.
 *
 * <h2>Conservative by construction</h2>
 *
 * Canonical bytes are bounded by {@link #DEFAULT_MAX_CANONICAL_BYTES}. Exceeding the budget does not
 * silently stop checking: it latches {@link #unproven()}, which declines exactly as a collision
 * does. Anything this class cannot PROVE, it refuses.
 *
 * <h2>Proving a column once</h2>
 *
 * Identity is a property of the column's DATA, not of the query: once every string in every leaf
 * dictionary of a column has been proven pairwise distinct under a fingerprint, any later scan over
 * that column meets a subset of those strings and needs no proof at all. The executor therefore
 * runs a FULL-coverage scan (no predicate can drop a leaf or a row) in {@link #proveEveryEntry()}
 * mode — every dictionary entry is proven in the dictionary pass, not merely the entries surviving
 * rows name — and, when the scan completes with {@link #identityProven()}, memoizes the verdict per
 * column on the projection handle. A later registry over a memoized column has the component
 * {@link #markPreProven marked pre-proven}: the kernel neither consults the registry nor builds a
 * {@link LocalProofCache} for it, and the registry retains no canonical bytes for it. A component
 * whose query supplies a literal in the column's domain (a conditional key's else branch) is never
 * pre-proven: the literal could collide with a stored string the empty registry has not seen. Both
 * modes are behaviour gates the executor sets before the workers start; the kernels read them once
 * per call.
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

  /**
   * Whether repeated proofs may probe without the monitor. A behaviour gate for an A/B, never a
   * format: the locked path proves exactly the same identities.
   */
  public static final String LOCK_FREE_PROBE_PROPERTY = "sirix.projection.compositeIdentity.lockFreeProbe";
  private static volatile boolean lockFreeProbe =
      Boolean.parseBoolean(System.getProperty(LOCK_FREE_PROBE_PROPERTY, "true"));

  /** Whether repeated proofs currently bypass the monitor. */
  public static boolean lockFreeProbe() {
    return lockFreeProbe;
  }

  /**
   * Flip the lock-free probe for a test; returns the previous setting so the caller can restore it.
   *
   * @param enabled {@code false} sends every proof through the monitor
   * @return the previous setting
   */
  public static boolean setLockFreeProbeForTesting(final boolean enabled) {
    final boolean previous = lockFreeProbe;
    lockFreeProbe = enabled;
    return previous;
  }

  /** Release/acquire access to one slot's canonical bytes — the slot's publication point. */
  private static final VarHandle VALUES = MethodHandles.arrayElementVarHandle(byte[][].class);

  /**
   * One component's open-addressed table. A slot is live once {@code values[slot]} is non-null;
   * {@code lanesA}/{@code lanesB} of a live slot were written BEFORE that reference was released and
   * are never written again. Only the monitor's holder writes; {@code size} is read and written under
   * it alone. {@link #grow} replaces the whole table rather than touching a published one.
   */
  private static final class Table {
    final long[] lanesA;
    final long[] lanesB;
    final byte[][] values;
    final int mask;
    int size;

    Table(final int capacity) {
      this.lanesA = new long[capacity];
      this.lanesB = new long[capacity];
      this.values = new byte[capacity][];
      this.mask = capacity - 1;
    }

    int growAt() {
      return (mask + 1) * 3 / 4;
    }
  }

  /** The current table per component; a grow publishes its replacement here. */
  private final AtomicReferenceArray<Table> tables;

  private long canonicalBytes;
  /** Proofs that took the monitor: first sightings, mismatches and every call while the fast path is off. */
  private long lockedProves;
  private volatile boolean collision;
  private volatile boolean unproven;
  /**
   * Components whose every value is already proven pairwise distinct — by a column memo or by a
   * completed full-coverage pass of this scan. Copy-on-write: a mark publishes a fresh array, so a
   * kernel that reads the field once sees a consistent snapshot without a lock.
   */
  private volatile boolean[] preProven;
  /** Whether a kernel must prove EVERY dictionary entry in its dictionary pass (full-coverage scans only). */
  private volatile boolean proveEveryEntry;

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
    this.tables = new AtomicReferenceArray<>(components);
    for (int c = 0; c < components; c++) {
      tables.set(c, new Table(INITIAL_CAPACITY));
    }
    this.preProven = new boolean[components];
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

  /** The fingerprint this registry proves identities under — what a column memo must be keyed by. */
  public Fingerprint fingerprint() {
    return fingerprint;
  }

  /** The number of key components this registry was built for. */
  public int components() {
    return components;
  }

  /**
   * Whether {@code component}'s values are already proven pairwise distinct, so a kernel may carry
   * its fingerprint lanes as exact identity without consulting this registry.
   *
   * @param component the key component ordinal
   * @return {@code true} when no proof is needed for the component
   */
  public boolean preProven(final int component) {
    if (component < 0 || component >= components) {
      throw new IllegalArgumentException("component " + component + " out of range");
    }
    return preProven[component];
  }

  /**
   * Mark {@code component} pre-proven: every value it can meet is known to be pairwise distinct
   * under {@link #fingerprint()}. Set by the executor from a column memo before the workers start,
   * or after a full-coverage pass completed with {@link #identityProven()}; never by a kernel.
   *
   * @param component the key component ordinal
   */
  public synchronized void markPreProven(final int component) {
    if (component < 0 || component >= components) {
      throw new IllegalArgumentException("component " + component + " out of range");
    }
    if (preProven[component]) {
      return;
    }
    final boolean[] next = preProven.clone();
    next[component] = true;
    preProven = next;
  }

  /** Whether kernels must prove every dictionary entry eagerly in their dictionary pass. */
  public boolean proveEveryEntry() {
    return proveEveryEntry;
  }

  /**
   * Switch eager proving on or off. On, a kernel proves EVERY entry of every leaf dictionary it
   * hashes — the precondition for memoizing the column, since a lazy pass proves only the entries
   * surviving rows name. Only meaningful for a scan whose coverage is complete; the executor decides.
   *
   * @param eager {@code true} to prove every entry
   */
  public void setProveEveryEntry(final boolean eager) {
    proveEveryEntry = eager;
  }

  /**
   * Prove that {@code (laneA, laneB)} denotes exactly one value for {@code component}.
   *
   * <p>
   * Lock-free for a value the registry has already seen: the probe reads the current table and
   * returns on a byte-equal hit. An unseen fingerprint or a byte mismatch goes through
   * {@link #proveLocked}, the single writer.
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
  public boolean prove(final int component, final long laneA, final long laneB, final byte[] utf8,
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
    if (lockFreeProbe) {
      final Table t = tables.get(component);
      final int mask = t.mask;
      final long[] a = t.lanesA;
      final long[] b = t.lanesB;
      final byte[][] values = t.values;
      int slot = (int) mix(laneA ^ mix(laneB)) & mask;
      for (;;) {
        // The table is kept below 3/4 full, so an empty slot always ends the walk.
        final byte[] stored = (byte[]) VALUES.getAcquire(values, slot);
        if (stored == null) {
          break; // unseen so far: the monitor's holder decides
        }
        if (a[slot] == laneA && b[slot] == laneB) {
          if (stored.length == len && Arrays.equals(stored, 0, len, utf8, off, off + len)) {
            return true;
          }
          break; // a mismatch is the locked path's verdict, never the reader's
        }
        slot = slot + 1 & mask;
      }
    }
    return proveLocked(component, laneA, laneB, utf8, off, len);
  }

  /** The authoritative path: re-probes under the monitor, inserts, latches a collision, grows. */
  private synchronized boolean proveLocked(final int component, final long laneA, final long laneB,
      final byte[] utf8, final int off, final int len) {
    lockedProves++;
    if (collision || unproven) {
      return false;
    }
    final Table t = tables.get(component);
    final int mask = t.mask;
    final long[] a = t.lanesA;
    final long[] b = t.lanesB;
    final byte[][] values = t.values;
    int slot = (int) mix(laneA ^ mix(laneB)) & mask;
    for (byte[] stored = values[slot]; stored != null; stored = values[slot]) {
      if (a[slot] == laneA && b[slot] == laneB) {
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
    // Lanes first, bytes last with release semantics: a lock-free reader that acquires the bytes
    // sees the lanes that belong to them.
    a[slot] = laneA;
    b[slot] = laneB;
    VALUES.setRelease(values, slot, Arrays.copyOfRange(utf8, off, off + len));
    canonicalBytes += charge;
    if (++t.size > t.growAt()) {
      grow(component, t);
    }
    return true;
  }

  /** Proofs that took the monitor so far; a repeated value must not add to this while the fast path is on. */
  public synchronized long lockedProves() {
    return lockedProves;
  }

  /** Builds the doubled table beside the old one and publishes it; the old table is never written again. */
  private void grow(final int component, final Table old) {
    final int oldCapacity = old.mask + 1;
    if (oldCapacity >= MAX_COMPONENT_CAPACITY) {
      // Refuse to double rather than allocate a table whose reference arrays alone are
      // major-GC-scale. Declining is always available; a pause is not.
      unproven = true;
      return;
    }
    final Table fresh = new Table(oldCapacity << 1);
    final int mask = fresh.mask;
    for (int i = 0; i < oldCapacity; i++) {
      final byte[] value = old.values[i];
      if (value == null) {
        continue;
      }
      int slot = (int) mix(old.lanesA[i] ^ mix(old.lanesB[i])) & mask;
      while (fresh.values[slot] != null) {
        slot = slot + 1 & mask;
      }
      fresh.lanesA[slot] = old.lanesA[i];
      fresh.lanesB[slot] = old.lanesB[i];
      fresh.values[slot] = value;
      fresh.size++;
    }
    // A volatile publication: every plain write above happens-before a reader's acquisition of
    // the new table.
    tables.set(component, fresh);
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
