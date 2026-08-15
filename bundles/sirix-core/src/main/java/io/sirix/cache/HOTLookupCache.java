/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Memoizes the answer to a HOT point lookup for a COMMITTED revision.
 *
 * <p>
 * A point lookup on a HOT index is pure CPU once the pages are resident — the descent, the in-leaf
 * binary search, the chunk walk and the posting-list reassembly all re-run on every call, because
 * nothing above {@code HOTIndexReader.get} memoizes anything. For a committed revision that work is
 * deterministic, so repeating it for a key already asked about is pure waste.
 * </p>
 *
 * <h2>Why there is no invalidation protocol</h2>
 * <p>
 * The revision number is part of {@link HOTLookupKey}, and a committed revision's index content
 * never changes — a write produces a NEW revision with new keys. An entry therefore cannot go
 * stale; it can only become unreachable, and the capacity bound ages it out. This is the whole
 * reason the cache is cheap to reason about, and it is also why a WRITER-backed reader must never
 * consult it: an uncommitted transaction mutates the index under a revision number that is already
 * a key here. Enforcing that is the caller's job — see {@code AbstractHOTIndexReader}'s
 * {@code lookupCache}.
 * </p>
 *
 * <h2>Why a set-associative array and not a general-purpose cache</h2>
 * <p>
 * This started out backed by Caffeine and it made the MISS path twice as expensive as no cache at
 * all: measured on a 33K-key index sized to 64 entries, so that every miss admitted and therefore
 * evicted, a point lookup went from 622 ns uncached to 1207 ns. The read path was never the problem
 * — the hit path measured 121 ns in the same run. The write path was: admitting under sustained
 * eviction pressure appends to a write buffer and runs the admission policy's maintenance, and that
 * work lands on the calling thread.
 * </p>
 * <p>
 * A cache whose miss costs more than the computation it is avoiding is worse than no cache, and a
 * workload with poor key reuse would have paid that with no signal. So the table is a fixed-size,
 * set-associative array instead: a lookup indexes one set and compares at most {@link #WAYS} keys,
 * and an admission is a SINGLE reference store — no buffers, no policy, no maintenance, and
 * eviction is just overwriting the slot. That gives up hit-RATE quality, which a general-purpose
 * cache is built to maximise, in exchange for both paths being predictable. Here that is the right
 * trade: the entries are immutable and re-derivable, so a wrong eviction costs one recomputation,
 * whereas an expensive admission costs every miss forever.
 * </p>
 *
 * <h2>Why the value is a long[] and not a NodeReferences</h2>
 * <p>
 * {@link NodeReferences} is MUTABLE — {@code addNodeKey}, {@code removeNodeKey}, and
 * {@code getNodeKeys()} which hands out the live set. Handing the same instance to two callers
 * would turn "every lookup returns a fresh object" into a shared-aliasing contract, so the first
 * consumer to mutate a result would silently corrupt every other query that asked for the same key.
 * Storing the sorted node keys as a bare {@code long[]} and rebuilding a fresh
 * {@code NodeReferences} per hit keeps the existing contract exactly as it was.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HOTLookupCache {

  /**
   * Largest posting list that is worth memoizing, in node keys.
   *
   * <p>
   * Two independent reasons for a bound, both pointing at a small number. Memory: a frequently
   * occurring indexed value can carry a posting list of millions of node keys, and caching a handful
   * of those would cost more than the entire page cache it is meant to complement. Time: a hit copies
   * the array, so the saving shrinks as the list grows and inverts once the copy costs more than the
   * walk it replaces. At 256 keys the copy is ~2 KB — still far cheaper than the walk — and the
   * entry's own footprint stays in the low kilobytes.
   * </p>
   */
  public static final int MAX_CACHED_NODE_KEYS = 256;

  /**
   * Entries per set.
   *
   * <p>
   * Associativity buys back the conflict misses a hash-indexed table suffers when several live keys
   * land in one set. Keys distribute over sets roughly as a Poisson variable, so at eight ways the
   * fraction of sets that overflow — and therefore evict something still wanted — is a small fraction
   * of what it is at four, which matters once the working set approaches capacity. Measured on a
   * 33K-key corpus in a 65536-slot table with every key resident: 308 ns per lookup at four ways, 223
   * ns at eight. Four ways lost enough entries to conflict that a policy-driven cache beat it (241
   * ns); eight ways beats that, while keeping the miss path a policy-driven cache cannot — see the
   * class comment.
   * </p>
   * <p>
   * Eight is where that stops being free: with compressed oops a set is 8 x 4 = 32 bytes, so a lookup
   * still touches ONE cache line whichever way it hits on. Sixteen would straddle two lines and start
   * paying for what it saves.
   * </p>
   */
  private static final int WAYS = 8;

  /** One immutable (key, answer) pair. Final fields, so publishing the reference publishes both. */
  private record Entry(HOTLookupKey key, long[] nodeKeys) {
  }

  /**
   * Slot table of {@code sets * WAYS} entries, or {@code null} when the cache is disabled.
   *
   * <p>
   * Accessed through {@link #TABLE} rather than directly. {@link Entry} is a record, so its fields
   * are final and the JLS freeze already guarantees that a thread reading the reference sees them
   * initialised even through a data race — the acquire/release is therefore defence in depth, not the
   * thing that makes publication safe. It is kept because it costs nothing on x86 (both compile to
   * plain moves) and it keeps the guarantee if {@code Entry} ever stops being a record; do NOT read
   * that as licence to give {@code Entry} mutable fields, which the VarHandle would not save.
   * </p>
   */
  private final @Nullable Object @Nullable [] table;

  /** {@code sets - 1}; a power-of-two set count turns the set index into one AND. */
  private final int setMask;

  /**
   * Per-set rotating victim, consulted only when a set is full.
   *
   * <p>
   * Plain {@code int} reads and writes, deliberately: a race here picks the same victim twice, which
   * costs one extra recomputation later and nothing else. Making it atomic would put a contended
   * write on the admission path to protect a counter whose exact value does not matter.
   * </p>
   */
  private final int @Nullable [] victims;

  private static final VarHandle TABLE = MethodHandles.arrayElementVarHandle(Object[].class);

  /**
   * Bumped by every sweep, so a read-through admission can tell whether the answer it computed
   * belongs to a scope that has since been invalidated.
   *
   * <p>
   * Ordering alone cannot close that race, which is worth stating because it is tempting to believe
   * otherwise: a lookup that read its pages BEFORE a sweep and calls {@link #put} AFTER it re-admits
   * the discarded history no matter which order the sweeper drops pages and answers in. Sweeping
   * answers last only narrows the window. Since {@code truncateTo} re-issues committed revision
   * numbers over different content, that resurrected entry is then served indefinitely, and a stale
   * memoized answer — unlike a stale page — is never re-derived and never re-validated.
   * </p>
   *
   * <p>
   * The counter is what makes the sweep actually authoritative. A reader captures it before it starts
   * computing and hands it back at admission; a sweep bumps it BEFORE clearing slots, so every put
   * either lands before the bump (and is then cleared by that same sweep) or sees a changed value
   * (and declines).
   * </p>
   */
  private volatile long generation;

  /** Handle on {@link #generation}, for the atomic bump on the sweep path. */
  private static final VarHandle GENERATION;

  static {
    try {
      GENERATION = MethodHandles.lookup().findVarHandle(HOTLookupCache.class, "generation", long.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Largest table this class will build, in slots.
   *
   * <p>
   * A ceiling is needed because {@code sets * WAYS} is int arithmetic reachable from an
   * operator-settable property: without it, {@code maxEntries >= 1073741832} made the product
   * overflow to {@code Integer.MIN_VALUE} and the constructor threw
   * {@code NegativeArraySizeException} — out of {@code BufferManagerImpl}'s constructor, so a single
   * typo'd system property made every database in the JVM unopenable. Sixteen million slots is
   * already far past any useful working set and keeps the product two orders of magnitude clear of
   * overflow.
   * </p>
   */
  public static final int MAX_SLOTS = 1 << 24;

  /**
   * @param maxEntries capacity ceiling in entries; the table is the largest power-of-two set count
   *        that does NOT exceed it, clamped to {@link #MAX_SLOTS}. A value below {@link #WAYS} still
   *        gets one set, since a set-associative table cannot be narrower than its associativity.
   * @throws IllegalArgumentException if {@code maxEntries} is not positive — use {@link #disabled()}
   */
  public HOTLookupCache(final int maxEntries) {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be positive, use disabled() instead: " + maxEntries);
    }
    // Round DOWN to a power-of-two set count: rounding up overshot the operator's budget by as much
    // as 2x (32776 entries asked, 65536 slots built), which mattered because the memory estimate the
    // default is justified against is computed from the requested number.
    final int requestedSets = Math.max(1, Integer.highestOneBit(maxEntries / WAYS));
    final int sets = Math.min(requestedSets, MAX_SLOTS / WAYS);
    this.setMask = sets - 1;
    this.table = new Object[sets * WAYS];
    this.victims = new int[sets];
  }

  private HOTLookupCache() {
    this.table = null;
    this.setMask = 0;
    this.victims = null;
  }

  /**
   * The one no-op instance. Stateless and immutable — {@link #table} and {@link #victims} are both
   * {@code null} — so minting a fresh one per caller bought nothing and made reference identity
   * against "the" disabled cache silently false.
   */
  private static final HOTLookupCache DISABLED = new HOTLookupCache();

  /**
   * A cache that never retains anything, for buffer managers that do no buffering at all. Reports
   * every lookup as a miss and refuses every admission, so callers need no null checks and no feature
   * flag — they simply never get a hit.
   *
   * @return the no-op instance
   */
  public static HOTLookupCache disabled() {
    return DISABLED;
  }

  /**
   * Whether this cache can retain anything.
   *
   * <p>
   * Lets a caller skip the work of building a key and draining a posting list for an admission that
   * would be refused anyway — which is what makes {@code maxEntries=0} a genuine measurement of the
   * uncached path rather than the uncached path plus this class's overhead.
   * </p>
   *
   * @return {@code false} for {@link #disabled()}
   */
  public boolean isEnabled() {
    return table != null;
  }

  /**
   * Slots this cache actually holds.
   *
   * <p>
   * NOT the {@code maxEntries} it was constructed with: the constructor rounds DOWN to a power-of-two
   * set count, so a request of 6990 builds 512 sets = 4096 slots. Exposed because the startup log
   * exists to let an operator see the effective size, and printing the requested figure there
   * reported a capacity up to twice the truth. {@link #size()} is the occupancy, not this.
   * </p>
   *
   * @return the number of slots, or {@code 0} for {@link #disabled()}
   */
  public int capacity() {
    final Object[] slots = table;
    return slots == null
        ? 0
        : slots.length;
  }

  /** Index of the first way of the set {@code hash} maps to. */
  private int setBase(final int hash) {
    return (hash & setMask) * WAYS;
  }

  /**
   * The memoized node keys for {@code key}, ascending, or {@code null} on a miss.
   *
   * <p>
   * The returned array is the CACHED instance and must be treated as immutable — callers rebuild a
   * {@code NodeReferences} from a copy. It is returned uncopied so a caller that only wants the
   * cardinality does not pay for one.
   * </p>
   *
   * @param key the lookup identity
   * @return the cached ascending node keys, or {@code null} if not present
   */
  public long @Nullable [] get(final HOTLookupKey key) {
    Objects.requireNonNull(key, "key");
    final Object[] slots = table;
    if (slots == null) {
      return null;
    }
    final int base = setBase(key.hashCode());
    for (int way = 0; way < WAYS; way++) {
      final Entry entry = (Entry) TABLE.getAcquire(slots, base + way);
      if (entry != null && entry.key.equals(key)) {
        return entry.nodeKeys;
      }
    }
    return null;
  }


  /**
   * The current sweep generation, to be captured BEFORE reading anything the admission will be
   * derived from and handed back to {@link #put(HOTLookupKey, long[], long)}.
   *
   * @return the generation to admit against
   */
  public long generation() {
    return generation;
  }


  /**
   * Admit an entry only if no sweep has run since {@code expectedGeneration} was captured.
   *
   * <p>
   * The read-through form, and the only one. {@code expectedGeneration} must come from
   * {@link #generation()} read BEFORE the answer was computed: a variant that captured it at the
   * moment of this call could never detect a sweep that overlapped the computation, which is
   * precisely the race the parameter exists to close.
   * </p>
   *
   * @param key the lookup identity, owning its bytes
   * @param nodeKeys ascending node keys answering the lookup; not retained by the caller
   * @param expectedGeneration the generation observed before the answer was computed
   * @return {@code true} if the entry was admitted
   */
  public boolean put(final HOTLookupKey key, final long[] nodeKeys, final long expectedGeneration) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(nodeKeys, "nodeKeys");
    final Object[] slots = table;
    // Both nullable finals are read together, so the compiler (and NullAway) can see the coupling
    // that makes victims[set] safe below rather than having to infer it from a check 30 lines up.
    final int[] rotation = victims;
    if (slots == null || rotation == null) {
      // BEFORE the ownership check, not after: disabled() promises callers "refuses every admission,
      // so callers need no null checks and no feature flag". A no-op that throws for one class of
      // argument is not a no-op, and the argument it would throw for — a borrowing probe key — is
      // exactly what a caller who took that promise at face value would hand it.
      return false;
    }
    if (!key.isOwned()) {
      // Storing a borrowing probe key aliases a reused serialization buffer: the next lookup on this
      // thread rewrites the stored key's bytes while its precomputed hash stays frozen, after which
      // the entry answers a DIFFERENT logical key. Previously this contract was documented in three
      // places and enforced in none.
      throw new IllegalArgumentException("only an owned() key may be stored; a probe key aliases a reusable buffer");
    }
    if (nodeKeys.length > MAX_CACHED_NODE_KEYS) {
      return false;
    }
    if (generation != expectedGeneration) {
      // A sweep ran while this answer was being computed, so it may describe content the sweep
      // exists to discard. Cheap and early: nothing has been written yet.
      return false;
    }
    final int base = setBase(key.hashCode());
    int free = -1;
    for (int way = 0; way < WAYS; way++) {
      final Entry entry = (Entry) TABLE.getAcquire(slots, base + way);
      if (entry == null) {
        if (free < 0) {
          free = base + way;
        }
      } else if (entry.key.equals(key)) {
        // Same key, same revision: the answer is identical, so refreshing it is pointless work.
        return true;
      }
    }
    final int slot;
    if (free >= 0) {
      slot = free;
    } else {
      final int set = base / WAYS;
      final int victim = rotation[set];
      rotation[set] = victim + 1;
      slot = base + (victim & (WAYS - 1));
    }
    final Entry admitted = new Entry(key, nodeKeys);
    TABLE.setRelease(slots, slot, admitted);
    // A StoreLoad barrier, which NEITHER the release store above NOR the volatile read below
    // supplies. `setRelease` is not a synchronization action, and HotSpot fences volatile STORES,
    // not volatile loads — so on x86-TSO the store can still be sitting in this thread's store
    // buffer when the read below is satisfied from cache. Without the fence the sequence
    // "store slot -> read old generation -> sweeper bumps -> sweeper scans slot and finds it empty
    // -> store drains" is legal, and it republishes an answer built from the very content the sweep
    // exists to discard, under a revision number truncateTo is about to re-issue.
    //
    // With the fence here and the sweeper's own getAndAdd fencing its side, this is Dekker: at
    // least one of the two threads observes the other, so either we see the bump and undo, or the
    // sweeper sees our entry and clears it.
    VarHandle.fullFence();
    // Re-check AFTER the store, which is what actually closes the race. The check before it can be
    // overtaken: a sweep may bump and finish scanning this slot between that check and the store,
    // leaving the entry behind the sweeper.
    if (generation != expectedGeneration) {
      // Undo OUR entry only. Between the store and here another thread may have taken this slot —
      // its own free-slot pick, the victim rotation, or a legitimate admission carrying the NEW
      // generation — and a blind null would evict that instead of ours, which is a lost memo the
      // sweep never asked for.
      TABLE.compareAndSet(slots, slot, admitted, null);
      return false;
    }
    return true;
  }

  /**
   * Drop every memoized lookup belonging to one resource.
   *
   * <p>
   * The reason this exists: {@code (databaseId, resourceId, revisionNumber)} does NOT uniquely
   * identify content over a resource's lifetime. {@code truncateTo} re-issues committed revision
   * numbers over different content on rollback and on the undo path of a failed multi-resource
   * commit, and crash recovery does the same. Both already sweep the page caches, and the memoized
   * ANSWERS derived from those pages have to go with them or a reader is served the discarded history
   * — silently, since a stale entry is indistinguishable from a fresh one.
   * </p>
   *
   * <p>
   * Note what is NOT a trigger, so a maintainer does not reason from it: resource ids are NOT reused.
   * {@code LocalDatabase.createResource} draws them from the monotonic persisted
   * {@code maxResourceID}, which {@code removeResource} never rolls back, and a database recreated at
   * a removed path mints a fresh random {@code databaseId}. Revision re-issue is the whole hazard.
   * </p>
   *
   * <p>
   * O(capacity) and rare, which is the right trade for a structure whose lookup path must stay a
   * single set probe: keeping a per-resource index would put bookkeeping on every admission to speed
   * up an operation that happens on rollback and resource deletion.
   * </p>
   *
   * @param databaseId the database the resource belongs to
   * @param resourceId the resource whose memoized lookups must go
   * @return the number of entries dropped
   */
  public int invalidateResource(final long databaseId, final long resourceId) {
    return invalidateMatching(databaseId, resourceId, true);
  }

  /**
   * Drop every memoized lookup belonging to one database, across all of its resources.
   *
   * @param databaseId the database whose memoized lookups must go
   * @return the number of entries dropped
   * @see #invalidateResource
   */
  public int invalidateDatabase(final long databaseId) {
    return invalidateMatching(databaseId, 0L, false);
  }

  private int invalidateMatching(final long databaseId, final long resourceId, final boolean matchResource) {
    final Object[] slots = table;
    if (slots == null) {
      return 0;
    }
    // BEFORE the scan, never after: a concurrent read-through that admits between the bump and its
    // slot being scanned is cleared by this very scan, and one that admits after the scan sees the
    // bumped value and declines. Bumping afterwards would leave both of those windows open.
    //
    // ONE counter for every scope, deliberately: a RESOURCE sweep bumps the same global generation a
    // DATABASE sweep does, so it also makes every unrelated in-flight admission process-wide decline.
    // That is the conservative direction — the cost of an over-broad bump is one recomputation, while
    // the cost of a missed one is a stale answer served for the rest of the revision — and it keeps
    // the admission path a single volatile read. Do NOT "optimise" this into per-resource stripes
    // without re-deriving the Dekker argument in put(): the counter a writer captures before its walk
    // and the counter a sweeper bumps must be the SAME location, or neither thread observes the other.
    GENERATION.getAndAdd(this, 1L);
    int dropped = 0;
    for (int i = 0; i < slots.length; i++) {
      final Entry entry = (Entry) TABLE.getAcquire(slots, i);
      if (entry == null || entry.key.databaseId() != databaseId) {
        continue;
      }
      if (matchResource && entry.key.resourceId() != resourceId) {
        continue;
      }
      TABLE.setRelease(slots, i, null);
      dropped++;
    }
    return dropped;
  }

  /** Drop every entry. Intended for shutdown and for tests that assert on a cold cache. */
  public void clear() {
    final Object[] slots = table;
    if (slots == null) {
      return;
    }
    // Same reason as invalidateMatching: bump first, so an in-flight admission cannot outlive it.
    GENERATION.getAndAdd(this, 1L);
    for (int i = 0; i < slots.length; i++) {
      TABLE.setRelease(slots, i, null);
    }
  }

  /**
   * Occupied slots. O(capacity) and racy — a diagnostic, never a hot-path call.
   *
   * @return the number of populated slots
   */
  public long size() {
    final Object[] slots = table;
    if (slots == null) {
      return 0L;
    }
    long occupied = 0L;
    // Through TABLE like every other reader in this class: the field's javadoc makes the
    // acquire/release the class's stated access discipline, and a plain load here would be the one
    // place it is not honoured — the place that would observe a half-initialised Entry the day Entry
    // stops being a record.
    for (int i = 0; i < slots.length; i++) {
      if (TABLE.getAcquire(slots, i) != null) {
        occupied++;
      }
    }
    return occupied;
  }
}
