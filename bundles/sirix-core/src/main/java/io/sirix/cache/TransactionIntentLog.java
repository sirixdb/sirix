package io.sirix.cache;

import io.sirix.HftBoundaryTelemetry;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import org.jspecify.annotations.Nullable;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2IntMap;
import it.unimi.dsi.fastutil.objects.Reference2IntOpenHashMap;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Transaction intent log (TIL) for caching all changes made by a read/write transaction.
 * <p>
 * The TIL stores modified pages during a transaction. When the transaction commits, pages are
 * written to storage. On rollback, the TIL is simply cleared.
 * <p>
 * Pages added to the TIL are removed from global caches since they represent uncommitted changes
 * that should not be visible to other transactions.
 * <p>
 * Supports epoch-based O(1) snapshotting for async auto-commit: {@link #snapshot()} swaps the
 * current arrays and increments the generation counter. A background thread can then flush the
 * frozen snapshot while the insert thread continues with a fresh TIL.
 *
 * @author Johannes Lichtenberger
 */
public final class TransactionIntentLog implements AutoCloseable {

  // ==================== CORE STORAGE (replaces ArrayList) ====================

  /** Page containers indexed by logKey. */
  private PageContainer[] entries;

  /** Back-references: entryRefs[i] is the PageReference that maps to entries[i]. */
  private PageReference[] entryRefs;

  /** Number of active entries in the current arrays. */
  private int size;

  /**
   * Scratch for {@link #releaseOrphanedHOTLeaves}: the identity set of candidate orphan leaves.
   *
   * <p>
   * Reference-keyed (identity, like {@code IdentityHashMap}) and reused across calls. The primitive
   * value is unused; this stays a map rather than allocating a second identity collection type. The
   * TIL is transaction-private, so a field is as safe here as a local was.
   * </p>
   */
  private final Reference2IntMap<HOTLeafPage> orphanCloseable = new Reference2IntOpenHashMap<>();

  /** Exact number of live current/pinned containers owning each HOT leaf instance. */
  private final Reference2IntMap<HOTLeafPage> hotLeafOwnerCounts = new Reference2IntOpenHashMap<>();

  /** O(1) ownership probes performed by orphan retirement (diagnostic/test evidence). */
  private long hotLeafOwnerProbeCount;

  {
    orphanCloseable.defaultReturnValue(-1);
    hotLeafOwnerCounts.defaultReturnValue(0);
  }

  /**
   * Page keys of the HOT leaves {@link #releaseOrphanedHOTLeaves} freed in this transaction.
   *
   * <p>
   * The log-identity tests ({@link #namesReleasedHOTLeafEntry}) only work while a reference still
   * NAMES the entry. Two ordinary events erase that: {@code refreshTransactionLogReference()} drops
   * the log key the moment a shared handle publishes a durable offset (which the pinned trie spill
   * does mid-transaction), and {@code PageReference#getPage()} clears the swizzle of a closed HOT
   * leaf on first read. After either, a reference to a merged-away leaf is indistinguishable from an
   * ordinary non-resident one, so the write path reloads it — and what comes back is the leaf's
   * <em>pre-merge</em> image, whose keys by then also live in the merge target.
   * </p>
   *
   * <p>
   * The page key survives both erasures: it is stamped into the page, so the durable image carries
   * it, and {@code pageKeyAllocator} issues it once — a split, a merge and a rebuild all take FRESH
   * keys for their outputs, and only a copy-on-write copy of the very same logical page repeats one
   * (and is released together with its source). The set therefore names exactly the logical pages
   * that no longer exist, and it is bounded by the number of leaves this transaction merged away —
   * merges, not records, so it does not grow with the load.
   * </p>
   *
   * <p>
   * Keyed by index scope because page keys are NOT globally unique: every {@code (indexType,
   * indexNumber)} pair has its own {@code maxHotPageKey} counter, and one log serves all of a
   * transaction's indexes at once. A single flat map would let a PATH merge blacklist a live
   * PROJECTION leaf that happens to carry the same number, which is a far worse failure than the one
   * this map exists to prevent.
   * </p>
   *
   * <p>
   * The value is the REFERENCE of the replacement the merge spliced in — the page that absorbed the
   * released leaf's entries — or {@code null} when the merge could not name one. Refusing the
   * pre-merge image is only half of the answer: a descent that reaches such a reference has to
   * continue SOMEWHERE, and the write path's "unresolvable means empty slot" arm answers that
   * question by fabricating a fresh empty leaf over it — a slot that then routes keys away from the
   * leaf that actually holds them. Forwarding to the replacement is what makes the stale copy route
   * correctly instead.
   * </p>
   *
   * <p>
   * The reference, not the {@code (generation, logKey)} identity it happened to carry while the merge
   * ran: that identity is exactly as perishable as the orphan's own. A pinned-trie spill publishes a
   * durable offset for the merge target mid-transaction and {@code refreshTransactionLogReference()}
   * drops its log key the moment it does, and an async-flush rotation retires the generation two
   * epochs later. In both cases a recorded identity resolves to nothing, and the forwarding degrades
   * back into the dead end it exists to prevent — silently, and only when the IO timing lines up,
   * which is why it survived as a Windows-lane-only failure. The reference survives both erasures
   * because it IS the slot the trie routes through: resolving it again reaches the log while the
   * replacement is resident and its durable offset once it is not. Stored, never copied — the copy
   * constructor refuses a reference with a pending page write, and a copy would stop tracking any
   * later rebinding of the slot.
   * </p>
   */
  private final Long2ObjectOpenHashMap<Long2ObjectOpenHashMap<PageReference>> releasedHOTLeafReplacements =
      new Long2ObjectOpenHashMap<>();

  /**
   * The same forwarding, keyed by the packed {@code (generation, logKey)} identity of the orphan.
   *
   * <p>
   * {@link #releasedHOTLeafReplacements} can only be consulted once the descent holds a page to read
   * a page key off, which means it has already reloaded the orphan's pre-merge image from storage. A
   * reference that still NAMES the released entry must not pay that read — nor seed the buffer cache
   * with a superseded image — so the identity it names maps to the replacement directly.
   * </p>
   */
  private final Long2ObjectOpenHashMap<PageReference> releasedHOTLeafIdentityReplacements =
      new Long2ObjectOpenHashMap<>();

  /**
   * Packed identity meaning "released, and the merge named no replacement to forward to". No real
   * identity can take it: {@link #packedIdentityOf} rejects a negative log key, so the low half is
   * never all-ones.
   */
  private static final long NO_REPLACEMENT = -1L;

  // ==================== RELEASE-SITE TAGS (diagnostic) ====================

  /** {@link #releaseOrphanedHOTLeaves} caller: the periodic leaf-consolidation sweep. */
  public static final int RELEASE_SITE_CONSOLIDATE = 1;
  /**
   * {@link #releaseOrphanedHOTLeaves} caller: a scoped {@code rebuildSubtree} at the insert depth.
   */
  public static final int RELEASE_SITE_REBUILD_SUBTREE = 2;
  /**
   * {@link #releaseOrphanedHOTLeaves} caller: {@code rebuildExistingSubtree} (self-heal / fold
   * repair).
   */
  public static final int RELEASE_SITE_REBUILD_EXISTING = 3;
  /** {@link #releaseOrphanedHOTLeaves} caller: the Direction-1 leaf-frontier splice. */
  public static final int RELEASE_SITE_FRONTIER_SPLICE = 4;
  /** {@link #releaseOrphanedHOTLeaves} caller: the root-leaf rebuild. */
  public static final int RELEASE_SITE_LEAF_REBUILD_ROOT = 5;
  /** {@link #releaseOrphanedHOTLeaves} caller: the off-path two-leaf strand migration. */
  public static final int RELEASE_SITE_TWO_LEAF_MIGRATION = 6;
  /** {@link #releaseOrphanedHOTLeaves} caller: an ordinary full-leaf split/integration. */
  public static final int RELEASE_SITE_LEAF_SPLIT = 7;
  /** {@link #releaseOrphanedHOTLeaves} caller: the canonical strand split/integration. */
  public static final int RELEASE_SITE_STRAND_SPLIT = 8;
  /** {@link #releaseOrphanedHOTLeaves} caller: a branch leaf paired under a fresh BiNode. */
  public static final int RELEASE_SITE_BRANCH_LEAF_PAIR = 9;

  /** A released page key nothing tagged — pre-instrumentation state or an unknown path. */
  public static final int RELEASE_SITE_UNKNOWN = -1;

  /**
   * Which call site released each page key, per index scope — pure diagnostics for the refusal path.
   * Parallel to {@link #releasedHOTLeafReplacements}: same keys, written by the same loop, so the two
   * cannot disagree about membership. Only the refusal path (which ends the transaction) reads it.
   */
  private final Long2ObjectOpenHashMap<Long2IntOpenHashMap> releasedHOTLeafSiteTags = new Long2ObjectOpenHashMap<>();

  /** The human-readable name of a {@code RELEASE_SITE_*} tag, for refusal messages. */
  public static String releaseSiteName(final int siteTag) {
    return switch (siteTag) {
      case RELEASE_SITE_CONSOLIDATE -> "consolidate-sweep";
      case RELEASE_SITE_REBUILD_SUBTREE -> "rebuild-subtree";
      case RELEASE_SITE_REBUILD_EXISTING -> "rebuild-existing-subtree";
      case RELEASE_SITE_FRONTIER_SPLICE -> "leaf-frontier-splice";
      case RELEASE_SITE_LEAF_REBUILD_ROOT -> "leaf-rebuild-root";
      case RELEASE_SITE_TWO_LEAF_MIGRATION -> "two-leaf-migration";
      case RELEASE_SITE_LEAF_SPLIT -> "leaf-split";
      case RELEASE_SITE_STRAND_SPLIT -> "strand-split";
      case RELEASE_SITE_BRANCH_LEAF_PAIR -> "branch-leaf-pair";
      default -> "unknown-site";
    };
  }

  /**
   * The {@code RELEASE_SITE_*} tag recorded for a released page key, or
   * {@link #RELEASE_SITE_UNKNOWN}.
   */
  public int releasedHOTLeafSiteTag(final long indexScope, final long pageKey) {
    final Long2IntOpenHashMap tags = releasedHOTLeafSiteTags.get(indexScope);
    return tags == null
        ? RELEASE_SITE_UNKNOWN
        : tags.getOrDefault(pageKey, RELEASE_SITE_UNKNOWN);
  }

  // ==================== GENERATION COUNTER ====================

  /** Current generation. Incremented on each snapshot(). Used for O(1) epoch membership. */
  private int currentGeneration;

  // ==================== SNAPSHOT STATE ====================

  /** Frozen entries from the last snapshot() call. Null if no active snapshot. */
  private PageContainer[] snapshotEntries;

  /** Frozen back-references from the last snapshot() call. */
  private PageReference[] snapshotRefs;

  /** Number of entries in the frozen snapshot. */
  private int snapshotSize;

  /** Generation counter at the time of the snapshot. */
  private int snapshotGeneration;

  /** Set to true by the background thread when snapshot flush is complete. */
  private volatile boolean snapshotCommitComplete;

  // ==================== SIDE-CHANNEL FOR BACKGROUND THREAD ====================
  // Background thread NEVER writes to PageReference directly — stores results here.
  // cleanupSnapshot() (insert thread, after semaphore) applies them.

  /** Disk offsets written by background thread. Initialized to NULL_ID_LONG sentinel. */
  private long[] snapshotDiskOffsets;

  /** Primitive page hashes computed by the background thread. */
  private long[] snapshotHashes;

  /** Presence bits for {@link #snapshotHashes}; zero is a valid present XXH3 checksum. */
  private boolean[] snapshotHashPresent;

  /**
   * Reachability-scoped identities captured with the frozen references. The reference objects may be
   * rebound while the snapshot is in flight, so cleanup must complete the identity that was frozen
   * rather than whichever identity the mutable reference carries by then.
   */
  private PageReference.TransactionLogReference[] snapshotLogReferences;

  // ==================== LEGACY STALE-REFERENCE FALLBACK ====================
  // Current PageReference copies share a reachability-scoped transaction-log handle, which is
  // completed by cleanupSnapshot() and then discarded as each copy resolves. These maps preserve
  // compatibility for a manually constructed reference that omitted that handle. They should stay
  // empty in current code; the scale gate asserts that invariant.

  /** Completed disk offsets indexed by packed (generation << 32 | logKey). No autoboxing. */
  private final Long2LongOpenHashMap completedDiskOffsets;

  /** Completed primitive page hashes indexed by the same packed key; map membership is presence. */
  private final Long2LongOpenHashMap completedDiskHashes = new Long2LongOpenHashMap();

  /** Counter: Layer 3 hits (stale references resolved from completed disk offsets). */
  private long layer3Hits;

  // ==================== SNAPSHOT BOOKKEEPING DIAGNOSTIC ====================
  // A bulk import is one long transaction with tens of thousands of flushes. The census verifies
  // that the compatibility maps above remain empty and that the pinned structural region grows only
  // with the reachable trie, rather than with every page written during the transaction.
  // -Dsirix.til.diag=<n> prints the census every n-th cleanupSnapshot (0 = off).

  /** Print the snapshot census every n-th {@link #cleanupSnapshot()}; 0 disables it. */
  private static final int DIAG_EVERY = Integer.getInteger("sirix.til.diag", 0);

  /** Number of completed {@link #cleanupSnapshot()} calls. */
  private long cleanupCount;

  /** Structural (non-{@link KeyValueLeafPage}) containers promoted back into the live TIL, total. */
  private long promotedStructuralTotal;

  // ==================== LEGACY FORWARDING FALLBACK (superseded-flush fix, #1077)
  // ====================
  // When a frozen page is CoW'd into the current generation (put() with a prior-generation
  // reference), the old (generation, logKey) identity is SUPERSEDED: the frozen page's flush —
  // whose offset lands in completedDiskOffsets at cleanupSnapshot() — describes an OUTDATED
  // version of the page. Stale reference copies still carrying the old identity (in CoW'd
  // IndirectPages that are never rewalked, e.g. for a leaf page that straddles an epoch
  // boundary during monotonic bulk inserts) must resolve to the NEW entry, not to the stale
  // flush — otherwise the final commit durably serializes the outdated page and every record
  // added after the boundary silently vanishes. This map stores packed(oldGen, oldLogKey) →
  // packed(newGen, newLogKey); get() follows the chain to the terminal identity before
  // consulting the TIL layers. Entries are retained until clear().
  //
  // READERS OF THIS MAP — audited in full when the pinned region below was introduced, because
  // the retention here is deliberate and a pinned page MUST stay reachable through every one of
  // them by references that still carry its old identity:
  //
  // 1. get(), the chain walk — the only reader with semantics. It follows the chain to a
  // terminal identity and then resolves a PINNED terminal against the pinned region before
  // trying the generation-scoped layers, rebinding the stale copy so it never walks again.
  // This is precisely what keeps an old reference working: the single link written at pin()
  // time targets (PINNED_GENERATION, slot), and that target never moves again, so the chain
  // is at most one hop where it used to gain a hop per flush.
  // 2. cleanupSnapshot(), the superseded guard `forwardedEntries.get(packedKey) < 0` that
  // decides whether a completed disk offset may be published. It is keyed on
  // (snapshotGeneration, i) of a KeyValueLeafPage slot. A pinned page never enters a
  // snapshot, so this reader never sees a pinned key — and a key WRITTEN by pin() cannot
  // collide with one either, because a structural page and a record page cannot occupy the
  // same index of the same generation's array.
  // 3. forwardedEntryCount() and the diag estimate — diagnostics, no semantics.
  // 4. clear() / close() — teardown.

  /** Forwarding chain for superseded (generation << 32 | logKey) identities. */
  private final Long2LongOpenHashMap forwardedEntries = new Long2LongOpenHashMap();

  {
    forwardedEntries.defaultReturnValue(-1L);
  }

  // ==================== PINNED ENTRIES (unflushable pages, stable identity) ====================
  // Only KeyValueLeafPages are attempted by a background snapshot flush. Everything else — the
  // IndirectPages of every trie, the per-index root pages, HOT pages — has to survive in memory
  // until the final commit serializes it. A KVL whose disposable serialization discovers an
  // unresolved OverflowPage joins this region dynamically for the same reason: only recursive
  // final commit can write its children before the leaf. Those pages used to ride the epoch
  // machinery anyway:
  // snapshot() froze them, cleanupSnapshot() promoted them back into the fresh log under a NEW
  // (generation, logKey), and each promotion recorded a permanent forwarding link so that stale
  // reference copies could still find them.
  //
  // That made the log's bookkeeping QUADRATIC in the number of flushes. The resident structural
  // set grows with the corpus (it is the trie spine of an uncommitted transaction) and ALL of it
  // was re-promoted on EVERY flush, so the forwarding map grew by the whole resident set each
  // time. Measured on a ClickBench one-pass load: ~3.4 resident pages added per flush, and
  // forwardedEntries(F) = 1.70 * F^2 — 17.4 million entries after 4M rows, and an extrapolated
  // 160 million by 12M rows, which is exactly where a 16 GB heap died inside a rehash of it.
  //
  // A pinned entry lives outside the generation-scoped arrays and its identity NEVER changes
  // again: snapshot() does not freeze it, cleanupSnapshot() does not promote it, and a reference
  // to it needs no forwarding link after the single one recorded when it was pinned. The
  // bookkeeping is therefore one map entry per structural page for the life of the transaction
  // instead of one per structural page per flush, and the per-flush re-put churn — two buffer
  // cache operations per resident page per flush — disappears with it.
  //
  // The index space is separate on purpose. A pinned reference is marked by a sentinel
  // generation no real generation can take, so it can never be confused with a live-array slot;
  // that is the same hazard the cross-generation guard in put() exists for (a stale record-page
  // copy resolving to a structural page and back, which surfaced as a ClassCastException).

  /**
   * The {@code activeTilGeneration} of a reference whose container lives in the pinned region.
   *
   * <p>
   * <b>Positive on purpose.</b> {@link #forwardedEntries} encodes "absent" as a negative value and
   * {@code get()} tests a hit with {@code forwarded >= 0}, so a forwarding target packed from a
   * NEGATIVE generation would read as a miss and a stale reference would silently lose its container.
   * Real generations count one per flush from zero and cannot reach this value.
   * </p>
   *
   * <p>
   * <b>Why the pinned region needs its own index space, and what goes wrong without it.</b> A log key
   * alone does not identify an entry — it is an index into an array, and there are now two arrays. If
   * pinned entries shared the generation-scoped index space, a log key would address a different
   * container in each, and the two page populations are not interchangeable: a record page resolving
   * to a structural page (or the reverse) is the exact hazard the cross-generation guard in
   * {@link #put} was written for, whose visible symptom was a {@code ClassCastException} in
   * {@code KeyedTrieWriter.prepareIndirectPage} when trie navigation walked a structural page's
   * reference and got a foreign container back. Its invisible symptom is worse: a container swapped
   * at an index is a page committed from the wrong data.
   * </p>
   *
   * <p>
   * The sentinel closes that off by construction rather than by discipline. A stale record-page copy
   * can only ever carry a real generation, never this value, so it can never enter the pinned region;
   * and a pinned reference always carries this value, so it can never be indexed into the
   * generation-scoped arrays. Every site that resolves a log key therefore branches on the generation
   * first — see {@link #get}, {@link #getOriginalRef} and {@link #releaseOrphanedHOTLeaves}.
   * </p>
   */
  public static final int PINNED_GENERATION = Integer.MAX_VALUE;

  /**
   * Escape hatch that restores the pre-fix behaviour — every unflushable page rides the epoch
   * machinery and is re-promoted per flush. Its only purpose is to let
   * {@code AsyncFlushLogBookkeepingTest} prove it is not vacuous by watching its assertions fail; a
   * {@code static final} read once at class initialisation, so the check folds away.
   */
  private static final boolean PINNING_ENABLED = !Boolean.getBoolean("sirix.til.disablePinning");

  /** Containers that no background flush can write, indexed by their pinned log key. */
  private PageContainer[] pinnedEntries = new PageContainer[64];

  /** The reference that owns each pinned slot — the canonical one when copies share it. */
  private PageReference[] pinnedRefs = new PageReference[64];

  /**
   * Next never-before-used pinned slot in the current identity epoch. A tombstoned slot is never
   * reused while any shared handle can still be unresolved. Teardown may reset the epoch only when
   * every surviving handle is proven durable and no prior teardown on this TIL ever retired an
   * unresolved identity; see {@link #clearPinnedEntries()}.
   */
  private int pinnedHighWater;

  /**
   * Fail-closed identity-retirement latch. Once teardown observes even one unresolved pinned handle,
   * no later (possibly empty) clear may reset {@link #pinnedHighWater} for this TIL instance. The
   * unresolved handle can outlive its canonical slot indefinitely; a later durable epoch therefore
   * cannot prove that the old numeric identity became unreachable.
   */
  private boolean pinnedSlotReusePermanentlyDisabled;

  /** Number of non-tombstoned entries in the pinned region. */
  private int pinnedLiveCount;

  /** Dense live-position to append-only pinned-slot mapping. */
  private int[] livePinnedSlots = new int[64];

  /** Append-only pinned-slot to dense live-position mapping; {@code -1} means tombstoned/unused. */
  private int[] livePinnedPositionBySlot = new int[64];

  /** Persistent dense scan position used by bounded trie-spill capture. */
  private int pinnedSpillScanCursor;

  /**
   * Pinned slot per page instance (identity-keyed), so a second reference to an already pinned page
   * rebinds to the existing slot instead of pinning the same page twice.
   */
  private final Reference2IntMap<Page> pinnedSlotByPage = new Reference2IntOpenHashMap<>();

  {
    pinnedSlotByPage.defaultReturnValue(-1);
  }

  /**
   * Fixed-capacity, owner-reused capture buffer for bounded pinned-page spilling.
   *
   * <p>
   * The buffer never grows. A production writer allocates one at construction and reuses it for every
   * epoch; tests may choose a smaller capacity to make scan/candidate bounds observable. The captured
   * tuple is deliberately redundant: publication must prove that the exact slot, reference,
   * container, modified page and reachability handle still agree after the bytes have been flushed
   * and before any durable identity is exposed.
   * </p>
   */
  public static final class PinnedSpillBatch {
    private final int[] slots;
    private final PageReference[] references;
    private final PageContainer[] containers;
    private final Page[] pages;
    private final PageReference.TransactionLogReference[] handles;
    private final long[] diskOffsets;
    private final long[] hashes;
    private final boolean[] hashPresent;
    private int size;

    public PinnedSpillBatch(final int capacity) {
      if (capacity <= 0) {
        throw new IllegalArgumentException("Pinned spill batch capacity must be > 0, got " + capacity);
      }
      slots = new int[capacity];
      references = new PageReference[capacity];
      containers = new PageContainer[capacity];
      pages = new Page[capacity];
      handles = new PageReference.TransactionLogReference[capacity];
      diskOffsets = new long[capacity];
      Arrays.fill(diskOffsets, Constants.NULL_ID_LONG);
      hashes = new long[capacity];
      hashPresent = new boolean[capacity];
    }

    public int capacity() {
      return slots.length;
    }

    public int size() {
      return size;
    }

    public int slotAt(final int index) {
      checkIndex(index);
      return slots[index];
    }

    public PageReference referenceAt(final int index) {
      checkIndex(index);
      return references[index];
    }

    public PageContainer containerAt(final int index) {
      checkIndex(index);
      return containers[index];
    }

    public Page pageAt(final int index) {
      checkIndex(index);
      return pages[index];
    }

    public PageReference.TransactionLogReference handleAt(final int index) {
      checkIndex(index);
      return handles[index];
    }

    public long diskOffsetAt(final int index) {
      checkIndex(index);
      return diskOffsets[index];
    }

    public long hashAt(final int index) {
      checkIndex(index);
      return hashes[index];
    }

    public boolean hashPresentAt(final int index) {
      checkIndex(index);
      return hashPresent[index];
    }

    public void setWriteResult(final int index, final long diskOffset, final long hash, final boolean hashIsPresent) {
      checkIndex(index);
      if (diskOffset == Constants.NULL_ID_LONG) {
        throw new IllegalArgumentException("A pinned spill write requires a durable offset");
      }
      diskOffsets[index] = diskOffset;
      hashes[index] = hash;
      hashPresent[index] = hashIsPresent;
    }

    /** Remove one rejected candidate without allocating or preserving order. */
    public void removeAtSwap(final int index) {
      checkIndex(index);
      final int last = --size;
      if (index != last) {
        slots[index] = slots[last];
        references[index] = references[last];
        containers[index] = containers[last];
        pages[index] = pages[last];
        handles[index] = handles[last];
        diskOffsets[index] = diskOffsets[last];
        hashes[index] = hashes[last];
        hashPresent[index] = hashPresent[last];
      }
      clearSlot(last);
    }

    /** Sever every captured object after success or failure; the buffer itself remains reusable. */
    public void clear() {
      while (size > 0) {
        clearSlot(--size);
      }
    }

    private void add(final int slot, final PageReference reference, final PageContainer container, final Page page,
        final PageReference.TransactionLogReference handle) {
      if (size == slots.length) {
        throw new IllegalStateException("Pinned spill batch capacity was exceeded");
      }
      slots[size] = slot;
      references[size] = reference;
      containers[size] = container;
      pages[size] = page;
      handles[size] = handle;
      diskOffsets[size] = Constants.NULL_ID_LONG;
      hashes[size] = 0L;
      hashPresent[size] = false;
      size++;
    }

    private void clearSlot(final int index) {
      slots[index] = 0;
      references[index] = null;
      containers[index] = null;
      pages[index] = null;
      handles[index] = null;
      diskOffsets[index] = Constants.NULL_ID_LONG;
      hashes[index] = 0L;
      hashPresent[index] = false;
    }

    private void checkIndex(final int index) {
      if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
      }
    }
  }

  // ==================== BUFFER MANAGER ====================

  /** The buffer manager. */
  private final BufferManager bufferManager;

  /**
   * Creates a new transaction intent log.
   *
   * @param bufferManager the buffer manager for cache operations
   * @param maxInMemoryCapacity the initial capacity (number of expected modified pages)
   */
  public TransactionIntentLog(final BufferManager bufferManager, final int maxInMemoryCapacity) {
    this.bufferManager = bufferManager;
    final int initialCapacity = Math.max(maxInMemoryCapacity, 64);
    entries = new PageContainer[initialCapacity];
    entryRefs = new PageReference[initialCapacity];
    size = 0;
    currentGeneration = 0;
    Arrays.fill(livePinnedSlots, -1);
    Arrays.fill(livePinnedPositionBySlot, -1);
    completedDiskOffsets = new Long2LongOpenHashMap();
    completedDiskOffsets.defaultReturnValue(Constants.NULL_ID_LONG);
  }

  /**
   * Retrieves an entry from the TIL using generation-based 3-layer lookup.
   * <p>
   * Layer 1: Current TIL (fast path — most common during insertion). Layer 2: Active snapshot (if any
   * — for reads of pages not yet cleaned up). Layer 3: Completed disk offsets (for stale reference
   * copies from CoW'd IndirectPages).
   *
   * @param ref the page reference whose associated container is to be returned
   * @return the page container, or {@code null} if not in any TIL layer
   */
  public PageContainer get(final PageReference ref) {
    HftBoundaryTelemetry.tilRead();
    // A copied reference may still carry the generation/log-key fields it had when it was copied.
    // Refreshing follows its reachability-scoped handle to either the newest TIL identity or the
    // durable offset, without consulting transaction-wide historical maps.
    ref.refreshTransactionLogReference();
    int logKey = ref.getLogKey();
    if (logKey < 0) {
      return null;
    }

    int generation = ref.getActiveTilGeneration();

    // Layers 0-2 (pinned region, current TIL, active snapshot) in one lookup. An identity that is
    // HERE but no longer resolvable — a HOT leaf an incremental merge released — is NOT a miss: the
    // merge forwarded it to the page that absorbed its entries, so it has to reach the forwarding
    // chain below rather than short-circuit to null.
    final PageContainer resident = resolvePackedIdentity(((long) generation << 32) | (logKey & 0xFFFFFFFFL));
    if (resident != null) {
      return resident;
    }
    // A tombstoned pinned slot names nothing at all; a pinned identity's resolution is final, so
    // there is no later layer for it either way.
    if (generation == PINNED_GENERATION && !isLivePinnedSlot(logKey)) {
      return null;
    }

    // Layer 2.5: Forwarding chain for superseded identities (#1077). A frozen page that was
    // CoW'd into a newer generation was re-logged under a new (generation, logKey); this stale
    // copy must resolve to that newer entry — NOT to the frozen page's (outdated) flush in the
    // completed-offsets map. Follow the chain to the terminal identity, then retry the layers.
    if (!forwardedEntries.isEmpty()) {
      long packed = ((long) generation << 32) | (logKey & 0xFFFFFFFFL);
      long forwarded = forwardedEntries.get(packed);
      if (forwarded >= 0) {
        while (true) {
          final long next = forwardedEntries.get(forwarded);
          if (next < 0) {
            break;
          }
          forwarded = next;
        }
        generation = (int) (forwarded >> 32);
        logKey = (int) forwarded;

        if (generation == PINNED_GENERATION && isLivePinnedSlot(logKey)) {
          // Terminal identity is a pinned slot — rebind so this copy never walks the chain again.
          ref.setLogKey(logKey);
          ref.setActiveTilGeneration(PINNED_GENERATION);
          return resolvableContainer(pinnedEntries[logKey]);
        }
        if (generation == currentGeneration && logKey < size) {
          // Rebind the stale copy to its terminal identity so later lookups take the fast path.
          ref.setLogKey(logKey);
          ref.setActiveTilGeneration(generation);
          return resolvableContainer(entries[logKey]);
        }
        if (snapshotEntries != null && generation == snapshotGeneration && logKey < snapshotSize) {
          ref.setLogKey(logKey);
          ref.setActiveTilGeneration(generation);
          return resolvableContainer(snapshotEntries[logKey]);
        }
        // Fall through to Layer 3 with the terminal identity (the newest flushed version).
      }
    }

    // A released entry that the merge did not forward: the identity names a page that is gone, and
    // Layer 3's disk offset addresses its PRE-MERGE image. Answering "not in the log" is the honest
    // result — the caller classifies it with namesReleasedHOTLeafEntry.
    if (namesReleasedHOTLeafEntry(logKey, generation)) {
      return null;
    }

    // Layer 3: Completed disk offsets — stale reference fix.
    // When an IndirectPage is CoW'd, its child references are copied. If cleanupSnapshot()
    // later applies a disk offset to the ORIGINAL reference, the COPY doesn't get updated.
    // This layer resolves stale copies by applying the disk offset from the completed map.
    //
    // Resolution is NON-destructive (#1077): an IndirectPage can be CoW'd more than once, so
    // several stale copies may carry the same (generation, logKey). The former remove() served
    // only the first copy; every later copy missed all three layers and was silently serialized
    // with child key -1 (or resurrected as a brand-new empty page on the write path). Entries
    // stay resolvable until clear() at commit/rollback.
    if (!completedDiskOffsets.isEmpty()) {
      final long packedKey = ((long) generation << 32) | (logKey & 0xFFFFFFFFL);
      final long diskOffset = completedDiskOffsets.get(packedKey);
      if (diskOffset != Constants.NULL_ID_LONG) {
        ref.setKey(diskOffset);
        if (completedDiskHashes.containsKey(packedKey)) {
          ref.setHash(completedDiskHashes.get(packedKey));
        } else {
          ref.clearHash();
        }
        // Reset logKey/generation so this ref is recognized as a "disk" reference
        ref.setLogKey(Constants.NULL_ID_INT);
        ref.setActiveTilGeneration(-1);
        layer3Hits++;
        return null; // Page is on disk, not in TIL — caller loads from disk
      }
    }

    return null;
  }

  /**
   * Resolve an exact transaction-log identity to the authoritative mutable document page.
   *
   * <p>
   * This deliberately does not follow frozen snapshot identities: a document page in the snapshot
   * must first pass through the ordinary key-based write path so copy-on-write can install a current
   * container. Dynamically pinned document pages are mutable transaction-owned pages and remain
   * eligible. Reading the container from the addressed slot, instead of trusting a cached page
   * reference, also makes same-generation container replacement safe.
   * </p>
   *
   * @param identity packed {@code (generation, logKey)} assigned to a {@link PageContainer}
   * @return the current modified DOCUMENT page, or {@code null} when the identity is stale or invalid
   */
  public @Nullable KeyValueLeafPage getAuthoritativeDocumentPage(final long identity) {
    final int generation = (int) (identity >> 32);
    final int logKey = (int) identity;

    final PageContainer container;
    final PageReference reference;
    if (generation == currentGeneration && logKey >= 0 && logKey < size) {
      container = entries[logKey];
      reference = entryRefs[logKey];
    } else if (generation == PINNED_GENERATION && isLivePinnedSlot(logKey)) {
      container = pinnedEntries[logKey];
      reference = pinnedRefs[logKey];
    } else {
      return null;
    }

    if (container == null || reference == null || container.getTransactionLogIdentity() != identity
        || reference.getActiveTilGeneration() != generation || reference.getLogKey() != logKey) {
      return null;
    }

    if (container.getModified() instanceof KeyValueLeafPage page && page.getIndexType() == IndexType.DOCUMENT
        && !page.isClosed() && !page.isOrphaned()) {
      return page;
    }
    return null;
  }

  /**
   * Adds an entry to the transaction intent log.
   * <p>
   * The page is removed from global caches as the TIL now owns it exclusively. Guards are released
   * since TIL pages are transaction-private.
   * <p>
   * CRITICAL: Pages removed from caches during this operation are closed if they differ from the
   * pages in the new PageContainer. This prevents memory leaks when a cached combined-page is
   * replaced by newly created pages from combineRecordPagesForModification().
   *
   * @param ref the page reference key
   * @param value the page container with complete and modified versions
   */
  public void put(final PageReference ref, final PageContainer value) {
    HftBoundaryTelemetry.tilWrite();
    // Clear cached hash before modifying key properties
    ref.clearCachedHash();

    // Remove from caches - TIL takes exclusive ownership of NEW pages.
    // removeAndGet, not get-then-remove: those were two separate cache operations, and with
    // concurrent readers repopulating the record cache a page inserted BETWEEN them was evicted by
    // the remove while the close below examined the page the get had seen. The page in the middle
    // ended up owned by nobody — out of the cache, never in a TIL container, never closed — which is
    // where the last of the -Dsirix.debug.memory.leaks survivors came from, growing with the number
    // of concurrent readers and commits.
    final KeyValueLeafPage oldCachedPage = bufferManager.getRecordPageCache().removeAndGet(ref);
    bufferManager.getPageCache().remove(ref);
    // HOT leaf pages: the same instance can back both a TIL container and the shared
    // HOT-leaf cache. Take it out of the cache so the background sweeper and pressure
    // eviction cannot close its off-heap slot while the TIL still owns the page for commit.
    removeHOTLeavesFromCache(value);

    // Close the old cached page if it's different from the pages going into TIL
    if (oldCachedPage != null && !oldCachedPage.isClosed()) {
      final boolean isInNewContainer = (oldCachedPage == value.getComplete() || oldCachedPage == value.getModified());
      if (!isInNewContainer) {
        oldCachedPage.markOrphaned();
        oldCachedPage.close();
      }
    }

    ref.setKey(Constants.NULL_ID_LONG);
    ref.setPage(null);

    final int existingKey = ref.getLogKey();

    // A pinned reference keeps its slot for the life of the transaction: replace the container in
    // place. Falling through to the cross-generation branch below would mint a fresh identity and
    // a forwarding link on every write to a structural page — the churn pinning exists to remove.
    if (ref.getActiveTilGeneration() == PINNED_GENERATION && isLivePinnedSlot(existingKey)) {
      replacePinnedContainer(existingKey, ref, value);
      releaseContainerGuards(value);
      return;
    }

    // Cross-generation guard: a reference whose activeTilGeneration belongs to a
    // PRIOR (snapshot) generation must NOT reuse its old logKey in the new TIL.
    // Without this check, a CoW of a frozen IndirectPage after
    // asyncFlush() can collide with a structural-page slot in the
    // new generation (NamePage / CASPage / ProjectionIndexPage etc. that
    // reAddStructuralPagesToTil packed into low logKeys), silently swapping
    // the container at that index. The visible symptom is a ClassCastException
    // in KeyedTrieWriter.prepareIndirectPage when the trie navigation later
    // walks the structural page's reference and gets back the foreign container.
    final boolean ownsExistingKey = existingKey != Constants.NULL_ID_INT && existingKey >= 0 && existingKey < size
        && ref.getActiveTilGeneration() == currentGeneration;
    if (ownsExistingKey) {
      // Close orphaned HOT leaf pages from the overwritten container.
      // After a leaf split the old complete page (original from disk/copy) is no longer
      // needed — the modified page has completeDump=true and all entries marked dirty.
      final PageContainer oldContainer = entries[existingKey];
      final boolean replacingContainer = oldContainer != value;
      if (replacingContainer) {
        // Stage the new ownership before inspecting the old container. Apart from making the
        // identity-map allocation a pre-publication operation, this also makes a HOT leaf reused by
        // the new container visibly shared while the old container is retired.
        noteHOTLeafOwners(value);
        try {
          if (oldContainer != null) {
            closeOrphanedHOTLeafPages(oldContainer, value);
            closeOrphanedRecordPages(oldContainer, value, existingKey);
            forgetHOTLeafOwners(oldContainer);
          }
        } catch (final RuntimeException | Error failure) {
          forgetHOTLeafOwners(value);
          throw failure;
        }
      }
      // Reuse existing logKey — update container in-place.
      // This ensures that PageReference copies (from COW operations) that share
      // the same logKey will resolve to the latest container.
      entries[existingKey] = value;
      entryRefs[existingKey] = ref;
      value.setTransactionLogIdentity(currentGeneration, existingKey);
      ref.setActiveTilGeneration(currentGeneration);
    } else {
      // A cross-generation re-put supersedes the frozen entry this reference used to identify:
      // the CoW'd page in the NEW entry is now the authoritative version, and the frozen page's
      // upcoming (or already completed) flush is outdated. Record a forwarding link so stale
      // reference copies that still carry the old identity resolve to the new entry instead of
      // the stale disk offset (#1077).
      final int priorGeneration = ref.getActiveTilGeneration();
      final boolean supersedesPriorEntry = existingKey != Constants.NULL_ID_INT && existingKey >= 0
          && priorGeneration >= 0 && priorGeneration != currentGeneration;

      // New entry
      ensureCapacity();
      noteHOTLeafOwners(value);
      final boolean forwardedByReference;
      try {
        forwardedByReference = ref.bindToTransactionLog(size, currentGeneration, supersedesPriorEntry);
      } catch (final RuntimeException | Error failure) {
        forgetHOTLeafOwners(value);
        throw failure;
      }
      entries[size] = value;
      entryRefs[size] = ref;
      value.setTransactionLogIdentity(currentGeneration, size);
      final int newEntryIndex = size++;

      // Legacy fallback for a reference constructed by old/manual field copying without sharing
      // its resolution handle. Every in-tree copy path shares the handle; retaining this fallback
      // makes an omitted integration path fail safe rather than lose a page.
      if (supersedesPriorEntry && !forwardedByReference) {
        final long oldPacked = ((long) priorGeneration << 32) | (existingKey & 0xFFFFFFFFL);
        final long newPacked = ((long) currentGeneration << 32) | (newEntryIndex & 0xFFFFFFFFL);
        forwardedEntries.put(oldPacked, newPacked);
      }
    }

    releaseContainerGuards(value);
  }

  /** Release the guards of a container the log has just taken ownership of. */
  private static void releaseContainerGuards(final PageContainer value) {
    // Release guards - TIL pages are transaction-private
    if (value.getComplete() instanceof KeyValueLeafPage completePage && completePage.getGuardCount() > 0) {
      completePage.releaseGuard();
    }
    if (value.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != value.getComplete()
        && modifiedPage.getGuardCount() > 0) {
      modifiedPage.releaseGuard();
    }
  }

  /**
   * Remove the HOT leaf pages of a TIL container from the shared HOT-leaf buffer cache.
   *
   * <p>
   * A {@link HOTLeafPage} instance can be referenced by both a {@link PageContainer} in this log and
   * the {@code hotLeafPageCache}. Once a page is in the log it is transaction-private and must
   * survive — unevicted — until commit serializes it. The eviction paths ({@code ClockSweeper.sweep},
   * {@code ShardedPageCache.evictUnderPressure}) only gate on guard count, so a cache-resident dirty
   * leaf would otherwise have its off-heap slot reclaimed under memory pressure, corrupting the
   * committed page. This mirrors the record-page-cache removal already performed above for
   * {@link KeyValueLeafPage}s.
   * </p>
   */
  private void removeHOTLeavesFromCache(final PageContainer value) {
    if (value == null) {
      return;
    }
    final Cache<PageReference, HOTLeafPage> hotLeafCache = bufferManager.getHOTLeafPageCache();
    if (value.getComplete() instanceof HOTLeafPage complete) {
      hotLeafCache.removePage(complete);
    }
    if (value.getModified() instanceof HOTLeafPage modified && modified != value.getComplete()) {
      hotLeafCache.removePage(modified);
    }
  }

  /** Add one current/pinned container owner for each distinct HOT leaf it holds. */
  private void noteHOTLeafOwners(final PageContainer container) {
    if (container == null) {
      return;
    }
    final Page complete = container.getComplete();
    final Page modified = container.getModified();
    if (complete instanceof HOTLeafPage completeLeaf) {
      incrementHOTLeafOwner(completeLeaf);
    }
    if (modified instanceof HOTLeafPage modifiedLeaf && modifiedLeaf != complete) {
      try {
        incrementHOTLeafOwner(modifiedLeaf);
      } catch (final RuntimeException | Error failure) {
        if (complete instanceof HOTLeafPage completeLeaf) {
          decrementHOTLeafOwner(completeLeaf);
        }
        throw failure;
      }
    }
  }

  /** Remove one current/pinned container owner for each distinct HOT leaf it holds. */
  private void forgetHOTLeafOwners(final PageContainer container) {
    if (container == null) {
      return;
    }
    final Page complete = container.getComplete();
    final Page modified = container.getModified();
    if (complete instanceof HOTLeafPage completeLeaf) {
      decrementHOTLeafOwner(completeLeaf);
    }
    if (modified instanceof HOTLeafPage modifiedLeaf && modifiedLeaf != complete) {
      decrementHOTLeafOwner(modifiedLeaf);
    }
  }

  private void incrementHOTLeafOwner(final HOTLeafPage leaf) {
    final int owners = hotLeafOwnerCounts.getInt(leaf);
    if (owners == Integer.MAX_VALUE) {
      throw new IllegalStateException("HOT leaf owner count overflow for page " + leaf.getPageKey());
    }
    hotLeafOwnerCounts.put(leaf, owners + 1);
  }

  private void decrementHOTLeafOwner(final HOTLeafPage leaf) {
    final int owners = hotLeafOwnerCounts.getInt(leaf);
    if (owners <= 0) {
      throw new IllegalStateException("HOT leaf owner count underflow for page " + leaf.getPageKey());
    }
    if (owners == 1) {
      hotLeafOwnerCounts.removeInt(leaf);
    } else {
      hotLeafOwnerCounts.put(leaf, owners - 1);
    }
  }

  /**
   * Ensure the entries/entryRefs arrays have room for one more element. Doubles capacity when full.
   */
  private void ensureCapacity() {
    if (size == entries.length) {
      final int newCap = entries.length << 1;
      entries = Arrays.copyOf(entries, newCap);
      entryRefs = Arrays.copyOf(entryRefs, newCap);
    }
  }

  /**
   * The container a resolution may hand out, or {@code null} once every page it holds has been
   * released.
   *
   * <p>
   * Releasing a merged-away HOT leaf ({@link #releaseOrphanedHOTLeaves}) frees its 64 KB off-heap
   * frame but deliberately leaves the container in place — the commit path iterates the log by index,
   * so a hole is not a state it may see. Without this filter the entry stayed RESOLVABLE: every
   * reference naming it — including the copies each indirect-page copy-on-write deep-copies — got the
   * freed page back, because both {@code loadHOTPage} implementations hand a container's page
   * straight to their caller. The HOT writer then dead-ended in {@code cowHOTLeafForModification}:
   * {@code acquireGuard()} can never succeed on a released page and re-resolution keeps producing
   * that same instance, so its retry could not win on ANY schedule. That is the
   * {@code windows-latest / query} lane's "HOT leaf disappeared while acquiring a copy-on-write
   * guard"; the same producer merely spins instead when the teardown is still deferred. Resolving to
   * nothing sends the caller to the durable/reload path instead of into dead memory.
   * </p>
   *
   * <p>
   * {@link HOTLeafPage#isOrphaned()} and not only {@link HOTLeafPage#isClosed()} is the predicate:
   * {@code close()} always orphans the page but defers the actual teardown to the last
   * {@code releaseGuard()} while a reader still holds it. In that window the page is already unusable
   * — {@code acquireGuard()} refuses it — while {@code isClosed()} still reads false, and that window
   * is precisely the one the writer used to spin in.
   * </p>
   *
   * @param container the container an entry holds, possibly {@code null}
   * @return {@code container}, or {@code null} if none of its pages can serve a resolution
   */
  private static @Nullable PageContainer resolvableContainer(final @Nullable PageContainer container) {
    if (container == null) {
      return null;
    }
    final Page complete = container.getComplete();
    final Page modified = container.getModified();
    // Two class checks on the hottest read path in the engine: record-page and structural containers
    // are never affected and leave immediately.
    if (!(complete instanceof HOTLeafPage) && !(modified instanceof HOTLeafPage)) {
      return container;
    }
    if (servesResolution(complete) || (modified != complete && servesResolution(modified))) {
      return container;
    }
    return null;
  }

  /** Whether {@code page} can still serve a resolution — absent and released HOT leaves cannot. */
  private static boolean servesResolution(final @Nullable Page page) {
    if (page == null) {
      return false;
    }
    return !(page instanceof HOTLeafPage leaf) || (!leaf.isClosed() && !leaf.isOrphaned());
  }

  /**
   * Whether {@code ref} names an entry that is still here but no longer resolvable, because every
   * page it holds is a HOT leaf {@link #releaseOrphanedHOTLeaves} already freed.
   *
   * <p>
   * {@link #get} answers {@code null} for two entirely different situations: this transaction never
   * logged the reference at all, and this transaction logged it and then released its leaf. Only the
   * first may fall back to durable storage. Falling back for the second resurrects the leaf's
   * <em>pre-merge</em> image — the entries it held before an incremental merge moved them into a
   * sibling — so the merged keys exist twice and the resulting descent contradicts the live routing.
   * A caller that can reach storage therefore has to ask which of the two it is looking at.
   * </p>
   *
   * <p>
   * Only the three direct layers are consulted, and no identity is rebound: a released entry is a
   * live array slot by construction, and callers ask this immediately after a {@link #get} that has
   * already followed and rebound any forwarding chain.
   * </p>
   *
   * @param ref the reference to classify
   * @return {@code true} iff the entry exists and every page it holds is a released HOT leaf
   */
  public boolean namesReleasedHOTLeafEntry(final PageReference ref) {
    return namesReleasedHOTLeafEntry(ref.getLogKey(), ref.getActiveTilGeneration());
  }

  /**
   * The same classification against an explicitly supplied identity.
   *
   * <p>
   * {@link #get} is not identity-preserving: its completed-offsets layer rewrites the reference into
   * a pure durable one, and {@code PageReference#refreshTransactionLogReference()} does the same as
   * soon as a shared handle publishes a durable offset. A caller that has to ask "was this entry
   * released?" after a {@code get} therefore has to ask about the identity the reference carried
   * BEFORE that call, not the one left behind by it.
   * </p>
   *
   * @param logKey the log key the reference named
   * @param generation the generation the reference named
   * @return {@code true} iff that entry exists and every page it holds is a released HOT leaf
   */
  public boolean namesReleasedHOTLeafEntry(final int logKey, final int generation) {
    return releasedEntry(logKey, generation) != null;
  }

  /**
   * The entry {@code (logKey, generation)} names, if it is still here but no longer resolvable.
   *
   * <p>
   * Shared by {@link #namesReleasedHOTLeafEntry} and {@link #releasedHOTLeafEntryPageKey} so the two
   * always agree on which region an identity resolves against.
   * </p>
   */
  private @Nullable PageContainer releasedEntry(final int logKey, final int generation) {
    if (logKey < 0) {
      return null;
    }
    final PageContainer entry;
    if (generation == PINNED_GENERATION) {
      entry = isLivePinnedSlot(logKey)
          ? pinnedEntries[logKey]
          : null;
    } else if (generation == currentGeneration && logKey < size) {
      entry = entries[logKey];
    } else if (snapshotEntries != null && generation == snapshotGeneration && logKey < snapshotSize) {
      entry = snapshotEntries[logKey];
    } else {
      entry = null;
    }
    return entry != null && resolvableContainer(entry) == null
        ? entry
        : null;
  }

  /**
   * The page key of the closed HOT leaf a released entry still holds.
   *
   * <p>
   * An unresolvable entry is NOT by itself proof that an incremental merge released its leaf. Two
   * ordinary events close a HOT leaf without merging anything away:
   * {@link #publishPinnedSpillCandidate} closes the exact container it has just written out to a
   * durable offset (the pinned-trie spill runs mid-transaction), and a superseded container the epoch
   * rotation left behind is closed together with its successor. In both cases the durable image
   * behind the reference IS the live image, and the write-path descent must be free to reload it —
   * whereas for a merged-away leaf that same image is the leaf's PRE-MERGE content and serving it
   * corrupts the trie.
   * </p>
   *
   * <p>
   * {@link #namesReleasedHOTLeafPage} is the authority that tells the two apart, and it needs a page
   * key. Reading it off the dead container is what lets a caller ask that question without first
   * reloading the very image it may have to refuse — and without seeding the buffer cache with it.
   * </p>
   *
   * @param logKey the log key the reference names
   * @param generation the generation the reference names
   * @return the released leaf's page key, or {@link Constants#NULL_ID_LONG} when that identity names
   *         no released entry or the entry holds no HOT leaf at all
   */
  public long releasedHOTLeafEntryPageKey(final int logKey, final int generation) {
    final PageContainer entry = releasedEntry(logKey, generation);
    if (entry == null) {
      return Constants.NULL_ID_LONG;
    }
    if (entry.getModified() instanceof HOTLeafPage modified) {
      return modified.getPageKey();
    }
    if (entry.getComplete() instanceof HOTLeafPage complete) {
      return complete.getPageKey();
    }
    return Constants.NULL_ID_LONG;
  }

  /**
   * Whether {@code pageKey} names a HOT leaf this transaction merged away and freed.
   *
   * <p>
   * The last resort of the write-path descent: once a reference has lost both its log identity and
   * its swizzle, its durable offset is the only thing left, and following it produces the leaf's
   * pre-merge image. That image is not garbage — it deserializes into a structurally plausible leaf —
   * so nothing downstream rejects it; it simply holds keys that now live in the merge target too, and
   * the descent it seeds contradicts the live routing. Checking the reloaded page's own key is what
   * survives every identity erasure.
   * </p>
   *
   * @param indexScope the page-key namespace to look in, from {@link #indexScope}
   * @param pageKey the page key of a page resolved from durable storage
   * @return {@code true} iff this transaction released the leaf with that page key
   */
  public boolean namesReleasedHOTLeafPage(final long indexScope, final long pageKey) {
    if (releasedHOTLeafReplacements.isEmpty()) {
      return false;
    }
    final Long2ObjectOpenHashMap<PageReference> released = releasedHOTLeafReplacements.get(indexScope);
    // containsKey, not a null test: a merge that named no replacement still released the page, and
    // serving its pre-merge image is exactly what this test exists to prevent.
    return released != null && released.containsKey(pageKey);
  }

  /**
   * The reference of the page that absorbed the entries of the released leaf with page key
   * {@code pageKey}.
   *
   * <p>
   * Refusing the pre-merge image answers "not this one" but leaves the descent with nowhere to go,
   * and the write path reads "nowhere" as "empty slot" — it fabricates a fresh empty leaf over the
   * reference, and from then on that slot routes its whole partial away from the leaf that actually
   * holds those keys. The reference returned here names where the entries went, so a stale reference
   * copy keeps routing correctly instead.
   * </p>
   *
   * @param indexScope the page-key namespace to look in, from {@link #indexScope}
   * @param pageKey the page key of a page resolved from durable storage
   * @return the replacement's reference, or {@code null} when the page was not released or the merge
   *         named no replacement
   */
  public @Nullable PageReference releasedHOTLeafReplacementReference(final long indexScope, final long pageKey) {
    if (releasedHOTLeafReplacements.isEmpty()) {
      return null;
    }
    final Long2ObjectOpenHashMap<PageReference> released = releasedHOTLeafReplacements.get(indexScope);
    return released == null
        ? null
        : released.get(pageKey);
  }

  /**
   * The same replacement, for a reference that still names the released entry.
   *
   * @param logKey the log key the reference names
   * @param generation the generation the reference names
   * @return the replacement's reference, or {@code null} when that identity names no released leaf or
   *         the merge named no replacement
   */
  public @Nullable PageReference releasedHOTLeafReplacementReference(final int logKey, final int generation) {
    if (logKey < 0 || releasedHOTLeafIdentityReplacements.isEmpty()) {
      return null;
    }
    return releasedHOTLeafIdentityReplacements.get(((long) generation << 32) | (logKey & 0xFFFFFFFFL));
  }

  /**
   * The reference the log itself keeps for a released entry — the one that owns it.
   *
   * <p>
   * A released entry that no merge produced has lost nothing: a spill closes the exact container it
   * has just written out, and a superseded container is closed together with its successor. In both
   * cases the page is still addressable, but only through the identity the ENTRY's own reference
   * received. A copy of that reference — every indirect-page copy-on-write deep-copies its child
   * references — does not: {@code refreshTransactionLogReference()} follows a shared handle, and a
   * copy taken before the publication (or one made without the handle) has none to follow, so it
   * carries a bare log key and no durable offset. Handing the copy's resolution over to the owning
   * reference is what lets it reach the page anyway.
   * </p>
   *
   * <p>
   * Scoped to released entries on purpose: for a resolvable entry the ordinary layers already answer,
   * and returning an owner there would let a caller bypass them. Nothing is rebound — the identity is
   * read exactly as given, through the same three regions {@link #releasedEntry} consults.
   * </p>
   *
   * @param logKey the log key the reference names
   * @param generation the generation the reference names
   * @return the owning reference, or {@code null} when that identity names no released entry
   */
  public @Nullable PageReference releasedEntryOwnerReference(final int logKey, final int generation) {
    if (releasedEntry(logKey, generation) == null) {
      return null;
    }
    if (generation == PINNED_GENERATION) {
      return pinnedRefs[logKey];
    }
    if (generation == currentGeneration) {
      return entryRefs[logKey];
    }
    return snapshotRefs == null
        ? null
        : snapshotRefs[logKey];
  }

  /** Resolve a packed {@code (generation, logKey)} through the same regions {@link #get} uses. */
  private @Nullable PageContainer resolvePackedIdentity(final long packed) {
    final int generation = (int) (packed >> 32);
    final int logKey = (int) packed;
    if (logKey < 0) {
      return null;
    }
    if (generation == PINNED_GENERATION) {
      return isLivePinnedSlot(logKey)
          ? resolvableContainer(pinnedEntries[logKey])
          : null;
    }
    if (generation == currentGeneration && logKey < size) {
      return resolvableContainer(entries[logKey]);
    }
    if (snapshotEntries != null && generation == snapshotGeneration && logKey < snapshotSize) {
      return resolvableContainer(snapshotEntries[logKey]);
    }
    return null;
  }

  /**
   * The page-key namespace a {@code (indexType, indexNumber)} pair owns.
   *
   * @param indexType the index type
   * @param indexNumber the index number within that type
   * @return a collision-free scope id for {@link #namesReleasedHOTLeafPage} and
   *         {@link #releaseOrphanedHOTLeaves}
   */
  public static long indexScope(final IndexType indexType, final int indexNumber) {
    return ((long) indexType.ordinal() << 32) | (indexNumber & 0xFFFF_FFFFL);
  }

  /**
   * Get the reference that owns the entry {@code ref} resolves to. Used to copy disk offsets when a
   * duplicate reference (from HOTIndirectPage COW) resolves to the same TIL entry.
   *
   * <p>
   * Takes the reference rather than a bare log key because a log key alone no longer names an entry:
   * pinned entries have their own index space, so the same integer addresses a different container in
   * each region and the generation is what tells them apart.
   * </p>
   *
   * @param ref the reference whose owning entry is wanted
   * @return the owning PageReference, or null if the reference names no entry
   */
  public @Nullable PageReference getOriginalRef(final PageReference ref) {
    ref.refreshTransactionLogReference();
    final int logKey = ref.getLogKey();
    if (logKey < 0) {
      return null;
    }
    if (ref.getActiveTilGeneration() == PINNED_GENERATION) {
      return isLivePinnedSlot(logKey)
          ? pinnedRefs[logKey]
          : null;
    }
    // Unchanged for the generation-scoped region: the log key indexes it directly, exactly as
    // before pinning existed.
    if (logKey < size) {
      return entryRefs[logKey];
    }
    return null;
  }

  // ==================== SNAPSHOT (O(1) array swap) ====================

  /**
   * Side-channel disk-offset sentinel: the background flush DECLINED to write this KVL entry (its
   * serialization left unresolved overflow references — the encoded bytes are only valid once the
   * recursive final commit writes the OverflowPages, #1076). {@link #cleanupSnapshot()} pins such an
   * authoritative entry outside later snapshot rotations instead of applying an offset and closing
   * it, so the final commit serializes it with real overflow keys. Distinct from the
   * {@code NULL_ID_LONG} init value, which still means "background write incomplete" and fails the
   * cleanup loudly.
   */
  public static final long SNAPSHOT_PROMOTE_TO_TIL = Long.MIN_VALUE;

  /**
   * Side-channel disk-offset sentinel for a KVL the background flush skipped WITHOUT serializing:
   * every one of its unresolved overflow carriers is an immutable side page staged in the writer's
   * append batch. Those carriers receive their durable keys from {@code publishCompletedWrites()},
   * which the writer runs immediately before {@link #cleanupSnapshot()}, so cleanup re-promotes the
   * page into the live log (never into the pinned region) and the next epoch serializes it with real
   * keys. One epoch of extra residency per such page, against a frame held until final commit.
   */
  public static final long SNAPSHOT_RETRY_NEXT_EPOCH = Long.MIN_VALUE + 1;

  /**
   * KVL pages that {@link #cleanupSnapshot()} pinned after a background-flush decline (diagnostics).
   */
  private static final LongAdder KVL_PAGES_PINNED_BY_PROMOTION = new LongAdder();

  /** KVL pages that {@link #cleanupSnapshot()} re-promoted for one more epoch (diagnostics). */
  private static final LongAdder KVL_PAGES_RETRIED_NEXT_EPOCH = new LongAdder();

  /** Total KVL pages pinned because a background flush could not write them, across all logs. */
  public static long kvlPagesPinnedByPromotion() {
    return KVL_PAGES_PINNED_BY_PROMOTION.sum();
  }

  /** Total KVL pages re-promoted for one more epoch while their staged carriers were published. */
  public static long kvlPagesRetriedNextEpoch() {
    return KVL_PAGES_RETRIED_NEXT_EPOCH.sum();
  }

  /** Reset the two diagnostic counters above; tests call this before a measured load. */
  public static void resetKvlPromotionDiagnostics() {
    KVL_PAGES_PINNED_BY_PROMOTION.reset();
    KVL_PAGES_RETRIED_NEXT_EPOCH.reset();
  }

  /**
   * Freeze current entries for background flush. O(1) — array reference swap + generation increment.
   * <p>
   * After this call, the insert thread continues with fresh empty arrays. The frozen arrays are
   * available to the background thread via {@link #getSnapshotEntry(int)} etc.
   *
   * @return snapshotSize (0 = nothing to flush)
   */
  public int snapshot() {
    // Move everything the background flush cannot write out of the epoch machinery FIRST, so the
    // frozen arrays hold record pages only and nothing has to be promoted back afterwards.
    if (PINNING_ENABLED) {
      pinUnflushableEntries();
    }

    // Capture current state
    snapshotEntries = entries;
    snapshotRefs = entryRefs;
    snapshotSize = size;
    snapshotGeneration = currentGeneration;
    snapshotCommitComplete = false;

    // Allocate side-channel arrays for background thread disk offsets.
    // Initialize offsets to NULL_ID_LONG sentinel — cleanupSnapshot() validates
    // each KVL entry got a valid offset before applying.
    snapshotDiskOffsets = new long[size];
    Arrays.fill(snapshotDiskOffsets, Constants.NULL_ID_LONG);
    snapshotHashes = new long[size];
    snapshotHashPresent = new boolean[size];
    snapshotLogReferences = new PageReference.TransactionLogReference[size];
    for (int i = 0; i < size; i++) {
      final PageReference ref = entryRefs[i];
      if (ref != null) {
        snapshotLogReferences[i] = ref.transactionLogReference();
      }
    }

    // Increment generation AFTER capturing snapshot generation
    currentGeneration++;

    // Allocate fresh arrays for continued insertion
    entries = new PageContainer[snapshotEntries.length];
    entryRefs = new PageReference[snapshotRefs.length];
    size = 0;

    return snapshotSize;
  }

  /**
   * Move every container the background flush cannot write into the pinned region, clearing its slot
   * so the snapshot about to be taken never sees it.
   *
   * <p>
   * "Cannot write" is exactly "is not a {@link KeyValueLeafPage}": that is the only page class
   * {@code serializeSnapshotWindowAsync} serializes, so before this existed every other page was
   * frozen for one epoch and then handed straight back by {@link #cleanupSnapshot()} under a new
   * identity — the quadratic bookkeeping described at {@link #PINNED_GENERATION}.
   * </p>
   *
   * <p>
   * A slot whose reference no longer identifies it is left alone. Such a container is an outdated
   * version whose successor (a CoW made during the epoch) already owns the page, and the snapshot
   * cleanup drops it — pinning it would resurrect the stale version, which is the failure mode the
   * {@code refStillIdentifiesSlot} guard was written for (#1077).
   * </p>
   */
  private void pinUnflushableEntries() {
    for (int i = 0; i < size; i++) {
      final PageContainer container = entries[i];
      if (container == null || container.getModified() instanceof KeyValueLeafPage
          || container.getComplete() instanceof KeyValueLeafPage) {
        continue;
      }
      final PageReference ref = entryRefs[i];
      if (ref == null || ref.getLogKey() != i || ref.getActiveTilGeneration() != currentGeneration) {
        continue;
      }
      pin(ref, container, i);
      entries[i] = null;
      entryRefs[i] = null;
    }
  }

  /**
   * Give {@code container} a pinned slot and rebind {@code ref} to it.
   *
   * @param ref the reference that identifies the container
   * @param container the container to pin
   * @param priorKey the log key the reference held in the current generation
   */
  private void pin(final PageReference ref, final PageContainer container, final int priorKey) {
    final int priorGeneration = ref.getActiveTilGeneration();
    final Page keyPage = pinKeyPage(container);
    final int existing = keyPage == null
        ? -1
        : pinnedSlotByPage.getInt(keyPage);

    final int slot;
    if (existing >= 0) {
      if (!isLivePinnedSlot(existing)) {
        throw new IllegalStateException("Pinned page identity index points at tombstoned slot " + existing);
      }
      // A second reference to a page that is already pinned — a CoW copy of the parent's child
      // reference. Both must resolve to one entry, which is the same contract the log has always
      // had for duplicate references sharing a log key. Routed through the replace path so the
      // identity index is rebuilt for the container that wins: a page left out of it would read as
      // unshared and could have its frame freed while still live.
      slot = existing;
      replacePinnedContainer(slot, ref, container);
    } else {
      ensurePinnedCapacity();
      // The container is still owned by its generation-scoped slot until pin() returns. Count the
      // pinned owner first; the transfer decrement below makes the completed move net-zero.
      noteHOTLeafOwners(container);
      slot = pinnedHighWater++;
      pinnedEntries[slot] = container;
      pinnedRefs[slot] = ref;
      livePinnedSlots[pinnedLiveCount] = slot;
      livePinnedPositionBySlot[slot] = pinnedLiveCount;
      pinnedLiveCount++;
      notePinnedPages(container, slot);
    }

    final boolean forwardedByReference = ref.bindToTransactionLog(slot, PINNED_GENERATION, true);
    container.setTransactionLogIdentity(PINNED_GENERATION, slot);

    // The single forwarding link this page will ever need: copies of the reference taken while it
    // lived in the generation-scoped arrays still carry the old identity, and the commit traversal
    // fails loudly rather than silently on an unresolvable one.
    if (!forwardedByReference && priorKey >= 0 && priorGeneration >= 0 && priorGeneration != PINNED_GENERATION) {
      forwardedEntries.put(((long) priorGeneration << 32) | (priorKey & 0xFFFFFFFFL),
          ((long) PINNED_GENERATION << 32) | (slot & 0xFFFFFFFFL));
    }
    // pinUnflushableEntries performs the corresponding non-throwing current-slot clear immediately
    // after this method returns. Until here both slots are real owners and must both be counted.
    forgetHOTLeafOwners(container);
  }

  /** Replace the container held by a pinned slot, retiring the pages the new one does not reuse. */
  private void replacePinnedContainer(final int slot, final PageReference ref, final PageContainer value) {
    if (!isLivePinnedSlot(slot)) {
      throw new IllegalStateException("Cannot replace tombstoned pinned slot " + slot);
    }
    final PageContainer oldContainer = pinnedEntries[slot];
    if (oldContainer != value) {
      // Make all allocation in the exact owner index pre-publication. It also records reuse by the
      // incoming container while the old one is inspected, so a shared frame cannot be retired.
      noteHOTLeafOwners(value);
      try {
        if (oldContainer != null) {
          // Drop the old pages from the pinned identity index FIRST, so its record-page sharing
          // check does not see this very slot and refuse to retire them. HOT sharing uses the exact
          // owner count, which deliberately still contains the old owner at this point.
          forgetPinnedPages(oldContainer, slot);
          closeOrphanedHOTLeafPages(oldContainer, value);
          closeOrphanedRecordPages(oldContainer, value, NO_ENTRY_INDEX);
          forgetHOTLeafOwners(oldContainer);
        }
      } catch (final RuntimeException | Error failure) {
        forgetHOTLeafOwners(value);
        throw failure;
      }
      pinnedEntries[slot] = value;
      notePinnedPages(value, slot);
    }
    pinnedRefs[slot] = ref;
    value.setTransactionLogIdentity(PINNED_GENERATION, slot);
  }

  /** Sentinel for "no generation-scoped entry is exempt from the sharing check". */
  private static final int NO_ENTRY_INDEX = -1;

  /** The page a pinned container is identified by — the modified one, or the complete one. */
  private static @Nullable Page pinKeyPage(final PageContainer container) {
    final Page modified = container.getModified();
    return modified != null
        ? modified
        : container.getComplete();
  }

  /** Record both of a container's pages as living at {@code slot}. */
  private void notePinnedPages(final PageContainer container, final int slot) {
    final Page complete = container.getComplete();
    final Page modified = container.getModified();
    if (complete != null) {
      pinnedSlotByPage.put(complete, slot);
    }
    if (modified != null && modified != complete) {
      pinnedSlotByPage.put(modified, slot);
    }
  }

  /** Drop a replaced container's pages from the identity index, but only where they still name it. */
  private void forgetPinnedPages(final PageContainer container, final int slot) {
    final Page complete = container.getComplete();
    final Page modified = container.getModified();
    if (complete != null && pinnedSlotByPage.getInt(complete) == slot) {
      pinnedSlotByPage.removeInt(complete);
    }
    if (modified != null && modified != complete && pinnedSlotByPage.getInt(modified) == slot) {
      pinnedSlotByPage.removeInt(modified);
    }
  }

  /** Ensure the pinned arrays have room for one more entry. Doubles capacity when full. */
  private void ensurePinnedCapacity() {
    if (pinnedHighWater == pinnedEntries.length) {
      final int oldCap = pinnedEntries.length;
      final int newCap = pinnedEntries.length << 1;
      if (newCap <= 0) {
        throw new IllegalStateException("Pinned transaction-log slot space exhausted");
      }
      pinnedEntries = Arrays.copyOf(pinnedEntries, newCap);
      pinnedRefs = Arrays.copyOf(pinnedRefs, newCap);
      livePinnedSlots = Arrays.copyOf(livePinnedSlots, newCap);
      Arrays.fill(livePinnedSlots, oldCap, newCap, -1);
      livePinnedPositionBySlot = Arrays.copyOf(livePinnedPositionBySlot, newCap);
      Arrays.fill(livePinnedPositionBySlot, oldCap, newCap, -1);
    }
  }

  /** Whether {@code slot} currently names a live pinned tuple rather than a tombstone. */
  private boolean isLivePinnedSlot(final int slot) {
    if (slot < 0 || slot >= pinnedHighWater) {
      return false;
    }
    final int position = livePinnedPositionBySlot[slot];
    return position >= 0 && position < pinnedLiveCount && livePinnedSlots[position] == slot
        && pinnedEntries[slot] != null && pinnedRefs[slot] != null;
  }

  /** Number of live pinned entries, for diagnostics and tests. */
  public int pinnedSize() {
    return pinnedLiveCount;
  }

  /** Next never-before-used slot in the current append-only pinned identity epoch. */
  public int pinnedHighWater() {
    return pinnedHighWater;
  }

  /**
   * Capture at most {@code batch.capacity()} exact trie pages while examining at most
   * {@code scanBudget} live pinned entries.
   *
   * <p>
   * The dense scan cursor persists across calls. Rejecting a page later (because one child is still
   * live, for example) therefore cannot turn each epoch into a fresh O(N) walk from slot zero, while
   * the fixed scan and candidate limits cap both CPU and retained references. Unsupported page types
   * are scanned past but never captured: top-level structural anchors are mutated directly and must
   * remain pinned until the ordinary final commit.
   * </p>
   *
   * @return the number of captured candidates
   */
  public int capturePinnedSpillCandidates(final int scanBudget, final PinnedSpillBatch batch) {
    if (scanBudget <= 0) {
      throw new IllegalArgumentException("Pinned spill scan budget must be > 0, got " + scanBudget);
    }
    if (batch == null) {
      throw new IllegalArgumentException("Pinned spill batch must not be null");
    }
    batch.clear();
    final int liveAtStart = pinnedLiveCount;
    if (liveAtStart == 0) {
      pinnedSpillScanCursor = 0;
      return 0;
    }

    int cursor = pinnedSpillScanCursor;
    if (cursor < 0 || cursor >= liveAtStart) {
      cursor = 0;
    }
    final int scanLimit = Math.min(scanBudget, liveAtStart);
    int scanned = 0;
    while (scanned < scanLimit && batch.size < batch.capacity()) {
      final int slot = livePinnedSlots[cursor];
      cursor++;
      if (cursor == liveAtStart) {
        cursor = 0;
      }
      scanned++;

      if (!isLivePinnedSlot(slot)) {
        throw new IllegalStateException("Dense pinned index contains tombstoned slot " + slot);
      }
      final PageReference reference = pinnedRefs[slot];
      final PageContainer container = pinnedEntries[slot];
      final Page page = container.getModified();
      if (page == null) {
        // A complete-only container is valid log state, but direct spill may serialize only the
        // exact modified page whose identity is captured and revalidated after the flush.
        continue;
      }
      final Class<?> pageClass = page.getClass();
      if ((pageClass != IndirectPage.class && pageClass != HOTIndirectPage.class && pageClass != HOTLeafPage.class)
          || page.isClosed()) {
        continue;
      }
      final PageReference.TransactionLogReference handle = reference.transactionLogReference();
      if (reference.getActiveTilGeneration() != PINNED_GENERATION || reference.getLogKey() != slot || handle == null) {
        continue;
      }
      batch.add(slot, reference, container, page, handle);
    }
    pinnedSpillScanCursor = cursor;
    return batch.size;
  }

  /**
   * Prove that a captured spill tuple still names the exact live pinned entry.
   *
   * <p>
   * Call this only after the candidate bytes have been flushed and before publishing any handle from
   * that batch. Validation is deliberately side-effect free, so a caller can validate the whole batch
   * before the first durable identity becomes visible.
   * </p>
   */
  public void validatePinnedSpillCandidate(final PinnedSpillBatch batch, final int index) {
    final int slot = batch.slotAt(index);
    final PageReference reference = batch.referenceAt(index);
    final PageContainer container = batch.containerAt(index);
    final Page page = batch.pageAt(index);
    final PageReference.TransactionLogReference handle = batch.handleAt(index);
    if (!isLivePinnedSlot(slot) || pinnedEntries[slot] != container || pinnedRefs[slot] != reference
        || container.getModified() != page || page.isClosed() || reference.getActiveTilGeneration() != PINNED_GENERATION
        || reference.getLogKey() != slot || reference.transactionLogReference() != handle
        || batch.diskOffsetAt(index) == Constants.NULL_ID_LONG) {
      throw new IllegalStateException("Pinned spill candidate changed before publication (slot=" + slot + ")");
    }
  }

  /**
   * Publish one already-flushed spill result, close its exact container and tombstone its pinned
   * slot.
   *
   * <p>
   * The caller must validate the entire batch first. This method validates again immediately before
   * mutation, then publishes through the reachability-scoped handle. A forwarded/superseded handle is
   * a hard identity fault, never permission to publish an outdated page.
   * </p>
   */
  public void publishPinnedSpillCandidate(final PinnedSpillBatch batch, final int index) {
    validatePinnedSpillCandidate(batch, index);
    final long diskOffset = batch.diskOffsetAt(index);
    if (diskOffset == Constants.NULL_ID_LONG) {
      throw new IllegalStateException("Pinned spill candidate has no flushed disk offset");
    }
    final PageReference reference = batch.referenceAt(index);
    final PageReference.TransactionLogReference handle = batch.handleAt(index);
    if (!PageReference.completeTransactionLogReference(handle, diskOffset, batch.hashAt(index),
        batch.hashPresentAt(index))) {
      throw new IllegalStateException(
          "Pinned spill handle was superseded before publication (slot=" + batch.slotAt(index) + ")");
    }
    if (!reference.refreshTransactionLogReference() || reference.getKey() != diskOffset
        || reference.getLogKey() != Constants.NULL_ID_INT) {
      throw new IllegalStateException(
          "Pinned spill handle did not publish its durable identity (slot=" + batch.slotAt(index) + ")");
    }

    final int slot = batch.slotAt(index);
    final PageContainer container = batch.containerAt(index);
    // Keep the slot live until close succeeds. If close itself faults, rollback can still find and
    // retry the container; the writer is poisoned because its canonical reference is already durable.
    closePageContainer(container);
    forgetHOTLeafOwners(container);
    forgetPinnedPages(container, slot);
    tombstonePinnedSlot(slot);
  }

  /** Swap-remove a closed pinned entry from the dense live view without reusing its numeric slot. */
  private void tombstonePinnedSlot(final int slot) {
    if (!isLivePinnedSlot(slot)) {
      throw new IllegalStateException("Cannot tombstone non-live pinned slot " + slot);
    }
    final int position = livePinnedPositionBySlot[slot];
    final int lastPosition = --pinnedLiveCount;
    final int movedSlot = livePinnedSlots[lastPosition];
    if (position != lastPosition) {
      livePinnedSlots[position] = movedSlot;
      livePinnedPositionBySlot[movedSlot] = position;
    }
    livePinnedSlots[lastPosition] = -1;
    livePinnedPositionBySlot[slot] = -1;
    pinnedEntries[slot] = null;
    pinnedRefs[slot] = null;
    if (pinnedLiveCount == 0) {
      pinnedSpillScanCursor = 0;
    } else if (pinnedSpillScanCursor >= pinnedLiveCount) {
      pinnedSpillScanCursor %= pinnedLiveCount;
    }
  }

  /**
   * Number of entries in the generation-scoped region alone — the ones a snapshot freezes.
   *
   * <p>
   * Distinct from {@link #size()}, which counts the whole log. This is what must stay bounded as a
   * bulk import runs: it holds one epoch's record pages, and nothing that survives across epochs.
   * </p>
   */
  public int liveEntryCount() {
    return size;
  }

  /** Number of forwarding links currently held, for diagnostics and tests. */
  public int forwardedEntryCount() {
    return forwardedEntries.size();
  }

  /**
   * How many containers {@link #cleanupSnapshot()} has promoted back into the generation-scoped
   * region over this transaction's life.
   *
   * <p>
   * This is the defect's own counter. Every promotion mints a fresh identity for a page that has not
   * changed and leaves a permanent forwarding link behind, and the whole resident structural set used
   * to be promoted on every single flush. With unflushable pages pinned it stays at zero.
   * </p>
   */
  public long structuralPromotionCount() {
    return promotedStructuralTotal;
  }

  /**
   * Check if a page reference is in the frozen snapshot zone. Used to trigger CoW when the insert
   * thread needs to modify a frozen page.
   *
   * @param ref the page reference to check
   * @return true if the reference is in the frozen snapshot
   */
  public boolean isFrozen(final PageReference ref) {
    return snapshotEntries != null && ref.getActiveTilGeneration() == snapshotGeneration && ref.getLogKey() >= 0
        && ref.getLogKey() < snapshotSize;
  }

  /**
   * Mark the snapshot flush as complete. Called by the background thread after all KVL pages have
   * been written to disk and their offsets stored in the side-channel arrays.
   */
  public void markSnapshotFlushComplete() {
    snapshotCommitComplete = true;
  }

  /**
   * Check if the snapshot commit has completed.
   *
   * @return true if the background thread has finished writing all snapshot pages
   */
  public boolean isSnapshotFlushComplete() {
    return snapshotCommitComplete;
  }

  // ==================== SNAPSHOT ACCESSORS (for background thread) ====================

  /**
   * Get snapshot entry at the given index. For background thread iteration.
   */
  public PageContainer getSnapshotEntry(final int index) {
    return snapshotEntries[index];
  }

  /**
   * Get snapshot reference at the given index. For background thread iteration.
   */
  public PageReference getSnapshotRef(final int index) {
    return snapshotRefs[index];
  }

  /**
   * Get the number of entries in the frozen snapshot.
   */
  public int getSnapshotSize() {
    return snapshotSize;
  }

  /**
   * Store a disk offset from the background thread into the side-channel. The background thread NEVER
   * writes to PageReference directly.
   */
  public void setSnapshotDiskOffset(final int index, final long offset) {
    snapshotDiskOffsets[index] = offset;
  }

  /**
   * Read one background-serialization outcome after its window-completion fence.
   *
   * <p>
   * The append coordinator uses this only after joining the window task. Each serializer writes a
   * distinct slot, and the join supplies the happens-before edge, so the plain array access needs no
   * per-page atomic or lock. {@link #SNAPSHOT_PROMOTE_TO_TIL} identifies a KVL page deliberately
   * declined by the disposable-frame path, {@link #SNAPSHOT_RETRY_NEXT_EPOCH} one skipped for a
   * single epoch while its staged carriers are published; {@link Constants#NULL_ID_LONG} still means
   * no outcome.
   */
  public long getSnapshotDiskOffset(final int index) {
    return snapshotDiskOffsets[index];
  }

  /**
   * Store a page hash from the background thread into the side-channel.
   */
  public void setSnapshotHash(final int index, final long hash, final boolean hashIsPresent) {
    snapshotHashes[index] = hash;
    snapshotHashPresent[index] = hashIsPresent;
  }

  // ==================== SNAPSHOT CLEANUP ====================

  /**
   * Clean up a completed snapshot: apply disk offsets to KVL page refs, close written KVL pages, and
   * retain pages that only recursive final commit can write.
   * <p>
   * MUST be called from the insert thread after the background thread has completed (after semaphore
   * acquire provides happens-before).
   *
   * @throws SirixIOException if any KVL entry is missing a disk offset (background write incomplete)
   */
  public void cleanupSnapshot() {
    if (snapshotEntries == null) {
      return;
    }

    int promotedStructural = 0;
    try {
      for (int i = 0; i < snapshotSize; i++) {
        final PageContainer container = snapshotEntries[i];
        if (container == null) {
          continue;
        }
        final Page modified = container.getModified();
        final PageReference ref = snapshotRefs[i];
        final PageReference.TransactionLogReference snapshotLogReference = snapshotLogReferences[i];
        final long packedSnapshotIdentity = ((long) snapshotGeneration << 32) | (i & 0xFFFFFFFFL);

        // A snapshot slot is authoritative only while BOTH its mutable reference fields and its
        // captured reachability handle still identify it. A CoW through this exact reference
        // changes the raw fields. A CoW through a copied PageReference changes only the copy's raw
        // fields but forwards the shared captured handle. Testing the raw fields alone therefore
        // let cleanup rebind the captured original to its stale container, append a forwarding
        // edge from the live copy back to that stale identity, and hijack every reachable copy.
        // The visible failure was an overlong page 0 losing its cross-page sibling pointer: JSON
        // rows 0..145 plus the outer array are exactly its 1,024 nodes, so output stopped at row 145.
        // A legacy/manual copy can omit the shared handle; put() records that supersession in the
        // primitive forwarding map instead, so consult it only when non-empty. The ordinary shared-
        // handle path remains one volatile read plus primitive field comparisons, with no map probe.
        final boolean refStillIdentifiesSlot = ref.getActiveTilGeneration() == snapshotGeneration
            && ref.getLogKey() == i && !PageReference.isSupersededTransactionLogReference(snapshotLogReference)
            && (forwardedEntries.isEmpty() || forwardedEntries.get(packedSnapshotIdentity) < 0);

        if (modified instanceof KeyValueLeafPage) {
          final long diskOffset = snapshotDiskOffsets[i];
          if (diskOffset == SNAPSHOT_PROMOTE_TO_TIL) {
            // The background flush declined this page (unresolved overflow references,
            // #1076): only the recursive final commit can produce its durable image,
            // because the OverflowPages must be written first. Keep the ORIGINAL
            // container alive. With pinning enabled, give the authoritative page a stable
            // unflushable identity so it never cycles through this decline/promotion path again.
            // A superseded slot (see refStillIdentifiesSlot above) is an outdated version whose
            // successor already owns the data — close it like the flushed path would.
            if (refStillIdentifiesSlot && ref.getActiveTilGeneration() != currentGeneration) {
              if (PINNING_ENABLED) {
                pin(ref, container, i);
                KVL_PAGES_PINNED_BY_PROMOTION.increment();
              } else {
                put(ref, container);
              }
            } else {
              closePageContainer(container);
            }
            snapshotEntries[i] = null;
            snapshotRefs[i] = null;
            continue;
          }
          if (diskOffset == SNAPSHOT_RETRY_NEXT_EPOCH) {
            // The page's carriers were staged side pages of this epoch; their keys were published
            // just before this cleanup. Back into the live log it goes, once, so the next epoch can
            // write it with durable references — the ordinary cross-generation put() records the
            // forwarding link exactly as a pre-pinning promotion did.
            if (refStillIdentifiesSlot && ref.getActiveTilGeneration() != currentGeneration) {
              put(ref, container);
              KVL_PAGES_RETRIED_NEXT_EPOCH.increment();
            } else {
              closePageContainer(container);
            }
            snapshotEntries[i] = null;
            snapshotRefs[i] = null;
            continue;
          }
          // KVL page: already written to disk by background thread.
          // Validate: side-channel offset must be valid (not sentinel).
          if (diskOffset == Constants.NULL_ID_LONG) {
            throw new SirixIOException(
                "Snapshot entry " + i + " has no disk offset — background write incomplete or failed");
          }
          final boolean completedByReference = PageReference.completeTransactionLogReference(snapshotLogReference,
              diskOffset, snapshotHashes[i], snapshotHashPresent[i]);

          // Apply the completed handle to the original reference, but ONLY if it still identifies
          // this slot. This copies the durable fields into the ordinary PageReference and drops the
          // handle, so pages without a genuinely reachable stale copy retain no side object.
          if (refStillIdentifiesSlot) {
            if (completedByReference) {
              ref.refreshTransactionLogReference();
            } else if (snapshotLogReference == null) {
              // Compatibility path for a reference that did not carry a shared handle.
              ref.setKey(diskOffset);
              if (snapshotHashPresent[i]) {
                ref.setHash(snapshotHashes[i]);
              } else {
                ref.clearHash();
              }
            }
          }

          // Legacy completed-map fallback. New references resolve through their shared handle;
          // only a manually copied reference that omitted the handle needs a historical map entry.
          //
          // Superseded entries excluded (#1077): if this frozen page was CoW'd into a newer
          // generation while the snapshot was active (forwarding entry present), the flush we
          // just completed is an OUTDATED version — storing its offset would let stale copies
          // resolve to it and silently drop every record added after the epoch boundary. The
          // forwarding chain in get() routes such copies to the newer entry instead.
          if (snapshotLogReference == null && forwardedEntries.get(packedSnapshotIdentity) < 0) {
            completedDiskOffsets.put(packedSnapshotIdentity, diskOffset);
            if (snapshotHashPresent[i]) {
              completedDiskHashes.put(packedSnapshotIdentity, snapshotHashes[i]);
            }
          }
          // Close both complete and modified pages (release MemorySegment)
          closePageContainer(container);
          snapshotEntries[i] = null;
          snapshotRefs[i] = null;
        } else {
          // IndirectPage / structural page: promote to current TIL
          // so final commit traversal can find them via Layer 1 lookup.
          //
          // GUARD: promote ONLY if the reference still identifies this slot. It does not when
          // (a) reAddStructuralPagesToTil() already re-added this page (same ref object,
          // generation already moved to currentGeneration) — re-putting would overwrite the
          // ref's logKey and orphan the earlier entry — or (b) the page was CoW'd during the
          // just-finished epoch (the ref was re-bound to the newer clone) — re-putting would
          // rebind the live trie to the STALE frozen page (#1077, see refStillIdentifiesSlot).
          if (refStillIdentifiesSlot && ref.getActiveTilGeneration() != currentGeneration) {
            put(ref, container);
            promotedStructural++;
          }
          snapshotEntries[i] = null;
          snapshotRefs[i] = null;
        }
      }

      // Legacy map entries cannot be age-pruned: an old manual copy may remain untouched until the
      // final traversal. Current copies resolve through their shared handle and add no map entry.
    } finally {
      // Release snapshot arrays for GC even if processing fails
      snapshotEntries = null;
      snapshotRefs = null;
      snapshotDiskOffsets = null;
      snapshotHashes = null;
      snapshotHashPresent = null;
      snapshotLogReferences = null;
      snapshotSize = 0;
      promotedStructuralTotal += promotedStructural;
      cleanupCount++;
      if (DIAG_EVERY > 0 && cleanupCount % DIAG_EVERY == 0) {
        System.out.printf(
            "[til] flush=%d gen=%d tilSize=%d pinned=%d promotedStructural=%d (total %d) "
                + "completedOffsets=%d completedHashes=%d forwarded=%d ~mapMiB=%d residents=%s%n",
            cleanupCount, currentGeneration, size, pinnedLiveCount, promotedStructural, promotedStructuralTotal,
            completedDiskOffsets.size(), completedDiskHashes.size(), forwardedEntries.size(),
            estimatedSideMapBytes() >> 20, residentCensus());
      }
    }
  }

  /**
   * Rough retained size of the three transaction-lifetime side maps, for the {@code -Dsirix.til.diag}
   * census. Open-addressed fastutil maps hold power-of-two arrays sized {@code size/0.75} rounded up,
   * so the estimate is per-entry rather than per-slot and reads low by up to 2×; it is meant to show
   * the growth CURVE, not to bill bytes exactly.
   */
  /**
   * Page-class histogram of the live TIL, for the {@code -Dsirix.til.diag} census: which pages are
   * the ones that survive every epoch. Walks {@code size} entries and allocates a map, so it runs
   * only on a census tick.
   */
  private String residentCensus() {
    final Map<String, Integer> byClass = new TreeMap<>();
    // Identity sets: an entry count above the distinct-page count means the log holds several
    // entries for the SAME page instance — a duplicate, not a bigger trie.
    final Set<Page> distinctPages = Collections.newSetFromMap(new IdentityHashMap<>());
    final Set<PageReference> distinctRefs = Collections.newSetFromMap(new IdentityHashMap<>());
    int indirect = 0;
    for (final PageContainer container : getList()) {
      if (container == null) {
        continue;
      }
      final Page modified = container.getModified();
      byClass.merge(modified == null
          ? "null"
          : modified.getClass().getSimpleName(), 1, Integer::sum);
      if (modified instanceof IndirectPage) {
        indirect++;
        distinctPages.add(modified);
      }
    }
    for (int i = 0; i < size; i++) {
      if (entryRefs[i] != null && entries[i] != null && entries[i].getModified() instanceof IndirectPage) {
        distinctRefs.add(entryRefs[i]);
      }
    }
    return byClass + " indirect=" + indirect + " distinctIndirectPages=" + distinctPages.size()
        + " distinctIndirectRefs=" + distinctRefs.size();
  }

  private long estimatedSideMapBytes() {
    final long offsets = (long) completedDiskOffsets.size() * (Long.BYTES + Long.BYTES);
    final long forwarded = (long) forwardedEntries.size() * (Long.BYTES + Long.BYTES);
    final long hashes = (long) completedDiskHashes.size() * (Long.BYTES + Long.BYTES);
    return offsets + forwarded + hashes;
  }

  /** Number of legacy completed-offset entries retained; expected to stay zero for current code. */
  public int completedDiskOffsetCount() {
    return completedDiskOffsets.size();
  }

  /** Number of legacy completed-hash entries retained; expected to stay zero for current code. */
  public int completedDiskHashCount() {
    return completedDiskHashes.size();
  }

  // ==================== CLEAR / CLOSE ====================

  /**
   * Clears the transaction intent log, closing all owned pages.
   * <p>
   * This is typically called on transaction rollback. All pages in the TIL are closed and their
   * memory is released. Also clears any active snapshot (best-effort, no offset validation).
   */
  public void clear() {
    Throwable closeFailure = null;
    try {
      bufferManager.getRecordPageCache().cleanUp();
    } catch (final RuntimeException | Error failure) {
      closeFailure = retainFailure(closeFailure, failure);
    }
    try {
      bufferManager.getRecordPageFragmentCache().cleanUp();
    } catch (final RuntimeException | Error failure) {
      closeFailure = retainFailure(closeFailure, failure);
    }
    try {
      bufferManager.getPageCache().cleanUp();
    } catch (final RuntimeException | Error failure) {
      closeFailure = retainFailure(closeFailure, failure);
    }

    for (int i = 0; i < size; i++) {
      final PageContainer container = entries[i];
      try {
        if (container != null) {
          closePageContainer(container);
        }
      } catch (final RuntimeException | Error failure) {
        closeFailure = retainFailure(closeFailure, failure);
      } finally {
        entries[i] = null;
        entryRefs[i] = null;
      }
    }
    size = 0;
    try {
      clearPinnedEntries();
    } catch (final RuntimeException | Error failure) {
      closeFailure = retainFailure(closeFailure, failure);
    }

    try {
      clearSnapshotPages();
    } catch (final RuntimeException | Error failure) {
      closeFailure = retainFailure(closeFailure, failure);
    }

    completedDiskOffsets.clear();
    forwardedEntries.clear();
    completedDiskHashes.clear();
    // Released page keys are scoped to the transaction that merged those leaves away; the next one
    // starts from the committed trie, where nothing is missing.
    releasedHOTLeafReplacements.clear();
    releasedHOTLeafIdentityReplacements.clear();
    hotLeafOwnerCounts.clear();
    hotLeafOwnerProbeCount = 0;
    rethrowFailure(closeFailure);
  }

  /**
   * Close and forget the pinned region. Pinned containers are owned by the log exactly like the
   * generation-scoped ones, so the transaction's end is the only thing that releases them.
   */
  private void clearPinnedEntries() {
    // A successful harden completed every shared handle before clearing the TIL. Refresh the
    // canonical copies to prove that fact before allowing the next identity epoch to reuse slot
    // zero. Rollback reaches this method with unresolved handles; in that case the high-water mark
    // stays append-only so an accidentally retained stale reference can never alias a later page.
    boolean allSurvivingHandlesDurable = true;
    Throwable closeFailure = null;
    for (int position = 0; position < pinnedLiveCount; position++) {
      final PageReference reference = pinnedRefs[livePinnedSlots[position]];
      try {
        if (reference == null || !reference.refreshesToUnclaimedDurableReference()) {
          allSurvivingHandlesDurable = false;
        }
      } catch (final RuntimeException | Error failure) {
        allSurvivingHandlesDurable = false;
        closeFailure = retainFailure(closeFailure, failure);
      }
    }
    if (!allSurvivingHandlesDurable) {
      // Latch before closing anything: closePageContainer may itself fail, and even that failure
      // must not let a retry forget that an unresolved identity escaped this epoch.
      pinnedSlotReusePermanentlyDisabled = true;
    }

    for (int position = 0; position < pinnedLiveCount; position++) {
      final int slot = livePinnedSlots[position];
      final PageContainer container = pinnedEntries[slot];
      try {
        if (container != null) {
          closePageContainer(container);
        }
      } catch (final RuntimeException | Error failure) {
        closeFailure = retainFailure(closeFailure, failure);
      } finally {
        pinnedEntries[slot] = null;
        pinnedRefs[slot] = null;
        livePinnedPositionBySlot[slot] = -1;
        livePinnedSlots[position] = -1;
      }
    }
    pinnedLiveCount = 0;
    pinnedSpillScanCursor = 0;
    pinnedSlotByPage.clear();
    if (allSurvivingHandlesDurable && !pinnedSlotReusePermanentlyDisabled) {
      // Every tombstoned slot was retired only by publishPinnedSpillCandidate, which first completed
      // its handle. Together with the live pre-pass above this covers the whole [0, highWater) epoch.
      pinnedHighWater = 0;
    }
    rethrowFailure(closeFailure);
  }

  /**
   * Closes the transaction intent log and releases all owned pages.
   */
  @Override
  public void close() {
    clear();
  }

  /**
   * Best-effort cleanup of snapshot pages. Does NOT validate disk offsets — used for rollback/error
   * paths where background writes may be incomplete.
   */
  private void clearSnapshotPages() {
    if (snapshotEntries != null) {
      Throwable closeFailure = null;
      for (int i = 0; i < snapshotSize; i++) {
        final PageContainer container = snapshotEntries[i];
        try {
          if (container != null) {
            closePageContainer(container);
          }
        } catch (final RuntimeException | Error failure) {
          closeFailure = retainFailure(closeFailure, failure);
        } finally {
          snapshotEntries[i] = null;
        }
      }
      snapshotEntries = null;
      snapshotRefs = null;
      snapshotDiskOffsets = null;
      snapshotHashes = null;
      snapshotHashPresent = null;
      snapshotLogReferences = null;
      snapshotSize = 0;
      rethrowFailure(closeFailure);
    }
  }

  private static Throwable retainFailure(final Throwable retained, final Throwable failure) {
    if (retained == null) {
      return failure;
    }
    if (retained != failure) {
      try {
        retained.addSuppressed(failure);
      } catch (final RuntimeException | Error ignored) {
        // Cleanup must retain the original failure and continue releasing later siblings even when
        // Throwable's suppressed-exception storage itself cannot grow (notably under OOME).
      }
    }
    return retained;
  }

  private static void rethrowFailure(final Throwable failure) {
    if (failure instanceof RuntimeException runtimeFailure) {
      throw runtimeFailure;
    }
    if (failure instanceof Error error) {
      throw error;
    }
  }

  /**
   * Close both pages in a container, handling identity (complete == modified).
   */
  private void closePageContainer(final PageContainer container) {
    final Page complete = container.getComplete();
    final Page modified = container.getModified();
    Throwable closeFailure = null;
    try {
      closePage(complete);
    } catch (final RuntimeException | Error failure) {
      closeFailure = failure;
    }
    if (modified != complete) {
      try {
        closePage(modified);
      } catch (final RuntimeException | Error failure) {
        closeFailure = retainFailure(closeFailure, failure);
      }
    }
    rethrowFailure(closeFailure);
  }

  /**
   * Close HOTLeafPages from an overwritten container that are not reused in the new container.
   * Prevents FrameSlot memory leaks when leaf splits overwrite TIL entries — the old complete page
   * (original from disk) is orphaned and its 65KB off-heap MemorySegment must be released.
   */
  private void closeOrphanedHOTLeafPages(final PageContainer oldContainer, final PageContainer newContainer) {
    final Page oldComplete = oldContainer.getComplete();
    final Page oldModified = oldContainer.getModified();
    final Page newComplete = newContainer.getComplete();
    final Page newModified = newContainer.getModified();

    if (oldComplete instanceof HOTLeafPage completeLeaf && completeLeaf != newComplete && completeLeaf != newModified
        && !isHOTLeafInOtherEntry(completeLeaf) && !bufferManager.getHOTLeafPageCache().containsPage(completeLeaf)) {
      completeLeaf.close();
    }
    if (oldModified != oldComplete && oldModified instanceof HOTLeafPage modifiedLeaf && modifiedLeaf != newComplete
        && modifiedLeaf != newModified && !isHOTLeafInOtherEntry(modifiedLeaf)
        && !bufferManager.getHOTLeafPageCache().containsPage(modifiedLeaf)) {
      modifiedLeaf.close();
    }
  }

  /**
   * Retire the {@link KeyValueLeafPage}s of an overwritten container that the new one does not reuse.
   *
   * <p>
   * The HOT counterpart above existed; this did not, so re-putting the same reference into its
   * existing log slot dropped the previous container's record pages with nothing left pointing at
   * them. {@link #put} has already taken them OUT of the record-page cache (that is what gives the
   * log exclusive ownership), so no eviction path can reclaim them either — every re-put stranded up
   * to two 64 KiB frames for the process's lifetime, and a write-heavy transaction re-puts the same
   * page on every modification. It showed up as the last population of survivors in the
   * {@code -Dsirix.debug.memory.leaks} census: cached at some point, never orphaned, never closed.
   * </p>
   *
   * <p>
   * Same three exemptions as the HOT path — reused by the new container, held by another log entry,
   * or still owned by the shared cache (which then owns the teardown).
   * </p>
   */
  private void closeOrphanedRecordPages(final PageContainer oldContainer, final PageContainer newContainer,
      final int excludeIndex) {
    final Page oldComplete = oldContainer.getComplete();
    final Page oldModified = oldContainer.getModified();
    final Page newComplete = newContainer.getComplete();
    final Page newModified = newContainer.getModified();

    if (oldComplete instanceof KeyValueLeafPage completePage && completePage != newComplete
        && completePage != newModified && !completePage.isClosed()
        && !isRecordPageInOtherEntry(completePage, excludeIndex)
        && !bufferManager.getRecordPageCache().containsPage(completePage)) {
      completePage.retire();
    }
    if (oldModified != oldComplete && oldModified instanceof KeyValueLeafPage modifiedPage
        && modifiedPage != newComplete && modifiedPage != newModified && !modifiedPage.isClosed()
        && !isRecordPageInOtherEntry(modifiedPage, excludeIndex)
        && !bufferManager.getRecordPageCache().containsPage(modifiedPage)) {
      modifiedPage.retire();
    }
  }

  private boolean isRecordPageInOtherEntry(final KeyValueLeafPage page, final int excludeIndex) {
    if (isPinnedElsewhere(page, NO_PINNED_SLOT)) {
      return true;
    }
    for (int i = 0; i < size; i++) {
      if (i == excludeIndex) {
        continue;
      }
      final PageContainer entry = entries[i];
      if (entry != null && (entry.getComplete() == page || entry.getModified() == page)) {
        return true;
      }
    }
    return false;
  }

  /** Sentinel for "no pinned slot is exempt from the sharing check". */
  private static final int NO_PINNED_SLOT = -1;

  /**
   * Whether {@code page} is held by a pinned entry other than {@code excludePinnedSlot}.
   *
   * <p>
   * The pinned region has to take part in every sharing check for the same reason the
   * generation-scoped one does: a page still held by another entry must never have its off-heap frame
   * freed. This answers in O(1) from the identity index rather than walking the region, which matters
   * because the region grows with the corpus and the check runs per replaced container.
   * </p>
   */
  private boolean isPinnedElsewhere(final Page page, final int excludePinnedSlot) {
    if (pinnedSlotByPage.isEmpty()) {
      return false;
    }
    final int slot = pinnedSlotByPage.getInt(page);
    return slot >= 0 && slot != excludePinnedSlot;
  }

  /** Whether {@code page} has an owner other than the container currently being retired. */
  private boolean isHOTLeafInOtherEntry(final HOTLeafPage page) {
    hotLeafOwnerProbeCount++;
    return hotLeafOwnerCounts.getInt(page) > 1;
  }

  /** Number of constant-time HOT-leaf owner probes, exposed only as HFT-bound test evidence. */
  public long hotLeafOwnerProbeCount() {
    return hotLeafOwnerProbeCount;
  }

  /**
   * Release the off-heap {@code MemorySegment}s of HOT leaf pages that incremental leaf consolidation
   * or a subtree rebuild merged away. Each page is no longer reachable from the trie, so the
   * tree-recursive commit never visits its entry — and a per-reference commit that did reach it would
   * skip it on the {@code isClosed()} guard. Closing here reclaims the 64KB slots instead of pinning
   * them until end-of-transaction {@link #clear()}.
   *
   * <p>
   * Sharing is decided from the identity owner count maintained by every current/pinned container
   * transition. Retirement is therefore {@code O(orphans)} with no transaction-log-sized scan. That
   * fixed bound matters on recurrent split and periodic-consolidation paths: even one full-log walk
   * per split would make a long ingestion transaction cumulatively quadratic.
   *
   * <p>
   * A leaf still shared by another TIL entry or held by the HOT-leaf buffer cache is never freed, so
   * no concurrent reader loses its segment.
   *
   * <p>
   * <b>Hazard, and the riskiest of the log-key conversions the pinned region forced.</b> This method
   * used to resolve each orphan with {@code entries[ref.getLogKey()]} behind a bare
   * {@code logKey < size} bound. A pinned reference's log key indexes the OTHER array, so that read
   * would have picked an unrelated container and closed a page that is still live — a use-after-free
   * on a 64 KB off-heap frame, silent until a reader touched it. HOT leaves really do get pinned (a
   * ClickBench load pins several hundred of them), so this is not hypothetical. Two things keep it
   * correct now: resolution branches on {@link #PINNED_GENERATION} before indexing, and the owner
   * count includes both regions and pinning transfers an owner without changing the total. Anything
   * not positively proven unique keeps its frame; the failure direction here is a leak, never a free.
   *
   * <p>
   * Coverage note: {@code TransactionIntentLogOrphanedHOTLeafTest} exercises unique, shared,
   * pinned/current and stale-generation identities directly, including a fixed owner-probe bound with
   * hundreds of unrelated HOT entries.
   *
   * <p>
   * <b>Every orphan forwards to {@code replacement}.</b> Freeing the frame is only half of a merge:
   * the entries went somewhere, and references naming the orphan survive it (every indirect-page
   * copy-on-write deep-copies its child references, and a copy that never took a log identity of its
   * own carries the original's). Leaving those copies unresolvable is not neutral — the HOT write
   * path's descent reads an unresolvable reference as an EMPTY SLOT and fabricates a fresh empty leaf
   * over it, so the slot stops routing to the leaf that absorbed the entries and its whole partial
   * silently disappears from the trie. Recording the forwarding link makes a stale copy resolve to
   * the replacement, which is the same mechanism {@code put} already uses for a superseded identity.
   * </p>
   *
   * @param indexScope the page-key namespace the released leaves belong to, from {@link #indexScope}
   * @param replacement the reference of the page that absorbed the orphans' entries, or {@code null}
   *        when the caller has none to name
   * @param orphanRefs references of the merged-away leaves — each carries its TIL log-key
   */
  public void releaseOrphanedHOTLeaves(final long indexScope, final @Nullable PageReference replacement,
      final List<PageReference> orphanRefs, final int siteTag) {
    if (orphanRefs == null || orphanRefs.isEmpty()) {
      return;
    }
    final long replacementIdentity = packedIdentityOf(replacement);
    // Collect each orphan leaf page once. The exact owner-count lookup below decides whether the
    // candidate is safe to close without walking either log region.
    final Reference2IntMap<HOTLeafPage> closeable = orphanCloseable;
    closeable.clear();
    for (int r = 0; r < orphanRefs.size(); r++) {
      final PageReference ref = orphanRefs.get(r);
      if (ref == null) {
        continue;
      }
      final int logKey = ref.getLogKey();
      if (logKey < 0) {
        continue;
      }
      // Resolve through the region AND the generation the reference names. A pinned log key indexes
      // a DIFFERENT array, so reading entries[logKey] for one would pick an unrelated container and
      // close a live page — the log key alone stopped naming an entry when the pinned region
      // appeared. A PRIOR generation's log key is the same hazard one epoch removed: snapshot()
      // installs fresh generation-scoped arrays and restarts log keys at zero, so index k of the
      // current generation holds an unrelated container, and reference COPIES (every indirect-page
      // CoW deep-copies its child references) keep the raw identity their original was rebound away
      // from. Resolving one of those against the live array picked a foreign container and freed the
      // 64 KB frame of a page the trie still pointed at; the next write to it then dead-ended in the
      // HOT writer's copy-on-write guard. Only the current generation's own log keys name an entry
      // here — anything else keeps its frame until commit, the leak-never-free direction this whole
      // method is built on.
      final PageContainer container;
      if (ref.getActiveTilGeneration() == PINNED_GENERATION) {
        container = isLivePinnedSlot(logKey)
            ? pinnedEntries[logKey]
            : null;
      } else {
        container = ref.getActiveTilGeneration() == currentGeneration && logKey < size
            ? entries[logKey]
            : null;
      }
      if (container == null) {
        continue;
      }
      final long orphanIdentity = ((long) ref.getActiveTilGeneration() << 32) | (logKey & 0xFFFFFFFFL);
      forwardReleasedIdentity(orphanIdentity, replacementIdentity);
      // The identity-keyed forwarding is what a reference that still names this entry follows, and
      // unlike the packed identity above it stays valid after the replacement has spilled to a
      // durable offset or its generation has been retired. Self-forwarding is refused for the same
      // reason as above: a merge re-using the orphan's own slot would install a cycle.
      if (replacement != null && replacement != ref) {
        releasedHOTLeafIdentityReplacements.put(orphanIdentity, replacement);
      }
      if (container.getModified() instanceof HOTLeafPage leaf) {
        closeable.putIfAbsent(leaf, 0);
      }
      if (container.getComplete() instanceof HOTLeafPage leaf) {
        closeable.putIfAbsent(leaf, 0);
      }
      // putIfAbsent keeps this an identity set when complete and modified name the same page.
    }
    if (closeable.isEmpty()) {
      return;
    }
    Long2ObjectOpenHashMap<PageReference> releasedForScope = null;
    for (final HOTLeafPage leaf : closeable.keySet()) {
      hotLeafOwnerProbeCount++;
      // The candidate's own container is one owner. Any higher count proves a live alias in either
      // the generation-scoped or pinned region and keeps the frame.
      if (!leaf.isClosed() && hotLeafOwnerCounts.getInt(leaf) == 1
          && !bufferManager.getHOTLeafPageCache().containsPage(leaf)) {
        // Record the logical page BEFORE closing it. Once closed, every reference naming it can lose
        // both its log identity and its swizzle, and only the page key still says this image is gone
        // (see releasedHOTLeafReplacements). Allocated on the first ACTUAL release, so the common
        // call that frees nothing costs no map entry.
        if (releasedForScope == null) {
          releasedForScope = releasedHOTLeafReplacements.get(indexScope);
          if (releasedForScope == null) {
            releasedForScope = new Long2ObjectOpenHashMap<>();
            releasedHOTLeafReplacements.put(indexScope, releasedForScope);
          }
        }
        releasedForScope.put(leaf.getPageKey(), replacement);
        // Same membership as the replacement map, by construction: recorded in the same branch.
        Long2IntOpenHashMap tagsForScope = releasedHOTLeafSiteTags.get(indexScope);
        if (tagsForScope == null) {
          tagsForScope = new Long2IntOpenHashMap();
          releasedHOTLeafSiteTags.put(indexScope, tagsForScope);
        }
        tagsForScope.put(leaf.getPageKey(), siteTag);
        leaf.close();
      }
    }
    // Do not let the scratch map pin the pages it just released until the next call.
    closeable.clear();
  }

  /**
   * The packed TIL identity a reference currently resolves to, or {@link #NO_REPLACEMENT}.
   *
   * <p>
   * Resolved through the same regions {@link #get} consults, and only accepted when the entry it
   * names is still resolvable: forwarding a stale copy to an identity that is itself gone would
   * merely move the dead end.
   * </p>
   */
  private long packedIdentityOf(final @Nullable PageReference ref) {
    if (ref == null) {
      return NO_REPLACEMENT;
    }
    ref.refreshTransactionLogReference();
    final int logKey = ref.getLogKey();
    if (logKey < 0) {
      return NO_REPLACEMENT;
    }
    final long packed = ((long) ref.getActiveTilGeneration() << 32) | (logKey & 0xFFFFFFFFL);
    return resolvePackedIdentity(packed) == null
        ? NO_REPLACEMENT
        : packed;
  }

  /**
   * Point every surviving copy of a released leaf's identity at the page that absorbed its entries.
   *
   * <p>
   * Self-forwarding is refused: a merge that re-uses the orphan's own entry would otherwise install a
   * one-element cycle, and {@link #get}'s chain walk follows links until one has no successor.
   * </p>
   */
  private void forwardReleasedIdentity(final long orphanIdentity, final long replacementIdentity) {
    if (replacementIdentity == NO_REPLACEMENT || replacementIdentity == orphanIdentity) {
      return;
    }
    forwardedEntries.put(orphanIdentity, replacementIdentity);
  }

  /**
   * Helper method to tear down a TIL-owned page without harming concurrent holders.
   */
  private void closePage(final Page page) {
    if (page instanceof KeyValueLeafPage kvPage) {
      if (!kvPage.isClosed()) {
        // NEVER force-release guards this transaction does not own: a concurrent reader may
        // have resolved this very instance through a shared reference and still hold its
        // guard mid-read — draining freed the frame under the reader (use-after-free / torn
        // reads), exactly like the HOTLeafPage case below. markOrphaned() + guard-aware
        // close(): immediate teardown when unguarded, deferred to the LAST releaseGuard()
        // otherwise.
        kvPage.markOrphaned();
        kvPage.close();
      }
    } else if (page instanceof HOTLeafPage hotLeaf && !hotLeaf.isClosed()) {
      // Do NOT free a HOT leaf still owned by the shared HOT-leaf buffer cache — the same
      // instance backs both a TIL PageContainer and the cache, so closing it here would
      // free the off-heap MemorySegment out from under concurrent readers.
      if (!bufferManager.getHOTLeafPageCache().containsPage(hotLeaf)) {
        // NEVER force-release guards this transaction does not own: a concurrent reader may
        // have resolved this very instance from the shared cache BEFORE the TIL's CoW removed
        // it and still holds its guard across the leaf visit — draining it freed the frame
        // under the reader (use-after-free / silently wrong index reads). HOTLeafPage.close()
        // is guard-aware: with live guards it only marks the page orphaned and the LAST
        // releaseGuard() performs the actual teardown.
        hotLeaf.close();
      }
    }
  }

  // ==================== API COMPATIBILITY ====================

  /**
   * Get a view of the current entries as a List. Used by the commit path
   * ({@code parallelSerializationOfKeyValuePages()}).
   *
   * @return an unmodifiable list view over current TIL entries
   */
  public List<PageContainer> getList() {
    if (pinnedLiveCount == 0) {
      return Collections.unmodifiableList(new ArraySliceList<>(entries, size));
    }
    // Pinned entries are part of the log — they are simply the part no background flush can
    // write — so every consumer that asks for "the containers in this log" must see them.
    return Collections.unmodifiableList(
        new IndexedConcatSliceList<>(pinnedEntries, livePinnedSlots, pinnedLiveCount, entries, size));
  }

  /**
   * Get the number of containers in the current TIL (not counting snapshot).
   */
  public int size() {
    return size + pinnedLiveCount;
  }

  /**
   * Get the current generation counter.
   *
   * @return the current generation value
   */
  public int getCurrentGeneration() {
    return currentGeneration;
  }

  /**
   * Get the snapshot generation counter (for diagnostics).
   */
  public int getSnapshotGeneration() {
    return snapshotGeneration;
  }

  /**
   * Lightweight fixed-size list view over two array prefixes in sequence, so {@link #getList()} can
   * present the pinned region and the generation-scoped region as one log without copying either.
   */
  private static final class IndexedConcatSliceList<T> extends AbstractList<T> {
    private final T[] first;
    private final int[] firstSlots;
    private final int firstLen;
    private final T[] second;
    private final int secondLen;

    IndexedConcatSliceList(final T[] first, final int[] firstSlots, final int firstLen, final T[] second,
        final int secondLen) {
      this.first = first;
      this.firstSlots = firstSlots;
      this.firstLen = firstLen;
      this.second = second;
      this.secondLen = secondLen;
    }

    @Override
    public T get(final int index) {
      if (index < 0 || index >= firstLen + secondLen) {
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + (firstLen + secondLen));
      }
      return index < firstLen
          ? first[firstSlots[index]]
          : second[index - firstLen];
    }

    @Override
    public int size() {
      return firstLen + secondLen;
    }
  }

  /**
   * Lightweight fixed-size list view over an array prefix. Avoids copying for getList().
   */
  private static final class ArraySliceList<T> extends AbstractList<T> {
    private final T[] array;
    private final int len;

    ArraySliceList(final T[] array, final int len) {
      this.array = array;
      this.len = len;
    }

    @Override
    public T get(final int index) {
      if (index < 0 || index >= len) {
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + len);
      }
      return array[index];
    }

    @Override
    public int size() {
      return len;
    }
  }
}
