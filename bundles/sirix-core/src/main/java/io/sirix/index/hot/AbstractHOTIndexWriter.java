/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.index.hot;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base class for HOT index writers.
 *
 * <p>
 * Provides common functionality for tree navigation, split handling, and transaction log
 * management. Subclasses implement key serialization.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Thread-local byte buffers for key/value serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Writer-local pre-allocated traversal arrays</li>
 * </ul>
 *
 * @param <K> the key type exposed by the writer
 * @author Johannes Lichtenberger
 */
public abstract class AbstractHOTIndexWriter<K> {

  /**
   * Thread-local buffer for value serialization (4KB default).
   */
  protected static final ThreadLocal<byte[]> VALUE_BUFFER = ThreadLocal.withInitial(() -> new byte[4096]);

  /** Maximum navigable tree depth — pre-allocates path arrays at this depth. */
  private static final int MAX_PATH_DEPTH = 64;

  /**
   * Tight-loop attempts before the guard retry starts yielding. Losing the guard once is the common
   * case — the loser only has to resolve the current instance again — so the first attempts stay
   * allocation- and syscall-free.
   */
  private static final int HOT_LEAF_GUARD_SPIN_ATTEMPTS = 256;

  /**
   * Attempts spent yielding before the guard retry starts parking.
   *
   * <p>
   * A yield is only a scheduling HINT, and on some platforms an ineffective one:
   * {@code SwitchToThread()} hands the core to a ready thread on the SAME processor and returns
   * immediately when there is none, so a retry loop that only ever yields keeps burning its own core
   * against the thread whose retirement it is waiting for. That is a livelock, not a slow wait, and
   * it is why the retry has to stop yielding after a bounded number of attempts.
   * </p>
   */
  private static final int HOT_LEAF_GUARD_YIELD_ATTEMPTS = HOT_LEAF_GUARD_SPIN_ATTEMPTS + 256;

  /** First parked back-off once yielding is exhausted. */
  private static final long HOT_LEAF_GUARD_PARK_MIN_NANOS = 1_000L;

  /**
   * Ceiling for the parked back-off. Also the worst-case latency this waiter adds on top of the
   * retirement it is waiting for, which is what keeps the wait bounded rather than merely finite.
   */
  private static final long HOT_LEAF_GUARD_PARK_MAX_NANOS = 100_000L;

  /**
   * Doubling ceiling for the parked back-off, applied to the shift COUNT so the shifted minimum can
   * never overflow into a negative park. Far above the handful of doublings that reach
   * {@link #HOT_LEAF_GUARD_PARK_MAX_NANOS}.
   */
  private static final long HOT_LEAF_GUARD_MAX_PARK_DOUBLINGS = 32L;

  /**
   * Wall-clock budget for re-resolving a HOT leaf that pressure eviction keeps retiring first.
   *
   * <p>
   * The budget has to be a DEADLINE rather than an attempt count. Under an allocator-pressure
   * eviction storm every freshly published instance can be retired again within microseconds of the
   * loader dropping its guard, so a fixed count of tight-loop attempts is spent in well under a
   * millisecond and aborts a transaction that only had to outlast the storm. A deadline keeps the
   * wait bounded — an unguardable leaf still fails rather than hanging — while making the retry
   * budget mean what its name says.
   * </p>
   *
   * <p>
   * It is a budget of WAITING, not of spinning: a leaf that can never become guardable again (a
   * closed one) is rejected structurally on reload, so nothing spends this budget except a retirement
   * that is genuinely still in flight.
   * </p>
   */
  private static final long HOT_LEAF_GUARD_RETRY_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(5L);

  /** Inserts between bounded, route-local leaf-consolidation attempts. */
  private static final int CONSOLIDATION_INTERVAL = 4096;

  /**
   * The largest union a consolidation merge produces — kept below page capacity so a merged leaf has
   * room before it re-splits. {@code MAX_ENTRIES * 3/4} packs leaves toward well-filled.
   */
  private static final int CONSOLIDATION_TARGET = (HOTLeafPage.MAX_ENTRIES * 3) / 4;

  /**
   * Hard physical bounds for exact structural guard traversals. These limits describe one ordinary
   * 32-leaf HOT frontier and are enforced by a read-only, early-stopping walk. A proper tree with 32
   * leaves can have up to 31 indirect pages (every indirect has at least two children), hence the
   * 63-page ceiling; the independent leaf/entry/reference/byte limits remain the payload-work bounds.
   */
  private static final int MAX_BOUNDED_REBUILD_LEAVES = HOTIndirectPage.MAX_NODE_ENTRIES;
  private static final int MAX_BOUNDED_REBUILD_PAGES = (MAX_BOUNDED_REBUILD_LEAVES << 1) - 1;
  private static final int MAX_BOUNDED_REBUILD_ENTRIES = HOTIndirectPage.MAX_NODE_ENTRIES * HOTLeafPage.MAX_ENTRIES;
  private static final int MAX_BOUNDED_REBUILD_SIDE_REFS = 8_192;
  private static final long MAX_BOUNDED_REBUILD_MATERIALIZED_BYTES = 8L << 20;

  private static final int EXTREME_RELATION_UNCERTAIN = 0;
  private static final int EXTREME_RELATION_OPPOSITE_AT_BETA = 1;
  private static final int EXTREME_RELATION_SAME_SIDE_AT_BETA = 2;
  private static final int WHOLE_NODE_PROOF_NOT_APPLICABLE = 0;
  private static final int WHOLE_NODE_PROOF_OPPOSITE = 1;
  private static final int WHOLE_NODE_PROOF_NOT_ONE_SIDED = 2;
  private static final int WHOLE_NODE_PROOF_UNCERTAIN = 3;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHOTIndexWriter.class);

  /** Package-private deterministic fault seam; non-null only inside the atomicity regression test. */
  private static volatile @Nullable Runnable structuralPublicationTestHook;

  /** Package-private deterministic fault seam for two-leaf migration publication atomicity tests. */
  private static volatile @Nullable Runnable twoLeafMigrationAfterPublicationTestHook;
  private static volatile @Nullable Consumer<HOTIndirectPage> twoLeafMigrationAfterReattachTestHook;

  protected final StorageEngineWriter storageEngineWriter;
  protected final IndexType indexType;
  protected final int indexNumber;

  /**
   * Persistent page-key allocator for this index, used to stamp the pages
   * {@link HOTIncrementalInsert} creates on the live insert path.
   */
  protected final LongSupplier pageKeyAllocator;

  /** Cached root page reference for the index. */
  protected PageReference rootReference;

  /** One writer-local scratch object: rebuild preflights do not allocate on recurring split paths. */
  private final RebuildFootprint rebuildFootprintScratch = new RebuildFootprint();

  /** Cached method reference shared by the preflight and full detector. */
  private final HOTMalformedSubtreeDetector.PageResolver traversalPageResolver;

  // ===== Pre-allocated path-tracking state — ZERO allocation per mutation on the hot path =====
  // The writer is transaction-confined. prepareLeafOfTree() overwrites these arrays and resets the
  // one navigation carrier below; structural recursive sub-inserts deliberately use independent
  // local routes so they cannot clobber the outer dispatch's spine.
  private final HOTIndirectPage[] _pathNodes = new HOTIndirectPage[MAX_PATH_DEPTH];
  private final PageReference[] _pathRefs = new PageReference[MAX_PATH_DEPTH];
  private final int[] _pathChildIndices = new int[MAX_PATH_DEPTH];
  private final LeafNavigationResult navigationScratch = new LeafNavigationResult();

  /** Writer-confined result carrier reused by the new-key descent analysis. */
  private final HOTIncrementalInsert.DescentScratch descentScratch = new HOTIncrementalInsert.DescentScratch();

  // ===== Last serialized value — replaces Object[] return from serializeValue =====
  /** The serialized value bytes from the most recent {@link #serializeValueInto} call. */
  protected byte[] lastSerializedValueBuf;
  /** The valid byte count in {@link #lastSerializedValueBuf}. */
  protected int lastSerializedValueLen;

  /** Inserts since the last route-local consolidation attempt. */
  private int insertsSinceConsolidation;

  /**
   * Root of the last published structural splice, captured at {@link #registerFreshSubtree} and used
   * only by fail-closed invariant validation. {@code null} on the non-structural merge fast path.
   */
  private PageReference structuralValidationScope;

  /**
   * Set by {@link #resolveHOTPageForTraversal} when it refused a MERGED-AWAY HOT leaf and had no
   * replacement to forward the descent to. Transaction-local like every other field here (the writer
   * is single-threaded) and cleared at the start of each {@link #prepareLeafOfTree}: the empty-slot
   * arm has to tell "this reference never had a page" from "this reference's page is gone", and the
   * reference itself can no longer say which (the identity erasures are what made the refusal
   * necessary in the first place).
   */
  private boolean releasedLeafRefused;

  /**
   * Why {@link #releasedLeafRefused} was set — cause, page key, and the release-site tag recorded by
   * {@link TransactionIntentLog#releaseOrphanedHOTLeaves}. Cold path only: a refusal ends the
   * transaction, so building the string costs nothing on any surviving path. Three causes share the
   * refusal, and nine fix rounds could not tell them apart from the outside; this names them.
   */
  private @Nullable String releasedLeafRefusalDetail;

  /**
   * Record a refusal over a KNOWN released page: names the cause, the page and the releasing site.
   */
  private void refuseReleasedLeaf(final TransactionIntentLog log, final String cause, final long pageKey) {
    releasedLeafRefused = true;
    final int siteTag = log.releasedHOTLeafSiteTag(indexScope(), pageKey);
    releasedLeafRefusalDetail =
        cause + " pageKey=" + pageKey + " releasedBy=" + TransactionIntentLog.releaseSiteName(siteTag);
  }

  /**
   * Scratch handing the replacement reference from {@link #resolveOneHopForTraversal} back to
   * {@link #resolveHOTPageForTraversal}'s forwarding loop. A field rather than a return value pair so
   * the hot descent path stays allocation-free; transaction-local like every other field here.
   */
  private @Nullable PageReference forwardedReplacementRef;

  /**
   * Cap on how many released-leaf forwardings one resolution may follow.
   *
   * <p>
   * A merge target can itself be merged away later in the same transaction, so the links form a
   * chain, and the log refuses only the one-element cycle (an orphan forwarded to its own slot). This
   * bound is what makes a longer cycle terminate; a legitimate chain is at most as long as the number
   * of merges that touched one slot, which is orders of magnitude below it.
   * </p>
   */
  private static final int MAX_RELEASED_LEAF_FORWARDS = 16;

  // ===== I8-onset localizer (opt-in, -Dhot.localize.i8=true). Pinpoints the per-insert dispatch
  // handler that first introduces an I8 (children-by-firstKey) violation under churn. Diagnostic
  // only; gated off in production. =====
  private static final boolean LOCALIZE_I8 = Boolean.getBoolean("hot.localize.i8");
  private static final int LOCALIZE_I8_FROM_REV = Integer.getInteger("hot.localize.fromRev", 0);
  private int i8ProbeReports;
  private boolean i8ProbeMerge;

  /**
   * Result of navigating to a leaf page, including the path from root. This is needed for proper
   * split handling.
   */
  protected static final class LeafNavigationResult {
    private HOTLeafPage leaf;
    private PageReference leafRef;
    private HOTIndirectPage[] pathNodes;
    private PageReference[] pathRefs;
    private int[] pathChildIndices;
    private int pathDepth;

    /** Private constructor for the writer-owned reusable carrier. */
    private LeafNavigationResult() {}

    /**
     * Stable route constructor retained for structural tests and recursive slow paths whose route must
     * outlive another descent.
     */
    protected LeafNavigationResult(final HOTLeafPage leaf, final PageReference leafRef,
        final HOTIndirectPage[] pathNodes, final PageReference[] pathRefs, final int[] pathChildIndices,
        final int pathDepth) {
      reset(leaf, leafRef, pathNodes, pathRefs, pathChildIndices, pathDepth);
    }

    private LeafNavigationResult reset(final HOTLeafPage leaf, final PageReference leafRef,
        final HOTIndirectPage[] pathNodes, final PageReference[] pathRefs, final int[] pathChildIndices,
        final int pathDepth) {
      this.leaf = requireNonNull(leaf, "leaf");
      this.leafRef = requireNonNull(leafRef, "leafRef");
      this.pathNodes = requireNonNull(pathNodes, "pathNodes");
      this.pathRefs = requireNonNull(pathRefs, "pathRefs");
      this.pathChildIndices = requireNonNull(pathChildIndices, "pathChildIndices");
      if (pathDepth < 0 || pathDepth > pathNodes.length || pathDepth > pathRefs.length
          || pathDepth > pathChildIndices.length) {
        throw new IllegalArgumentException("pathDepth " + pathDepth + " exceeds supplied path buffers");
      }
      this.pathDepth = pathDepth;
      return this;
    }

    public HOTLeafPage leaf() {
      return leaf;
    }

    public PageReference leafRef() {
      return leafRef;
    }

    public HOTIndirectPage[] pathNodes() {
      return pathNodes;
    }

    public PageReference[] pathRefs() {
      return pathRefs;
    }

    public int[] pathChildIndices() {
      return pathChildIndices;
    }

    public int pathDepth() {
      return pathDepth;
    }
  }

  /**
   * Operation code for the single foreground HOT mutation driver.
   *
   * <p>
   * The enum values are JVM singletons, so selecting an operation does not allocate. Keeping the
   * operation in the shared driver is also what prevents posting-list deletes from growing a second,
   * subtly different copy-on-write/navigation path beside inserts and replacements.
   * </p>
   */
  private enum MutationOperation {
    UPSERT, REMOVE_POSTING_BIT
  }

  /** Why the bounded source-footprint walk stopped. */
  enum RebuildFootprintStatus {
    WITHIN_BUDGET(false), PAGE_LIMIT(true), LEAF_LIMIT(true), ENTRY_LIMIT(true), SIDE_REFERENCE_LIMIT(
        true), MATERIALIZED_BYTE_LIMIT(true), DEPTH_LIMIT(
            false), UNRESOLVABLE_REFERENCE(false), REPEATED_PAGE(false), INVALID_PAGE(false), UNSUPPORTED_PAGE(false);

    private final boolean budgetLimit;

    RebuildFootprintStatus(final boolean budgetLimit) {
      this.budgetLimit = budgetLimit;
    }

    boolean isBudgetLimit() {
      return budgetLimit;
    }
  }

  /**
   * Mutable, writer-local result of the bounded source walk. The fixed page-identity array doubles as
   * a cycle/DAG guard without a per-preflight hash-table allocation.
   */
  static final class RebuildFootprint {
    private RebuildFootprintStatus status = RebuildFootprintStatus.WITHIN_BUDGET;
    private int pages;
    private int leaves;
    private int entries;
    private int sideReferences;
    private long materializedBytes;
    private final Page[] visitedPages = new Page[MAX_BOUNDED_REBUILD_PAGES];
    private final int[] visitedPageOwnerSlots = new int[MAX_BOUNDED_REBUILD_PAGES];

    private void reset() {
      Arrays.fill(visitedPages, 0, pages, null);
      Arrays.fill(visitedPageOwnerSlots, 0, pages, -1);
      status = RebuildFootprintStatus.WITHIN_BUDGET;
      pages = 0;
      leaves = 0;
      entries = 0;
      sideReferences = 0;
      materializedBytes = 0;
    }

    RebuildFootprintStatus status() {
      return status;
    }

    int pages() {
      return pages;
    }

    int leaves() {
      return leaves;
    }

    int entries() {
      return entries;
    }

    int sideReferences() {
      return sideReferences;
    }

    long materializedBytes() {
      return materializedBytes;
    }

    boolean withinBudget() {
      return status == RebuildFootprintStatus.WITHIN_BUDGET;
    }

    private void reject(final RebuildFootprintStatus rejection) {
      if (withinBudget()) {
        status = rejection;
      }
    }

    private String summary() {
      return "status=" + status + " pages=" + pages + "/" + MAX_BOUNDED_REBUILD_PAGES + " leaves=" + leaves + "/"
          + MAX_BOUNDED_REBUILD_LEAVES + " entries=" + entries + "/" + MAX_BOUNDED_REBUILD_ENTRIES + " sideRefs="
          + sideReferences + "/" + MAX_BOUNDED_REBUILD_SIDE_REFS + " materializedBytes=" + materializedBytes + "/"
          + MAX_BOUNDED_REBUILD_MATERIALIZED_BYTES;
    }
  }

  /** Internal fail-closed signal; optional structural fallbacks must never swallow it. */
  private static final class MutationTraversalRefusal extends IllegalStateException {
    private MutationTraversalRefusal(final String message) {
      super(message);
    }
  }

  /**
   * Protected constructor.
   *
   * @param storageEngineWriter the storage engine writer
   * @param indexType the HOT index type (PATH, CAS, NAME, PROJECTION, or VALIDTIME)
   * @param indexNumber the index number
   */
  protected AbstractHOTIndexWriter(StorageEngineWriter storageEngineWriter, IndexType indexType, int indexNumber) {
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
    this.pageKeyAllocator = createPageKeyAllocator(storageEngineWriter, indexType, indexNumber);
    this.traversalPageResolver = this::resolveHOTPageForTraversal;
  }

  /**
   * Measure a rebuild source without materializing a key, value, side-reference array, or child list.
   * The walk refuses to resolve page {@code MAX_BOUNDED_REBUILD_PAGES + 1}, so even its negative path
   * has a total-index-size-independent I/O/allocation ceiling.
   *
   * <p>
   * Package-private with an explicit scratch argument so the same production primitive is directly
   * regression-testable. Writers reuse one scratch object; tests may provide their own.
   * </p>
   */
  static RebuildFootprint measureBoundedRebuildFootprint(final Page root,
      final HOTMalformedSubtreeDetector.PageResolver resolver, final RebuildFootprint footprint) {
    requireNonNull(root, "root");
    requireNonNull(resolver, "resolver");
    requireNonNull(footprint, "footprint");
    footprint.reset();
    measureBoundedRebuildPage(root, resolver, footprint, 0);
    return footprint;
  }

  private RebuildFootprint measureBoundedRebuildFootprint(final Page root) {
    return measureBoundedRebuildFootprint(root, traversalPageResolver, rebuildFootprintScratch);
  }

  private static void measureBoundedRebuildPage(final Page page,
      final HOTMalformedSubtreeDetector.PageResolver resolver, final RebuildFootprint footprint, final int depth) {
    if (!footprint.withinBudget()) {
      return;
    }
    if (depth > MAX_PATH_DEPTH) {
      footprint.reject(RebuildFootprintStatus.DEPTH_LIMIT);
      return;
    }
    if (page.isClosed()) {
      footprint.reject(RebuildFootprintStatus.UNRESOLVABLE_REFERENCE);
      return;
    }
    for (int i = 0; i < footprint.pages; i++) {
      if (footprint.visitedPages[i] == page) {
        footprint.reject(RebuildFootprintStatus.REPEATED_PAGE);
        return;
      }
    }
    if (footprint.pages >= MAX_BOUNDED_REBUILD_PAGES) {
      footprint.reject(RebuildFootprintStatus.PAGE_LIMIT);
      return;
    }
    footprint.visitedPages[footprint.pages++] = page;

    if (page instanceof HOTLeafPage leaf) {
      if (footprint.leaves >= MAX_BOUNDED_REBUILD_LEAVES) {
        footprint.reject(RebuildFootprintStatus.LEAF_LIMIT);
        return;
      }
      footprint.leaves++;
      final int entryCount = leaf.getEntryCount();
      final int sideReferenceCount = leaf.segmentRefCount();
      final int usedSlotBytes = leaf.getUsedSlotsSize();
      final int commonPrefixLength = leaf.getCommonPrefixLen();
      if (entryCount < 0 || entryCount > HOTLeafPage.MAX_ENTRIES || sideReferenceCount < 0 || usedSlotBytes < 0
          || commonPrefixLength < 0) {
        footprint.reject(RebuildFootprintStatus.INVALID_PAGE);
        return;
      }
      if (entryCount > MAX_BOUNDED_REBUILD_ENTRIES - footprint.entries) {
        footprint.entries += entryCount;
        footprint.reject(RebuildFootprintStatus.ENTRY_LIMIT);
        return;
      }
      footprint.entries += entryCount;
      if ((long) sideReferenceCount > (long) MAX_BOUNDED_REBUILD_SIDE_REFS - footprint.sideReferences) {
        footprint.sideReferences = MAX_BOUNDED_REBUILD_SIDE_REFS + 1;
        footprint.reject(RebuildFootprintStatus.SIDE_REFERENCE_LIMIT);
        return;
      }
      footprint.sideReferences += sideReferenceCount;
      // getKey(i) reconstructs the leaf's common prefix for every materialized key; used slots
      // already cover suffixes and values. This is therefore the relevant transient byte footprint.
      final long leafMaterializedBytes = usedSlotBytes + (long) entryCount * commonPrefixLength;
      if (leafMaterializedBytes > MAX_BOUNDED_REBUILD_MATERIALIZED_BYTES - footprint.materializedBytes) {
        footprint.materializedBytes = MAX_BOUNDED_REBUILD_MATERIALIZED_BYTES + 1;
        footprint.reject(RebuildFootprintStatus.MATERIALIZED_BYTE_LIMIT);
        return;
      }
      footprint.materializedBytes += leafMaterializedBytes;
      return;
    }

    if (!(page instanceof HOTIndirectPage indirect)) {
      footprint.reject(RebuildFootprintStatus.UNSUPPORTED_PAGE);
      return;
    }
    final int childCount = indirect.getNumChildren();
    if (childCount < 1 || childCount > HOTIndirectPage.MAX_NODE_ENTRIES) {
      footprint.reject(RebuildFootprintStatus.INVALID_PAGE);
      return;
    }
    for (int i = 0; i < childCount; i++) {
      // Stop BEFORE resolving the first page beyond the cap. The source can therefore be arbitrarily
      // large without making the refusal path itself an unbounded read.
      if (footprint.pages >= MAX_BOUNDED_REBUILD_PAGES) {
        footprint.reject(RebuildFootprintStatus.PAGE_LIMIT);
        return;
      }
      final PageReference childReference = indirect.getChildReference(i);
      if (childReference == null) {
        footprint.reject(RebuildFootprintStatus.UNRESOLVABLE_REFERENCE);
        return;
      }
      final Page child = resolver.resolve(childReference);
      if (child == null) {
        footprint.reject(RebuildFootprintStatus.UNRESOLVABLE_REFERENCE);
        return;
      }
      measureBoundedRebuildPage(child, resolver, footprint, depth + 1);
      if (!footprint.withinBudget()) {
        return;
      }
    }
  }

  /** Preserve the primary structural/rebuild failure even if poisoning itself also fails. */
  private void markTransactionRollbackOnly(final Throwable failure) {
    try {
      storageEngineWriter.markTransactionRollbackOnly(failure);
    } catch (final RuntimeException | Error poisonFailure) {
      addSuppressedSafely(failure, poisonFailure);
    }
  }

  /** Release an unpublished off-heap leaf without replacing the failure that made it disposable. */
  private static void closeSpeculativeLeaf(final HOTLeafPage leaf, final Throwable failure) {
    try {
      leaf.close();
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(failure, cleanupFailure);
    }
  }

  /** Retry-safe variant for an outer ownership catch that may observe an inner cleanup. */
  private static void closeSpeculativeLeafIfOpen(final HOTLeafPage leaf, final Throwable failure) {
    try {
      if (!leaf.isClosed()) {
        leaf.close();
      }
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(failure, cleanupFailure);
    }
  }

  /**
   * Retire a fresh leaf only while ownership is still local to the structural mutation.
   *
   * <p>
   * Fresh-subtree registration is post-order: one or more leaves can already belong to the
   * transaction-intent log when registration of their containing indirect page fails. Closing such a
   * leaf in the caller's publication catch would leave the TIL holding a freed 64 KiB frame. The
   * leaf's own reference is the exact O(1) ownership witness; an exact container match transfers
   * cleanup to the TIL. If the ownership probe itself fails, retain the page and poison the already
   * failing transaction rather than risk freeing a log-owned page.
   * </p>
   */
  private void closeFreshLeafUnlessLogOwned(final @Nullable PageReference ref, final HOTLeafPage leaf,
      final Throwable failure) {
    if (leaf.isClosed()) {
      return;
    }
    if (ref != null && ref.getLogKey() >= 0) {
      final PageContainer owner;
      try {
        owner = requireNonNull(storageEngineWriter.getLog(), "transaction intent log").get(ref);
      } catch (final RuntimeException | Error ownershipFailure) {
        addSuppressedSafely(failure, ownershipFailure);
        return;
      }
      if (containerOwnsPage(owner, leaf)) {
        return;
      }
    }
    closeSpeculativeLeafIfOpen(leaf, failure);
  }

  /** Populate a fresh one-entry leaf or retire its off-heap frame before propagating failure. */
  private void putFreshSingleEntryOrThrow(final HOTLeafPage leaf, final byte[] key, final byte[] value) {
    try {
      if (!leaf.put(key, value)) {
        throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
      }
    } catch (final RuntimeException | Error failure) {
      closeSpeculativeLeaf(leaf, failure);
      throw failure;
    }
  }

  /**
   * Admit a mandatory mutation-time subtree scan only when its complete physical footprint is bounded
   * by one HOT frontier. The preflight stops before resolving page 64 and allocates nothing;
   * therefore both the accepted scan and its refusal have a total-index-size-independent ceiling.
   *
   * <p>
   * A stranding/routing guard is correctness-critical: treating an uninspected suffix as "no match"
   * can publish a cross-leaf duplicate. A refusal consequently poisons the transaction before it can
   * be caught and committed by a higher layer.
   * </p>
   */
  private void requireBoundedMutationTraversal(final Page root, final String operation) {
    final RebuildFootprint footprint = measureBoundedRebuildFootprint(root);
    if (!footprint.withinBudget()) {
      throw refuseMutationTraversal(operation, footprint.summary());
    }
  }

  /** Create and latch one fail-closed mutation-traversal refusal. */
  private MutationTraversalRefusal refuseMutationTraversal(final String operation, final String detail) {
    MUTATION_TRAVERSAL_REFUSED.incrementAndGet();
    final MutationTraversalRefusal failure = new MutationTraversalRefusal(indexType + " index " + indexNumber
        + " cannot complete mandatory bounded mutation traversal '" + operation + "' (" + detail + ")");
    markTransactionRollbackOnly(failure);
    return failure;
  }

  /**
   * Create a persistent page key allocator backed by the index page's maxHotPageKey counter.
   *
   * <p>
   * The returned {@link LongSupplier} allocates monotonically increasing page keys that are persisted
   * across transactions via the owning Path/CAS/Name/Projection/ValidTime container page. The
   * allocator therefore never restarts at a hard-coded transaction-local value.
   * </p>
   *
   * @param writer the storage engine writer
   * @param type the index type
   * @param indexNo the index number
   * @return a persistent page key allocator
   */
  private static LongSupplier createPageKeyAllocator(final StorageEngineWriter writer, final IndexType type,
      final int indexNo) {
    return switch (type) {
      case PATH ->
        () -> writer.<PathPage>prepareSecondaryIndexPage(IndexType.PATH).incrementAndGetMaxHotPageKey(indexNo);
      case CAS -> () -> writer.<CASPage>prepareSecondaryIndexPage(IndexType.CAS).incrementAndGetMaxHotPageKey(indexNo);
      case NAME ->
        () -> writer.<NamePage>prepareSecondaryIndexPage(IndexType.NAME).incrementAndGetMaxHotPageKey(indexNo);
      case PROJECTION -> () -> writer.<ProjectionIndexPage>prepareSecondaryIndexPage(IndexType.PROJECTION)
                                     .incrementAndGetMaxHotPageKey(indexNo);
      case VALIDTIME -> () -> writer.<ValidTimeIndexPage>prepareSecondaryIndexPage(IndexType.VALIDTIME)
                                    .incrementAndGetMaxHotPageKey(indexNo);
      default -> throw new IllegalArgumentException("Unsupported index type for HOT: " + type);
    };
  }

  /**
   * Get the storage engine writer.
   *
   * @return the storage engine writer
   */
  public StorageEngineWriter getStorageEngineReader() {
    return storageEngineWriter;
  }

  /**
   * Get the index type.
   *
   * @return the index type
   */
  public IndexType getIndexType() {
    return indexType;
  }

  /**
   * Get the index number.
   *
   * @return the index number
   */
  public int getIndexNumber() {
    return indexNumber;
  }

  // ===== Abstract methods for key serialization =====

  /**
   * Get the thread-local key buffer.
   *
   * @return the key buffer
   */
  protected abstract byte[] getKeyBuffer();

  /**
   * Set a new key buffer if the current one is too small.
   *
   * @param newBuffer the new buffer
   */
  protected abstract void setKeyBuffer(byte[] newBuffer);

  /**
   * Serialize a key to bytes.
   *
   * @param key the key to serialize
   * @param buffer the buffer to write to
   * @param offset the offset in the buffer
   * @return the number of bytes written
   */
  protected abstract int serializeKey(K key, byte[] buffer, int offset);

  // ===== Common methods =====

  /**
   * Get the root reference for the index from the index page. This ensures we always use the same
   * reference object as the storage engine.
   *
   * @return the root page reference
   */
  protected PageReference getRootReference() {
    // Prefer the cached field — initialise*Index() points it at the CoW'd index page's slot,
    // which is what same-trx writes/reads must traverse to see in-progress mutations. Falling
    // back to the disk-loaded page would yield the un-modified slot whose subtree never received
    // the writer's puts.
    if (rootReference != null) {
      return rootReference;
    }
    final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = storageEngineWriter.getPathPage(revisionRootPage);
        yield pathPage.getIndirectPageReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = storageEngineWriter.getCASPage(revisionRootPage);
        yield casPage.getIndirectPageReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = storageEngineWriter.getNamePage(revisionRootPage);
        yield namePage.getOrCreateReference(indexNumber);
      }
      case PROJECTION -> {
        final ProjectionIndexPage projPage = storageEngineWriter.getProjectionIndexPage(revisionRootPage);
        yield projPage.getIndirectPageReference(indexNumber);
      }
      case VALIDTIME -> {
        final ValidTimeIndexPage vtPage = storageEngineWriter.getValidTimeIndexPage(revisionRootPage);
        yield vtPage.getIndirectPageReference(indexNumber);
      }
      default -> throw new IllegalStateException("Unsupported index type for HOT: " + indexType);
    };
  }

  /**
   * Mark the index page as dirty so changes are persisted.
   */
  protected void prepareIndexPage() {
    switch (indexType) {
      case PATH, CAS, NAME, PROJECTION, VALIDTIME -> storageEngineWriter.prepareSecondaryIndexPage(indexType);
      default -> {
        /* ignore */ }
    }
  }

  /**
   * Navigate to the correct leaf page for a key, tracking the path from root.
   *
   * <p>
   * <b>Zero allocation design:</b> Path nodes/refs/indices are accumulated in pre-allocated instance
   * arrays ({@code _pathNodes}, {@code _pathRefs}, {@code _pathChildIndices}) and returned through
   * one writer-owned mutable carrier. The carrier remains valid until this writer starts its next
   * top-level descent. Recursive structural insertions use independent stable routes.
   * </p>
   *
   * <p>
   * <b>Thread safety:</b> {@code AbstractHOTIndexWriter} is per-transaction (single-threaded), so the
   * pre-allocated arrays are safe.
   * </p>
   *
   * @param rootRef the root reference (must be obtained ONCE and reused)
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return navigation result with leaf and path
   */
  protected LeafNavigationResult prepareLeafOfTree(PageReference rootRef, byte[] keyBuf, int keyLen) {
    storageEngineWriter.assertTransactionWritable();
    if (rootRef == null) {
      throw new IllegalStateException("HOT index not initialized");
    }
    requireNonNull(keyBuf, "keyBuf");
    if (keyLen < 0 || keyLen > keyBuf.length) {
      throw new IllegalArgumentException("keyLen " + keyLen + " outside keyBuf length " + keyBuf.length);
    }
    // Any write-path navigation invalidates the read-side leaf cache —
    // splits/merges change key ranges and leaf identities, so the cached
    // firstKey/lastKey may no longer match the resident page.
    invalidateLeafCache();
    releasedLeafRefused = false;
    releasedLeafRefusalDetail = null;

    // Top-down CoW (task #57): the caller hands us a cached root reference taken from the
    // *original* index page (NamePage / CASPage / PathPage / ProjectionIndexPage). That instance
    // is shared with historical revisions through the page's reference array. CoW the index
    // page first so subsequent mutations to the root reference (TIL.put resetting key/page,
    // chain-bump on pageFragments) target a private copy, then re-resolve the root reference
    // from the CoW'd index page so the rest of this method works against the fresh instance.
    prepareIndexPage();
    final PageReference cowedRootRef = prepareIndexPageRootReference(rootRef);

    // Reset path depth counter — no allocation
    int pathDepth = 0;
    PageReference currentRef = cowedRootRef;
    Page page = resolveHOTPageForTraversal(currentRef);

    // Top-down CoW (task #57): on every indirect along the path, deep-copy it on first
    // touch in this trx via the HOTIndirectPage copy ctor, which itself deep-copies every
    // child PageReference. This mirrors KeyedTrieWriter.prepareIndirectPage for the
    // document trie. With this, the leaf reference handed back at the bottom is a fresh
    // PageReference owned by the CoW'd indirect — mutations to its key/pageFragments
    // never bleed back into the historical revision's view through cache aliasing.
    while (page instanceof HOTIndirectPage indirectPage) {
      if (pathDepth >= MAX_PATH_DEPTH) {
        throw new IllegalStateException("HOT tree depth exceeds MAX_PATH_DEPTH=" + MAX_PATH_DEPTH);
      }
      final HOTIndirectPage cowedIndirect = prepareIndirectPage(currentRef, indirectPage);
      _pathNodes[pathDepth] = cowedIndirect;
      _pathRefs[pathDepth] = currentRef;

      int childIndex = cowedIndirect.findChildIndex(keyBuf, keyLen);
      if (childIndex < 0) {
        childIndex = 0; // Default to first child
      }
      _pathChildIndices[pathDepth] = childIndex;
      pathDepth++;

      final PageReference childRef = cowedIndirect.getChildReference(childIndex);
      if (childRef == null) {
        throw new IllegalStateException("Null child reference in HOTIndirectPage");
      }

      currentRef = childRef;
      page = resolveHOTPageForTraversal(currentRef);
    }

    if (page instanceof HOTLeafPage hotLeaf) {
      // If leaf is already in log, return the modified instance directly.
      final PageContainer existingLeafContainer = storageEngineWriter.getLog().get(currentRef);
      if (existingLeafContainer != null && existingLeafContainer.getModified() instanceof HOTLeafPage modifiedLeaf
          && servesTraversal(modifiedLeaf)) {
        return buildNavigationResult(modifiedLeaf, currentRef, pathDepth);
      }

      final HOTLeafPage cowedLeaf = cowHOTLeafForModification(currentRef, hotLeaf);
      return buildNavigationResult(cowedLeaf, currentRef, pathDepth);
    }

    // Empty tree path: create a new leaf at currentRef (root or missing child).
    // currentRef here is owned by the CoW'd parent's children array (top-down CoW above).
    //
    // Only a reference that names NOTHING may be answered this way. A reference whose HOT leaf an
    // incremental merge RELEASED is a different situation entirely, and fabricating an empty leaf
    // over it is the worst available answer: the slot keeps its routing partial but loses its
    // content, so every key of that partial reads as absent while the entries sit in the merge
    // target, and the first later insert whose mismatch bit reaches an ancestor's discriminative bit
    // fails as an unplaceable branch and ends the transaction. The release forwards the orphan's
    // identity to the page that absorbed it, so
    // this is reachable only when the merge named no replacement at all: fail loudly rather than
    // corrupt the trie.
    if (releasedLeafRefused) {
      throw new IllegalStateException("HOT " + indexType + " index " + indexNumber
          + " descent reached a released leaf with no replacement to forward to at depth " + pathDepth
          + "; refusing to replace it with an empty leaf [" + (releasedLeafRefusalDetail == null
              ? "cause=unrecorded"
              : releasedLeafRefusalDetail)
          + "]");
    }
    //
    // The page key comes from the allocator, like every other page this writer creates. It used to
    // be currentRef.getKey() — a durable BYTE OFFSET — which is not in the page-key space at all:
    // two different pages can carry the same number (an offset and an allocated key collide freely,
    // and the root case fell back to a hard-coded 0), and page keys are identity in this trie —
    // strandConfinedToLeaf compares them, and the released-leaf test below resolveHOTPageForTraversal
    // relies on one key naming one logical page.
    final HOTLeafPage newLeaf =
        new HOTLeafPage(pageKeyAllocator.getAsLong(), storageEngineWriter.getRevisionNumber(), indexType);
    PageContainer container = null;
    TransactionIntentLog log = null;
    boolean putStarted = false;
    try {
      container = PageContainer.getInstance(newLeaf, newLeaf);
      log = requireNonNull(storageEngineWriter.getLog(), "transaction intent log");
      putStarted = true;
      log.put(currentRef, container);
    } catch (final RuntimeException | Error failure) {
      recoverFromRegistrationFailure(failure, currentRef, newLeaf, container, log, putStarted);
      throw failure;
    }

    return buildNavigationResult(newLeaf, currentRef, pathDepth);
  }

  /**
   * Resolve the root reference of this HOT sub-tree from the CoW'd index page now in the transaction
   * log. Required because the cached {@link #rootReference} field points at the pre-CoW index page's
   * slot — that instance is shared with the historical revision's view. After
   * {@link #prepareIndexPage()} has put a deep-copied page in the log, the slot returned by typed
   * index-reference accessor on the CoW'd page is a fresh {@link PageReference} owned exclusively by
   * this writer's transaction.
   *
   * @param fallbackRef returned when no CoW'd page is in the log (e.g. unsupported index types)
   * @return the writer-private root reference
   */
  private PageReference prepareIndexPageRootReference(final PageReference fallbackRef) {
    final RevisionRootPage rrp = storageEngineWriter.getActualRevisionRootPage();
    final PageReference indexPageRef = switch (indexType) {
      case PATH -> rrp.getPathPageReference();
      case CAS -> rrp.getCASPageReference();
      case NAME -> rrp.getNamePageReference();
      case PROJECTION -> rrp.getProjectionIndexPageReference();
      case VALIDTIME -> rrp.getValidTimeIndexPageReference();
      default -> null;
    };
    if (indexPageRef == null)
      return fallbackRef;
    final PageContainer container = storageEngineWriter.getLog().get(indexPageRef);
    if (container == null)
      return fallbackRef;
    final Page modified = container.getModified();
    final PageReference cowed = switch (indexType) {
      case PATH -> ((PathPage) modified).getIndirectPageReference(indexNumber);
      case CAS -> ((CASPage) modified).getIndirectPageReference(indexNumber);
      case NAME -> ((NamePage) modified).getOrCreateReference(indexNumber);
      case PROJECTION -> ((ProjectionIndexPage) modified).getIndirectPageReference(indexNumber);
      case VALIDTIME -> ((ValidTimeIndexPage) modified).getIndirectPageReference(indexNumber);
      default -> fallbackRef;
    };
    return cowed != null
        ? cowed
        : fallbackRef;
  }

  /**
   * Top-down CoW for a HOT indirect page on the write path. Mirrors
   * {@link io.sirix.access.trx.page.KeyedTrieWriter#prepareIndirectPage} for the document trie: if
   * not already in the transaction log this trx, deep-copy the page via
   * {@link HOTIndirectPage#HOTIndirectPage(HOTIndirectPage)} — the copy ctor allocates a fresh
   * children array and a fresh {@link PageReference} per occupied slot, so subsequent mutations to a
   * child reference (its key, pageFragments, swizzled page) cannot bleed back to the historical
   * revision's view of the parent indirect through cache aliasing. Idempotent within a transaction:
   * subsequent calls return the same in-log copy.
   *
   * @param reference the reference whose page is to be CoW'd into the log
   * @param indirectPage the resolved indirect page (must not be {@code null})
   * @return the CoW'd indirect page (newly created or already in log)
   */
  private HOTIndirectPage prepareIndirectPage(final PageReference reference, final HOTIndirectPage indirectPage) {
    final PageContainer cont = storageEngineWriter.getLog().get(reference);
    if (cont != null && cont.getModified() instanceof HOTIndirectPage cowed) {
      return cowed;
    }
    final HOTIndirectPage cowed = new HOTIndirectPage(indirectPage);
    storageEngineWriter.getLog().put(reference, PageContainer.getInstance(cowed, cowed));
    return cowed;
  }

  /**
   * Navigate to a HOT leaf immediately before a caller attempts to acquire its lifetime guard.
   *
   * <p>
   * Uses the storage engine's versioning-aware page loading. Navigates through the tree structure
   * when splits have occurred.
   * </p>
   *
   * @param keyBuf the key buffer
   *        <p>
   *        The returned page is deliberately private to the guarded-read primitive below. A resolved
   *        committed leaf remains evictable until {@link HOTLeafPage#acquireGuard()} succeeds;
   *        exposing the bare page let callers mistake a frame reclaimed in that window for an absent
   *        key.
   *        </p>
   *
   * @return the HOT leaf page, or {@code null} if the tree cannot resolve one
   */
  private @Nullable HOTLeafPage navigateToLeafBeforeReadGuard(final byte[] keyBuf, final int keyLen) {
    // NOTE: min/max-range leaf caching is UNSAFE for HOT. Leaves partition
    // by PEXT of disc bits, not by total key order — two distinct leaves
    // can have overlapping [firstKey, lastKey]. A key K matching cached
    // leaf's range may actually belong to a different leaf. The HOT tree
    // is log_K-shallow so re-navigation is cheap; no cache needed.

    PageReference currentRef = getRootReference();
    if (currentRef == null)
      return null;

    Page page = resolveHOTPageForTraversal(currentRef);
    while (page instanceof HOTIndirectPage indirectPage) {
      int childIndex = indirectPage.findChildIndex(keyBuf, keyLen);
      if (childIndex < 0)
        childIndex = 0;
      final PageReference childRef = indirectPage.getChildReference(childIndex);
      if (childRef == null) {
        LOG.warn("HOT navigation: null child ref at index {} in indirect page {}", childIndex,
            indirectPage.getPageKey());
        return null;
      }
      currentRef = childRef;
      page = resolveHOTPageForTraversal(currentRef);
      if (page == null) {
        LOG.warn("HOT navigation: unresolvable page for ref key={}", currentRef.getKey());
        return null;
      }
    }
    return page instanceof HOTLeafPage hotLeaf
        ? hotLeaf
        : null;
  }

  /**
   * Resolve and guard a HOT leaf for writer-side read-before-write work.
   *
   * <p>
   * A failed guard acquisition means eviction won the resolve-to-guard race, never that the key is
   * absent. Re-resolve with the same bounded back-off used by copy-on-write and return only a page
   * whose off-heap frame cannot be reclaimed until the caller invokes
   * {@link #releaseLeafReadGuard(HOTLeafPage, Throwable)}. This keeps no-op probes page-write-free
   * without reading an unpinned frame.
   * </p>
   *
   * @param keyBuf exact serialized key used for HOT navigation
   * @return a guard-held leaf, or {@code null} only when the tree itself resolves no leaf
   */
  protected final @Nullable HOTLeafPage acquireLeafForRead(final byte[] keyBuf) {
    return acquireLeafForRead(keyBuf, keyBuf.length);
  }

  /**
   * {@link #acquireLeafForRead(byte[])} over {@code keyBuf[0..keyLen)}. The returned leaf is guarded
   * identically; only routing ignores spare bytes in a reusable serializer buffer.
   */
  protected final @Nullable HOTLeafPage acquireLeafForRead(final byte[] keyBuf, final int keyLen) {
    requireNonNull(keyBuf, "keyBuf");
    if (keyLen < 0 || keyLen > keyBuf.length) {
      throw new IllegalArgumentException("keyLen " + keyLen + " outside keyBuf length " + keyBuf.length);
    }
    long retryDeadlineNanos = 0L;
    for (long attempt = 0L;; attempt++) {
      final HOTLeafPage leaf = navigateToLeafBeforeReadGuard(keyBuf, keyLen);
      if (leaf == null) {
        return null;
      }
      if (leaf.acquireGuard()) {
        return leaf;
      }
      retryDeadlineNanos = backOffBeforeHOTLeafGuardRetry(attempt, retryDeadlineNanos);
    }
  }

  /**
   * Release a writer-side read guard without replacing a failure raised while the guard was held.
   *
   * @param leaf the guard-held leaf
   * @param guardedFailure primary failure from guarded work, or {@code null} on its success path
   */
  protected static final void releaseLeafReadGuard(final HOTLeafPage leaf, final @Nullable Throwable guardedFailure) {
    try {
      leaf.releaseGuard();
    } catch (final RuntimeException | Error releaseFailure) {
      if (guardedFailure == null) {
        throw releaseFailure;
      }
      addSuppressedSafely(guardedFailure, releaseFailure);
    }
  }


  /** No-op: leaf cache was removed (unsafe for HOT's PEXT-based partitioning). */
  protected final void invalidateLeafCache() {
    // kept as a public hook in case callers rely on it; nothing to invalidate.
  }

  /**
   * Resolve a HOT page from TIL/swizzled/storage for traversal.
   *
   * <p>
   * Prefers the modified TIL page so in-transaction reads see latest writes.
   * </p>
   *
   * <p>
   * A reference whose HOT leaf an incremental merge released resolves to the page that ABSORBED its
   * entries instead — that page is where the keys the reference still routes for actually live, and
   * answering nothing lets the caller's empty-slot arm fabricate an empty leaf over the slot. The
   * forwarding is followed here rather than inside a single lookup because the replacement may have
   * been merged away in turn, so the links form a chain; {@link #MAX_RELEASED_LEAF_FORWARDS} bounds
   * it. Only a MERGE chain that ends nowhere sets {@link #releasedLeafRefused} — an entry no merge
   * released lost its page, not its keys to another page, so it is answered with nothing instead.
   * </p>
   */
  private @Nullable Page resolveHOTPageForTraversal(final PageReference ref) {
    PageReference current = ref;
    for (int hop = 0; hop < MAX_RELEASED_LEAF_FORWARDS; hop++) {
      forwardedReplacementRef = null;
      final Page page = resolveOneHopForTraversal(current);
      if (page != null) {
        return page;
      }
      final PageReference next = forwardedReplacementRef;
      forwardedReplacementRef = null;
      if (next == null || next == current) {
        return null;
      }
      current = next;
    }
    // A cycle longer than one link: refuse rather than spin, and let the caller's empty-slot arm
    // fail loudly instead of fabricating a leaf over a slot whose content is somewhere in the cycle.
    releasedLeafRefused = true;
    releasedLeafRefusalDetail = "cause=forward-cycle hops=" + MAX_RELEASED_LEAF_FORWARDS + " start(key=" + ref.getKey()
        + ",logKey=" + ref.getLogKey() + ",gen=" + ref.getActiveTilGeneration() + ") last(key=" + current.getKey()
        + ",logKey=" + current.getLogKey() + ",gen=" + current.getActiveTilGeneration() + ")";
    return null;
  }

  /**
   * One step of {@link #resolveHOTPageForTraversal}: the page for {@code ref}, or {@code null} with
   * either {@link #forwardedReplacementRef} set to the reference the descent must continue at or
   * {@link #releasedLeafRefused} set because there is none.
   */
  private @Nullable Page resolveOneHopForTraversal(final PageReference ref) {
    final TransactionIntentLog log = storageEngineWriter.getLog();
    // Capture the identity BEFORE resolving: get() rewrites a reference whose shared handle has
    // published a durable offset into a pure disk reference, so asking it afterwards which entry it
    // named answers "none" for an entry that very much exists and has been released.
    final int logKeyBeforeResolution = ref.getLogKey();
    final int generationBeforeResolution = ref.getActiveTilGeneration();
    final Page resident = servingPageOf(log.get(ref));
    if (resident != null) {
      return resident;
    }

    // The log has the entry but every page it holds is a closed HOT leaf. Two very different things
    // produce that state, and they want opposite answers — see disposeOfMergedAwayEntry.
    boolean releasedWithoutMerge = false;
    if (namesReleasedEntry(log, ref, logKeyBeforeResolution, generationBeforeResolution)) {
      if (disposeOfMergedAwayEntry(log, ref, logKeyBeforeResolution, generationBeforeResolution)) {
        return null;
      }
      // Unresolvable, but no merge released it: fall through to the durable image, which IS the live
      // one for this leaf. See mergeReleasedEntry.
      releasedWithoutMerge = true;
    }

    final Page swizzled = ref.getPage();
    if (servesTraversal(swizzled)) {
      return swizzled;
    }

    if (releasedWithoutMerge && ref.getKey() < 0) {
      continueAtReleasedEntryOwner(log, ref, logKeyBeforeResolution, generationBeforeResolution);
      return null;
    }

    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null;
    }

    return reloadUnlessAlreadyMergedAway(log, ref);
  }

  /** The page a resolved container can hand a write-path descent, or {@code null} if neither can. */
  private static @Nullable Page servingPageOf(final @Nullable PageContainer container) {
    if (container == null) {
      return null;
    }
    final Page modified = container.getModified();
    if (servesTraversal(modified)) {
      return modified;
    }
    final Page complete = container.getComplete();
    return servesTraversal(complete)
        ? complete
        : null;
  }

  /**
   * Dispose of the descent for an unresolvable entry <em>if an incremental merge is what released
   * it</em>.
   *
   * <p>
   * For a merged-away leaf neither remaining source describes this slot any more: the instance still
   * swizzled on the reference is the freed page, and the durable image behind it is the leaf's
   * PRE-MERGE bytes, whose keys now also live in the merge target. Handing either one to the descent
   * seeds it with a leaf whose key set contradicts the live routing, and the insert dispatch reads
   * that contradiction as a branch escape (mismatch bit at or above an ancestor's discriminative bit)
   * it cannot place and ends the transaction. The merge forwarded the orphan to the page that
   * absorbed its entries, and that page is the one the descent must continue at.
   * </p>
   *
   * <p>
   * It is NOT the only way an entry loses every page: a spill closes the very container it has just
   * written out, and a superseded container is closed with its successor. Those merged nothing away
   * and their durable image is the live one, so they must be left to the caller's fall-through.
   * </p>
   *
   * @return {@code true} iff a merge released this entry, in which case the descent has already been
   *         given its forwarding target or refused
   */
  private boolean disposeOfMergedAwayEntry(final TransactionIntentLog log, final PageReference ref,
      final int logKeyBeforeResolution, final int generationBeforeResolution) {
    // get() has already tried the packed-identity forwarding; that link only resolves while the
    // replacement is still an addressable log entry, so ask for the replacement's REFERENCE, which
    // outlives both a mid-transaction spill to a durable offset and a retired generation.
    final PageReference replacement =
        releasedEntryReplacement(log, ref, logKeyBeforeResolution, generationBeforeResolution);
    if (replacement != null) {
      forwardedReplacementRef = replacement;
      return true;
    }
    return mergeReleasedEntry(log, ref, logKeyBeforeResolution, generationBeforeResolution);
  }

  /**
   * Send the descent to the reference the log owns for a released entry no merge produced.
   *
   * <p>
   * The fall-through has nowhere to land ON THIS REFERENCE: the entry's pages are closed, the swizzle
   * cannot serve, and this reference carries no durable offset. The ENTRY's own reference usually
   * does — the publication that closed the container (a pinned-trie spill, a supersession) applies
   * the durable identity to the reference the log holds for it, and a copy taken before that only
   * sees it through a shared handle it may not have.
   * </p>
   *
   * <p>
   * When there is no owner to continue at either, the descent is answered with nothing rather than
   * refused. {@link #releasedLeafRefused} exists to stop the empty-slot arm fabricating a leaf over a
   * slot whose keys MOVED, and only {@code releaseOrphanedHOTLeaves} moves them — it is the sole
   * producer of a merge, and it always records both the page-key blacklist entry
   * {@link #mergeReleasedEntry} just failed to find and a forwarding link. An entry unresolvable
   * without one lost its page, not its keys to another page, so there is nothing left for a
   * fabricated leaf to shadow. Refusing here instead ended the {@code windows-latest / query} lane's
   * transaction on a slot no merge had ever touched — the dead end that outlived three successive
   * fixes to the forwarding, because none of them was on the path it takes.
   * </p>
   */
  private void continueAtReleasedEntryOwner(final TransactionIntentLog log, final PageReference ref,
      final int logKeyBeforeResolution, final int generationBeforeResolution) {
    final PageReference owner = releasedEntryOwner(log, ref, logKeyBeforeResolution, generationBeforeResolution);
    if (owner != null && owner != ref && (owner.getKey() >= 0 || owner.getLogKey() >= 0)) {
      forwardedReplacementRef = owner;
    }
  }

  /**
   * The durable fallback, minus the images of leaves this transaction already merged away.
   *
   * <p>
   * The last door, and the only one still open once a reference has lost BOTH its log identity (a
   * shared handle published a durable offset — the pinned trie spill does this mid-transaction) and
   * its swizzle ({@code PageReference#getPage()} clears a closed HOT leaf's swizzle on first read).
   * Such a reference is indistinguishable from a merely non-resident one, so no test on the reference
   * can see that its leaf is gone. The page that comes back says so itself: it carries the page key
   * {@code releaseOrphanedHOTLeaves} recorded when it freed the frame, and its bytes are the leaf's
   * PRE-MERGE image, whose keys by then also live in the merge target.
   * </p>
   */
  private @Nullable Page reloadUnlessAlreadyMergedAway(final TransactionIntentLog log, final PageReference ref) {
    final Page durable = storageEngineWriter.loadHOTPage(ref);
    if (!(durable instanceof HOTLeafPage durableLeaf)
        || !log.namesReleasedHOTLeafPage(indexScope(), durableLeaf.getPageKey())) {
      return durable;
    }
    ref.clearPageIfSame(durableLeaf);
    // The merge's replacement, not nothing: the entries this image still shows went into that page,
    // so continuing the descent there is what keeps a stale reference copy routing to them. The
    // replacement is named by its reference, so it resolves whether it is still a log entry or has
    // since spilled to a durable offset of its own.
    final PageReference replacement = log.releasedHOTLeafReplacementReference(indexScope(), durableLeaf.getPageKey());
    if (replacement != null && replacement != ref) {
      forwardedReplacementRef = replacement;
      return null;
    }
    refuseReleasedLeaf(log, replacement == null
        ? "cause=null-replacement(durable)"
        : "cause=self-replacement(durable)", durableLeaf.getPageKey());
    return null;
  }

  /**
   * Whether {@code ref} names a transaction-log entry whose HOT leaf an incremental merge released.
   *
   * <p>
   * Asked against both identities on purpose. {@link TransactionIntentLog#get} is not
   * identity-preserving — it rewrites a reference whose shared handle has published a durable offset
   * into a pure disk reference — so the identity the reference carried on the way IN is the one that
   * still names the entry, while the one it carries on the way out is what every later layer sees.
   * </p>
   */
  private static boolean namesReleasedEntry(final TransactionIntentLog log, final PageReference ref,
      final int logKeyBeforeResolution, final int generationBeforeResolution) {
    return log.namesReleasedHOTLeafEntry(ref)
        || log.namesReleasedHOTLeafEntry(logKeyBeforeResolution, generationBeforeResolution);
  }

  /**
   * The replacement a released entry forwards to, asked against both identities for the same reason
   * {@link #namesReleasedEntry} is.
   */
  private static @Nullable PageReference releasedEntryReplacement(final TransactionIntentLog log,
      final PageReference ref, final int logKeyBeforeResolution, final int generationBeforeResolution) {
    final PageReference replacement =
        log.releasedHOTLeafReplacementReference(ref.getLogKey(), ref.getActiveTilGeneration());
    return replacement != null
        ? replacement
        : log.releasedHOTLeafReplacementReference(logKeyBeforeResolution, generationBeforeResolution);
  }

  /**
   * The reference that OWNS a released entry, asked against both identities for the same reason
   * {@link #namesReleasedEntry} is.
   *
   * <p>
   * Distinct from {@link #releasedEntryReplacement}: that one names where a merge moved the entries,
   * this one names the same entry from the log's side. It is what a reference COPY is missing when
   * the publication that closed the container applied a durable offset the copy never received.
   * </p>
   */
  private static @Nullable PageReference releasedEntryOwner(final TransactionIntentLog log, final PageReference ref,
      final int logKeyBeforeResolution, final int generationBeforeResolution) {
    final PageReference owner = log.releasedEntryOwnerReference(ref.getLogKey(), ref.getActiveTilGeneration());
    return owner != null
        ? owner
        : log.releasedEntryOwnerReference(logKeyBeforeResolution, generationBeforeResolution);
  }

  /**
   * Whether an unresolvable log entry is unresolvable <em>because an incremental merge released its
   * leaf</em> — and, if so, dispose of the descent for it.
   *
   * <p>
   * The distinction matters because the two cases want opposite answers. A merged-away leaf must
   * never fall back to storage: the durable image is its PRE-MERGE content, whose keys by then also
   * live in the merge target, and descending into it contradicts the live routing. But a leaf whose
   * container was closed for any OTHER reason has lost nothing — {@code publishPinnedSpillCandidate}
   * closes the exact container it has just written out to a durable offset, and a superseded
   * container the epoch rotation left behind is closed together with its successor — and for those
   * the durable image IS the live image. Dead-ending on them is what ended the transaction on the
   * {@code windows-latest / query} lane, where the pinned-trie spill runs often enough to close a
   * container the descent later walks into ({@code pinnedTrieSpillPages} in the job's
   * {@code HFT_ASYNC_FLUSH} line); the identity test alone cannot tell them apart, because a spill
   * records no forwarding link — it merged nothing away.
   * </p>
   *
   * <p>
   * The page-key blacklist {@code releaseOrphanedHOTLeaves} records is the authority, and the dead
   * container still carries the page key to ask it with, so the question is answered without
   * reloading the image it may have to refuse. When it answers yes, the same blacklist also names the
   * page that absorbed the entries — consulted here as well as through the orphan's identity, since a
   * reference copy may name a different entry of the same released leaf than the merge did.
   * </p>
   *
   * @return {@code true} iff a merge released this entry's leaf, in which case the descent has
   *         already been given its forwarding target or refused
   */
  private boolean mergeReleasedEntry(final TransactionIntentLog log, final PageReference ref,
      final int logKeyBeforeResolution, final int generationBeforeResolution) {
    long pageKey = log.releasedHOTLeafEntryPageKey(ref.getLogKey(), ref.getActiveTilGeneration());
    if (pageKey == Constants.NULL_ID_LONG) {
      pageKey = log.releasedHOTLeafEntryPageKey(logKeyBeforeResolution, generationBeforeResolution);
    }
    if (pageKey == Constants.NULL_ID_LONG || !log.namesReleasedHOTLeafPage(indexScope(), pageKey)) {
      return false;
    }
    final PageReference replacement = log.releasedHOTLeafReplacementReference(indexScope(), pageKey);
    if (replacement != null && replacement != ref) {
      forwardedReplacementRef = replacement;
    } else {
      refuseReleasedLeaf(log, replacement == null
          ? "cause=null-replacement(entry)"
          : "cause=self-replacement(entry)", pageKey);
    }
    return true;
  }

  /** This writer's page-key namespace — page keys are unique only within one index. */
  private long indexScope() {
    return TransactionIntentLog.indexScope(indexType, indexNumber);
  }

  /**
   * Whether {@code page} may be handed to a write-path descent.
   *
   * <p>
   * {@code isClosed()} alone is not the predicate for a HOT leaf: {@link HOTLeafPage#close()} marks
   * the page orphaned immediately but defers the teardown to the last {@code releaseGuard()}, and
   * throughout that window the page already refuses {@code acquireGuard()} while {@code isClosed()}
   * still reads false. This is the same test {@code TransactionIntentLog} applies to a container, so
   * the log layer and the reference layer agree on which pages are gone.
   * </p>
   */
  private static boolean servesTraversal(final @Nullable Page page) {
    if (page == null || page.isClosed()) {
      return false;
    }
    return !(page instanceof HOTLeafPage leaf) || !leaf.isOrphaned();
  }

  /**
   * Cap on a permitted nodeKey for chunked-bitmap storage. The chunkIdx is stored as a 32-bit
   * big-endian unsigned int trailer; with {@code chunkIdx = (int)(nodeKey >>> 16)} this gives a full
   * 48-bit nodeKey range — well above any practical Sirix dataset.
   */
  static final long MAX_NODE_KEY = (1L << 48) - 1L;

  /**
   * Reject a node key the chunked-bitmap encoding cannot represent.
   *
   * @param nodeKey the node key to validate
   * @throws IllegalArgumentException if {@code nodeKey} is negative or exceeds {@link #MAX_NODE_KEY}
   */
  static void checkNodeKeyRange(final long nodeKey) {
    if (nodeKey < 0L) {
      throw new IllegalArgumentException("nodeKey must be non-negative: " + nodeKey);
    }
    if (nodeKey > MAX_NODE_KEY) {
      throw new IllegalArgumentException(
          "nodeKey " + nodeKey + " exceeds chunked-bitmap range (max " + MAX_NODE_KEY + ")");
    }
  }

  /**
   * Whether the index tree currently holds no entry at all.
   *
   * <p>
   * True exactly for a freshly initialized index: its root reference resolves to the single empty
   * leaf {@code create*IndexTree} planted. An indirect root only exists once a leaf has split, so it
   * always covers at least one entry.
   * </p>
   */
  public final boolean isEmptyTree() {
    if (rootReference == null) {
      return false;
    }
    return resolveHOTPageForTraversal(rootReference) instanceof HOTLeafPage leaf && leaf.getEntryCount() == 0;
  }

  /**
   * Replace the whole index tree with one bulk-built from {@code sortedEntries}.
   *
   * <p>
   * This is the initial-build-only path: externally supplied, strictly sorted entries become the root
   * in one publication. Populated trees never call it and use incremental HOT mutation exclusively.
   * Registering the fresh subtree also re-puts the root reference, which closes the empty leaf it
   * displaces.
   * </p>
   *
   * @param sortedEntries entries sorted strictly ascending by unsigned key, with no duplicates
   * @throws IllegalStateException if the tree is not empty — the bulk build replaces rather than
   *         merges, so any pre-existing entry would be dropped
   */
  final void spliceBulkBuiltRoot(final List<HOTBulkBuilder.Entry> sortedEntries) {
    requireNonNull(sortedEntries, "sortedEntries");
    if (sortedEntries.isEmpty()) {
      return;
    }
    if (!isEmptyTree()) {
      throw new IllegalStateException(
          "Bulk load requires an empty " + indexType + " index tree (index " + indexNumber + ')');
    }
    Page freshRoot = null;
    boolean published = false;
    boolean registrationCompleted = false;
    try {
      final HOTBulkBuilder.BuildResult built =
          HOTBulkBuilder.build(sortedEntries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
      freshRoot = built.rootPage();
      rootReference.setPage(freshRoot);
      published = true;
      registerFreshSubtree(rootReference);
      registrationCompleted = true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
        closeLocallyOwnedFreshSubtree(rootReference, freshRoot, registrationCompleted, failure);
      } else {
        closeFreshHOTSubtree(freshRoot, failure);
      }
      throw failure;
    }
  }

  /** Reset and return the writer-confined navigation carrier without copying its path buffers. */
  private LeafNavigationResult buildNavigationResult(final HOTLeafPage leaf, final PageReference leafRef,
      final int pathDepth) {
    return navigationScratch.reset(leaf, leafRef, _pathNodes, _pathRefs, _pathChildIndices, pathDepth);
  }

  /**
   * Copy-on-write a HOT leaf into the transaction log for modification under the resource's
   * versioning strategy, returning the writable modified leaf and registering the
   * {@code (complete, modified)} container against {@code currentRef}.
   *
   * <p>
   * The per-strategy CoW policy (chain bump + which entries the sparse emit must re-materialize) is
   * encapsulated in {@link VersioningType#combineHOTLeafPagesForModification}; this method is the
   * writer-side counterpart of KVLP's {@code dereferenceRecordPageForModification} — it supplies the
   * engine context, then records the produced fragment in the transaction log.
   * </p>
   *
   * @param currentRef the leaf reference being CoW'd (chain mutated in place)
   * @param hotLeaf the combined (complete) leaf resolved for {@code currentRef}
   * @return the writable modified leaf now registered in the transaction log
   */
  private HOTLeafPage cowHOTLeafForModification(final PageReference currentRef, final HOTLeafPage hotLeaf) {
    try {
      return cowHOTLeafForModificationUnpoisoned(currentRef, hotLeaf);
    } catch (final RuntimeException | Error failure) {
      // prepareLeafOfTree has already published the writer-private index page and possibly an
      // indirect spine before it reaches this leaf. A failed combine, fragment release, guard
      // transfer, or TIL admission is therefore never safe to catch-and-commit, even when the
      // versioning layer restored the leaf reference's exact pre-bump fragment chain.
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  private HOTLeafPage cowHOTLeafForModificationUnpoisoned(final PageReference currentRef, final HOTLeafPage hotLeaf) {
    final ResourceConfiguration cfg = storageEngineWriter.getResourceSession().getResourceConfig();
    final TransactionIntentLog log = storageEngineWriter.getLog();
    HOTLeafPage sourceLeaf = hotLeaf;
    long retryDeadlineNanos = 0L;
    for (long attempt = 0L;; attempt++) {
      final PageContainer existing = log.get(currentRef);
      if (existing != null && existing.getModified() instanceof HOTLeafPage modifiedLeaf && !modifiedLeaf.isClosed()) {
        return modifiedLeaf;
      }

      if (sourceLeaf.acquireGuard()) {
        Throwable guardedFailure = null;
        try {
          // Serialize exact cache removal with pressure eviction. A guard acquired after eviction's
          // zero-count observation cannot by itself prevent retirement; removal either transfers
          // ownership here or waits for that retirement, which the orphan check then rejects.
          storageEngineWriter.getBufferManager().getHOTLeafPageCache().removePage(sourceLeaf);
          if (!sourceLeaf.isOrphaned() && !sourceLeaf.isClosed()) {
            HOTLeafPage modifiedLeaf = null;
            final PageContainer leafContainer;
            try {
              // Combining can load fragment windows and copy() allocates before reading source
              // bytes. The complete source stays locally owned and guarded throughout both.
              modifiedLeaf = cfg.versioningType.combineHOTLeafPagesForModification(sourceLeaf,
                  cfg.maxNumberOfRevisionsToRestore, storageEngineWriter, currentRef);
              leafContainer = PageContainer.getInstance(sourceLeaf, modifiedLeaf);
            } catch (final RuntimeException | Error copyFailure) {
              currentRef.clearPageIfSame(sourceLeaf);
              retireDetachedHOTLeavesAfterFailure(sourceLeaf, modifiedLeaf, copyFailure);
              throw copyFailure;
            }

            try {
              log.put(currentRef, leafContainer);
            } catch (final RuntimeException | Error logFailure) {
              cleanupFailedHOTLeafLogTransfer(log, currentRef, leafContainer, sourceLeaf, modifiedLeaf, logFailure);
              throw logFailure;
            }
            return modifiedLeaf;
          }
        } catch (final RuntimeException | Error failure) {
          guardedFailure = failure;
          throw failure;
        } finally {
          try {
            sourceLeaf.releaseGuard();
          } catch (final RuntimeException | Error releaseFailure) {
            if (guardedFailure == null) {
              throw releaseFailure;
            }
            addSuppressedSafely(guardedFailure, releaseFailure);
          }
        }
      }

      // The guard was lost to a retirement, so this instance is dead and the current one has to be
      // resolved again. Back off before doing so: spinning first keeps the common single lost race
      // cheap, and yielding afterwards lets the evicting thread finish instead of burning the whole
      // budget against it.
      retryDeadlineNanos = backOffBeforeHOTLeafGuardRetry(attempt, retryDeadlineNanos);

      currentRef.clearPageIfSame(sourceLeaf);
      final Page reloaded = resolveHOTPageForTraversal(currentRef);
      // Re-resolving THE SAME CLOSED instance is a dead end, not a race worth retrying: closing is
      // terminal, so that instance can never become guardable, and a resolution that just produced it
      // will keep producing it. Resolution really does — the transaction-intent log keeps containers
      // whose HOT leaves an incremental merge already released, and a page-reference reload consults
      // the log before storage — so without this the retry spins against a page whose off-heap slot is
      // already gone until the wall-clock budget runs out. A DIFFERENT closed instance is a lost race
      // with eviction and still worth another round; only standing still is fatal.
      if (!(reloaded instanceof HOTLeafPage reloadedLeaf) || (reloadedLeaf == sourceLeaf && reloadedLeaf.isClosed())) {
        throw new IllegalStateException("HOT leaf disappeared while acquiring a copy-on-write guard");
      }
      sourceLeaf = reloadedLeaf;
    }
  }

  /**
   * Pace one lost guard race and enforce the retry deadline.
   *
   * <p>
   * Three stages, because the thing being waited for is another thread FINISHING a retirement, and
   * only the last stage actually lets it: spin while the race is plausibly still in the acquire
   * window, yield while the holder may just need the scheduler's nod, then park — releasing this core
   * outright — for a geometrically growing slice capped at {@link #HOT_LEAF_GUARD_PARK_MAX_NANOS}.
   * Yielding forever is what turned this retry into a livelock on a small runner, where a yield with
   * no same-core candidate returns immediately and the waiter simply out-competes the retiring
   * thread.
   * </p>
   *
   * @param attempt the number of guard acquisitions already lost, counted in {@code long} so an
   *        unbounded storm can never wrap the counter back into the spin budget
   * @param deadlineNanos the deadline armed on the first attempt past the spin budget; unread until
   *        then
   * @return the armed deadline to carry into the next attempt
   * @throws IllegalStateException when the deadline passed without a single guarded attempt
   */
  private static long backOffBeforeHOTLeafGuardRetry(final long attempt, final long deadlineNanos) {
    if (attempt < HOT_LEAF_GUARD_SPIN_ATTEMPTS) {
      Thread.onSpinWait();
      return deadlineNanos;
    }
    if (attempt == HOT_LEAF_GUARD_SPIN_ATTEMPTS) {
      Thread.yield();
      return System.nanoTime() + HOT_LEAF_GUARD_RETRY_DEADLINE_NANOS;
    }
    if (System.nanoTime() - deadlineNanos >= 0L) {
      throw new IllegalStateException("HOT leaf was retired before it could be guarded within "
          + TimeUnit.NANOSECONDS.toMillis(HOT_LEAF_GUARD_RETRY_DEADLINE_NANOS) + " ms and " + attempt + " attempts");
    }
    if (attempt < HOT_LEAF_GUARD_YIELD_ATTEMPTS) {
      Thread.yield();
    } else {
      LockSupport.parkNanos(hotLeafGuardParkNanos(attempt));
    }
    return deadlineNanos;
  }

  /**
   * Parked back-off for one attempt: doubles per attempt past the yield budget, capped so the wait
   * this waiter adds stays bounded.
   *
   * @param attempt the number of guard acquisitions already lost
   * @return the park duration in nanoseconds
   */
  private static long hotLeafGuardParkNanos(final long attempt) {
    // Clamp the doubling count BEFORE shifting. The cap is reached within a handful of doublings, so
    // the clamp costs nothing — but leaving it out lets a long wait shift the minimum past 2^63,
    // where the product goes negative, Math.min picks the negative, and parkNanos returns instantly.
    // That failure mode is silently the very spin this back-off exists to stop.
    final long doublings = Math.min(attempt - HOT_LEAF_GUARD_YIELD_ATTEMPTS, HOT_LEAF_GUARD_MAX_PARK_DOUBLINGS);
    return Math.min(HOT_LEAF_GUARD_PARK_MAX_NANOS, HOT_LEAF_GUARD_PARK_MIN_NANOS << doublings);
  }

  /**
   * Resolve the log's ownership after a failed put. Identity with {@code attemptedContainer} proves
   * publication; otherwise only pages not reachable through the published container remain locally
   * owned and may be retired. If the ownership check itself fails, retain rather than risk closing a
   * TIL-owned page.
   */
  private static void cleanupFailedHOTLeafLogTransfer(final TransactionIntentLog log, final PageReference currentRef,
      final PageContainer attemptedContainer, final HOTLeafPage sourceLeaf, final HOTLeafPage modifiedLeaf,
      final Throwable logFailure) {
    final PageContainer published;
    try {
      published = log.get(currentRef);
    } catch (final RuntimeException | Error ownershipCheckFailure) {
      addSuppressedSafely(logFailure, ownershipCheckFailure);
      return;
    }
    if (published == attemptedContainer) {
      return;
    }

    currentRef.clearPageIfSame(sourceLeaf);
    final HOTLeafPage unownedSource = containerOwnsPage(published, sourceLeaf)
        ? null
        : sourceLeaf;
    final HOTLeafPage unownedModified = containerOwnsPage(published, modifiedLeaf)
        ? null
        : modifiedLeaf;
    retireDetachedHOTLeavesAfterFailure(unownedSource, unownedModified, logFailure);
  }

  private static boolean containerOwnsPage(final @Nullable PageContainer container, final Page page) {
    return container != null && (container.getComplete() == page || container.getModified() == page);
  }

  /** Retire locally owned HOT pages after combine/container/log transfer failure. */
  private static void retireDetachedHOTLeavesAfterFailure(final @Nullable HOTLeafPage sourceLeaf,
      final @Nullable HOTLeafPage modifiedLeaf, final Throwable primaryFailure) {
    if (modifiedLeaf != null && modifiedLeaf != sourceLeaf) {
      try {
        modifiedLeaf.retire();
      } catch (final RuntimeException | Error retirementFailure) {
        addSuppressedSafely(primaryFailure, retirementFailure);
      }
    }
    if (sourceLeaf != null) {
      try {
        sourceLeaf.retire();
      } catch (final RuntimeException | Error retirementFailure) {
        addSuppressedSafely(primaryFailure, retirementFailure);
      }
    }
  }

  /** Never let secondary cleanup failure replace the combine/copy failure being propagated. */
  private static void addSuppressedSafely(final Throwable primary, final Throwable secondary) {
    if (primary == secondary) {
      return;
    }
    try {
      primary.addSuppressed(secondary);
    } catch (final RuntimeException | Error ignored) {
      // The primary allocation/copy failure remains authoritative.
    }
  }

  /**
   * Initialize the HOT index tree structure for PATH index.
   *
   * @throws SirixIOException if initialization fails
   */
  protected void initializePathIndex() {
    try {
      final PathPage pathPage = storageEngineWriter.prepareSecondaryIndexPage(IndexType.PATH);

      // Get existing reference first to check if index already exists
      final PageReference existingRef = pathPage.getIndirectPageReference(indexNumber);
      final boolean indexExists = !existingRef.isVirginStructuralPlaceholder();

      if (!indexExists) {
        pathPage.createPathIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = pathPage.getIndirectPageReference(indexNumber);
    } catch (SirixIOException e) {
      throw new IllegalStateException("Failed to initialize HOT PATH index", e);
    }
  }

  /**
   * Initialize the HOT index tree structure for CAS index.
   *
   * @throws SirixIOException if initialization fails
   */
  protected void initializeCASIndex() {
    try {
      final CASPage casPage = storageEngineWriter.prepareSecondaryIndexPage(IndexType.CAS);

      // Get existing reference first to check if index already exists
      final PageReference existingRef = casPage.getIndirectPageReference(indexNumber);
      final boolean indexExists = !existingRef.isVirginStructuralPlaceholder();

      if (!indexExists) {
        casPage.createCASIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = casPage.getIndirectPageReference(indexNumber);
    } catch (SirixIOException e) {
      throw new IllegalStateException("Failed to initialize HOT CAS index", e);
    }
  }

  /**
   * Initialize the HOT index tree structure for NAME index.
   *
   * @throws SirixIOException if initialization fails
   */
  protected void initializeNameIndex() {
    try {
      final NamePage namePage = storageEngineWriter.prepareSecondaryIndexPage(IndexType.NAME);

      // Get existing reference first to check if index already exists
      PageReference existingRef = namePage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        namePage.createNameIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = namePage.getOrCreateReference(indexNumber);
    } catch (SirixIOException e) {
      throw new IllegalStateException("Failed to initialize HOT NAME index", e);
    }
  }

  /**
   * Initialize the HOT index tree structure for VALIDTIME interval index.
   *
   * @throws SirixIOException if initialization fails
   */
  protected void initializeValidTimeIndex() {
    try {
      final ValidTimeIndexPage vtPage = storageEngineWriter.prepareSecondaryIndexPage(IndexType.VALIDTIME);

      // Get existing reference first to check if index already exists
      final PageReference existingRef = vtPage.getIndirectPageReference(indexNumber);
      final boolean indexExists = !existingRef.isVirginStructuralPlaceholder();

      if (!indexExists) {
        vtPage.createValidTimeIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = vtPage.getIndirectPageReference(indexNumber);
    } catch (SirixIOException e) {
      throw new IllegalStateException("Failed to initialize HOT VALIDTIME index", e);
    }
  }

  /**
   * Insert a {@code (key, value)} pair into the HOT secondary index — the live driver of the faithful
   * incremental port ({@code docs/HOT_INCREMENTAL_PORT_PLAN.md} step 5).
   *
   * <p>
   * {@link #prepareLeafOfTree} copy-on-writes the descent path to a leaf page;
   * {@link HOTIncrementalInsert#analyzeDescent} then locates the mismatch bit {@code beta} between
   * the new key and the routed leaf. Two outcomes follow (plan §1.2):
   * <ul>
   * <li><b>merge</b> — {@code beta} lies inside the leaf's {@code R(S)}-subtree (or the index has no
   * compound node yet): the entry is merged into the leaf bucket. On bucket overflow the leaf page is
   * split ({@link HOTIncrementalInsert#splitLeafPage}) and the resulting {@code BiNode} is integrated
   * at the leaf's depth.</li>
   * <li><b>branch</b> — {@code beta} is at or above an ancestor's discriminative bit: HOT's
   * subset-match routing landed the key in a leaf it does not fully belong to, so the index is
   * rebuilt canonically with the key included ({@link #branchAboveLeaf}).</li>
   * </ul>
   * Every page produced is registered in the transaction-intent log ({@link #registerFreshSubtree}).
   *
   * @param keyBuf the serialized key (may be longer than {@code keyLen})
   * @param keyLen the key length
   * @param valueBuf the serialized value (may be longer than {@code valueLen})
   * @param valueLen the value length
   * @throws SirixIOException if the index is uninitialized or the entry cannot be stored
   */
  protected final void doIndex(byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {
    doMutation(MutationOperation.UPSERT, keyBuf, keyLen, valueBuf, valueLen, 0L);
  }

  /**
   * Remove one low-16 posting-list bit from an already-serialized composite chunk key.
   *
   * <p>
   * This is the posting-list delete arm of the same mutation driver used by
   * {@link #doIndex(byte[], int, byte[], int)}. Concrete writers are responsible only for their key
   * serialization; copy-on-write navigation, tombstoning and replacement live here once.
   * </p>
   *
   * @param keyBuf buffer containing the composite key
   * @param keyLen valid composite-key bytes in {@code keyBuf}
   * @param bit16 low 16 bits of the node key stored in this chunk
   * @return {@code true} iff the bit existed and was removed
   */
  protected final boolean doRemovePostingBit(final byte[] keyBuf, final int keyLen, final long bit16) {
    if ((bit16 & ~0xFFFFL) != 0L) {
      throw new IllegalArgumentException("posting-list chunk bit must be in [0, 65535]: " + bit16);
    }
    return doMutation(MutationOperation.REMOVE_POSTING_BIT, keyBuf, keyLen, null, 0, bit16);
  }

  /**
   * The one operation-coded foreground mutation driver for HOT indexes.
   *
   * <p>
   * The private method is deliberately reached through final, operation-specific wrappers. The JIT
   * therefore sees a constant operation at every production call site and can eliminate the unused
   * arm; no command object, lambda or per-call result object is allocated.
   * </p>
   */
  private boolean doMutation(final MutationOperation operation, final byte[] keyBuf, final int keyLen,
      final @Nullable byte[] valueBuf, final int valueLen, final long operationArgument) {
    storageEngineWriter.assertTransactionWritable();
    if (rootReference == null) {
      throw new SirixIOException("HOT index not initialized for " + indexType);
    }

    requireNonNull(operation, "operation");
    requireNonNull(keyBuf, "keyBuf");
    if (keyLen < 0 || keyLen > keyBuf.length) {
      throw new IllegalArgumentException("keyLen " + keyLen + " outside keyBuf length " + keyBuf.length);
    }
    if (operation == MutationOperation.UPSERT && (valueBuf == null || valueLen < 0 || valueLen > valueBuf.length)) {
      throw new IllegalArgumentException("invalid UPSERT value range: valueLen=" + valueLen + ", bufferLength="
          + (valueBuf == null
              ? "null"
              : valueBuf.length));
    }

    if (operation == MutationOperation.REMOVE_POSTING_BIT) {
      return removePostingBit(keyBuf, keyLen, operationArgument);
    }

    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);

    // The operation guard above proves this non-null. Keeping the nullable carrier private lets the
    // delete arm avoid fabricating a dummy value buffer while the public insert contract stays
    // non-null and allocation-free.
    final byte[] upsertValue = requireNonNull(valueBuf);

    final boolean localize =
        LOCALIZE_I8 && storageEngineWriter.getRevisionNumber() >= LOCALIZE_I8_FROM_REV && i8ProbeReports < 60;
    final String i8Before = localize
        ? firstStructuralViolationFromRoot()
        : null;
    final long[] cntBefore = localize
        ? i8ProbeSnapshot()
        : null;

    // Factored merge-vs-branch dispatch — re-used by {@link #subInsertAt} on a C2 re-descend
    // (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §4.1).
    dispatchInsert(navResult, keyBuf, keyLen, upsertValue, valueLen);

    if (localize && i8Before == null) {
      i8ProbeReport("dispatch(" + (i8ProbeMerge
          ? "merge"
          : "branch") + ")", keyBuf, keyLen, cntBefore);
    }

    // Periodic leaf consolidation (the thesis's underflow rule), bounded to the direct parent of
    // the route just updated. The former implementation started at pathRefs[0] and recursively
    // CoW'd every indirect in the index every 4,096 inserts: a deterministic O(index) latency cliff
    // on an ordinary foreground put. One HOT block has at most MAX_NODE_ENTRIES children, so this
    // attempt is O(path depth + fixed fanout), independent of total index size.
    if (navResult.pathDepth() > 0 && ++insertsSinceConsolidation >= CONSOLIDATION_INTERVAL) {
      insertsSinceConsolidation = 0;
      // A split/fold may have replaced the pre-dispatch parent, so re-descend only after a
      // structural splice. A plain value merge leaves navResult current and avoids a second walk.
      final LeafNavigationResult consolidationRoute = structuralValidationScope == null
          ? navResult
          : prepareLeafOfTree(rootReference, keyBuf, keyLen);
      final String consBefore = localize
          ? firstStructuralViolationFromRoot()
          : null;
      final long[] consCntBefore = localize
          ? i8ProbeSnapshot()
          : null;
      consolidateLeafParent(consolidationRoute);
      if (localize && consBefore == null) {
        i8ProbeReport("consolidate", keyBuf, keyLen, consCntBefore);
      }
    }
    return true;
  }

  /**
   * Shared implementation of a chunked posting-list delete.
   *
   * <p>
   * The first descent is read-only. An absent key/bit therefore returns without copy-on-writing the
   * index page, indirect spine or leaf. A confirmed hit is then applied to the ordinary CoW route.
   * The writer is transaction-confined, so nothing can change the slot between those two descents.
   * </p>
   */
  private boolean removePostingBit(final byte[] keyBuf, final int keyLen, final long bit16) {
    try {
      final int replacementLength = preparePostingBitRemovalUnderGuard(keyBuf, keyLen, bit16);
      if (replacementLength == NodeReferencesSerializer.PACKED_REMOVE_ABSENT) {
        return false;
      }

      lastDispatchHandler = "h:remove-posting-bit";
      final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
      final HOTLeafPage writableLeaf = navResult.leaf();
      final int writableIndex = writableLeaf.findEntry(keyBuf, keyLen);
      if (writableIndex < 0) {
        throw new IllegalStateException(
            "HOT posting-list slot disappeared between read and CoW descents; leaf=" + writableLeaf.getPageKey());
      }

      if (replacementLength == NodeReferencesSerializer.PACKED_REMOVE_EMPTY) {
        if (!writableLeaf.deleteAt(writableIndex)) {
          throw new IllegalStateException("HOT posting-list slot could not be tombstoned at leaf "
              + writableLeaf.getPageKey() + ", slot " + writableIndex);
        }
        return true;
      }

      final byte[] replacementBuffer = requireNonNull(lastSerializedValueBuf, "posting removal buffer");
      if (writableLeaf.updateValueRange(writableIndex, replacementBuffer, 0, replacementLength)) {
        return true;
      }

      // A remove can grow only when its representation changes (notably a 65-entry Roaring chunk
      // becoming a 64-entry packed chunk). updateValueRange handles ordinary growth and compaction
      // directly; a false result means the compacted replacement still cannot fit. Preserve an exact
      // payload only for the rare tombstone + re-entry through the SAME structural dispatcher, whose
      // split machinery creates the required space.
      final byte[] exactReplacement = Arrays.copyOf(replacementBuffer, replacementLength);
      if (writableLeaf.updateValue(writableIndex, exactReplacement)) {
        return true;
      }
      if (!writableLeaf.deleteAt(writableIndex)) {
        throw new IllegalStateException("HOT posting-list overflow fallback could not tombstone leaf "
            + writableLeaf.getPageKey() + ", slot " + writableIndex);
      }
      dispatchInsert(navResult, keyBuf, keyLen, exactReplacement, exactReplacement.length);
      return true;
    } catch (final RuntimeException | Error failure) {
      // This includes stable slot corruption discovered by the read-only preflight. Never allow a
      // caller that catches the exception to commit a transaction whose index state is suspect; it
      // must roll back before doing further work.
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  /** Compute a posting replacement while the resolved leaf's off-heap frame is pinned. */
  private int preparePostingBitRemovalUnderGuard(final byte[] keyBuf, final int keyLen, final long bit16) {
    final HOTLeafPage readLeaf = acquireLeafForRead(keyBuf, keyLen);
    if (readLeaf == null) {
      throw new IllegalStateException(
          "HOT posting-list read descent did not resolve a leaf for index " + indexType + '/' + indexNumber);
    }
    Throwable guardedFailure = null;
    try {
      return preparePostingBitRemoval(readLeaf, keyBuf, keyLen, bit16);
    } catch (final RuntimeException | Error failure) {
      guardedFailure = failure;
      throw failure;
    } finally {
      releaseLeafReadGuard(readLeaf, guardedFailure);
    }
  }

  /**
   * Compute one posting-bit removal without mutating {@code leaf}.
   *
   * <p>
   * Packed chunks use the slot-native primitive and the writer's thread-local scratch: the dominant
   * delete path neither copies the old slot nor materializes a bitmap. Roaring is the cold arm and
   * remains bounded to one 16-bit chunk, never the full logical posting list.
   * </p>
   *
   * @return {@link NodeReferencesSerializer#PACKED_REMOVE_ABSENT},
   *         {@link NodeReferencesSerializer#PACKED_REMOVE_EMPTY}, or the positive exact byte length
   *         now stored in {@link #lastSerializedValueBuf}
   */
  private int preparePostingBitRemoval(final HOTLeafPage leaf, final byte[] keyBuf, final int keyLen,
      final long bit16) {
    final int index = leaf.findEntry(keyBuf, keyLen);
    if (index < 0) {
      return NodeReferencesSerializer.PACKED_REMOVE_ABSENT;
    }

    final byte[] scratch = VALUE_BUFFER.get();
    final int packedResult =
        NodeReferencesSerializer.removePackedSingleBitFromSlot(leaf, leaf.valueRef(index), bit16, scratch, 0);
    if (packedResult != NodeReferencesSerializer.PACKED_REMOVE_NOT_APPLICABLE) {
      if (packedResult > 0) {
        lastSerializedValueBuf = scratch;
        lastSerializedValueLen = packedResult;
      }
      return packedResult;
    }

    // Exact-copy is intentional for the non-packed arm: unlike getValue(), it distinguishes a
    // corrupt/unreadable slot from an empty value. Posting indexes never use a zero-length tombstone.
    final byte[] valueBytes = leaf.copyStoredValue(index);
    if (valueBytes.length == 0) {
      throw new IllegalStateException(
          "HOT posting-list slot has a zero-length value at leaf " + leaf.getPageKey() + ", slot " + index);
    }
    if (NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
      return NodeReferencesSerializer.PACKED_REMOVE_ABSENT;
    }

    final NodeReferences chunkReferences = NodeReferencesSerializer.deserializeChunk(valueBytes);
    if (!chunkReferences.removeNodeKey(bit16)) {
      return NodeReferencesSerializer.PACKED_REMOVE_ABSENT;
    }
    if (!chunkReferences.hasNodeKeys()) {
      return NodeReferencesSerializer.PACKED_REMOVE_EMPTY;
    }
    serializeValueInto(chunkReferences);
    return lastSerializedValueLen;
  }

  // ===== I8-onset localizer helpers (diagnostic; see field declarations). =====

  private long[] i8ProbeSnapshot() {
    return new long[] {OFF_PATH_OVERFLOW_OK.get(), OFF_PATH_OVERFLOW_FALLBACK.get(), DIRECTION_ONE_SUBINSERT.get(),
        DIRECTION_ONE_FALLBACK.get(), STRAND_COMPLETE_FRONTIER.get(), STRAND_TWO_LEAF_MIGRATE.get()};
  }

  private void i8ProbeReport(final String phase, final byte[] keyBuf, final int keyLen, final long[] before) {
    final String viol = firstStructuralViolationFromRoot();
    if (viol == null) {
      return;
    }
    i8ProbeReports++;
    final long[] after = i8ProbeSnapshot();
    final String[] names =
        {"offPathOk", "offPathFallback", "dir1Subinsert", "dir1Fallback", "strandFrontier", "strandMigrate"};
    final StringBuilder deltas = new StringBuilder();
    for (int i = 0; i < names.length; i++) {
      if (after[i] != before[i]) {
        deltas.append(names[i]).append('+').append(after[i] - before[i]).append(' ');
      }
    }
    System.err.println("[I8-LOCALIZE] rev=" + storageEngineWriter.getRevisionNumber() + " phase=" + phase + " key="
        + HexFormat.of().formatHex(keyBuf, 0, Math.min(keyLen, 22)) + " handlers={" + deltas.toString().trim()
        + "} onset=" + viol);
  }

  /**
   * First cheap structural violation (I4 first-partial-zero, I7 partials-ascending, I8
   * children-by-firstKey) reachable from the index root, or {@code null}. These are the
   * O(children)/O(children×height) invariants — the expensive I5 constancy walk is left to the
   * per-revision {@code HOTInvariantValidator}. Diagnostic only (localizer).
   */
  private @Nullable String firstStructuralViolationFromRoot() {
    if (rootReference == null) {
      return null;
    }
    final Page page = resolveHOTPageForTraversal(rootReference);
    if (!(page instanceof HOTIndirectPage)) {
      return null;
    }
    final RebuildFootprint footprint = measureBoundedRebuildFootprint(page);
    if (!footprint.withinBudget()) {
      DIAGNOSTIC_TRAVERSAL_SKIPPED.incrementAndGet();
      return null;
    }
    return structuralDfs(rootReference, 0);
  }

  private @Nullable String structuralDfs(@Nullable PageReference ref, int depth) {
    if (ref == null || depth > MAX_PATH_DEPTH) {
      return null;
    }
    if (!(resolveHOTPageForTraversal(ref) instanceof HOTIndirectPage indirect)) {
      return null;
    }
    final int n = indirect.getNumChildren();
    final int[] partials = indirect.getPartialKeysRef();
    if (partials != null && partials.length >= n && n > 0) {
      int minPartial = partials[0];
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], minPartial) < 0) {
          minPartial = partials[i];
        }
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          return "I7 node=" + indirect.getPageKey() + " nChildren=" + n + " partial[" + (i - 1) + "]=0x"
              + Integer.toHexString(partials[i - 1]) + " >= partial[" + i + "]=0x" + Integer.toHexString(partials[i]);
        }
      }
      if (minPartial != 0) {
        return "I4 node=" + indirect.getPageKey() + " nChildren=" + n + " smallestPartial=0x"
            + Integer.toHexString(minPartial) + " (must be 0)";
      }
    }
    byte[] prev = null;
    for (int i = 0; i < n; i++) {
      final byte[] fk = firstKeyOfSubtree(indirect.getChildReference(i));
      if (fk == null) {
        continue;
      }
      if (prev != null && Arrays.compareUnsigned(prev, fk) >= 0) {
        return "I8 node=" + indirect.getPageKey() + " nChildren=" + n + " child[" + i + "].fk="
            + HexFormat.of().formatHex(fk, 0, Math.min(fk.length, 22)) + " <= prev.fk="
            + HexFormat.of().formatHex(prev, 0, Math.min(prev.length, 22));
      }
      prev = fk;
    }
    for (int i = 0; i < n; i++) {
      final PageReference cr = indirect.getChildReference(i);
      if (cr != null && resolveHOTPageForTraversal(cr) instanceof HOTIndirectPage) {
        final String r = structuralDfs(cr, depth + 1);
        if (r != null) {
          return r;
        }
      }
    }
    return null;
  }

  /**
   * The merge-vs-branch dispatch core of {@link #doIndex}: run {@code analyzeDescent}, decide between
   * merge and branch via the merge-vs-branch bound (Â§1.2 of the port plan), invoke the corresponding
   * handler. Factored out so {@link #subInsertAt} can re-use it on a C2 re-descend
   * ({@code docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md} Â§4.1).
   */
  private void dispatchInsert(final LeafNavigationResult navResult, final byte[] keyBuf, final int keyLen,
      final byte[] valueBuf, final int valueLen) {
    final int pathDepth = navResult.pathDepth();
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    HOTIncrementalInsert.analyzeDescentInto(pathNodes, navResult.pathChildIndices(), pathDepth, navResult.leaf(),
        keyBuf, keyLen, descentScratch);

    // Copy these primitives before dispatch: a structural handler may recursively sub-insert and
    // reuse this writer's DescentScratch. The outer route remains independent and these locals retain
    // its decision without allocating a recursion frame object.
    final int beta = descentScratch.mismatchBit();
    final int insertDepth = descentScratch.insertDepth();
    final int affectedChildIndex = descentScratch.affectedChildIndex();

    // Merge-vs-branch: the key merges into the routed leaf when there is no compound ancestor,
    // when it is already present or the leaf is empty (beta < 0), or when the mismatch bit beta
    // is strictly less significant than every ancestor discriminative bit -- i.e. beta lies
    // inside the leaf's R(S)-subtree (I5 holds). The bound is the deepest compound node's least
    // significant disc bit (I11 dominates the shallower bits). Larger absolute bit index = less
    // significant.
    final boolean merge = beta < 0 || pathDepth == 0 || beta > leastSignificantDiscBit(pathNodes[pathDepth - 1]);
    if (LOCALIZE_I8) {
      i8ProbeMerge = merge;
    }

    structuralValidationScope = null; // set by registerFreshSubtree iff this dispatch splices a subtree
    lastDispatchHandler = merge
        ? "merge"
        : "branch";
    final byte[] structuralKey;
    final boolean structurallyChanged;
    if (merge) {
      structuralKey = mergeIntoLeaf(navResult, keyBuf, keyLen, valueBuf, valueLen);
      structurallyChanged = structuralKey != null;
    } else {
      structuralKey = exactKeyForStructuralMutation(keyBuf, keyLen);
      structurallyChanged =
          branchAboveLeaf(navResult, beta, insertDepth, affectedChildIndex, structuralKey, valueBuf, valueLen);
    }

    // Defense-in-depth only. Every structural handler must finish all checks before its sole
    // publication boundary; this post-publication pass is forbidden to mutate. If a primitive ever
    // violates that contract, poison the transaction and surface the exact invariant immediately.
    if (structurallyChanged && VALIDATE_STRUCTURAL_MUTATIONS) {
      final byte[] exactStructuralKey = requireNonNull(structuralKey, "structuralKey");
      validatePublishedStructuralScope(structuralValidationScope, exactStructuralKey);
      validatePublishedStructuralPath(exactStructuralKey);
    }
  }

  /** Materialize an exact key only after a mutation has entered a structural slow path. */
  private static byte[] exactKeyForStructuralMutation(final byte[] keyBuf, final int keyLen) {
    return keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);
  }

  /** Run the full invariant detector over the bounded published splice, without repairing it. */
  private void validatePublishedStructuralScope(@Nullable PageReference scope, byte[] keySlice) {
    if (scope == null) {
      return;
    }
    try {
      validatePublishedStructuralScopeBounded(scope, keySlice);
    } catch (final RuntimeException | Error failure) {
      // The splice is already published. A validation failure must be impossible to catch-and-commit.
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  private void validatePublishedStructuralScopeBounded(final PageReference scope, final byte[] keySlice) {
    final Page scopeRoot = resolveHOTPageForTraversal(scope);
    if (scopeRoot instanceof HOTLeafPage) {
      return;
    }
    if (!(scopeRoot instanceof HOTIndirectPage)) {
      throw new IllegalStateException("HOT structural validation scope is unresolvable or is not a HOT page");
    }
    final RebuildFootprint footprint = measureBoundedRebuildFootprint(scopeRoot);
    if (!footprint.withinBudget()) {
      if (footprint.status().isBudgetLimit()) {
        // The full detector is defense-in-depth over formally verified incremental primitives and
        // includes I5's subtree-key walk. On a scope larger than one bounded HOT block, running it
        // would reintroduce the O(index) foreground cliff this gate exists to remove. The mandatory
        // route-local structural guard still runs immediately after this method; any defect it
        // observes is transaction-fatal and cannot trigger a second mutation route.
        STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED.incrementAndGet();
        return;
      }
      throw new IllegalStateException(
          "HOT structural validation cannot safely inspect its scope: " + footprint.summary());
    }
    final List<HOTMalformedSubtreeDetector.MalformedSubtree> malformed =
        HOTMalformedSubtreeDetector.detect(scope, traversalPageResolver);
    if (malformed.isEmpty()) {
      return;
    }
    for (final HOTMalformedSubtreeDetector.MalformedSubtree defect : malformed) {
      STRUCTURAL_VALIDATION_TALLY.computeIfAbsent(defect.invariant() + "|" + lastDispatchHandler,
          ignored -> new AtomicLong()).incrementAndGet();
      if (Boolean.getBoolean("hot.diag.validationDump")) {
        final Page malformedPage = resolveHOTPageForTraversal(defect.reference());
        final int height = malformedPage instanceof HOTIndirectPage indirect
            ? indirect.getHeight()
            : 0;
        System.err.println("[validation-dump] inv=" + defect.invariant() + " handler=" + lastDispatchHandler
            + " height=" + height + " atScopeRoot=" + (defect.reference() == scope) + " K="
            + HexFormat.of().formatHex(keySlice) + " detail=" + defect.detail());
      }
    }
    STRUCTURAL_VALIDATION_FAILURE.incrementAndGet();
    final String detail = "HOT published structural splice is malformed (first: " + malformed.getFirst().invariant()
        + " — " + malformed.getFirst().detail() + ')';
    LOG.error(detail);
    throw new IllegalStateException(detail);
  }

  /**
   * Insert {@code (key, value)} into the subtree rooted at {@code subtreeRef}. Used by the
   * C2-collision handlers: when {@code addChildAtCombination}'s {@code comboPartial} coincides with
   * an existing child of d* (or of the boundary node), K structurally belongs INSIDE that child's
   * subtree -- the descent stopped one level too shallow. This method extends the descent through
   * {@code subtreeRef} and runs the standard merge-vs-branch dispatch at the deeper depth
   * ({@code docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md} Â§4.1).
   *
   * <p>
   * Uses local descent arrays (not the shared {@code _pathNodes} field) so it is safe under recursive
   * invocation (a sub-insert that itself triggers another C2). Bounded by tree depth
   * ({@code MAX_PATH_DEPTH}).
   *
   * @return {@code true} iff the insert succeeded incrementally; {@code false} before publication on
   *         an unresolvable descent or depth overflow, so the caller can use the complete frontier
   */
  private boolean subInsertAt(PageReference subtreeRef, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {
    if (subtreeRef == null) {
      return false;
    }
    final byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);

    // Local descent arrays -- subInsertAt is recursion-safe (the shared _pathNodes are reserved
    // for the outer doIndex's prepareLeafOfTree).
    final HOTIndirectPage[] subPathNodes = new HOTIndirectPage[MAX_PATH_DEPTH];
    final PageReference[] subPathRefs = new PageReference[MAX_PATH_DEPTH];
    final int[] subPathChildIndices = new int[MAX_PATH_DEPTH];
    int subPathDepth = 0;
    PageReference currentRef = subtreeRef;
    Page page = resolveHOTPageForTraversal(currentRef);

    while (page instanceof HOTIndirectPage indirectPage) {
      if (subPathDepth >= MAX_PATH_DEPTH) {
        return false; // defensive: tree-depth overflow
      }
      final HOTIndirectPage cowedIndirect = prepareIndirectPage(currentRef, indirectPage);
      subPathNodes[subPathDepth] = cowedIndirect;
      subPathRefs[subPathDepth] = currentRef;
      final int childIndex = cowedIndirect.findChildIndex(keySlice);
      if (childIndex < 0) {
        return false; // defensive: descent failed
      }
      subPathChildIndices[subPathDepth] = childIndex;
      subPathDepth++;
      currentRef = cowedIndirect.getChildReference(childIndex);
      if (currentRef == null) {
        return false;
      }
      page = resolveHOTPageForTraversal(currentRef);
    }
    if (!(page instanceof HOTLeafPage hotLeaf)) {
      return false; // defensive: expected a leaf
    }

    // CoW the leaf into the TIL (mirrors prepareLeafOfTree's leaf handling).
    final HOTLeafPage modifiedLeaf;
    final PageContainer existing = storageEngineWriter.getLog().get(currentRef);
    if (existing != null && existing.getModified() instanceof HOTLeafPage existingModified
        && !existingModified.isClosed()) {
      modifiedLeaf = existingModified;
    } else {
      modifiedLeaf = cowHOTLeafForModification(currentRef, hotLeaf);
    }

    final LeafNavigationResult subNav =
        new LeafNavigationResult(modifiedLeaf, currentRef, Arrays.copyOf(subPathNodes, subPathDepth),
            Arrays.copyOf(subPathRefs, subPathDepth), Arrays.copyOf(subPathChildIndices, subPathDepth), subPathDepth);

    dispatchInsert(subNav, keyBuf, keyLen, valueBuf, valueLen);
    return true;
  }

  /**
   * Find the slot of {@code node}'s child whose stored partial equals {@code partial}, or {@code -1}
   * if none. Used by the C2-collision handlers to find the colliding child for {@link #subInsertAt}.
   */
  private static int findChildSlotByPartial(HOTIndirectPage node, int partial) {
    final int[] partials = node.getPartialKeysRef();
    if (partials == null) {
      return -1;
    }
    for (int i = 0; i < node.getNumChildren(); i++) {
      if (partials[i] == partial) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Walk the leftmost path from {@code ref} to its leaf and return that leaf's first key -- the
   * smallest key contained in the subtree rooted at {@code ref}. Bounded by tree height
   * ({@link #MAX_PATH_DEPTH}); returns {@code null} on an empty subtree or an unresolvable descent
   * (defensive). Used by the Direction 1 I8-safety pre-check to compare K's lex position against
   * {@code affected}'s neighbouring siblings.
   */
  private byte @Nullable [] firstKeyOfSubtree(@Nullable PageReference ref) {
    if (ref == null) {
      return null;
    }
    PageReference cur = ref;
    for (int depth = 0; depth <= MAX_PATH_DEPTH; depth++) {
      final Page page = resolveHOTPageForTraversal(cur);
      if (page == null) {
        return null;
      }
      if (page instanceof HOTLeafPage leaf) {
        if (leaf.getEntryCount() == 0) {
          return null;
        }
        return leaf.getFirstKey();
      }
      if (!(page instanceof HOTIndirectPage indirect) || indirect.getNumChildren() == 0) {
        return null;
      }
      cur = indirect.getChildReference(0);
      if (cur == null) {
        return null;
      }
    }
    return null;
  }

  /**
   * The last (lex-greatest) key of the subtree at {@code ref}, by rightmost descent — the true
   * maximum when the subtree is internally ordered, which is what the propagation boundary check
   * needs (a genuinely disordered subtree is the detector's to flag, not this walk's).
   */
  private byte @Nullable [] lastKeyOfSubtree(@Nullable PageReference ref) {
    if (ref == null) {
      return null;
    }
    PageReference cur = ref;
    for (int depth = 0; depth <= MAX_PATH_DEPTH; depth++) {
      final Page page = resolveHOTPageForTraversal(cur);
      if (page == null) {
        return null;
      }
      if (page instanceof HOTLeafPage leaf) {
        final int n = leaf.getEntryCount();
        return n == 0
            ? null
            : leaf.getKey(n - 1);
      }
      if (!(page instanceof HOTIndirectPage indirect) || indirect.getNumChildren() == 0) {
        return null;
      }
      cur = indirect.getChildReference(indirect.getNumChildren() - 1);
      if (cur == null) {
        return null;
      }
    }
    return null;
  }

  /**
   * Multi-entry-leaf stranding guard ([[hot-multientry-leaf-quirks]] #1). Returns {@code true} iff
   * folding the new key {@code newKey} into {@code oldNode} — producing the candidate {@code
   * newNode} where {@code newKey} routes to a freshly created single-key child — would re-route an
   * EXISTING key to that child without migrating it (a cross-leaf duplicate / I6 misroute).
   *
   * <p>
   * The faithful HOT port assumes the affected subtree is one-sided on the split bit (Binna's
   * single-TID leaves trivially satisfy this). Sirix's multi-entry leaves can straddle it, so a
   * sibling subtree may already hold keys captured by the new child's partial. PEXT routing is
   * equality-/most-specific-preferred, so the new child silently steals them. On a detected strand
   * the caller rejects that unpublished candidate and delegates to the common complete-frontier
   * splice.
   */
  final boolean branchAddStrandsExisting(HOTIndirectPage oldNode, HOTIndirectPage newNode, byte[] newKey) {
    final int newSlot = newNode.findChildIndex(newKey);
    if (newSlot < 0) {
      return false;
    }
    final long endpointProof = classifyWholeNodeOneSidedOnAddedBit(oldNode, newNode, newSlot, newKey);
    if (wholeNodeProofStatus(endpointProof) == WHOLE_NODE_PROOF_OPPOSITE) {
      return false;
    }
    try {
      return existingKeyRoutesToSlot(oldNode, newNode, newSlot, newKey);
    } catch (MutationTraversalRefusal refusal) {
      // Failure-path-only structural evidence for diagnosing a bounded traversal refusal. The
      // packed proof contains enum-like integers and a bit position, never key/value bytes.
      refusal.addSuppressed(new IllegalStateException("whole-node endpoint proof status="
          + wholeNodeProofStatus(endpointProof) + " beta=" + wholeNodeProofBeta(endpointProof) + " first="
          + wholeNodeProofFirstRelation(endpointProof) + " last=" + wholeNodeProofLastRelation(endpointProof)));
      throw refusal;
    }
  }

  /**
   * Prove that a one-bit whole-node branch cannot strand an existing key, using only the old
   * subtree's physical extrema.
   *
   * <p>
   * The proof is intentionally restricted to the exact flattened-BiNode encoding. The candidate adds
   * one bit {@code beta} above every old bit and retains the complete old node on the side opposite
   * {@code newKey}. Its fresh slot consequently selects precisely the old keys on {@code newKey}'s
   * beta side. Existing valid HOT subtrees are globally ordered (I8/I12), so if both physical extrema
   * share {@code newKey}'s prefix before beta and differ from it exactly at beta, every key in the
   * closed subtree range has the opposite beta value. There can be no false negative: no physical key
   * can route to the new slot.
   *
   * <p>
   * The candidate shape, sparse partials, child-reference identity/order, and both endpoint paths are
   * validated before accepting the proof. A short key, equal/zero-padding-ambiguous key, prefix
   * mismatch, mixed endpoint, closed/null/unsupported page, cycle, or excessive depth returns
   * uncertainty and falls through to the ordinary shared-budget exact scan. The successful path is
   * allocation-free and resolves at most two root-to-leaf paths, independent of index size.
   * </p>
   */
  private long classifyWholeNodeOneSidedOnAddedBit(final HOTIndirectPage oldNode, final HOTIndirectPage newNode,
      final int newSlot, final byte[] newKey) {
    final int[] oldBits = HOTIncrementalInsert.discriminativeBits(oldNode);
    final int[] newBits = HOTIncrementalInsert.discriminativeBits(newNode);
    if (!isExactWholeNodeSingleBitBranch(oldNode, newNode, newSlot, newKey, oldBits, newBits)) {
      return packWholeNodeProof(WHOLE_NODE_PROOF_NOT_APPLICABLE, -1, EXTREME_RELATION_UNCERTAIN,
          EXTREME_RELATION_UNCERTAIN);
    }
    final int beta = newBits[0];
    final int firstRelation = extremeRelationToKeyAtBit(oldNode, false, newKey, beta);
    final int lastRelation = extremeRelationToKeyAtBit(oldNode, true, newKey, beta);
    final int status;
    if (firstRelation == EXTREME_RELATION_OPPOSITE_AT_BETA && lastRelation == EXTREME_RELATION_OPPOSITE_AT_BETA) {
      status = WHOLE_NODE_PROOF_OPPOSITE;
    } else if (firstRelation == EXTREME_RELATION_SAME_SIDE_AT_BETA
        || lastRelation == EXTREME_RELATION_SAME_SIDE_AT_BETA) {
      status = WHOLE_NODE_PROOF_NOT_ONE_SIDED;
    } else {
      status = WHOLE_NODE_PROOF_UNCERTAIN;
    }
    return packWholeNodeProof(status, beta, firstRelation, lastRelation);
  }

  /** Package-private white-box seam for the bounded whole-node proof. */
  final boolean wholeNodeOneSidedOnAddedBit(final HOTIndirectPage oldNode, final HOTIndirectPage newNode,
      final int newSlot, final byte[] newKey) {
    return wholeNodeProofStatus(
        classifyWholeNodeOneSidedOnAddedBit(oldNode, newNode, newSlot, newKey)) == WHOLE_NODE_PROOF_OPPOSITE;
  }

  /**
   * Validate the exact flattened-BiNode encoding produced when one new bit above the complete old
   * node separates {@code newKey} from every old child. No inferred child mapping is trusted.
   */
  private static boolean isExactWholeNodeSingleBitBranch(final HOTIndirectPage oldNode, final HOTIndirectPage newNode,
      final int newSlot, final byte[] newKey, final int[] oldBits, final int[] newBits) {
    final int oldChildCount = oldNode.getNumChildren();
    if (oldChildCount < 1 || oldChildCount >= HOTIndirectPage.MAX_NODE_ENTRIES
        || newNode.getNumChildren() != oldChildCount + 1 || oldBits.length < 1 || oldBits.length >= Integer.SIZE
        || newBits.length != oldBits.length + 1 || newBits[0] >= oldBits[0] || !hasStrictlyAscendingPartials(oldNode)
        || !hasStrictlyAscendingPartials(newNode) || oldNode.getPartialKey(0) != 0 || newNode.getPartialKey(0) != 0
        || !partialsFitDiscriminativeWidth(oldNode, oldBits) || !partialsFitDiscriminativeWidth(newNode, newBits)) {
      return false;
    }
    for (int i = 0; i < oldBits.length; i++) {
      if (newBits[i + 1] != oldBits[i]) {
        return false;
      }
    }

    final int beta = newBits[0];
    final boolean newBitValue = HOTBulkBuilder.bitAt(newKey, beta);
    final int betaMask = 1 << oldBits.length;
    final int expectedNewSlot = newBitValue
        ? oldChildCount
        : 0;
    if (newSlot != expectedNewSlot || newNode.findChildIndex(newKey) != newSlot
        || newNode.getPartialKey(newSlot) != (newBitValue
            ? betaMask
            : 0)) {
      return false;
    }
    final PageReference insertedReference = newNode.getChildReference(newSlot);
    if (insertedReference == null) {
      return false;
    }
    for (int oldSlot = 0; oldSlot < oldChildCount; oldSlot++) {
      final int candidateSlot = oldSlot + (newBitValue
          ? 0
          : 1);
      final PageReference oldReference = oldNode.getChildReference(oldSlot);
      if (oldReference == null || newNode.getChildReference(candidateSlot) != oldReference
          || insertedReference == oldReference) {
        return false;
      }
      final int expectedPartial = newBitValue
          ? oldNode.getPartialKey(oldSlot)
          : betaMask | oldNode.getPartialKey(oldSlot);
      if (newNode.getPartialKey(candidateSlot) != expectedPartial) {
        return false;
      }
    }
    return true;
  }

  /**
   * Relate one physical subtree extreme to {@code key} at {@code beta} without materializing the
   * extreme key. A non-extreme/cyclic/deep/malformed path is uncertainty, never a proof.
   */
  private int extremeRelationToKeyAtBit(final Page root, final boolean last, final byte[] key, final int beta) {
    return extremeRelationToKeyAtBit(root, last, key, beta, 0);
  }

  /**
   * Variant whose {@code initialDepth} accounts for already-traversed containing nodes. This keeps
   * child-local endpoint probes under the same root-to-leaf depth ceiling as the exact walker.
   */
  private int extremeRelationToKeyAtBit(final Page root, final boolean last, final byte[] key, final int beta,
      final int initialDepth) {
    if (beta < 0 || key.length <= (beta >>> 3)) {
      return EXTREME_RELATION_UNCERTAIN;
    }
    Page page = root;
    for (int depth = initialDepth; depth <= MAX_PATH_DEPTH; depth++) {
      if (page == null || page.isClosed()) {
        return EXTREME_RELATION_UNCERTAIN;
      }
      if (page instanceof HOTLeafPage leaf) {
        final int entryCount = leaf.getEntryCount();
        if (entryCount == 0) {
          return EXTREME_RELATION_UNCERTAIN;
        }
        final int entryIndex = last
            ? entryCount - 1
            : 0;
        if (leaf.getKeyLength(entryIndex) <= (beta >>> 3) || leaf.compareKeyWithBound(entryIndex, key) == 0) {
          return EXTREME_RELATION_UNCERTAIN;
        }
        final int msdb;
        try {
          msdb = leaf.msdbWith(entryIndex, key);
        } catch (IllegalStateException equalUnderZeroPadding) {
          return EXTREME_RELATION_UNCERTAIN;
        }
        if (msdb < beta) {
          return EXTREME_RELATION_UNCERTAIN;
        }
        return msdb == beta
            ? EXTREME_RELATION_OPPOSITE_AT_BETA
            : EXTREME_RELATION_SAME_SIDE_AT_BETA;
      }
      if (!(page instanceof HOTIndirectPage indirect) || indirect.getNumChildren() == 0 || depth == MAX_PATH_DEPTH) {
        return EXTREME_RELATION_UNCERTAIN;
      }
      final int childSlot = last
          ? indirect.getNumChildren() - 1
          : 0;
      final PageReference childReference = indirect.getChildReference(childSlot);
      if (childReference == null) {
        return EXTREME_RELATION_UNCERTAIN;
      }
      page = resolveHOTPageForTraversal(childReference);
    }
    return EXTREME_RELATION_UNCERTAIN;
  }

  static long packWholeNodeProof(final int status, final int beta, final int firstRelation, final int lastRelation) {
    return (status & 0xFFL) | (Integer.toUnsignedLong(beta + 1) << 8) | ((firstRelation & 0xFFL) << 40)
        | ((lastRelation & 0xFFL) << 48);
  }

  private static int wholeNodeProofStatus(final long proof) {
    return (int) (proof & 0xFFL);
  }

  static int wholeNodeProofBeta(final long proof) {
    return (int) ((proof >>> 8) & 0xFFFF_FFFFL) - 1;
  }

  private static int wholeNodeProofFirstRelation(final long proof) {
    return (int) ((proof >>> 40) & 0xFFL);
  }

  private static int wholeNodeProofLastRelation(final long proof) {
    return (int) ((proof >>> 48) & 0xFFL);
  }

  /**
   * Stranding check for adding a combo child to {@code oldNode}. Returns {@code true} iff some
   * physical key currently stored under {@code oldNode} (other than {@code excludeKey}) would, on the
   * candidate {@code newNode}, route to {@code newSlot} — the freshly added child that holds only the
   * new key. Such a key would be silently re-routed to the new child without being migrated into it
   * (PEXT routing is equality-/most-specific-preferred), i.e. it would become a cross-leaf duplicate.
   * Resolves pages writer-side ({@link #resolveHOTPageForTraversal}) so it sees the in-progress (TIL)
   * subtree. Short-circuits on the first captured key. Before inspecting a key, an allocation-free
   * endpoint proof discharges any retained child whose complete ordered range is on the opposite side
   * of the candidate's one newly inserted discriminative bit. The remaining exact-scan source must
   * fit one bounded HOT block; a larger or invalid source is a fail-closed transaction refusal rather
   * than an unbounded foreground scan.
   */
  final boolean existingKeyRoutesToSlot(HOTIndirectPage oldNode, HOTIndirectPage newNode, int newSlot,
      byte[] excludeKey) {
    if (newSlot < 0 || newSlot >= newNode.getNumChildren()) {
      throw refuseMutationTraversal("existing-key routing-to-new-slot", "invalid candidate slot " + newSlot);
    }
    final RebuildFootprint footprint = requireBoundedExistingKeyRoutingTraversal(oldNode, newNode, newSlot, excludeKey);
    // The preflight's fixed identity array is also the scan plan. Every relevant page was resolved
    // exactly once above; scanning its leaf entries directly avoids a second tree walk/resolution
    // pass and needs no per-mutation collection.
    for (int pageIndex = 0; pageIndex < footprint.pages; pageIndex++) {
      if (footprint.visitedPages[pageIndex] instanceof HOTLeafPage leaf) {
        final int entryCount = leaf.getEntryCount();
        for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
          final byte[] key = leaf.getKey(entryIndex);
          if (key == null) {
            throw refuseMutationTraversal("existing-key routing-to-new-slot",
                "unreadable key at leaf " + leaf.getPageKey() + " slot " + entryIndex);
          }
          if (!Arrays.equals(key, excludeKey) && newNode.findChildIndex(key) == newSlot) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Prove that the complete set of old child subtrees which the candidate partial can steal fits the
   * mutation-traversal budget. The parent node itself is deliberately not charged: the subsequent
   * scan never descends through it, and charging unrelated structure would consume budget without
   * bounding any work the exact key scan performs.
   *
   * <p>
   * The scratch is reset once for the whole candidate union, not once per child. Consequently a
   * candidate with several possible source children still has one aggregate hard ceiling; it cannot
   * multiply the budget by the node fanout.
   * </p>
   */
  private RebuildFootprint requireBoundedExistingKeyRoutingTraversal(final HOTIndirectPage oldNode,
      final HOTIndirectPage newNode, final int newSlot, final byte[] newKey) {
    final int oldChildCount = oldNode.getNumChildren();
    final int newChildCount = newNode.getNumChildren();
    if (oldChildCount < 1 || newChildCount != oldChildCount + 1) {
      throw refuseMutationTraversal("existing-key routing-to-new-slot",
          "candidate child-count mismatch old=" + oldChildCount + " new=" + newChildCount);
    }
    final PageReference insertedReference = newNode.getChildReference(newSlot);
    if (insertedReference == null) {
      throw refuseMutationTraversal("existing-key routing-to-new-slot", "null inserted child reference");
    }
    // Every production branch-add primitive is pure: it retains the old references in order and
    // inserts exactly one new reference. Validate that premise before using old-slot partials to
    // prune; a future primitive that replaces/reorders children must provide its own mapping rather
    // than silently inheriting this proof.
    int oldSlot = 0;
    for (int candidateSlot = 0; candidateSlot < newChildCount; candidateSlot++) {
      if (candidateSlot == newSlot) {
        continue;
      }
      if (newNode.getChildReference(candidateSlot) != oldNode.getChildReference(oldSlot)) {
        throw refuseMutationTraversal("existing-key routing-to-new-slot",
            "candidate does not preserve old child " + oldSlot + " at slot " + candidateSlot);
      }
      if (insertedReference == oldNode.getChildReference(oldSlot)) {
        throw refuseMutationTraversal("existing-key routing-to-new-slot",
            "inserted child aliases old child " + oldSlot);
      }
      oldSlot++;
    }

    final RebuildFootprint footprint = rebuildFootprintScratch;
    footprint.reset();
    final int[] oldBits = HOTIncrementalInsert.discriminativeBits(oldNode);
    final int[] newBits = HOTIncrementalInsert.discriminativeBits(newNode);
    final boolean partialOrderProven = hasStrictlyAscendingPartials(oldNode) && hasStrictlyAscendingPartials(newNode)
        && oldNode.getPartialKey(0) == 0 && newNode.getPartialKey(0) == 0
        && partialsFitDiscriminativeWidth(oldNode, oldBits) && partialsFitDiscriminativeWidth(newNode, newBits);
    final int addedBitColumn = partialOrderProven
        ? exactSingleAddedBitColumn(oldNode, newNode, newSlot, newKey, oldBits, newBits)
        : -1;
    final boolean sameMaskInsertionProven = partialOrderProven && addedBitColumn < 0
        && exactSameMaskChildInsertion(oldNode, newNode, newSlot, newKey, oldBits, newBits);
    int feasibleChildCount = 0;
    for (int i = 0; i < oldChildCount; i++) {
      if (partialOrderProven && !oldChildMayRouteToNewSlot(oldNode, i, oldBits, newNode, newSlot, newBits)) {
        continue;
      }
      feasibleChildCount++;
      final int childStartPages = footprint.pages;
      final int childStartLeaves = footprint.leaves;
      final int childStartEntries = footprint.entries;
      // Preserve the traversal's negative-path hard cap even for the optional endpoint proof.
      // Resolving a child after the exact-scan plan has filled the identity array would make the
      // refusal path perform one more page read than the bounded walker permits.
      if (footprint.pages >= MAX_BOUNDED_REBUILD_PAGES) {
        footprint.reject(RebuildFootprintStatus.PAGE_LIMIT);
        throw refuseMutationTraversal("existing-key routing-to-new-slot",
            existingKeyRoutingRefusalDetail(footprint, feasibleChildCount, i, oldChildCount, newSlot, oldBits, newBits,
                oldNode, newNode, childStartPages, childStartLeaves, childStartEntries));
      }
      final PageReference childReference = oldNode.getChildReference(i);
      if (childReference == null) {
        throw refuseMutationTraversal("existing-key routing-to-new-slot",
            "null candidate child reference at slot " + i);
      }
      final Page child = resolveHOTPageForTraversal(childReference);
      if (child == null) {
        throw refuseMutationTraversal("existing-key routing-to-new-slot", "unresolvable candidate child at slot " + i);
      }
      if (addedBitColumn >= 0
          && oldChildRangeOppositeOnAddedBit(child, i, newNode, newSlot, newKey, newBits, addedBitColumn)) {
        // The candidate's retained child still owns every key in this physical range. Its two
        // extrema differ from K exactly at beta; the existing valid subtree's inductively maintained
        // I8/I12 range ordering therefore makes the complete child beta-opposite. Depending on K's
        // beta value, either K's partial cannot match those keys or the retained child's beta-bearing
        // partial shadows it. No entry scan is needed; an arbitrarily malformed interior is not a
        // premise this local proof attempts to certify.
        continue;
      }
      if (sameMaskInsertionProven
          && oldChildRangeCannotMatchFreshPartial(child, i, newNode, newSlot, newKey, newBits)) {
        // The candidate added a sparse combination over the existing mask. The fresh partial
        // requires a one-bit absent from this retained partial, while both physical extrema prove
        // the complete retained range has zero at that bit. It therefore cannot match the fresh
        // combination, regardless of suffix bits, and needs no entry scan.
        continue;
      }
      measureBoundedRebuildPage(child, traversalPageResolver, footprint, 0);
      // Consumers splice against the candidate node, whose slots after the fresh insertion are
      // shifted by one. Retain that physical owner coordinate alongside every planned page.
      final int candidateOwnerSlot = i < newSlot
          ? i
          : i + 1;
      for (int pageIndex = childStartPages; pageIndex < footprint.pages; pageIndex++) {
        footprint.visitedPageOwnerSlots[pageIndex] = candidateOwnerSlot;
      }
      if (!footprint.withinBudget()) {
        throw refuseMutationTraversal("existing-key routing-to-new-slot",
            existingKeyRoutingRefusalDetail(footprint, feasibleChildCount, i, oldChildCount, newSlot, oldBits, newBits,
                oldNode, newNode, childStartPages, childStartLeaves, childStartEntries));
      }
    }
    return footprint;
  }

  /**
   * Validate the exact structural shape in which {@code newNode} inserts one discriminative bit and
   * one child while retaining every old child in order. Returns the inserted bit's column in
   * {@code newBits}, or {@code -1} when any bit/partial/routing premise is uncertain.
   *
   * <p>
   * This is intentionally stricter than merely observing {@code newBits.length == oldBits.length
   * + 1}. Removing the inserted column from every retained candidate partial must reproduce the old
   * partial byte-for-byte, and the inserted child's partial must encode {@code newKey}'s value at the
   * new bit. Consequently the endpoint proof below cannot bless an unrelated or malformed branch-add
   * primitive. The reference-preservation premise is validated by the caller before this method.
   * </p>
   */
  private static int exactSingleAddedBitColumn(final HOTIndirectPage oldNode, final HOTIndirectPage newNode,
      final int newSlot, final byte[] newKey, final int[] oldBits, final int[] newBits) {
    if (newBits.length != oldBits.length + 1 || newBits.length > Integer.SIZE || !strictlyAscendingBits(oldBits)
        || !strictlyAscendingBits(newBits) || newNode.findChildIndex(newKey) != newSlot) {
      return -1;
    }

    int addedColumn = -1;
    int oldColumn = 0;
    for (int newColumn = 0; newColumn < newBits.length; newColumn++) {
      if (oldColumn < oldBits.length && newBits[newColumn] == oldBits[oldColumn]) {
        oldColumn++;
      } else if (addedColumn < 0) {
        addedColumn = newColumn;
      } else {
        return -1;
      }
    }
    if (addedColumn < 0 || oldColumn != oldBits.length) {
      return -1;
    }

    final int addedBit = newBits[addedColumn];
    final int packedBitIndex = newBits.length - 1 - addedColumn;
    final int addedBitMask = 1 << packedBitIndex;
    final boolean newKeyBit = HOTBulkBuilder.bitAt(newKey, addedBit);
    if (((newNode.getPartialKey(newSlot) & addedBitMask) != 0) != newKeyBit) {
      return -1;
    }

    for (int oldSlot = 0; oldSlot < oldNode.getNumChildren(); oldSlot++) {
      final int candidateSlot = oldSlot < newSlot
          ? oldSlot
          : oldSlot + 1;
      if (removePackedPartialBit(newNode.getPartialKey(candidateSlot),
          packedBitIndex) != oldNode.getPartialKey(oldSlot)) {
        return -1;
      }
    }
    return addedColumn;
  }

  /**
   * Validate a pure child/partial insertion over an unchanged discriminative-bit mask. This is the
   * {@link HOTIncrementalInsert#addChildAtCombination} shape: every retained reference and partial
   * stays in order and the new key routes to the one fresh slot. Reference preservation and unique
   * insertion are validated by the caller before this method.
   */
  private static boolean exactSameMaskChildInsertion(final HOTIndirectPage oldNode, final HOTIndirectPage newNode,
      final int newSlot, final byte[] newKey, final int[] oldBits, final int[] newBits) {
    if (oldBits.length > Integer.SIZE || !strictlyAscendingBits(oldBits) || !strictlyAscendingBits(newBits)
        || !Arrays.equals(oldBits, newBits) || !hasStrictlyAscendingPartials(oldNode)
        || !hasStrictlyAscendingPartials(newNode) || newNode.findChildIndex(newKey) != newSlot) {
      return false;
    }
    final int freshPartial = newNode.getPartialKey(newSlot);
    for (int oldSlot = 0; oldSlot < oldNode.getNumChildren(); oldSlot++) {
      final int candidateSlot = oldSlot < newSlot
          ? oldSlot
          : oldSlot + 1;
      final int oldPartial = oldNode.getPartialKey(oldSlot);
      if (newNode.getPartialKey(candidateSlot) != oldPartial || freshPartial == oldPartial) {
        return false;
      }
    }
    return true;
  }

  private static boolean strictlyAscendingBits(final int[] bits) {
    for (int i = 1; i < bits.length; i++) {
      if (bits[i] <= bits[i - 1]) {
        return false;
      }
    }
    return true;
  }

  /** Reject malformed sparse partials that set bits outside the node's packed mask width. */
  private static boolean partialsFitDiscriminativeWidth(final HOTIndirectPage node, final int[] bits) {
    final int childCount = node.getNumChildren();
    final int[] partials = node.getPartialKeysRef();
    if (bits.length < 1 || bits.length > Integer.SIZE || partials == null || partials.length < childCount) {
      return false;
    }
    final int validBits = bits.length == Integer.SIZE
        ? -1
        : (1 << bits.length) - 1;
    for (int slot = 0; slot < childCount; slot++) {
      if ((partials[slot] & ~validBits) != 0) {
        return false;
      }
    }
    return true;
  }

  /** Remove one zero-based-from-LSB bit and compact every more-significant packed bit down by one. */
  private static int removePackedPartialBit(final int partial, final int packedBitIndex) {
    final int lowerMask = packedBitIndex == 0
        ? 0
        : -1 >>> (Integer.SIZE - packedBitIndex);
    final int upper = packedBitIndex == Integer.SIZE - 1
        ? 0
        : partial >>> (packedBitIndex + 1);
    return (upper << packedBitIndex) | (partial & lowerMask);
  }

  /**
   * Prove one retained old child's complete ordered key range is opposite {@code newKey} at the one
   * newly inserted discriminative bit.
   *
   * <p>
   * Both extrema must share K's prefix above beta and differ exactly at beta. Every key between them
   * therefore has that same opposite beta value. The candidate partials were already proven to be the
   * old partials with exactly beta inserted. For K(beta)=1, K's partial requires beta and cannot
   * match this child. For K(beta)=0, the retained beta=1 partial must follow K's partial and wins the
   * lookup's equality-/later-subset preference. Any ambiguity falls through to the bounded exact
   * scan.
   * </p>
   */
  private boolean oldChildRangeOppositeOnAddedBit(final Page child, final int oldSlot, final HOTIndirectPage newNode,
      final int newSlot, final byte[] newKey, final int[] newBits, final int addedBitColumn) {
    final int addedBit = newBits[addedBitColumn];
    if (extremeRelationToKeyAtBit(child, false, newKey, addedBit, 1) != EXTREME_RELATION_OPPOSITE_AT_BETA
        || extremeRelationToKeyAtBit(child, true, newKey, addedBit, 1) != EXTREME_RELATION_OPPOSITE_AT_BETA) {
      return false;
    }

    final int packedBitIndex = newBits.length - 1 - addedBitColumn;
    final int addedBitMask = 1 << packedBitIndex;
    final boolean newKeyBit = HOTBulkBuilder.bitAt(newKey, addedBit);
    final int retainedSlot = oldSlot < newSlot
        ? oldSlot
        : oldSlot + 1;
    final int retainedPartial = newNode.getPartialKey(retainedSlot);
    final int freshPartial = newNode.getPartialKey(newSlot);
    final boolean retainedBit = (retainedPartial & addedBitMask) != 0;
    if (retainedBit == newKeyBit) {
      return false;
    }
    // A sparse partial cannot encode a required zero. When K is on beta=0, correctness relies on the
    // retained beta=1 partial shadowing K's less-specific partial: it must be a strict superset of
    // the fresh partial and occupy a later slot under the lookup's later-subset preference.
    return newKeyBit || (retainedSlot > newSlot && (freshPartial & ~retainedPartial) == 0);
  }

  /**
   * Prove that one retained child cannot match a fresh sparse combination over the same bit mask. The
   * proof deliberately probes only one bit, keeping work at two bounded extreme paths per feasible
   * child. A required fresh bit absent from the retained partial is selected; when both physical
   * extrema differ from {@code newKey} exactly there, global HOT range ordering proves every key in
   * the child has zero at a bit the fresh partial requires to be one.
   */
  private boolean oldChildRangeCannotMatchFreshPartial(final Page child, final int oldSlot,
      final HOTIndirectPage newNode, final int newSlot, final byte[] newKey, final int[] bits) {
    final int retainedSlot = oldSlot < newSlot
        ? oldSlot
        : oldSlot + 1;
    final int freshOnlyBits = newNode.getPartialKey(newSlot) & ~newNode.getPartialKey(retainedSlot);
    if (freshOnlyBits == 0) {
      return false;
    }
    final int packedBitIndex = Integer.SIZE - 1 - Integer.numberOfLeadingZeros(freshOnlyBits);
    final int column = bits.length - 1 - packedBitIndex;
    if (column < 0 || !HOTBulkBuilder.bitAt(newKey, bits[column])) {
      return false;
    }
    final int beta = bits[column];
    return extremeRelationToKeyAtBit(child, false, newKey, beta, 1) == EXTREME_RELATION_OPPOSITE_AT_BETA
        && extremeRelationToKeyAtBit(child, true, newKey, beta, 1) == EXTREME_RELATION_OPPOSITE_AT_BETA;
  }

  /** Failure-path-only structural diagnostics; never includes an index key or value. */
  private static String existingKeyRoutingRefusalDetail(final RebuildFootprint footprint, final int feasibleChildCount,
      final int currentOldSlot, final int oldChildCount, final int newSlot, final int[] oldBits, final int[] newBits,
      final HOTIndirectPage oldNode, final HOTIndirectPage newNode, final int childStartPages,
      final int childStartLeaves, final int childStartEntries) {
    return footprint.summary() + " feasibleChildren=" + feasibleChildCount + " currentOldSlot=" + currentOldSlot + "/"
        + oldChildCount + " newSlot=" + newSlot + " oldDiscBits=" + oldBits.length + " newDiscBits=" + newBits.length
        + " childStartPages=" + childStartPages + " childStartLeaves=" + childStartLeaves + " childStartEntries="
        + childStartEntries + " oldBitPositions=" + Arrays.toString(oldBits) + " newBitPositions="
        + Arrays.toString(newBits) + " oldPartials="
        + Arrays.toString(Arrays.copyOf(oldNode.getPartialKeysRef(), oldNode.getNumChildren())) + " newPartials="
        + Arrays.toString(Arrays.copyOf(newNode.getPartialKeysRef(), newNode.getNumChildren()));
  }

  /**
   * Whether an existing key physically owned by {@code oldSlot} can possibly be selected by the
   * freshly added {@code newSlot}. This is a zero-allocation sparse-routing feasibility filter; the
   * surviving subtrees are still inspected key-for-key, so the final answer remains exact.
   *
   * <p>
   * For every physical key under old slot {@code i} that candidate-routes to {@code newSlot}, two
   * partials are subsets of its candidate-mask dense key: {@code i}'s old sparse partial (I5), after
   * translating its columns by absolute discriminative-bit position, and the new slot's partial (the
   * routing match being tested). Their union is therefore a set of mandatory 1-bits. If any candidate
   * partial after {@code newSlot} is already a subset of that union, it matches every such dense key
   * and shadows {@code newSlot}; likewise, if any translated old partial after {@code i} is already a
   * subset, every such key would have routed to that later old slot and therefore cannot be
   * physically owned by {@code i}. Either contradiction makes the child impossible as a source.
   * Otherwise it remains conservatively feasible and is scanned. Equality preference needs no
   * separate case once both partial arrays are proven strictly unsigned-ascending: an exact partial
   * is a subset match, and no numerically later partial can be a subset of that exact dense key. Bits
   * not proved mandatory are never guessed, and a mask translation that cannot map every old absolute
   * bit fails open.
   * </p>
   */
  private static boolean oldChildMayRouteToNewSlot(final HOTIndirectPage oldNode, final int oldSlot,
      final int[] oldBits, final HOTIndirectPage newNode, final int newSlot, final int[] newBits) {
    final long translatedOldPartial = translatePartialToCandidateMask(oldNode.getPartialKey(oldSlot), oldBits, newBits);
    if (translatedOldPartial < 0) {
      return true;
    }
    final int mandatoryBits = (int) translatedOldPartial | newNode.getPartialKey(newSlot);
    for (int i = oldSlot + 1; i < oldNode.getNumChildren(); i++) {
      final long translatedLaterPartial = translatePartialToCandidateMask(oldNode.getPartialKey(i), oldBits, newBits);
      if (translatedLaterPartial < 0) {
        return true;
      }
      if ((((int) translatedLaterPartial) & ~mandatoryBits) == 0) {
        return false;
      }
    }
    for (int i = newSlot + 1; i < newNode.getNumChildren(); i++) {
      if ((newNode.getPartialKey(i) & ~mandatoryBits) == 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Translate an old sparse partial by absolute discriminative-bit position; {@code -1} is fail-open.
   */
  private static long translatePartialToCandidateMask(final int oldPartial, final int[] oldBits, final int[] newBits) {
    int translatedOldPartial = 0;
    int newColumn = 0;
    for (int oldColumn = 0; oldColumn < oldBits.length; oldColumn++) {
      final int absoluteBit = oldBits[oldColumn];
      while (newColumn < newBits.length && newBits[newColumn] < absoluteBit) {
        newColumn++;
      }
      if (newColumn >= newBits.length || newBits[newColumn] != absoluteBit) {
        return -1;
      }
      if ((oldPartial & (1 << (oldBits.length - 1 - oldColumn))) != 0) {
        translatedOldPartial |= 1 << (newBits.length - 1 - newColumn);
      }
      newColumn++;
    }
    return Integer.toUnsignedLong(translatedOldPartial);
  }

  /**
   * The shadow proof relies on the same strict unsigned partial order as PEXT lookup invariant I7.
   */
  private static boolean hasStrictlyAscendingPartials(final HOTIndirectPage node) {
    final int childCount = node.getNumChildren();
    final int[] partials = node.getPartialKeysRef();
    if (childCount < 1 || partials == null || partials.length < childCount) {
      return false;
    }
    for (int i = 1; i < childCount; i++) {
      if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Cheap single-node structural check covering the O(children)-class invariants that a combo-add /
   * fold can break under multi-value leaves: I3/I7 (stored partials unique and strictly ascending),
   * I4 (smallest stored partial must be 0 — Binna's "first mask always zero"), I8 (children ordered
   * by ascending subtree first-key), I11 (child discriminative bits below the parent), and I12
   * (consecutive children's key RANGES must not interleave — first-key order alone misses a preceding
   * sibling whose subtree spans past the next child's start). Returns {@code true} on the first
   * violation. The expensive I5 constancy walk is intentionally excluded here; the bounded full
   * detector covers it, while oversized scopes rely on the verified incremental primitives plus this
   * mandatory local guard. These checks are O(children) / O(children×height). Used as a
   * pre-publication combo-add guard (the ordering complement to the routing-only
   * {@link #branchAddStrandsExisting}) and by the fail-closed post-publication validator. It is
   * sufficient as a single-node scan because a fold leaves every existing child subtree untouched.
   */
  private boolean nodeStructurallyMalformed(HOTIndirectPage candidate) {
    final int n = candidate.getNumChildren();
    final int[] partials = candidate.getPartialKeysRef();
    if (partials != null && partials.length >= n && n > 0) {
      int minPartial = partials[0];
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          return true; // I3/I7: strict ascent also proves uniqueness
        }
        if (Integer.compareUnsigned(partials[i], minPartial) < 0) {
          minPartial = partials[i];
        }
      }
      if (minPartial != 0) {
        return true; // I4: smallest stored partial must be 0
      }
    }
    byte[] previousFirstKey = null;
    byte[] previousLastKey = null;
    final int parentMostSignificantBit = candidate.getMostSignificantBitIndex();
    for (int i = 0; i < n; i++) {
      final PageReference childRef = candidate.getChildReference(i);
      if (childRef == null) {
        return true;
      }
      final Page childPage = resolveHOTPageForTraversal(childRef);
      if (childPage == null) {
        return true;
      }
      if (parentMostSignificantBit >= 0 && childPage instanceof HOTIndirectPage childIndirect) {
        final int childMostSignificantBit = childIndirect.getMostSignificantBitIndex();
        if (childMostSignificantBit >= 0 && childMostSignificantBit <= parentMostSignificantBit) {
          return true; // I11: child bits must be strictly less significant than the parent's
        }
      }
      final byte[] firstKey = firstKeyOfSubtree(childRef);
      if (firstKey == null) {
        continue;
      }
      if (previousFirstKey != null && Arrays.compareUnsigned(previousFirstKey, firstKey) >= 0) {
        return true; // I8: children not ordered by first-key
      }
      if (previousLastKey != null && Arrays.compareUnsigned(previousLastKey, firstKey) >= 0) {
        return true; // I12: the preceding sibling's range reaches into this child's
      }
      previousFirstKey = firstKey;
      if (i < n - 1) {
        // Only a PRECEDING sibling's maximum is ever compared, so the last child's rightmost
        // descent (O(height) page resolutions plus a key materialization) would be pure waste on
        // a guard that runs at every combo/fold site and every level of the path probe.
        final byte[] lastKey = lastKeyOfSubtree(childRef);
        if (lastKey != null) {
          previousLastKey = lastKey;
        }
      }
    }
    return false;
  }

  /**
   * Returns {@code true} iff some physical key under the subtree at {@code ref} (other than
   * {@code excludeKey}) has bit {@code beta} (MSB-first absolute position) equal to {@code
   * bitValue}. Used by the BiNode-wrap stranding guards: wrapping a whole subtree on one side of
   * {@code beta} strands any key inside it that sits on the opposite ({@code bitValue}) side.
   */
  final boolean subtreeHasKeyWithBit(@Nullable PageReference ref, int beta, int bitValue, byte[] excludeKey) {
    if (beta < 0 || bitValue < 0 || bitValue > 1) {
      throw refuseMutationTraversal("subtree beta-bit stranding", "invalid beta/bitValue " + beta + '/' + bitValue);
    }
    if (ref == null) {
      throw refuseMutationTraversal("subtree beta-bit stranding", "null root reference");
    }
    final Page page = resolveHOTPageForTraversal(ref);
    if (page == null) {
      throw refuseMutationTraversal("subtree beta-bit stranding", "unresolvable root reference");
    }
    // Both whole-subtree branch sites ask whether an old subtree contains K's value at beta. If
    // its physical extrema share K's prefix above beta and differ exactly at beta, global HOT
    // range ordering proves every key between them is on the opposite side. Two root-to-leaf
    // probes therefore discharge arbitrarily large healthy subtrees without weakening the fixed
    // exact-scan budget or turning a rare structural insert into a whole-index walk.
    if (excludeKey != null && (beta >>> 3) < excludeKey.length && (HOTBulkBuilder.bitAt(excludeKey, beta)
        ? 1
        : 0) == bitValue
        && extremeRelationToKeyAtBit(page, false, excludeKey, beta) == EXTREME_RELATION_OPPOSITE_AT_BETA
        && extremeRelationToKeyAtBit(page, true, excludeKey, beta) == EXTREME_RELATION_OPPOSITE_AT_BETA) {
      return false;
    }
    requireBoundedMutationTraversal(page, "subtree beta-bit stranding");
    return subtreeHasKeyWithBitBounded(page, beta, bitValue, excludeKey, 0);
  }

  /** Recursive half of {@link #subtreeHasKeyWithBit}; its source was preflighted by the wrapper. */
  private boolean subtreeHasKeyWithBitBounded(final Page page, final int beta, final int bitValue,
      final byte[] excludeKey, final int depth) {
    if (depth > MAX_PATH_DEPTH) {
      throw refuseMutationTraversal("subtree beta-bit stranding", "depth exceeded " + MAX_PATH_DEPTH);
    }
    if (page.isClosed()) {
      throw refuseMutationTraversal("subtree beta-bit stranding", "closed page during scan");
    }
    if (page instanceof HOTLeafPage leaf) {
      final int n = leaf.getEntryCount();
      final int bytePos = beta / 8;
      final int mask = 1 << (7 - (beta % 8));
      for (int i = 0; i < n; i++) {
        final byte[] k = leaf.getKey(i);
        if (k == null) {
          throw refuseMutationTraversal("subtree beta-bit stranding",
              "unreadable key at leaf " + leaf.getPageKey() + " slot " + i);
        }
        if (Arrays.equals(k, excludeKey)) {
          continue;
        }
        final int bit = (bytePos < k.length) && ((k[bytePos] & mask) != 0)
            ? 1
            : 0;
        if (bit == bitValue) {
          return true;
        }
      }
      return false;
    }
    if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference childReference = indirect.getChildReference(i);
        if (childReference == null) {
          throw refuseMutationTraversal("subtree beta-bit stranding", "null child reference during scan");
        }
        final Page child = resolveHOTPageForTraversal(childReference);
        if (child == null) {
          throw refuseMutationTraversal("subtree beta-bit stranding", "unresolvable child during scan");
        }
        if (subtreeHasKeyWithBitBounded(child, beta, bitValue, excludeKey, depth + 1)) {
          return true;
        }
      }
      return false;
    }
    throw refuseMutationTraversal("subtree beta-bit stranding", "unsupported page " + page.getClass().getName());
  }

  /**
   * I8 (children-sorted-by-firstkey) safety predicate for sub-inserting {@code K} into the
   * {@code affected} subtree at {@code insertDepth}. Direction 1 sub-insert
   * ({@code docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md} §11) is routing-correct by the descent
   * tautology -- but if K becomes the new {@code firstKey} of {@code affected}, that change
   * PROPAGATES up the spine through every ancestor where {@code affected}'s slot at that level is 0
   * (the leftmost child). At each such ancestor, I8 demands {@code K} also fits between the left and
   * right siblings' first keys. An MSDB-closure gap in the ancestor's mask can put K outside that
   * interval -- a real failure mode (a regression surfaced by HOTVersionedLeafStressTest's
   * interleavedInsertDeleteMultiRev).
   *
   * <p>
   * Returns {@code true} iff sub-inserting K is safe at every affected level. The cost is O(height)
   * per check (leftmost-walk per inspected sibling, capped at {@link #MAX_PATH_DEPTH}).
   *
   * <p>
   * <b>Short-circuit.</b> When {@code K >= affected.firstKey}, K cannot become the new leftmost key
   * of {@code affected}, so no firstKey changes on the spine -- I8 is trivially preserved.
   */
  private boolean isDirectionOneI8Safe(LeafNavigationResult navResult, int insertDepth, int affectedIdx,
      byte[] keySlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int[] childSlots = navResult.pathChildIndices();
    final HOTIndirectPage dStar = pathNodes[insertDepth];

    final byte[] affectedFirstKey = firstKeyOfSubtree(dStar.getChildReference(affectedIdx));
    if (affectedFirstKey == null) {
      return false; // defensive: unresolvable subtree
    }
    if (Arrays.compareUnsigned(keySlice, affectedFirstKey) >= 0) {
      return true; // K >= affected.firstKey: no firstKey change.
    }

    // K < affected.firstKey -> K becomes new firstKey of affected. Check I8 at d*.
    if (!isRangeStartSafeAtSlot(dStar, affectedIdx, keySlice)) {
      return false;
    }
    // K's firstKey-change propagates upward as long as the current slot is 0 (leftmost).
    int currentSlot = affectedIdx;
    for (int depth = insertDepth - 1; depth >= 0 && currentSlot == 0; depth--) {
      final int parentSlot = childSlots[depth];
      if (!isRangeStartSafeAtSlot(pathNodes[depth], parentSlot, keySlice)) {
        return false;
      }
      currentSlot = parentSlot;
    }
    return true;
  }

  /**
   * Check the ordered-range boundary around {@code slot} of {@code node} given {@code keySlice} as
   * the slot's new (smaller) first key. The preceding subtree's <em>last</em> key must stay below K
   * (I12, which strictly implies the first-key-only I8 check) and K must stay below the following
   * subtree's first key.
   */
  private boolean isRangeStartSafeAtSlot(HOTIndirectPage node, int slot, byte[] keySlice) {
    final int n = node.getNumChildren();
    if (slot > 0) {
      final byte[] previousLastKey = lastKeyOfSubtree(node.getChildReference(slot - 1));
      if (previousLastKey == null || Arrays.compareUnsigned(previousLastKey, keySlice) >= 0) {
        return false;
      }
    }
    if (slot + 1 < n) {
      final byte[] nextFirstKey = firstKeyOfSubtree(node.getChildReference(slot + 1));
      if (nextFirstKey == null || Arrays.compareUnsigned(keySlice, nextFirstKey) >= 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Direction-1 ordering guard for a child of one freshly compressed half produced by
   * {@link HOTIncrementalInsert#splitIndirect}. The half is not on {@code navResult}'s original
   * spine, so the regular guard cannot describe the first-key propagation. This method checks the new
   * boundary inside the half, the boundary between both split halves, and—only when the changed half
   * is the left half and its first slot changed—the original ancestors above {@code d*}.
   */
  private boolean isSplitHalfDirectionOneSafe(final LeafNavigationResult navResult, final int insertDepth,
      final HOTIncrementalInsert.BiNode split, final HOTIndirectPage half, final boolean rightHalf,
      final int affectedIdx, final byte[] keySlice) {
    final byte[] affectedFirstKey = firstKeyOfSubtree(half.getChildReference(affectedIdx));
    if (affectedFirstKey == null) {
      return false;
    }
    if (Arrays.compareUnsigned(keySlice, affectedFirstKey) >= 0) {
      return true; // K cannot change any subtree minimum.
    }
    if (!isRangeStartSafeAtSlot(half, affectedIdx, keySlice)) {
      return false;
    }
    if (affectedIdx != 0) {
      return true; // The half's own first key is unchanged.
    }

    if (rightHalf) {
      final byte[] leftLastKey = lastKeyOfSubtree(split.left());
      return leftLastKey != null && Arrays.compareUnsigned(leftLastKey, keySlice) < 0;
    }

    final byte[] rightFirstKey = firstKeyOfSubtree(split.right());
    if (rightFirstKey == null || Arrays.compareUnsigned(keySlice, rightFirstKey) >= 0) {
      return false;
    }

    // K is the split subtree's new first key. Propagate the boundary check through the original
    // spine until the first non-leftmost slot, exactly as for the ordinary Direction-1 path.
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int[] childSlots = navResult.pathChildIndices();
    int currentSlot = 0;
    for (int depth = insertDepth - 1; depth >= 0 && currentSlot == 0; depth--) {
      final int parentSlot = childSlots[depth];
      if (!isRangeStartSafeAtSlot(pathNodes[depth], parentSlot, keySlice)) {
        return false;
      }
      currentSlot = parentSlot;
    }
    return true;
  }

  /**
   * Direction 1 outcome counter -- how often the C2 catch sub-inserts vs falls back to scoped
   * rebuild. Useful for empirical hit-rate measurement; never read by the writer.
   */
  public static final AtomicLong DIRECTION_ONE_SUBINSERT = new AtomicLong();
  public static final AtomicLong DIRECTION_ONE_FALLBACK = new AtomicLong();
  /** I8/I12-unsafe C2 collisions resolved by a complete direct-leaf-frontier splice. */
  public static final AtomicLong DIRECTION_ONE_LEAF_FRONTIER_SPLICE = new AtomicLong();
  /** Frontier splices whose minimal complete range was exactly one adjacent BiNode pair. */
  public static final AtomicLong DIRECTION_ONE_LEAF_PAIR_SPLICE = new AtomicLong();
  /** Frontier splices whose minimal complete range contained three or more direct leaves. */
  public static final AtomicLong DIRECTION_ONE_MULTI_LEAF_FRONTIER_SPLICE = new AtomicLong();
  /** Indirect complete frontiers wrapped with K after a two-endpoint opposite-side proof. */
  public static final AtomicLong DIRECTION_ONE_OPPOSITE_FRONTIER_WRAP = new AtomicLong();
  /** C2 continuations performed inside a freshly split full-node half. */
  public static final AtomicLong FULL_EXISTING_BIT_DIRECTION_ONE_SUBINSERT = new AtomicLong();

  /**
   * Issue B outcome counters -- how often handleOffPathOverflow succeeds directly versus delegating
   * pre-publication to the complete frontier. Plan §4.3.
   */
  public static final AtomicLong OFF_PATH_OVERFLOW_OK = new AtomicLong();
  public static final AtomicLong OFF_PATH_OVERFLOW_FALLBACK = new AtomicLong();

  /** Ancestors re-encoded in place solely to refresh exact structural height. */
  public static final AtomicLong STRUCTURAL_HEIGHT_REENCODE = new AtomicLong();

  /** Post-publication sibling-boundary failures missed by the mandatory preflight. Must stay zero. */
  public static final AtomicLong STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE = new AtomicLong();

  /** Mandatory stranding/routing scans refused before exceeding one bounded HOT block. */
  public static final AtomicLong MUTATION_TRAVERSAL_REFUSED = new AtomicLong();

  /** Opt-in diagnostic whole-tree walks skipped because their source exceeded the same hard bound. */
  public static final AtomicLong DIAGNOSTIC_TRAVERSAL_SKIPPED = new AtomicLong();


  /**
   * Characterize an I8-unsafe Direction 1 fallback (Stage 4b iter-3 diagnostic). Gated on
   * {@code -Dhot.diag.directionOneFallback=true}. Dumps the trigger key, d*'s shape, the affected
   * slot's lex position vs. K, and -- as a routing-encoding-rewrite Phase 1 probe
   * (docs/HOT_ROUTING_ENCODING_REWRITE.md) -- the candidate disc bit β'' = MSDB(K XOR
   * affected.firstKey) AND β''' = MSDB(K XOR prev.firstKey), plus whether each is fresh to d*'s
   * current mask. The Phase 1 hypothesis: β'' (and ideally β''') is always present + fresh, so a
   * proactive mask extension at d* can fix the ambiguity that drove the I8-unsafe fallback.
   */
  private void dumpDirectionOneFallback(String site, LeafNavigationResult navResult, int affectedIdx, int insertDepth,
      int beta, int betaValue, int comboPartial, byte[] keySlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int[] childSlots = navResult.pathChildIndices();
    final HOTIndirectPage dStar = pathNodes[insertDepth];
    final int n = dStar.getNumChildren();
    final byte[] affectedFirstKey = firstKeyOfSubtree(dStar.getChildReference(affectedIdx));
    final byte[] prevFirstKey = affectedIdx > 0
        ? firstKeyOfSubtree(dStar.getChildReference(affectedIdx - 1))
        : null;
    final byte[] nextFirstKey = affectedIdx + 1 < n
        ? firstKeyOfSubtree(dStar.getChildReference(affectedIdx + 1))
        : null;
    final StringBuilder spine = new StringBuilder(128);
    int currentSlot = affectedIdx;
    spine.append('[').append(insertDepth).append("=slot").append(currentSlot);
    for (int d = insertDepth - 1; d >= 0 && currentSlot == 0; d--) {
      final int parentSlot = childSlots[d];
      spine.append(",h=").append(d).append("=slot").append(parentSlot);
      currentSlot = parentSlot;
    }
    spine.append(']');
    final HexFormat hex = HexFormat.of();
    final String hexKey = hex.formatHex(keySlice, 0, Math.min(keySlice.length, 22));
    final String hexAffected = affectedFirstKey == null
        ? "null"
        : hex.formatHex(affectedFirstKey, 0, Math.min(affectedFirstKey.length, 22));
    final String hexPrev = prevFirstKey == null
        ? "<none>"
        : hex.formatHex(prevFirstKey, 0, Math.min(prevFirstKey.length, 22));
    final String hexNext = nextFirstKey == null
        ? "<none>"
        : hex.formatHex(nextFirstKey, 0, Math.min(nextFirstKey.length, 22));

    // Routing-encoding-rewrite Phase 1 probe (docs/HOT_ROUTING_ENCODING_REWRITE.md):
    // compute candidate disc bits + freshness. Empirically (2026-05-20) all 4 canary
    // firings have β'' (= MSDB(K XOR affected.fk)) IN d*'s mask -- the bit is there but
    // off-path-straddled at affected's slot. The §2.2 "proactive mask extension"
    // hypothesis is therefore refuted: the right Phase 2 primitive is to SPLIT
    // affected on β'' (force the straddled bit onto path), not add the bit to d*.
    final int[] dStarDiscBits = HOTIncrementalInsert.discriminativeBits(dStar);
    final int betaPrimePrime = affectedFirstKey == null
        ? -1
        : msdbOfKeyXor(keySlice, affectedFirstKey);
    final int betaTriple = prevFirstKey == null
        ? -1
        : msdbOfKeyXor(keySlice, prevFirstKey);
    final boolean bppFresh = betaPrimePrime >= 0 && Arrays.binarySearch(dStarDiscBits, betaPrimePrime) < 0;
    final boolean btFresh = betaTriple >= 0 && Arrays.binarySearch(dStarDiscBits, betaTriple) < 0;

    // Paper-grade single-entry-leaf-for-K probe (2026-05-20). Classifies whether
    // each firing's K can be carved out as its own slot at d* with partial = K's
    // densePK without colliding with affected's stored partial. Two cases:
    // (a) K's densePK == affected's stored -> COLLISION (cannot give K its
    // own slot under d*'s current mask; would need mask extension or other
    // structural change first).
    // (b) K's densePK is a strict superset -> CARVABLE (K's densePK is
    // unique to its slot; adding a new slot with that partial preserves I7
    // AND I8 because K's densePK < prev's stored as integers at the β'''
    // packed position where K=0, prev=1 and they agree above).
    // If ALL firings are case (b), the localized fix is theoretically viable
    // (still needs to verify routing doesn't break for other keys); if ALL are
    // case (a), the impossibility evidence strengthens.
    final int densePkK = dStar.computeDensePartialKey(keySlice);
    final int affectedStored = dStar.getPartialKey(affectedIdx);
    final int prevStored = affectedIdx > 0
        ? dStar.getPartialKey(affectedIdx - 1)
        : -1;
    final boolean subsetOk = (densePkK & affectedStored) == affectedStored;
    final boolean strictSuperset = subsetOk && densePkK != affectedStored;
    final String carveClass = !subsetOk
        ? "ROUTING-BUG"
        : (densePkK == affectedStored
            ? "COLLISION"
            : "CARVABLE");
    // For CARVABLE cases, verify K's densePK sorts BEFORE prev's stored (so K's
    // new slot lands at I7 position < prev's, satisfying I8 with K's firstKey <
    // prev.firstKey). When prev is absent (affectedIdx=0) the firing must still
    // respect d*'s outer ancestors -- record as N/A here.
    final String prevOrderOk;
    if (prevStored < 0) {
      prevOrderOk = "n/a";
    } else if (strictSuperset) {
      prevOrderOk = Integer.compareUnsigned(densePkK, prevStored) < 0
          ? "yes"
          : "NO";
    } else {
      prevOrderOk = "skip";
    }
    final int collisionSlot = findChildSlotByPartial(dStar, comboPartial);
    String frontierShape = "<none>";
    if (collisionSlot >= 0 && collisionSlot < affectedIdx) {
      final HOTIncrementalInsert.ChildRange range =
          HOTIncrementalInsert.minimalBiNodeRangeContaining(dStar, collisionSlot, affectedIdx);
      final StringBuilder shape = new StringBuilder().append('[')
                                                     .append(range.fromInclusive())
                                                     .append(',')
                                                     .append(range.toExclusive())
                                                     .append("):");
      for (int slot = range.fromInclusive(); slot < range.toExclusive(); slot++) {
        final Page page = resolveHOTPageForTraversal(dStar.getChildReference(slot));
        if (slot > range.fromInclusive()) {
          shape.append(',');
        }
        shape.append(page instanceof HOTLeafPage leaf
            ? "L" + leaf.getEntryCount()
            : page instanceof HOTIndirectPage indirect
                ? "Ih" + indirect.getHeight() + 'x' + indirect.getNumChildren()
                : "?");
      }
      frontierShape = shape.toString();
    }

    System.err.println("[D1-FALLBACK " + site + "] revision=" + storageEngineWriter.getRevisionNumber() + " K=" + hexKey
        + " (lenK=" + keySlice.length + ")" + " pathDepth=" + navResult.pathDepth() + " insertDepth=" + insertDepth
        + " dStar.children=" + n + " dStar.height=" + dStar.getHeight() + " affectedIdx=" + affectedIdx + " spine="
        + spine + " beta=" + beta + " betaValue=" + betaValue + " comboPartial=0x" + Integer.toHexString(comboPartial)
        + " affected.fk=" + hexAffected + " (lenA=" + (affectedFirstKey == null
            ? "n/a"
            : Integer.toString(affectedFirstKey.length))
        + ")" + " prev.fk=" + hexPrev + " (lenP=" + (prevFirstKey == null
            ? "n/a"
            : Integer.toString(prevFirstKey.length))
        + ")" + " next.fk=" + hexNext + " // Phase1-probe: beta''=" + betaPrimePrime + (bppFresh
            ? "(fresh)"
            : "(IN-MASK)")
        + " beta'''=" + betaTriple + (btFresh
            ? "(fresh)"
            : "(IN-MASK)")
        + " mask=" + Arrays.toString(dStarDiscBits) + " // CarveProbe: densePK_K=0x" + Integer.toHexString(densePkK)
        + " affectedStored=0x" + Integer.toHexString(affectedStored) + " prevStored=" + (prevStored < 0
            ? "<none>"
            : "0x" + Integer.toHexString(prevStored))
        + " class=" + carveClass + " prevOrderOk=" + prevOrderOk + " frontier=" + frontierShape);
  }

  /** Failure-path-only shape dump for a malformed unpublished combo-add candidate. */
  private void dumpMalformedComboAdd(final String site, final HOTIndirectPage oldNode, final HOTIndirectPage candidate,
      final byte[] keySlice, final int beta) {
    final int newSlot = candidate.findChildIndex(keySlice);
    int badBoundary = -1;
    String violation = "unknown";
    byte[] previousFirst = null;
    byte[] previousLast = null;
    for (int slot = 0; slot < candidate.getNumChildren(); slot++) {
      final PageReference childRef = candidate.getChildReference(slot);
      final byte[] first = firstKeyOfSubtree(childRef);
      if (first == null) {
        badBoundary = slot;
        violation = "unreadable";
        break;
      }
      if (previousFirst != null && Arrays.compareUnsigned(previousFirst, first) >= 0) {
        badBoundary = slot;
        violation = "I8";
        break;
      }
      if (previousLast != null && Arrays.compareUnsigned(previousLast, first) >= 0) {
        badBoundary = slot;
        violation = "I12";
        break;
      }
      previousFirst = first;
      previousLast = slot + 1 < candidate.getNumChildren()
          ? lastKeyOfSubtree(childRef)
          : null;
    }
    final HexFormat hex = HexFormat.of();
    final String keyHex = diagnosticHex(keySlice, hex);
    final String leftFirst = badBoundary > 0
        ? diagnosticHex(firstKeyOfSubtree(candidate.getChildReference(badBoundary - 1)), hex)
        : "<none>";
    final String leftLast = badBoundary > 0
        ? diagnosticHex(lastKeyOfSubtree(candidate.getChildReference(badBoundary - 1)), hex)
        : "<none>";
    final String rightFirst = badBoundary >= 0 && badBoundary < candidate.getNumChildren()
        ? diagnosticHex(firstKeyOfSubtree(candidate.getChildReference(badBoundary)), hex)
        : "<none>";
    String frontierShape = "<none>";
    if (badBoundary > 0 && newSlot >= 0 && badBoundary != newSlot) {
      final int firstSlot = Math.min(badBoundary - 1, newSlot);
      final int lastSlot = Math.max(badBoundary, newSlot);
      final HOTIncrementalInsert.ChildRange range =
          HOTIncrementalInsert.minimalBiNodeRangeContaining(candidate, firstSlot, lastSlot);
      final StringBuilder shape = new StringBuilder().append('[')
                                                     .append(range.fromInclusive())
                                                     .append(',')
                                                     .append(range.toExclusive())
                                                     .append("):");
      for (int slot = range.fromInclusive(); slot < range.toExclusive(); slot++) {
        final Page page = resolveHOTPageForTraversal(candidate.getChildReference(slot));
        if (slot > range.fromInclusive()) {
          shape.append(',');
        }
        shape.append(page instanceof HOTLeafPage leaf
            ? "L" + leaf.getEntryCount()
            : page instanceof HOTIndirectPage indirect
                ? "Ih" + indirect.getHeight() + 'x' + indirect.getNumChildren()
                : "?");
      }
      frontierShape = shape.toString();
    } else if (badBoundary > 0 && newSlot == badBoundary) {
      final HOTIncrementalInsert.ChildRange range =
          HOTIncrementalInsert.minimalBiNodeRangeContaining(candidate, badBoundary - 1, badBoundary);
      final StringBuilder shape = new StringBuilder().append('[')
                                                     .append(range.fromInclusive())
                                                     .append(',')
                                                     .append(range.toExclusive())
                                                     .append("):");
      for (int slot = range.fromInclusive(); slot < range.toExclusive(); slot++) {
        final Page page = resolveHOTPageForTraversal(candidate.getChildReference(slot));
        if (slot > range.fromInclusive()) {
          shape.append(',');
        }
        shape.append(page instanceof HOTLeafPage leaf
            ? "L" + leaf.getEntryCount()
            : page instanceof HOTIndirectPage indirect
                ? "Ih" + indirect.getHeight() + 'x' + indirect.getNumChildren()
                : "?");
      }
      frontierShape = shape.toString();
    }
    System.err.println("[MALFORMED-COMBO " + site + "] revision=" + storageEngineWriter.getRevisionNumber() + " beta="
        + beta + " K=" + keyHex + " oldBits=" + Arrays.toString(HOTIncrementalInsert.discriminativeBits(oldNode))
        + " oldPartials=" + Arrays.toString(Arrays.copyOf(oldNode.getPartialKeysRef(), oldNode.getNumChildren()))
        + " newBits=" + Arrays.toString(HOTIncrementalInsert.discriminativeBits(candidate)) + " newPartials="
        + Arrays.toString(Arrays.copyOf(candidate.getPartialKeysRef(), candidate.getNumChildren())) + " newSlot="
        + newSlot + " violation=" + violation + " boundary=" + badBoundary + " left.first=" + leftFirst + " left.last="
        + leftLast + " right.first=" + rightFirst + " frontier=" + frontierShape);
  }

  private static String diagnosticHex(final byte @Nullable [] key, final HexFormat hex) {
    return key == null
        ? "null"
        : hex.formatHex(key, 0, Math.min(key.length, 22));
  }

  /**
   * Most-significant differing bit between two byte arrays (MSB-first absolute index). The
   * routing-encoding-rewrite candidate bit for closing a MSDB gap at an ancestor's mask is always the
   * MSDB of the trigger key XOR'd with the lex-correct neighbour's first key.
   */
  private static int msdbOfKeyXor(byte[] a, byte[] b) {
    final int len = Math.min(a.length, b.length);
    for (int i = 0; i < len; i++) {
      final int diff = (a[i] ^ b[i]) & 0xFF;
      if (diff != 0) {
        return i * 8 + Integer.numberOfLeadingZeros(diff) - 24;
      }
    }
    return a.length == b.length
        ? -1
        : len * 8;
  }

  /**
   * Plan §4.3 -- Issue B incremental off-path-overflow handler. Called from {@link #mergeIntoLeaf}
   * BEFORE {@link HOTIncrementalInsert#integrate}, when {@link HOTIncrementalInsert#splitLeafPage}
   * produces a {@link HOTIncrementalInsert.BiNode} whose split bit β coincides with an
   * already-existing discriminative bit of L's parent N.
   *
   * <p>
   * The standard {@code addEntry} fold rejects β-already-disc-bit. The incremental fix (when
   * applicable): slot-replace L → L₀ in L's slot (β-column-0 partial unchanged) and add L₁ at
   * {@code comboPartial = L.partial | β-bit} via {@link HOTIncrementalInsert#addChildAtCombination}.
   * β is NOT added as a new disc bit (it was already one); the structure is invariant-clean by Stage
   * 0's off-path-straddle canonicity finding.
   *
   * <p>
   * Returns {@code false} before publication when β is not in D(N), L's β-column is already 1, a C2
   * collision occurs, or a precondition is uncertain. The shared branch driver then uses the complete
   * structural-frontier primitive.
   *
   * @return {@code true} if the off-path-overflow was handled incrementally
   */
  private boolean handleOffPathOverflow(LeafNavigationResult navResult, HOTIncrementalInsert.BiNode biNode,
      byte[] keySlice, byte[] valueSlice) {
    final int pathDepth = navResult.pathDepth();
    if (pathDepth == 0) {
      return false; // L is the root; no parent to fold into
    }
    final HOTIndirectPage parentN = navResult.pathNodes()[pathDepth - 1];
    final int beta = biNode.discriminativeBitIndex();
    final int[] discBits = HOTIncrementalInsert.discriminativeBits(parentN);
    final int betaCol = Arrays.binarySearch(discBits, beta);
    if (betaCol < 0) {
      return false; // β fresh to N -- standard integrate handles
    }
    final int slotOfL = navResult.pathChildIndices()[pathDepth - 1];
    final int[] oldPartials = parentN.getPartialKeysRef();
    if (oldPartials == null || slotOfL >= oldPartials.length) {
      return false; // defensive: malformed partial array
    }
    final int lPartial = oldPartials[slotOfL];
    final int betaBitWeight = 1 << (discBits.length - 1 - betaCol);
    if ((lPartial & betaBitWeight) != 0) {
      // L's β-column is already 1 -- not the off-path-straddle case. The plan §3.2 proof
      // says this can't happen (L's keys would all be β=1, contradicting splitLeafPage's β
      // = msdb(L ∪ {K})), but stay defensive.
      return false;
    }
    final int comboPartial = lPartial | betaBitWeight;
    if (parentN.getNumChildren() >= HOTIndirectPage.MAX_NODE_ENTRIES) {
      // N is full. The N-full handler (handleOffPathOverflowFullN) operates incrementally at
      // every pathDepth. The historical `pathDepth < 2` guard was a workaround for the silent
      // legacy subtree reconstruction path that escalated to depth 0 mid-revision -- producing
      // a freshly canonical h=1 root that competed with the handler's h=1→h=2 growth and
      // surfaced as I1+I6 corruption at rev 9 of interleavedInsertDeleteMultiRev. Plan §12
      // Stage 3c (in-spine height/partial propagation) removed the escalation, eliminating
      // the structural divergence. The handler now applies at pathDepth==1 too.
      return handleOffPathOverflowFullN(navResult, biNode, slotOfL, comboPartial);
    }

    // Step 1: slot-replace L → L₀ in N's children array (in-place on the CoW'd N).
    // The partial at slotOfL is unchanged -- it still has β-column-0, which matches
    // L₀'s β=0 keys. The follow-on addChildAtCombination snapshots the mutated children.
    final PageReference originalLeafRef = navResult.leafRef();
    parentN.setChildReference(slotOfL, biNode.left());

    // Step 2: add L₁ at comboPartial. The live CoW node is temporarily staged with L₀, so every
    // construction failure must restore the original leaf before it can escape or fall back.
    final HOTIndirectPage newN;
    try {
      newN = HOTIncrementalInsert.addChildAtCombination(parentN, comboPartial, biNode.right(), parentN.getHeight(),
          storageEngineWriter.getRevisionNumber(), pageKeyAllocator);
    } catch (final RuntimeException | Error constructionFailure) {
      try {
        parentN.setChildReference(slotOfL, originalLeafRef);
      } catch (final RuntimeException | Error restoreFailure) {
        addSuppressedSafely(constructionFailure, restoreFailure);
        markTransactionRollbackOnly(constructionFailure);
        closeFreshBiNode(biNode, constructionFailure);
        throw constructionFailure;
      }
      if (constructionFailure instanceof IllegalArgumentException
          && findChildSlotByPartial(parentN, comboPartial) >= 0) {
        // C2: comboPartial collides with an existing c'. The unmodified caller still owns both
        // split halves and may continue with its standard incremental integration.
        OFF_PATH_OVERFLOW_FALLBACK.incrementAndGet();
        return false;
      }
      closeFreshBiNode(biNode, constructionFailure);
      throw constructionFailure;
    }

    // Step 3: re-point N's reference at its parent + register fresh subtree.
    boolean published = false;
    try {
      navResult.pathRefs()[pathDepth - 1].setPage(newN);
      published = true;
      lastDispatchHandler = "h:merge-integrate";
      final PageReference replacementRef = navResult.pathRefs()[pathDepth - 1];
      registerFreshSubtree(replacementRef);
      retireReplacedLeaf(originalLeafRef, replacementRef, TransactionIntentLog.RELEASE_SITE_LEAF_SPLIT);
      OFF_PATH_OVERFLOW_OK.incrementAndGet();
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
        closeFreshBiNode(biNode, failure);
      } else {
        try {
          parentN.setChildReference(slotOfL, originalLeafRef);
        } catch (final RuntimeException | Error restoreFailure) {
          addSuppressedSafely(failure, restoreFailure);
          markTransactionRollbackOnly(failure);
        }
        closeFreshBiNode(biNode, failure);
      }
      throw failure;
    }
  }

  /** Retire the still-local portions of both halves of a failed incremental split. */
  private void closeFreshBiNode(final HOTIncrementalInsert.BiNode biNode, final Throwable failure) {
    closeUnregisteredFreshSubtree(biNode.left(), failure);
    if (biNode.right() != biNode.left()) {
      closeUnregisteredFreshSubtree(biNode.right(), failure);
    }
  }

  /**
   * The full-N counterpart of {@link #handleOffPathOverflow}'s not-full path. When N (= L's parent)
   * already has {@link HOTIndirectPage#MAX_NODE_ENTRIES} children, the not-full strategy
   * (slot-replace + {@link HOTIncrementalInsert#addChildAtCombination}) cannot fit L₁ — N has no room
   * for a new child. The standard {@link HOTIncrementalInsert#integrate} capacity cascade would then
   * split N at {@code N.MSB} and call {@link HOTIncrementalInsert#addEntry} on the half that holds
   * L's slot — but {@code addEntry} rejects when β ∈ D(half), which holds whenever the half retains β
   * as a discriminative bit (= some half-children have β=0 and some have β=1; the common non-1:31
   * case).
   *
   * <p>
   * The fix: do the slot-replace + insertion of {@code (comboPartial, L₁)} in N's coordinate space
   * FIRST, then split the resulting (n+1)-wide virtual node at {@code N.MSB} via
   * {@link HOTIncrementalInsert#splitIndirectWithSlotReplaceAndInsertion}. The half containing the
   * modified slot retains β as a disc bit (L₀ has β=0, L₁ has β=1 — varies ⟹ live), so the half is
   * canonical without needing a separate β-fold step.
   *
   * <p>
   * The {@link HOTIncrementalInsert.BiNode} produced is on {@code N.MSB}; we then call
   * {@link HOTIncrementalInsert#integrate} at {@code currentDepth = pathDepth - 1} to splice it where
   * N sat in the spine. When N is the root, that grows the tree by one level (the new root is a
   * 2-entry compound at {@code N.MSB}, height = N.height + 1).
   *
   * @return {@code true} if the N-full off-path-overflow was handled incrementally
   */
  private boolean handleOffPathOverflowFullN(LeafNavigationResult navResult, HOTIncrementalInsert.BiNode biNode,
      int slotOfL, int comboPartial) {
    final int pathDepth = navResult.pathDepth();
    final HOTIndirectPage parentN = navResult.pathNodes()[pathDepth - 1];
    final int revision = storageEngineWriter.getRevisionNumber();
    if (findChildSlotByPartial(parentN, comboPartial) >= 0) {
      // Verified C2: the would-be inserted partial already has a physical owner. No construction or
      // allocation has started, so the caller may safely take its standard incremental path.
      OFF_PATH_OVERFLOW_FALLBACK.incrementAndGet();
      return false;
    }
    final HOTIncrementalInsert.BiNode parentSplit = HOTIncrementalInsert.splitIndirectWithSlotReplaceAndInsertion(
        parentN, slotOfL, biNode.left(), comboPartial, biNode.right(), revision, pageKeyAllocator);

    final int currentDepth = pathDepth - 1;
    final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(navResult.pathNodes(),
        buildSpineRefs(navResult), navResult.pathChildIndices(), currentDepth, parentSplit, revision, pageKeyAllocator);
    try {
      lastDispatchHandler = "h:merge-offpath";
      registerFreshSubtree(result.touchedRef());
      retireReplacedLeaf(navResult.leafRef(), result.touchedRef(), TransactionIntentLog.RELEASE_SITE_LEAF_SPLIT);
    } catch (final RuntimeException | Error failure) {
      // integrate has already re-pointed its one touched spine reference.
      markTransactionRollbackOnly(failure);
      closeFreshBiNode(biNode, failure);
      throw failure;
    }
    OFF_PATH_OVERFLOW_OK.incrementAndGet();
    return true;
  }

  /**
   * The merge outcome of {@link #doIndex}: the key belongs inside the routed leaf/bucket. Merges it
   * in; on bucket overflow defragments and retries once, then splits the leaf page and integrates the
   * resulting {@link HOTIncrementalInsert.BiNode} at the leaf's depth.
   */
  private byte @Nullable [] mergeIntoLeaf(final LeafNavigationResult navResult, final byte[] keyBuf, final int keyLen,
      final byte[] valueBuf, final int valueLen) {
    final HOTLeafPage leaf = navResult.leaf();
    // Posting indexes union NodeReferences for duplicate keys. A projection key identifies one
    // physical slot whose payload is opaque bytes (including the zero-length tombstone), so a
    // duplicate must replace the prior value byte-for-byte. Keep the projection slice exact once;
    // callers normally already provide an exact array, so the hot path allocates nothing.
    final byte[] projectionValue = indexType == IndexType.PROJECTION
        ? valueLen == valueBuf.length
            ? valueBuf
            : Arrays.copyOf(valueBuf, valueLen)
        : null;
    // Fast path: the entry fits the bucket. The leaf is mutated in place — already in the TIL.
    // No indirect structure changes, so no structural validation scope is needed (return null).
    // Both leaf APIs consume the valid key prefix directly. The serializer's spare buffer capacity
    // never participates in ordering and no exact-size key array is created on this path.
    if (indexType == IndexType.PROJECTION
        ? leaf.putOrReplace(keyBuf, keyLen, projectionValue)
        : leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
      return null;
    }
    // The bucket is full. compact() only repacks the physically present entries; tombstones remain
    // because removing them without also publishing a complete version boundary could resurrect an
    // older value. Retry the merge once if the bounded, leaf-local repack reclaimed space.
    if (leaf.compact() > 0 && (indexType == IndexType.PROJECTION
        ? leaf.putOrReplace(keyBuf, keyLen, projectionValue)
        : leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen))) {
      return null;
    }
    // Genuine overflow: split the leaf page at its key-set MSDB and integrate the BiNode.
    if (!leaf.canSplit()) {
      throw new SirixIOException(
          "HOT leaf page cannot store the entry and cannot split — a " + "single value exceeds page capacity. index="
              + indexType + ", entries=" + leaf.getEntryCount() + ", remaining=" + leaf.getRemainingSpace());
    }
    final int revision = storageEngineWriter.getRevisionNumber();
    final byte[] valueSlice = projectionValue != null
        ? projectionValue
        : valueLen == valueBuf.length
            ? valueBuf
            : Arrays.copyOf(valueBuf, valueLen);
    final byte[] keySlice = exactKeyForStructuralMutation(keyBuf, keyLen);
    final HOTIncrementalInsert.BiNode biNode =
        HOTIncrementalInsert.splitLeafPage(leaf, keySlice, valueSlice, revision, indexType, pageKeyAllocator);
    boolean published = false;
    try {
      ensurePathChildrenLoaded(navResult.pathNodes(), navResult.pathDepth());

      // Issue B (plan §4.3): if β = msdb(L ∪ {K}) is already a disc bit of L's parent N,
      // standard addEntry will reject. Apply the incremental off-path-overflow handler before
      // calling integrate -- it slot-replaces L with L₀ and adds L₁ at comboPartial, sidestepping
      // the historical over-partitioning observed in iterations 3/5/6/7/8.
      if (handleOffPathOverflow(navResult, biNode, keySlice, valueSlice)) {
        return keySlice;
      }

      // Plan §12 Stage 3b: an exception escaping integrate after the clean preflight is a real
      // bug. integrate allocates first and re-points exactly one spine reference as its final step.
      final HOTIncrementalInsert.IntegrationResult result =
          HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
              navResult.pathDepth(), biNode, revision, pageKeyAllocator);
      published = true;
      lastDispatchHandler = "h:merge-offpath-fullN";
      registerFreshSubtree(result.touchedRef());
      retireReplacedLeaf(navResult.leafRef(), result.touchedRef(), TransactionIntentLog.RELEASE_SITE_LEAF_SPLIT);
      return keySlice;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
      }
      closeFreshBiNode(biNode, failure);
      throw failure;
    }
  }

  /**
   * The branch outcome of {@link #doIndex} — Binna's {@code insertNewValueIntoNode}
   * ({@code HOTSingleThreaded.hpp:413}). HOT's subset-match descent landed the new key in a leaf it
   * does not fully belong to: its mismatch bit {@code beta} is at or above an ancestor's
   * discriminative bit, so the key must branch off as its own subtree.
   *
   * <p>
   * The faithful port computes {@code beta} (the genuine first-differing bit, never an existing
   * discriminative bit of the branch node) and lets {@link HOTIncrementalInsert#getInsertInformation}
   * locate the affected subtree at the insert-depth node {@code d*}; one of three outcomes follows:
   * <ul>
   * <li><b>leaf pair</b> — the affected subtree is the descended leaf itself: pair it with the new
   * key's single-entry leaf under a {@code BiNode} on {@code beta} and integrate at the leaf's depth
   * (Binna's {@code createFromExistingAndNewEntry} + {@code integrateBiNodeIntoTree}).</li>
   * <li><b>new partition root</b> — the affected subtree is a single boundary <em>node</em> (the
   * MSB-stack insert depth was one level too shallow — Binna's "false positive"): the new key joins
   * that child node as a new partition root.</li>
   * <li><b>add entry</b> — the affected subtree spans several children: the new key's leaf is folded
   * into {@code d*}'s block beside it ({@link HOTIncrementalInsert#addEntryWithInsertInfo}).</li>
   * </ul>
   * Every {@link #tryBranchIncremental} false return rejects an unpublished candidate and converges
   * on the same complete structural-frontier splice.
   */
  private boolean branchAboveLeaf(final LeafNavigationResult navResult, final int mismatchBit, final int insertDepth,
      final int affectedChildIndex, final byte[] keySlice, final byte[] valueBuf, final int valueLen) {
    final byte[] valueSlice = valueLen == valueBuf.length
        ? valueBuf
        : Arrays.copyOf(valueBuf, valueLen);
    if (!tryBranchIncremental(navResult, mismatchBit, insertDepth, affectedChildIndex, keySlice, valueSlice)) {
      // Sparse routing and lexicographic placement disagree (Direction 1 / stranding / integration
      // collision). Persistently split only the smallest complete structural frontier around those
      // two positions, copy at most its one boundary leaf, and splice K there. This is the total
      // incremental discharge: no subtree entry collection and no posting-payload rebuild.
      spliceCompleteFrontierIncrementally(navResult, insertDepth, keySlice, valueSlice);
    }
    return true; // incremental branch/frontier splice — verify the path structurally
  }

  /**
   * Attempt the incremental branch insert — Binna's {@code insertNewValueIntoNode}. Returns
   * {@code false} (caller recanonicalizes) when the case needs a path not yet ported: {@code beta}
   * colliding with an existing discriminative bit, or a full node that would have to split.
   *
   * @return {@code true} iff the key was inserted incrementally
   */
  private boolean tryBranchIncremental(final LeafNavigationResult navResult, final int beta, final int insertDepth,
      final int affectedChildIndex, final byte[] keySlice, final byte[] valueSlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final PageReference[] pathRefs = navResult.pathRefs();
    final int[] childSlots = navResult.pathChildIndices();
    final int pathDepth = navResult.pathDepth();
    final int betaValue = HOTBulkBuilder.bitAt(keySlice, beta)
        ? 1
        : 0;
    final int revision = storageEngineWriter.getRevisionNumber();

    final HOTIndirectPage node = pathNodes[insertDepth];
    final HOTIncrementalInsert.InsertInfo info =
        HOTIncrementalInsert.getInsertInformation(node, affectedChildIndex, beta);
    // beta colliding with an existing discriminative bit of d* means the approximate descent
    // misrouted the key across that bit (Binna's addEntry with DiscriminativeBitsRepresentation.insert
    // a no-op). The key branches off the affected subtree — which is one-sided on beta, since
    // beta = msdb(key, that subtree) — so it becomes a new child of d* at the sparse-path partial
    // {@code subtreePrefix | beta-bit}: the above-beta prefix it shares with that subtree, the
    // beta bit set to the key's value, every below-beta column zero (a fresh single-entry leaf is
    // its own subtree root). The discriminative bits are unchanged — beta is already one of them.
    if (info.betaIsDiscBit()) {
      if (node.getNumChildren() >= HOTIndirectPage.MAX_NODE_ENTRIES) {
        // betaIsDiscBit + full d* — split + dispatch decomposition
        // (docs/HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md §4.1).
        if (branchFullNodeAtExistingBit(navResult, node, insertDepth, beta, betaValue, keySlice, valueSlice)) {
          return true;
        }
        // The decomposition dead-ended (§6 C1: its MSB split left K's half a LONE child, so there
        // is no half node to fold into; or a C2 fold precondition failed). Its speculative split
        // was never published, so the state is untouched and a different primitive may still
        // apply. When the affected subtree is the descended leaf itself, that primitive is the
        // generic leaf pair below: BiNode(beta, leaf, K) integrated at the leaf's depth, where
        // integrate() decomposes the full parent whose mask already contains beta through
        // splitIndirectWithSlotReplaceAndInsertion — the same decomposition, taken in the
        // parent's own coordinate space, and it has no lone-child dead end. Every ordering and
        // stranding guard on that path still applies.
        if (info.affectedCount() != 1 || insertDepth + 1 != pathDepth) {
          return false;
        }
      } else {
        final int[] nodeDiscBits = HOTIncrementalInsert.discriminativeBits(node);
        final int betaColumn = Arrays.binarySearch(nodeDiscBits, beta);
        final int comboPartial = info.subtreePrefix() | (betaValue == 1
            ? 1 << (nodeDiscBits.length - 1 - betaColumn)
            : 0);
        final HOTLeafPage comboLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
        putFreshSingleEntryOrThrow(comboLeaf, keySlice, valueSlice);
        final HOTIndirectPage newNode;
        try {
          newNode = HOTIncrementalInsert.addChildAtCombination(node, comboPartial, swizzle(comboLeaf), node.getHeight(),
              revision, pageKeyAllocator);
        } catch (final RuntimeException | Error constructionFailure) {
          final int collisionSlot = findChildSlotByPartial(node, comboPartial);
          if (!(constructionFailure instanceof IllegalArgumentException) || collisionSlot < 0) {
            // addChildAtCombination owns the only C2 classification point. An exception without
            // an actual partial collision is a violated construction precondition, not permission
            // to enter Direction 1.
            closeSpeculativeLeaf(comboLeaf, constructionFailure);
            throw constructionFailure;
          }
          // C2 -- comboPartial coincides with an existing child of d*. Direction 1 sub-insert
          // into affected (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §11) is routing-correct
          // by the descent tautology; the only remaining risk is I8 (range-scan ordering) when
          // K becomes affected's new firstKey and the trie has an MSDB-closure gap at some
          // ancestor's mask. Pre-check via isDirectionOneI8Safe; if safe, sub-insert; else
          // try the bounded incremental frontier primitives below.
          comboLeaf.close();
          if (isDirectionOneI8Safe(navResult, insertDepth, affectedChildIndex, keySlice)) {
            lastDispatchHandler = "h:d1-subinsert";
            DIRECTION_ONE_SUBINSERT.incrementAndGet();
            return subInsertAt(node.getChildReference(affectedChildIndex), keySlice, keySlice.length, valueSlice,
                valueSlice.length);
          }
          if (tryDirectionOneLeafPairSplice(navResult, node, insertDepth, collisionSlot, affectedChildIndex, keySlice,
              valueSlice)) {
            return true;
          }
          if (tryDirectionOneOppositeFrontierWrap(navResult, node, insertDepth, collisionSlot, affectedChildIndex, beta,
              betaValue, keySlice, valueSlice)) {
            return true;
          }
          DIRECTION_ONE_FALLBACK.incrementAndGet();
          if (Boolean.getBoolean("hot.diag.directionOneFallback")) {
            dumpDirectionOneFallback("site1", navResult, affectedChildIndex, insertDepth, beta, betaValue, comboPartial,
                keySlice);
          }
          return false;
        }
        boolean published = false;
        try {
          if (branchAddStrandsExisting(node, newNode, keySlice)) {
            comboLeaf.close();
            return dischargeStrandViaLeafFrontier(navResult, node, newNode, insertDepth, keySlice, valueSlice);
          }
          if (nodeStructurallyMalformed(newNode)) {
            if (Boolean.getBoolean("hot.diag.branchFallback")) {
              dumpMalformedComboAdd("site1", node, newNode, keySlice, beta);
            }
            if (trySpliceMalformedComboLeafFrontier(navResult, node, newNode, insertDepth, comboLeaf, keySlice)) {
              return true;
            }
            comboLeaf.close();
            BRANCH_COMPLETE_FRONTIER.incrementAndGet();
            return false; // I8-unsafe combo-add -> complete structural frontier
          }
          pathRefs[insertDepth].setPage(newNode);
          published = true;
          lastDispatchHandler = "h:combo-site1";
          registerFreshSubtree(pathRefs[insertDepth]);
          return true;
        } catch (final RuntimeException | Error failure) {
          if (published) {
            // setPage is the sole publication boundary. Nothing after it may be reclassified as
            // a C2 construction collision or allowed to continue into another structural handler.
            markTransactionRollbackOnly(failure);
          }
          closeSpeculativeLeaf(comboLeaf, failure);
          throw failure;
        }
      }
    }
    final boolean singleEntry = info.affectedCount() == 1;
    final boolean leafEntry = insertDepth + 1 == pathDepth;
    // Decide portability before allocating K's leaf page, so a fallback never orphans it.
    if (!singleEntry && node.getNumChildren() >= HOTIndirectPage.MAX_NODE_ENTRIES) {
      if (info.affectedCount() == node.getNumChildren()) {
        // The affected subtree is the whole node — beta is more significant than every
        // discriminative bit of the full node, so K branches above it (Binna's insertNewValue
        // full-node case, mismatch bit above node.MSB). Wrap the whole node under a BiNode on
        // beta and integrate at insertDepth; integrate's intermediate-node / split-cascade keeps
        // the height bounded. Both BiNode children need fresh references — integrate may
        // re-point insertDepth's spine slot, and aliasing it would make a page its own child.
        final HOTLeafPage pullUpLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
        putFreshSingleEntryOrThrow(pullUpLeaf, keySlice, valueSlice);
        if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, insertDepth, beta)) {
          pullUpLeaf.close();
          return false;
        }
        // Stranding guard: the whole node goes on beta's (1-betaValue) side. If its subtree holds
        // a key with beta==betaValue, that key would strand under the pull-up leaf. Delegate instead.
        try {
          if (subtreeHasKeyWithBit(pathRefs[insertDepth], beta, betaValue, keySlice)) {
            pullUpLeaf.close();
            STRAND_COMPLETE_FRONTIER.incrementAndGet();
            return false; // BiNode-wrap strand: complete structural frontier
          }
        } catch (MutationTraversalRefusal refusal) {
          closeSpeculativeLeaf(pullUpLeaf, refusal);
          throw refusal;
        }
        final int biHeight = node.getHeight() + 1;
        final HOTIncrementalInsert.BiNode biNode;
        try {
          final PageReference pullUpLeafRef = swizzle(pullUpLeaf);
          final PageReference wrappedNodeRef = swizzle(node);
          biNode = betaValue == 1
              ? new HOTIncrementalInsert.BiNode(beta, biHeight, wrappedNodeRef, pullUpLeafRef)
              : new HOTIncrementalInsert.BiNode(beta, biHeight, pullUpLeafRef, wrappedNodeRef);
        } catch (final RuntimeException | Error constructionFailure) {
          closeSpeculativeLeaf(pullUpLeaf, constructionFailure);
          throw constructionFailure;
        }
        boolean published = false;
        try {
          ensurePathChildrenLoaded(pathNodes, navResult.pathDepth());
          final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(pathNodes,
              buildSpineRefs(navResult), childSlots, insertDepth, biNode, revision, pageKeyAllocator);
          published = true;
          lastDispatchHandler = "h:integrate-existing-bit";
          registerFreshSubtree(result.touchedRef());
          return true;
        } catch (final RuntimeException | Error failure) {
          if (published) {
            markTransactionRollbackOnly(failure);
          }
          closeSpeculativeLeaf(pullUpLeaf, failure);
          throw failure;
        }
      }
      return branchSplitFullNode(navResult, info, node, insertDepth, beta, betaValue, keySlice, valueSlice);
    }
    if (singleEntry && !leafEntry) {
      final HOTIndirectPage child = pathNodes[insertDepth + 1];
      final int[] childDiscBits = HOTIncrementalInsert.discriminativeBits(child);
      final int betaColAtChild = Arrays.binarySearch(childDiscBits, beta);
      if (betaColAtChild >= 0) {
        // beta already a disc bit of the boundary child — apply the betaIsDiscBit handling
        // one level down (docs/HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md §4.2).
        if (child.getNumChildren() >= HOTIndirectPage.MAX_NODE_ENTRIES) {
          // Full boundary child + betaIsDiscBit — re-use Stage 1's full-d* decomposition,
          // anchored at insertDepth+1.
          return branchFullNodeAtExistingBit(navResult, child, insertDepth + 1, beta, betaValue, keySlice, valueSlice);
        }
        // Not-full boundary child + betaIsDiscBit — addChildAtCombination on the child (the
        // Q1-verified not-full pattern, applied at depth+1).
        final int childEntryIndex = childSlots[insertDepth + 1];
        final HOTIncrementalInsert.InsertInfo childInfo =
            HOTIncrementalInsert.getInsertInformation(child, childEntryIndex, beta);
        final int comboPartial = childInfo.subtreePrefix() | (betaValue == 1
            ? 1 << (childDiscBits.length - 1 - betaColAtChild)
            : 0);
        final HOTLeafPage comboLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
        putFreshSingleEntryOrThrow(comboLeaf, keySlice, valueSlice);
        final HOTIndirectPage newChild;
        try {
          newChild = HOTIncrementalInsert.addChildAtCombination(child, comboPartial, swizzle(comboLeaf),
              child.getHeight(), revision, pageKeyAllocator);
        } catch (final RuntimeException | Error constructionFailure) {
          final int collisionSlot = findChildSlotByPartial(child, comboPartial);
          if (!(constructionFailure instanceof IllegalArgumentException) || collisionSlot < 0) {
            closeSpeculativeLeaf(comboLeaf, constructionFailure);
            throw constructionFailure;
          }
          // Site 3 C2 -- comboPartial collides with an existing child of the boundary node.
          // Apply Direction 1 at the boundary level: sub-insert K into the boundary child's
          // affected slot if I8-safe (the routing tautology holds at depth+1 just as at d*),
          // otherwise try the bounded incremental frontier primitives below.
          comboLeaf.close();
          if (isDirectionOneI8Safe(navResult, insertDepth + 1, childEntryIndex, keySlice)) {
            lastDispatchHandler = "h:d1-subinsert";
            DIRECTION_ONE_SUBINSERT.incrementAndGet();
            return subInsertAt(child.getChildReference(childEntryIndex), keySlice, keySlice.length, valueSlice,
                valueSlice.length);
          }
          if (tryDirectionOneLeafPairSplice(navResult, child, insertDepth + 1, collisionSlot, childEntryIndex, keySlice,
              valueSlice)) {
            return true;
          }
          if (tryDirectionOneOppositeFrontierWrap(navResult, child, insertDepth + 1, collisionSlot, childEntryIndex,
              beta, betaValue, keySlice, valueSlice)) {
            return true;
          }
          DIRECTION_ONE_FALLBACK.incrementAndGet();
          if (Boolean.getBoolean("hot.diag.directionOneFallback")) {
            dumpDirectionOneFallback("site3", navResult, childEntryIndex, insertDepth + 1, beta, betaValue,
                comboPartial, keySlice);
          }
          return false;
        }
        boolean published = false;
        try {
          if (branchAddStrandsExisting(child, newChild, keySlice)) {
            comboLeaf.close();
            return dischargeStrandViaLeafFrontier(navResult, child, newChild, insertDepth + 1, keySlice, valueSlice);
          }
          if (nodeStructurallyMalformed(newChild)) {
            if (trySpliceMalformedComboLeafFrontier(navResult, child, newChild, insertDepth + 1, comboLeaf, keySlice)) {
              return true;
            }
            comboLeaf.close();
            BRANCH_COMPLETE_FRONTIER.incrementAndGet();
            return false; // I8-unsafe combo-add -> complete structural frontier
          }
          pathRefs[insertDepth + 1].setPage(newChild);
          published = true;
          lastDispatchHandler = "h:combo-site3";
          registerFreshSubtree(pathRefs[insertDepth + 1]);
          return true;
        } catch (final RuntimeException | Error failure) {
          if (published) {
            markTransactionRollbackOnly(failure);
          }
          closeSpeculativeLeaf(comboLeaf, failure);
          throw failure;
        }
      }
    }

    // K's fresh single-entry leaf page — its own R(S)-subtree root.
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    putFreshSingleEntryOrThrow(keyLeaf, keySlice, valueSlice);
    PageReference newLeafRef = null;
    try {
      newLeafRef = swizzle(keyLeaf);

      if (singleEntry && leafEntry) {
        // The affected subtree is the descended leaf page itself — pair it with K's leaf under a
        // BiNode on beta and integrate at the leaf's depth. The leaf needs a fresh reference:
        // integrate's materialize cases re-point the leaf's own spine slot, and aliasing it would
        // make a page its own descendant (a cycle).
        //
        // Canonical-cut guard. Binna's BiNode pairing at beta IS the R(S) recursion step only when
        // beta is the MSDB of the union {leaf ∪ K} — single-TID leaves satisfy that by
        // construction, but a multi-value leaf buckets keys across bits the trie never
        // discriminated, so its internal spread can reach a bit MORE significant than beta (beta
        // is computed against the leaf's discriminated prefix, not its content). Two disqualifying
        // shapes, both discharged by splitting the leaf at the union's own MSDB (the strand
        // discharge — the same R(S) cut, taken at the right bit):
        // (a) the leaf straddles beta itself (holds a key with beta==betaValue) — pairing would
        // re-route that key to K's leaf without migrating it (cross-leaf dup);
        // (b) the leaf's spread crosses a bit above beta — bit-constancy at beta still holds, yet
        // the union's true MSDB lies inside the leaf, so pairing puts K lex-inside the leaf's
        // range (13 of the 19 residual detector heals attributed here as I8/I12 at the
        // integrated node before this guard existed).
        final HOTLeafPage pairLeaf = navResult.leaf();
        final byte[] pairLeafFirst = pairLeaf.getFirstKey();
        final byte[] pairLeafLast = pairLeaf.getEntryCount() > 0
            ? pairLeaf.getKey(pairLeaf.getEntryCount() - 1)
            : null;
        final boolean canonicalCut = pairLeafFirst != null && pairLeafLast != null
            && HOTBulkBuilder.msdb(Arrays.compareUnsigned(keySlice, pairLeafFirst) < 0
                ? keySlice
                : pairLeafFirst,
                Arrays.compareUnsigned(keySlice, pairLeafLast) > 0
                    ? keySlice
                    : pairLeafLast) == beta
            && pairLeaf.isBitConstantAtAbsBit(beta) == 1 - betaValue;
        if (!canonicalCut) {
          keyLeaf.close();
          if (strandDischargeSplitIntegrate(navResult, keySlice, valueSlice)) {
            return true;
          }
          return false; // total complete-frontier splice copies only the boundary leaf
        }
        if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, pathDepth, beta)) {
          keyLeaf.close();
          return false;
        }
        // Direction-1-dual pre-guard: the pair keeps the leaf's slot, so K becomes the slot's new
        // minimum (betaValue == 0) or maximum (betaValue == 1). Subset routing brought K here, but
        // subset routing does not imply lex position — if K falls outside the slot's boundary with
        // its neighbour, the pairing would break I8/I12 (the shape the impossibility analysis
        // proves no narrow primitive fixes). Detect it before splicing and delegate to the complete
        // frontier instead of publishing a violation.
        if (pathDepth > 0) {
          final HOTIndirectPage pairParent = pathNodes[pathDepth - 1];
          final int leafSlot = childSlots[pathDepth - 1];
          if (betaValue == 0 && leafSlot > 0) {
            final byte[] prevLast = lastKeyOfSubtree(pairParent.getChildReference(leafSlot - 1));
            if (prevLast != null && Arrays.compareUnsigned(prevLast, keySlice) >= 0) {
              keyLeaf.close();
              return false; // K sorts at or below the previous sibling: complete frontier
            }
          } else if (betaValue == 1 && leafSlot + 1 < pairParent.getNumChildren()) {
            final byte[] nextFirst = firstKeyOfSubtree(pairParent.getChildReference(leafSlot + 1));
            if (nextFirst != null && Arrays.compareUnsigned(keySlice, nextFirst) >= 0) {
              keyLeaf.close();
              return false; // K sorts at or above the next sibling: complete frontier
            }
          }
        }
        // A height-1 parent fold keeps the old leaf's logical identity live under the rebuilt parent;
        // a copied PageReference satisfies integrate's no-wrapper-alias precondition without minting
        // a second page identity or copying 64 KiB. Root and mixed-height/intermediate placement
        // publish at the source reference itself, so sharing its handle there would make the child
        // resolve to its containing pair root (a cycle). Those two shapes need an independent physical
        // full-page copy with a fresh page key, after which the old source can be uniquely retired.
        final boolean preserveSourceIdentity = pathDepth > 0 && pathNodes[pathDepth - 1].getHeight() == 1;
        HOTLeafPage copiedSourceLeaf = null;
        PageReference copiedSourceRef = null;
        final PageReference leafRef;
        final HOTIncrementalInsert.BiNode biNode;
        try {
          if (preserveSourceIdentity) {
            leafRef = new PageReference(navResult.leafRef());
            // The copy constructor deliberately drops a swizzle when a durable/TIL identity exists.
            // integrate's fixed-fanout height accounting is resolver-free, so lend it the already
            // guarded resident leaf; PageReference.getPage clears this pointer if its owner later
            // retires the instance, while the shared handle remains the authoritative identity.
            leafRef.setPage(navResult.leaf());
          } else {
            copiedSourceLeaf = navResult.leaf().copyAsFreshPage(pageKeyAllocator.getAsLong(), revision);
            copiedSourceRef = swizzle(copiedSourceLeaf);
            leafRef = copiedSourceRef;
          }
          biNode = betaValue == 1
              ? new HOTIncrementalInsert.BiNode(beta, 1, leafRef, newLeafRef)
              : new HOTIncrementalInsert.BiNode(beta, 1, newLeafRef, leafRef);
        } catch (final RuntimeException | Error constructionFailure) {
          if (copiedSourceLeaf != null) {
            closeFreshLeafUnlessLogOwned(copiedSourceRef, copiedSourceLeaf, constructionFailure);
          }
          throw constructionFailure;
        }
        boolean published = false;
        try {
          ensurePathChildrenLoaded(pathNodes, navResult.pathDepth());
          final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(pathNodes,
              buildSpineRefs(navResult), childSlots, pathDepth, biNode, revision, pageKeyAllocator);
          published = true;
          lastDispatchHandler = "h:pair-leaf";
          registerFreshSubtree(result.touchedRef());
          if (!preserveSourceIdentity) {
            retireReplacedLeaf(navResult.leafRef(), result.touchedRef(),
                TransactionIntentLog.RELEASE_SITE_BRANCH_LEAF_PAIR);
          }
          return true;
        } catch (final RuntimeException | Error failure) {
          if (published) {
            markTransactionRollbackOnly(failure);
          }
          closeFreshLeafUnlessLogOwned(newLeafRef, keyLeaf, failure);
          if (copiedSourceLeaf != null) {
            closeFreshLeafUnlessLogOwned(copiedSourceRef, copiedSourceLeaf, failure);
          }
          throw failure;
        }
      }

      if (singleEntry) {
        // Binna's "false positive": the single affected entry is a boundary node, not the leaf —
        // the MSB-stack insert depth was one level too shallow. beta is more significant than every
        // discriminative bit of that child, so K joins it as a new partition root.
        final int childDepth = insertDepth + 1;
        final HOTIndirectPage child = pathNodes[childDepth];
        if (child.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
          boolean published = false;
          try {
            final HOTIndirectPage newChild = HOTIncrementalInsert.addEntryWithInsertInfo(child, beta, betaValue, 0,
                child.getNumChildren(), 0, newLeafRef, child.getHeight(), revision, pageKeyAllocator);
            if (branchAddStrandsExisting(child, newChild, keySlice)) {
              keyLeaf.close();
              return dischargeStrandViaLeafFrontier(navResult, child, newChild, childDepth, keySlice, valueSlice);
            }
            if (nodeStructurallyMalformed(newChild)) {
              if (trySpliceMalformedComboLeafFrontier(navResult, child, newChild, childDepth, keyLeaf, keySlice)) {
                return true;
              }
              keyLeaf.close();
              BRANCH_COMPLETE_FRONTIER.incrementAndGet();
              return false; // I8-unsafe combo-add -> complete structural frontier
            }
            pathRefs[childDepth].setPage(newChild);
            published = true;
            lastDispatchHandler = "h:boundary-addentry";
            registerFreshSubtree(pathRefs[childDepth]);
            return true;
          } catch (final RuntimeException | Error failure) {
            if (published) {
              markTransactionRollbackOnly(failure);
            }
            closeFreshLeafUnlessLogOwned(newLeafRef, keyLeaf, failure);
            throw failure;
          }
        }
        // The boundary node is full — wrap it whole under a BiNode on beta and integrate. It needs
        // a fresh reference (integrate may re-point the boundary node's own spine slot).
        if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, childDepth, beta)) {
          keyLeaf.close();
          return false;
        }
        // Stranding guard: the whole boundary child goes on beta's (1-betaValue) side; if its
        // subtree holds a key with beta==betaValue, that key would strand. Delegate instead.
        try {
          if (subtreeHasKeyWithBit(pathRefs[childDepth], beta, betaValue, keySlice)) {
            keyLeaf.close();
            STRAND_COMPLETE_FRONTIER.incrementAndGet();
            return false; // BiNode-wrap strand: complete structural frontier
          }
        } catch (MutationTraversalRefusal refusal) {
          closeSpeculativeLeaf(keyLeaf, refusal);
          throw refusal;
        }
        final PageReference childRef = swizzle(child);
        final int biHeight = child.getHeight() + 1;
        final HOTIncrementalInsert.BiNode biNode = betaValue == 1
            ? new HOTIncrementalInsert.BiNode(beta, biHeight, childRef, newLeafRef)
            : new HOTIncrementalInsert.BiNode(beta, biHeight, newLeafRef, childRef);
        boolean published = false;
        try {
          ensurePathChildrenLoaded(pathNodes, navResult.pathDepth());
          final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(pathNodes,
              buildSpineRefs(navResult), childSlots, childDepth, biNode, revision, pageKeyAllocator);
          published = true;
          lastDispatchHandler = "h:wrap-full";
          registerFreshSubtree(result.touchedRef());
          return true;
        } catch (final RuntimeException | Error failure) {
          if (published) {
            markTransactionRollbackOnly(failure);
          }
          closeFreshLeafUnlessLogOwned(newLeafRef, keyLeaf, failure);
          throw failure;
        }
      }

      // affectedCount > 1 — K's leaf is folded into d*'s block beside the affected subtree. beta
      // becomes a new discriminative bit; the node keeps its height (a leaf child never raises it).
      boolean published = false;
      try {
        final HOTIndirectPage newNode =
            HOTIncrementalInsert.addEntryWithInsertInfo(node, beta, betaValue, info.firstAffected(),
                info.affectedCount(), info.subtreePrefix(), newLeafRef, node.getHeight(), revision, pageKeyAllocator);
        if (branchAddStrandsExisting(node, newNode, keySlice)) {
          keyLeaf.close();
          return dischargeStrandViaLeafFrontier(navResult, node, newNode, insertDepth, keySlice, valueSlice);
        }
        if (nodeStructurallyMalformed(newNode)) {
          if (trySpliceMalformedComboLeafFrontier(navResult, node, newNode, insertDepth, keyLeaf, keySlice)) {
            return true;
          }
          keyLeaf.close();
          BRANCH_COMPLETE_FRONTIER.incrementAndGet();
          return false; // I8-unsafe combo-add -> complete structural frontier
        }
        pathRefs[insertDepth].setPage(newNode);
        published = true;
        lastDispatchHandler = "h:fold-multi";
        registerFreshSubtree(pathRefs[insertDepth]);
        return true;
      } catch (final RuntimeException | Error failure) {
        if (published) {
          markTransactionRollbackOnly(failure);
        }
        closeFreshLeafUnlessLogOwned(newLeafRef, keyLeaf, failure);
        throw failure;
      }
    } catch (final RuntimeException | Error failure) {
      // Inner publication catches perform their own exact cleanup. This outer owner covers the
      // allocation-to-handler seams (swizzle, endpoint/order guards and BiNode construction) that
      // run before those catches; it retries only when no inner cleanup completed.
      closeFreshLeafUnlessLogOwned(newLeafRef, keyLeaf, failure);
      throw failure;
    }
  }

  /**
   * Branch insert into a <em>full</em> compound node — Binna's {@code insertNewValue} full-node
   * {@code split} ({@code HOTSingleThreaded.hpp:475}). {@code beta} is a genuinely new discriminative
   * bit (the descent reached this node via {@code !betaIsDiscBit}) and the affected subtree spans
   * more than one child but not the whole node, so Binna's {@code split} applies:
   * {@link HOTIncrementalInsert#splitIndirectWithEntry} partitions the node at its own MSB while
   * folding the new key's leaf into the affected half, and the resulting {@code BiNode} on the node's
   * MSB is integrated where the node sat (the integration may cascade further up).
   */
  private boolean branchSplitFullNode(LeafNavigationResult navResult, HOTIncrementalInsert.InsertInfo info,
      HOTIndirectPage node, int insertDepth, int beta, int betaValue, byte[] keySlice, byte[] valueSlice) {
    final int revision = storageEngineWriter.getRevisionNumber();
    // splitIndirectWithEntry returns a BiNode on node.MSB; pre-check the integrate cascade for an
    // un-mergeable cross-level overlap and delegate to the complete frontier if found.
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(), insertDepth,
        node.getMostSignificantBitIndex())) {
      return false;
    }
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    putFreshSingleEntryOrThrow(keyLeaf, keySlice, valueSlice);
    boolean published = false;
    try {
      ensurePathChildrenLoaded(navResult.pathNodes(), navResult.pathDepth());
      final HOTIncrementalInsert.BiNode biNode = HOTIncrementalInsert.splitIndirectWithEntry(node, info, beta,
          betaValue, swizzle(keyLeaf), revision, pageKeyAllocator);
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(navResult.pathNodes(),
          buildSpineRefs(navResult), navResult.pathChildIndices(), insertDepth, biNode, revision, pageKeyAllocator);
      published = true;
      lastDispatchHandler = "h:branch-integrate";
      registerFreshSubtree(result.touchedRef());
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
      }
      closeSpeculativeLeaf(keyLeaf, failure);
      throw failure;
    }
  }

  /**
   * Branch insert into a <em>full</em> compound node at an <em>existing</em> discriminative bit —
   * Binna's {@code betaIsDiscBit + full d*} case
   * ({@code docs/HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md} §4.1). The case decomposes into
   * already-verified primitives:
   * <ol>
   * <li>{@link HOTIncrementalInsert#splitIndirect} the full node at its {@code node.MSB} into a
   * {@code BiNode} of two not-full halves.</li>
   * <li>K routes (by {@code node.MSB}) into one half.</li>
   * <li>In that half, dispatch on whether {@code beta} survived {@code compressHalf} (the crux the
   * prior attempts missed):
   * <ul>
   * <li>{@code beta} survived (still a disc bit of the half) →
   * {@link HOTIncrementalInsert#addChildAtCombination} (still {@code betaIsDiscBit} for the half —
   * Q1-verified routing-correct).</li>
   * <li>{@code beta} dropped (constant across the half) → {@code beta} is a genuinely new disc bit
   * for the half → {@link HOTIncrementalInsert#addEntryWithInsertInfo} (the existing multi-affected
   * branch primitive).</li>
   * </ul>
   * </li>
   * <li>{@link HOTIncrementalInsert#integrate} the {@code BiNode} at {@code insertDepth} — the
   * standard capacity cascade.</li>
   * </ol>
   *
   * <p>
   * {@code BetaIsDiscBitRoutingProbe} Q4 verified 74/74 cases route strictly correctly, including
   * 40-byte MultiMask {@code widespan} keys. The prior 7 decomposition attempts failed by using
   * {@code addChildAtCombination} unconditionally — the β-survival dispatch is mandatory.
   *
   * <p>
   * C1 (1:31 lone-child half) and C2 ({@code comboPartial} collision) return {@code false} before
   * publication so the shared complete-frontier primitive handles them.
   *
   * @return {@code true} iff the key was inserted incrementally
   */
  /**
   * Pre-check whether {@link HOTIncrementalInsert#integrate}'s cascade — starting at
   * {@code currentDepth} with a BiNode on {@code biNodeBeta} — will fold cleanly, or whether any
   * level requires an un-mergeable cross-level-overlap fold (which would otherwise throw out of
   * integrate). Returns {@code false} before publication so the caller uses the complete frontier.
   *
   * <p>
   * <b>Crash-safety.</b> The walk is conservative: it never returns {@code true} when integrate would
   * throw. It checks {@link HOTIncrementalInsert#canMergeBiNodeAtExistingDiscBit} at every level
   * whose mask contains the running β. The β evolution exactly matches integrate's full-node cascade
   * (β becomes {@code parent.MSB} after a split). It does not model integrate's
   * intermediate-placement short-circuit (a height comparison) — skipping it can only choose the
   * complete-frontier arm unnecessarily, never miss a crash, because integrate never folds at an
   * intermediate level.
   *
   * @param pathNodes the spine, root-to-leaf
   * @param childSlots the child slot taken at each spine node
   * @param currentDepth the depth at which the initial BiNode integrates
   * @param biNodeBeta the initial BiNode's discriminative bit
   * @return {@code true} iff the integrate cascade folds without an un-mergeable overlap
   */
  private boolean canIntegrateBiNodeCleanly(HOTIndirectPage[] pathNodes, int[] childSlots, int currentDepth,
      int biNodeBeta) {
    int beta = biNodeBeta;
    int depth = currentDepth;
    while (depth > 0) {
      final HOTIndirectPage parent = pathNodes[depth - 1];
      if (parent.isDiscriminativeBit(beta)
          && !HOTIncrementalInsert.canMergeBiNodeAtExistingDiscBit(parent, beta, childSlots[depth - 1])) {
        return false;
      }
      if (parent.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
        return true; // addEntry/merge fits; cascade terminates
      }
      beta = parent.getMostSignificantBitIndex(); // parent full → split → cascade with parent.MSB
      depth--;
    }
    return true; // reached the root
  }

  private boolean branchFullNodeAtExistingBit(LeafNavigationResult navResult, HOTIndirectPage node, int insertDepth,
      int beta, int betaValue, byte[] keySlice, byte[] valueSlice) {
    final int revision = storageEngineWriter.getRevisionNumber();
    // splitIndirect produces a BiNode on node.MSB; pre-check the integrate cascade for an
    // un-mergeable cross-level overlap and delegate to the complete frontier if found.
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(), insertDepth,
        node.getMostSignificantBitIndex())) {
      return false;
    }
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    putFreshSingleEntryOrThrow(keyLeaf, keySlice, valueSlice);
    boolean published = false;
    try {
      ensurePathChildrenLoaded(navResult.pathNodes(), navResult.pathDepth());

      // 1. Split the full node at its own MSB into BiNode(node.MSB, leftHalf, rightHalf).
      final HOTIncrementalInsert.BiNode split = HOTIncrementalInsert.splitIndirect(node, revision, pageKeyAllocator);

      // 2. K routes by node.MSB into one half.
      final int nodeMsb = node.getMostSignificantBitIndex();
      final boolean kMsbBit = HOTBulkBuilder.bitAt(keySlice, nodeMsb);
      final PageReference halfRef = kMsbBit
          ? split.right()
          : split.left();
      if (!(halfRef.getPage() instanceof HOTIndirectPage half)) {
        // C1 — K's half is a lone child (1:31 split, the half is the bare child reference).
        // The half is not a compound frontier; let the shared complete-frontier arm place K.
        keyLeaf.close();
        return false;
      }

      // 3. In the half: dispatch on whether beta survived compressHalf.
      final int[] halfDiscBits = HOTIncrementalInsert.discriminativeBits(half);
      final int betaCol = Arrays.binarySearch(halfDiscBits, beta);
      final int childIdx = half.findChildIndex(keySlice);
      if (childIdx < 0) {
        // Defensive — a canonical half's descent should always find a child.
        keyLeaf.close();
        return false;
      }
      final HOTIncrementalInsert.InsertInfo halfInfo = HOTIncrementalInsert.getInsertInformation(half, childIdx, beta);
      final PageReference keyLeafRef = swizzle(keyLeaf);
      final HOTIndirectPage foldedHalf;
      if (betaCol >= 0) {
        // beta survived as a disc bit of the half — still betaIsDiscBit for the half.
        final int comboPartial = halfInfo.subtreePrefix() | (betaValue == 1
            ? 1 << (halfDiscBits.length - 1 - betaCol)
            : 0);
        if (findChildSlotByPartial(half, comboPartial) >= 0) {
          // C2: the split did not make K a new combination. K belongs below the child selected by
          // the half's own descent; finish that descent, then recompress only the two freshly split
          // parent pages from their (now updated) child references. This is page-local structural
          // maintenance—not entry collection or a subtree rebuild.
          keyLeaf.close();
          return directionOneIntoSplitHalf(navResult, node, insertDepth, split, half, kMsbBit, childIdx, keySlice,
              valueSlice, revision);
        }
        foldedHalf = HOTIncrementalInsert.addChildAtCombination(half, comboPartial, keyLeafRef, half.getHeight(),
            revision, pageKeyAllocator);
      } else {
        // beta was dropped from the half (constant across it) — beta is genuinely new to the
        // half; addEntryWithInsertInfo folds it as a new disc bit.
        foldedHalf = HOTIncrementalInsert.addEntryWithInsertInfo(half, beta, betaValue, halfInfo.firstAffected(),
            halfInfo.affectedCount(), halfInfo.subtreePrefix(), keyLeafRef, half.getHeight(), revision,
            pageKeyAllocator);
      }
      // Multi-entry-leaf stranding guard ([[hot-multientry-leaf-quirks]] #1): the fold added K's
      // single-key leaf to the half; if an existing key in the half would now route to it, the half
      // straddles the fold bit. Discard the uncommitted split and splice the canonical leaf frontier.
      try {
        if (branchAddStrandsExisting(half, foldedHalf, keySlice)) {
          keyLeaf.close();
          return dischargeStrandViaLeafFrontier(navResult, half, foldedHalf, -1, keySlice, valueSlice);
        }
      } catch (MutationTraversalRefusal refusal) {
        closeSpeculativeLeaf(keyLeaf, refusal);
        throw refusal;
      }
      if (nodeStructurallyMalformed(foldedHalf)) {
        keyLeaf.close();
        BRANCH_COMPLETE_FRONTIER.incrementAndGet();
        return false; // I8-unsafe combo-add -> complete structural frontier
      }
      halfRef.setPage(foldedHalf);

      // 4. Integrate the split BiNode at insertDepth — the standard capacity cascade.
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(navResult.pathNodes(),
          buildSpineRefs(navResult), navResult.pathChildIndices(), insertDepth, split, revision, pageKeyAllocator);
      published = true;
      lastDispatchHandler = "h:combo-site2-fold";
      registerFreshSubtree(result.touchedRef());
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
      }
      closeSpeculativeLeaf(keyLeaf, failure);
      throw failure;
    }
  }

  /** Complete a full-node C2 collision by descending into the selected child of the split half. */
  private boolean directionOneIntoSplitHalf(final LeafNavigationResult navResult, final HOTIndirectPage originalNode,
      final int insertDepth, final HOTIncrementalInsert.BiNode split, final HOTIndirectPage half,
      final boolean rightHalf, final int affectedIdx, final byte[] keySlice, final byte[] valueSlice,
      final int revision) {
    if (!isSplitHalfDirectionOneSafe(navResult, insertDepth, split, half, rightHalf, affectedIdx, keySlice)) {
      DIRECTION_ONE_FALLBACK.incrementAndGet();
      return false;
    }

    boolean subInsertPublished = false;
    try {
      if (!subInsertAt(half.getChildReference(affectedIdx), keySlice, keySlice.length, valueSlice, valueSlice.length)) {
        return false;
      }
      // subInsertAt CoWs/logs the selected child reference. From here onward a failure cannot fall
      // back to another structural handler: the child already contains K even though the refreshed
      // split parent may not yet have been integrated.
      subInsertPublished = true;

      // TIL.put clears ref.page. splitIndirect computes each compressed half's exact height from the
      // resident child pages, so re-resolve this one fixed-fanout block before recompressing it. This
      // is O(MAX_NODE_ENTRIES), never a subtree scan.
      ensureNodeChildrenLoaded(originalNode);

      // subInsertAt may have re-pointed (or grown) the affected child. Re-compress both halves from
      // the original full node's now-current child references so their heights and masks describe the
      // post-insert structure exactly. The first speculative split was never published or registered.
      final HOTIncrementalInsert.BiNode refreshedSplit =
          HOTIncrementalInsert.splitIndirect(originalNode, revision, pageKeyAllocator);
      final HOTIncrementalInsert.IntegrationResult result =
          HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
              insertDepth, refreshedSplit, revision, pageKeyAllocator);
      lastDispatchHandler = "h:combo-site2-d1";
      registerFreshSubtree(result.touchedRef());
      DIRECTION_ONE_SUBINSERT.incrementAndGet();
      FULL_EXISTING_BIT_DIRECTION_ONE_SUBINSERT.incrementAndGet();
      return true;
    } catch (final RuntimeException | Error failure) {
      if (subInsertPublished) {
        markTransactionRollbackOnly(failure);
      }
      throw failure;
    }
  }

  /**
   * Apply the leaf-underflow rule to one fixed-fanout HOT block: the direct parent on the route just
   * updated. This deliberately never descends into an indirect child. Consequently an ordinary put
   * cannot turn maintenance into a whole-index walk, regardless of index size.
   *
   * <p>
   * Direct children are resolved only for this block and their temporary swizzles are cleared on
   * exit. A changed parent is published once and every merged-away leaf is retired in one bounded
   * batch. The pure consolidation primitive preserves the parent's height, key set, and range, so no
   * ancestor rewrite is required.
   * </p>
   */
  private void consolidateLeafParent(final LeafNavigationResult route) {
    final int pathDepth = route.pathDepth();
    if (pathDepth == 0) {
      return;
    }
    final int parentDepth = pathDepth - 1;
    final PageReference parentRef = route.pathRefs()[parentDepth];
    final HOTIndirectPage parent = route.pathNodes()[parentDepth];
    final int childCount = parent.getNumChildren();
    final PageReference[] borrowedRefs = new PageReference[childCount];
    final Page[] borrowedPages = new Page[childCount];
    int borrowedCount = 0;
    HOTIndirectPage freshConsolidated = null;
    try {
      // consolidateNodeLeaves considers only direct leaf/leaf pairs. Swizzle this one bounded block
      // so it can distinguish those pairs without recursing into any indirect child.
      for (int i = 0; i < childCount; i++) {
        final PageReference childRef = parent.getChildReference(i);
        if (childRef == null || childRef.getPage() != null) {
          continue;
        }
        final Page child = resolveHOTPageForTraversal(childRef);
        if (child != null) {
          childRef.setPage(child);
          borrowedRefs[borrowedCount] = childRef;
          borrowedPages[borrowedCount++] = child;
        }
      }

      final List<PageReference> orphanedLeaves = new ArrayList<>(HOTIndirectPage.MAX_NODE_ENTRIES);
      final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(parent, CONSOLIDATION_TARGET,
          storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator, orphanedLeaves);
      if (consolidated == parent) {
        return;
      }
      freshConsolidated = consolidated;
      parentRef.setPage(consolidated);
      lastDispatchHandler = "h:consolidate-local-parent";
      registerFreshSubtree(parentRef);
      storageEngineWriter.getLog()
                         .releaseOrphanedHOTLeaves(indexScope(), parentRef, orphanedLeaves,
                             TransactionIntentLog.RELEASE_SITE_CONSOLIDATE);
    } catch (final RuntimeException | Error failure) {
      // Consolidation runs only after the primary dispatch has already mutated the index. Even a
      // failure before this maintenance pass publishes its parent therefore leaves a transaction
      // whose preceding put must not be committed as though the maintenance cadence completed.
      markTransactionRollbackOnly(failure);
      if (freshConsolidated != null) {
        // The shared publication hook runs before post-order TIL registration. At that boundary the
        // consolidated parent can already own one or more fresh off-heap replacement leaves, while
        // its unchanged children remain durable/TIL-owned. Release exactly the still-local children;
        // the identity guards also make this correct after a later partial-registration failure.
        closeUnregisteredFreshChildren(freshConsolidated, 0, failure);
      }
      throw failure;
    } finally {
      for (int i = 0; i < borrowedCount; i++) {
        borrowedRefs[i].clearPageIfSame(borrowedPages[i]);
      }
    }
  }

  /** Wrap a freshly created page in a new {@link PageReference} with the page swizzled in. */
  private static PageReference swizzle(Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  /** Per-{@code (invariant|handler)} tally for fail-closed post-publication validation failures. */
  public static final ConcurrentHashMap<String, AtomicLong> STRUCTURAL_VALIDATION_TALLY = new ConcurrentHashMap<>();

  /**
   * The structural handler that produced a validated tree state. Written as a constant-string field
   * store at each dispatch site; read only when validation fails.
   */
  protected String lastDispatchHandler = "?";

  /** Disable hook for post-dispatch invariant validation (default ON — correctness first). */
  private static final boolean VALIDATE_STRUCTURAL_MUTATIONS = !Boolean.getBoolean("hot.validate.structural.disable");

  /**
   * Full defense-in-depth detector passes skipped because their scope exceeded the universal bounded
   * source footprint. The route-local mandatory guard still runs and every observed defect remains
   * fail-closed.
   */
  public static final AtomicLong STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED = new AtomicLong();

  /**
   * Published structural candidates rejected by the defense-in-depth validator. Must stay zero.
   */
  public static final AtomicLong STRUCTURAL_VALIDATION_FAILURE = new AtomicLong();

  /**
   * Validate the current key route after publication. This method never repairs: every structural
   * candidate must have been proved before publication, so a violation poisons the transaction.
   */
  private void validatePublishedStructuralPath(byte[] keySlice) {
    try {
      PageReference cur = rootReference;
      if (cur == null) {
        throw new IllegalStateException("HOT structural path validation has no root reference");
      }
      for (int depth = 0; depth <= MAX_PATH_DEPTH; depth++) {
        final Page page = resolveHOTPageForTraversal(cur);
        if (page instanceof HOTLeafPage) {
          return; // the one valid terminus: the current route reached a live leaf
        }
        if (!(page instanceof HOTIndirectPage indirect)) {
          throw new IllegalStateException(
              "HOT structural path validation cannot resolve a HOT page at depth " + depth + " (refKey=" + cur.getKey()
                  + ", logKey=" + cur.getLogKey() + ", generation=" + cur.getActiveTilGeneration() + ')');
        }
        if (nodeStructurallyMalformed(indirect)) {
          STRUCTURAL_VALIDATION_FAILURE.incrementAndGet();
          throw new IllegalStateException(
              "HOT published structural path is malformed at page " + indirect.getPageKey());
        }
        final int childIndex = indirect.findChildIndex(keySlice);
        if (childIndex < 0 || childIndex >= indirect.getNumChildren()) {
          throw new IllegalStateException("HOT structural path validation has no valid child at depth " + depth
              + " for indirect page " + indirect.getPageKey() + " (childIndex=" + childIndex + ')');
        }
        cur = indirect.getChildReference(childIndex);
        if (cur == null) {
          throw new IllegalStateException("HOT structural path validation found a null child " + childIndex
              + " at depth " + depth + " on indirect page " + indirect.getPageKey());
        }
      }
      throw new IllegalStateException("HOT structural path validation exceeded the maximum depth of " + MAX_PATH_DEPTH);
    } catch (final RuntimeException | Error failure) {
      // This guard is reached only after the dispatch has published the primary mutation. Any
      // inability to prove the current ancestor route safe is therefore transaction-fatal.
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  /** Strand cases delegated to the complete structural-frontier primitive. */
  public static final AtomicLong STRAND_COMPLETE_FRONTIER = new AtomicLong();
  /**
   * Strands discharged canonically: the descended leaf held keys on both sides of the branch bit, so
   * the union {@code leaf ∪ {K}} was split at its own key-set MSDB and the BiNode integrated at the
   * leaf's depth — Binna's insert, leaving no straddled leaf behind.
   */
  public static final AtomicLong STRAND_SPLIT_INTEGRATE = new AtomicLong();
  /** Off-path strands discharged by the two-leaf migration ({@link #tryTwoLeafMigration}). */
  public static final AtomicLong STRAND_TWO_LEAF_MIGRATE = new AtomicLong();
  /**
   * Branch combo-adds delegated to the complete structural-frontier primitive because the direct
   * candidate is structurally unsafe.
   */
  public static final AtomicLong BRANCH_COMPLETE_FRONTIER = new AtomicLong();
  /** Malformed combo-adds handled by a complete direct-leaf frontier splice. */
  public static final AtomicLong BRANCH_LOCAL_LEAF_FRONTIER_SPLICE = new AtomicLong();

  /**
   * Surgical strand discharge ({@code O(one leaf + path)}). When a branch-add stranding guard fires
   * and <em>all</em> strandable keys are confined to the descended leaf {@code
   * navResult.leaf()}, splice just that leaf together with the new key {@code K} into a canonical
   * mini-HOT ({@link HOTBulkBuilder}) and splice it into the leaf's slot, propagating height/partial
   * up the spine. Returns {@code true} when so handled; {@code false} delegates to the complete
   * structural-frontier primitive.
   *
   * <p>
   * Correctness: K and the strandable keys all route (via {@code node.findChildIndex}) to the
   * descended leaf's slot, so canonicalizing {@code leaf ∪ {K}} in that frontier preserves routing
   * and re-discriminates them straddle-free (Fact R1). 99%+ of strands (empirically) hit this path.
   */
  /**
   * Canonical strand discharge — Binna's insert applied to a leaf that spans the branch bit. The
   * strand state ("the descended leaf holds keys on both sides of {@code beta}") means the canonical
   * R(S) cut runs <em>through</em> the leaf: the union {@code leaf ∪ {K}} splits at its own key-set
   * MSDB into two complete R(S) halves (Fact R1), and the resulting BiNode integrates at the leaf's
   * depth exactly as a full-leaf overflow does. Both primitives are the merge path's — the single
   * most exercised pipeline in this writer.
   *
   * <p>
   * The integrate cascade is pre-checked ({@link #canIntegrateBiNodeCleanly}) <em>before</em> the
   * split allocates pages, so a {@code false} return leaves no half-built state and no leaked pages;
   * the caller falls back to the splice. An exception escaping {@code integrate} after a clean
   * pre-check is a real bug, exactly as on the merge path, and propagates.
   *
   * @return {@code true} iff the strand was discharged canonically
   */
  private boolean strandDischargeSplitIntegrate(LeafNavigationResult navResult, byte[] keySlice, byte[] valueSlice) {
    final HOTLeafPage leaf = navResult.leaf();
    if (!leaf.canSplit()) {
      return false;
    }
    // The union's MSDB, from its extremes: leaf entries are lex-sorted (I2), so the union's min
    // and max are min/max of (firstKey, lastKey, K). Computable without building the union.
    final byte[] leafFirst = leaf.getFirstKey();
    final byte[] leafLast = leaf.getKey(leaf.getEntryCount() - 1);
    if (leafFirst == null || leafLast == null) {
      return false;
    }
    final byte[] unionMin = Arrays.compareUnsigned(keySlice, leafFirst) < 0
        ? keySlice
        : leafFirst;
    final byte[] unionMax = Arrays.compareUnsigned(keySlice, leafLast) > 0
        ? keySlice
        : leafLast;
    final int unionMsdb = HOTBulkBuilder.msdb(unionMin, unionMax);
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(), navResult.pathDepth(),
        unionMsdb)) {
      return false;
    }
    final int revision = storageEngineWriter.getRevisionNumber();
    final HOTIncrementalInsert.BiNode biNode =
        HOTIncrementalInsert.splitLeafPage(leaf, keySlice, valueSlice, revision, indexType, pageKeyAllocator);
    boolean published = false;
    try {
      ensurePathChildrenLoaded(navResult.pathNodes(), navResult.pathDepth());
      lastDispatchHandler = "h:strand-split-integrate";
      final HOTIncrementalInsert.IntegrationResult result =
          HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
              navResult.pathDepth(), biNode, revision, pageKeyAllocator);
      published = true;
      registerFreshSubtree(result.touchedRef());
      retireReplacedLeaf(navResult.leafRef(), result.touchedRef(), TransactionIntentLog.RELEASE_SITE_STRAND_SPLIT);
      STRAND_SPLIT_INTEGRATE.incrementAndGet();
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
      }
      closeFreshBiNode(biNode, failure);
      throw failure;
    }
  }

  private boolean dischargeStrandViaLeafFrontier(LeafNavigationResult navResult, HOTIndirectPage oldNode,
      HOTIndirectPage newNode, int nodeDepth, byte[] keySlice, byte[] valueSlice) {
    final int newSlot = newNode.findChildIndex(keySlice);
    if (newSlot < 0 || navResult.pathDepth() < 1) {
      STRAND_COMPLETE_FRONTIER.incrementAndGet();
      return false;
    }
    // Reconstruct the exact bounded route-feasible old-page plan. It deliberately excludes the
    // speculative fresh slot (which callers may already have closed) and every old child whose
    // sparse routing cannot reach newSlot. Both discharge arms consume this same immutable plan.
    final RebuildFootprint routingPlan = requireBoundedExistingKeyRoutingTraversal(oldNode, newNode, newSlot, keySlice);
    // (a) On-path: strandable keys confined to the descended leaf -> O(one leaf + path).
    if (strandConfinedToLeaf(routingPlan, newNode, newSlot, keySlice, navResult.leaf().getPageKey())) {
      if (strandDischargeSplitIntegrate(navResult, keySlice, valueSlice)) {
        return true;
      }
      return false; // caller uses the total complete-frontier splice
    }
    // (b) Off-path: strandable keys in a single sibling leaf -> two-leaf migration (split that
    // leaf, fold its matching keys + K into the new child), validated, else the complete frontier.
    if (tryTwoLeafMigration(navResult, newNode, newSlot, nodeDepth, keySlice, valueSlice, routingPlan)) {
      STRAND_TWO_LEAF_MIGRATE.incrementAndGet();
      return true;
    }
    STRAND_COMPLETE_FRONTIER.incrementAndGet();
    return false;
  }

  /**
   * Off-path strand discharge ({@code O(two leaves + node re-encode + path)}). When every key the
   * fresh slot would steal is confined to one direct sibling leaf {@code L_src}, build the fresh
   * child as {@code bulk-build(K ∪ strandable)}, replace {@code L_src} with
   * {@code bulk-build(L_src \ strandable)}, and re-encode the candidate in the same sparse parent
   * coordinates. The route-feasible preflight plan excludes the closed speculative K leaf and every
   * irrelevant sibling; it is reused here without a second tree traversal.
   *
   * <p>
   * Before the sole path-reference publication, the method proves every migrated parent route, local
   * I3/I4/I7/I8/I11/I12, and ancestor propagation. The bulk builder establishes the two fresh
   * subtrees' internal invariants. Unsupported shapes return {@code false} with both fresh trees
   * closed; unexpected pre-publication failures are cleaned and rethrown, while every failure after
   * publication poisons the transaction and is rethrown. On success, the replaced source leaf is
   * retired only after registration and spine propagation complete.
   * </p>
   */
  private boolean tryTwoLeafMigration(LeafNavigationResult navResult, HOTIndirectPage newNode, int comboSlot,
      int nodeDepth, byte[] keySlice, byte[] valueSlice, RebuildFootprint routingPlan) {
    if (nodeDepth < 0 || nodeDepth >= navResult.pathDepth()) {
      return false; // node is not a spliceable path node
    }
    // Identify the unique source slot/leaf and collect the strandable keys; require a single
    // source leaf (so the migration touches exactly one sibling leaf). Strandable keys all have
    // comboPartial ⊆ densePK, so the new child is I5-clean; bulk-build discriminates the rest.
    final long[] info = {-1L, -1L, 1L, 0L}; // {sourceSlot, sourceLeafPageKey, ok, strandCount}
    for (int pageIndex = 0; pageIndex < routingPlan.pages && info[2] == 1L; pageIndex++) {
      if (routingPlan.visitedPages[pageIndex] instanceof HOTLeafPage leaf) {
        collectMigratableLeafKeys(leaf, newNode, comboSlot, keySlice, routingPlan.visitedPageOwnerSlots[pageIndex],
            info);
      }
    }
    if (info[2] != 1L || info[3] == 0L || info[0] < 0) {
      return false; // not a single source leaf
    }
    final int sourceSlot = (int) info[0];
    final long sourceLeafPageKey = info[1];
    final int revision = storageEngineWriter.getRevisionNumber();

    // Build the migrated child = bulk-build(K ∪ strandable). All keys have comboPartial ⊆ densePK,
    // so the child is I5-clean under newNode's mask; bulk-build discriminates them internally.
    final List<HOTBulkBuilder.Entry> childEntries = new ArrayList<>((int) info[3] + 1);
    childEntries.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    final Page sourceLeafPage = resolveHOTPageForTraversal(newNode.getChildReference(sourceSlot));
    if (!(sourceLeafPage instanceof HOTLeafPage sourceLeaf) || sourceLeaf.getPageKey() != sourceLeafPageKey) {
      return false; // source slot is not the single source leaf
    }
    // Capture the bounded source leaf's projection side map before building either replacement.
    // The two fresh roots partition every source key, so the existing two-pass owner resolver can
    // re-home every reference locally before publication. Declining this shape would send an
    // otherwise two-leaf mutation into the wider complete-frontier arm solely because projection
    // stores out-of-line segments.
    final List<CapturedSegmentRef> sourceSegmentRefs = new ArrayList<>(sourceLeaf.segmentRefCount());
    for (final long refKey : sourceLeaf.overflowPageRefKeysSorted()) {
      sourceSegmentRefs.add(new CapturedSegmentRef(refKey, sourceLeaf.getPageReference(refKey)));
    }
    final List<HOTBulkBuilder.Entry> remaining = new ArrayList<>(sourceLeaf.getEntryCount());
    for (int i = 0; i < sourceLeaf.getEntryCount(); i++) {
      final byte[] k = sourceLeaf.getKey(i);
      if (k == null) {
        throw refuseMutationTraversal("two-leaf strand source identification",
            "unreadable key at source leaf " + sourceLeaf.getPageKey() + " slot " + i);
      }
      if (!Arrays.equals(k, keySlice) && newNode.findChildIndex(k) == comboSlot) {
        childEntries.add(new HOTBulkBuilder.Entry(k, sourceLeaf.copyStoredValue(i)));
      } else {
        if (newNode.findChildIndex(k) != sourceSlot) {
          return false; // the replacement would leave a non-migrated key outside its physical owner
        }
        remaining.add(new HOTBulkBuilder.Entry(k, sourceLeaf.copyStoredValue(i)));
      }
    }
    if (childEntries.size() != info[3] + 1 || remaining.isEmpty()) {
      return false; // source-leaf removal needs the complete structural frontier
    }
    childEntries.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> childDeduped = dedupMergeEntries(childEntries);

    final PageReference oldSourceRef = newNode.getChildReference(sourceSlot);
    if (oldSourceRef == null) {
      throw refuseMutationTraversal("two-leaf strand source identification",
          "null source reference at candidate slot " + sourceSlot);
    }

    Page childRoot = null;
    Page sourceRoot = null;
    PageReference candidateRef = null;
    boolean published = false;
    try {
      childRoot = HOTBulkBuilder.build(childDeduped, revision, indexType, pageKeyAllocator).rootPage();
      sourceRoot = HOTBulkBuilder.build(remaining, revision, indexType, pageKeyAllocator).rootPage();

      // Re-encode newNode with the same sparse coordinates. Only the fresh slot and the direct
      // source-leaf slot change ownership; every other reference remains shared. Recompute height
      // from the actual child roots because K plus a full source leaf can make the migrated child
      // split even when the speculative K-only child was a leaf.
      final int n = newNode.getNumChildren();
      final PageReference[] children = new PageReference[n];
      int maxChildHeight = 0;
      for (int i = 0; i < n; i++) {
        final Page childPage;
        if (i == comboSlot) {
          childPage = childRoot;
          children[i] = swizzle(childRoot);
        } else if (i == sourceSlot) {
          childPage = sourceRoot;
          children[i] = swizzle(sourceRoot);
        } else {
          final PageReference childRef = newNode.getChildReference(i);
          if (childRef == null) {
            throw refuseMutationTraversal("two-leaf candidate invariant validation",
                "null retained child reference at slot " + i);
          }
          childPage = resolveHOTPageForTraversal(childRef);
          if (childPage == null || childPage.isClosed()) {
            throw refuseMutationTraversal("two-leaf candidate invariant validation",
                "unresolvable retained child at slot " + i);
          }
          children[i] = childRef;
        }
        final int childHeight = childPage instanceof HOTIndirectPage childIndirect
            ? childIndirect.getHeight()
            : 0;
        maxChildHeight = Math.max(maxChildHeight, childHeight);
      }

      // Keep newNode's original SPARSE partials: the new children's keys still route by them
      // (comboPartial ⊆ migrated densePK; s_src.partial ⊆ remaining densePK). Recomputing from
      // firstKeys would yield dense PEXT values that break Binna's I4 (leftmost partial = 0).
      final int[] partials = newNode.getPartialKeysRef().clone();
      final int[] discBits = HOTIncrementalInsert.discriminativeBits(newNode);
      final HOTIndirectPage candidate =
          HOTBulkBuilder.assembleIndirect(discBits, partials, children, maxChildHeight + 1, revision, pageKeyAllocator);
      // From this point on candidateRef is the single cleanup owner for both replacement roots.
      // registerFreshSubtree transfers those roots to the TIL one leaf at a time, so a later
      // failure must stop at every child reference whose durable/log identity proves that transfer
      // completed. Closing childRoot/sourceRoot directly after a partial registration would close
      // a frame that the TIL already owns.
      candidateRef = swizzle(candidate);
      reattachSegmentRefs(candidate, sourceSegmentRefs);
      final Consumer<HOTIndirectPage> afterReattachTestHook = twoLeafMigrationAfterReattachTestHook;
      if (afterReattachTestHook != null) {
        afterReattachTestHook.accept(candidate);
      }

      // The bulk-built replacements are canonical internally. Validate their exact parent routes,
      // the candidate's local I3/I4/I7/I8/I11/I12 invariants, and the entire ancestor propagation
      // before the sole publication boundary. This is O(two leaves + one node + path); it never
      // traverses unrelated descendant subtrees or turns into a whole-node rebuild.
      for (int i = 0; i < childDeduped.size(); i++) {
        if (candidate.findChildIndex(childDeduped.get(i).key()) != comboSlot) {
          closeFreshHOTSubtree(childRoot);
          closeFreshHOTSubtree(sourceRoot);
          return false;
        }
      }
      if (!hasStrictlyAscendingPartials(candidate) || candidate.getPartialKey(0) != 0
          || !partialsFitDiscriminativeWidth(candidate, discBits) || nodeStructurallyMalformed(candidate)
          || !canPropagateIncrementalSplice(navResult, nodeDepth, candidateRef)) {
        closeFreshHOTSubtree(childRoot);
        closeFreshHOTSubtree(sourceRoot);
        return false;
      }

      final PageReference pathRef = navResult.pathRefs()[nodeDepth];
      pathRef.setPage(candidate);
      published = true;
      final Runnable afterPublicationTestHook = twoLeafMigrationAfterPublicationTestHook;
      if (afterPublicationTestHook != null) {
        afterPublicationTestHook.run();
      }
      lastDispatchHandler = "h:twoleaf-migrate";
      registerFreshSubtree(pathRef);
      if (nodeDepth > 0) {
        propagateStructuralSpliceUpSpine(navResult, nodeDepth, keySlice);
      }
      // A stale copy of the source reference must forward through the replacement subtree, which
      // owns both partitions of its former key range. Release only after the candidate and spine are
      // fully registered; a pre-publication failure leaves the original source untouched.
      storageEngineWriter.getLog()
                         .releaseOrphanedHOTLeaves(indexScope(), pathRef, List.of(oldSourceRef),
                             TransactionIntentLog.RELEASE_SITE_TWO_LEAF_MIGRATION);
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
      }
      if (candidateRef != null) {
        closeUnregisteredFreshSubtree(candidateRef, failure);
      } else {
        // A builder/assembly failure happened before the two roots acquired their common owner.
        // At this boundary neither root can have been published or registered.
        closeFreshHOTSubtree(childRoot, failure);
        if (sourceRoot != childRoot) {
          closeFreshHOTSubtree(sourceRoot, failure);
        }
      }
      // Never turn an unexpected builder/runtime fault into a fallback, and never continue after a
      // publication failure. The latter has already poisoned the writer above.
      throw failure;
    }
  }

  /**
   * Splice the only local malformation an otherwise valid combo-add can introduce: the fresh leaf's
   * sparse-partial slot disagrees with its lexicographic boundary. The smallest complete flattened
   * BiNode frontier containing that boundary is canonicalized from direct leaves only; no indirect
   * source is scanned and no ancestor or root is reconstructed from entries.
   */
  private boolean trySpliceMalformedComboLeafFrontier(final LeafNavigationResult navResult,
      final HOTIndirectPage oldNode, final HOTIndirectPage candidate, final int nodeDepth,
      final HOTLeafPage insertedLeaf, final byte[] keySlice) {
    final HOTIncrementalInsert.ChildRange frontier =
        malformedComboLeafFrontier(oldNode, candidate, insertedLeaf, keySlice);
    if (frontier == null) {
      return false;
    }
    final boolean spliced =
        tryCanonicalLeafFrontierSplice(navResult, candidate, nodeDepth, frontier, null, null, insertedLeaf, false);
    if (spliced) {
      BRANCH_LOCAL_LEAF_FRONTIER_SPLICE.incrementAndGet();
    }
    return spliced;
  }

  /** The two persistent halves of a subtree split immediately before an absent key. */
  private record StructuralKeySplit(@Nullable PageReference left, @Nullable PageReference right) {
  }

  /** The direct-child interval which brackets a key's physical and sparse-routing positions. */
  private record StructuralFrontier(int fromInclusive, int toExclusive) {
    private int size() {
      return toExclusive - fromInclusive;
    }
  }

  /**
   * Total branch slow path: insert {@code K} through the smallest complete structural frontier which
   * contains both its sparse-routing slot and its lexicographic position.
   *
   * <p>
   * The frontier is persistently split immediately before {@code K}. Every indirect split copies only
   * one bounded child table and shares all untouched descendants. At most one physical leaf is
   * copied, namely the leaf whose key range contains the insertion point. The two retained halves and
   * a one-entry K leaf are joined into a canonical Patricia mini-root and the complete frontier is
   * replaced atomically. No posting payload outside that boundary leaf is read or copied.
   * </p>
   *
   * <p>
   * If the candidate would move an outer ancestor boundary, it is discarded before publication and
   * the operation is retried one level higher. Thus the sole {@link PageReference#setPage}
   * publication is preceded by every deterministic routing/order/height check; after it, only
   * registration and the already-preflighted height propagation remain.
   * </p>
   */
  private void spliceCompleteFrontierIncrementally(final LeafNavigationResult navResult, final int initialDepth,
      final byte[] keySlice, final byte[] valueSlice) {
    final int deepest = Math.min(initialDepth, navResult.pathDepth() - 1);
    for (int nodeDepth = deepest; nodeDepth >= 0; nodeDepth--) {
      final HOTIndirectPage node = navResult.pathNodes()[nodeDepth];
      ensureNodeChildrenLoaded(node);
      final StructuralFrontier minimal = structuralFrontierForKey(node, keySlice);
      if (trySpliceCompleteFrontier(navResult, node, nodeDepth, minimal, keySlice, valueSlice)) {
        return;
      }

      // A complete sub-frontier can still be too narrow when its replacement changes a boundary
      // discriminator shared with another direct child. Retry the whole bounded block before moving
      // up the spine; this remains reference-only except for the same one boundary leaf.
      if ((minimal.fromInclusive() != 0 || minimal.toExclusive() != node.getNumChildren()) && trySpliceCompleteFrontier(
          navResult, node, nodeDepth, new StructuralFrontier(0, node.getNumChildren()), keySlice, valueSlice)) {
        return;
      }
    }
    throw new IllegalStateException(
        "HOT could not construct an invariant-clean incremental frontier for an otherwise valid branch insert");
  }

  /** Build, validate and publish one exact complete-frontier candidate. */
  private boolean trySpliceCompleteFrontier(final LeafNavigationResult navResult, final HOTIndirectPage node,
      final int nodeDepth, final StructuralFrontier frontier, final byte[] keySlice, final byte[] valueSlice) {
    final int revision = storageEngineWriter.getRevisionNumber();
    final List<PageReference> replacedLeafRefs = new ArrayList<>(1);
    StructuralKeySplit split = null;
    PageReference keyRef = null;
    PageReference replacementRef = null;
    PageReference candidateRef = null;
    Page candidatePage = null;
    PageReference publicationRef = null;
    boolean published = false;
    boolean registrationCompleted = false;
    try {
      final PageReference sourceRef = frontier.size() == 1
          ? node.getChildReference(frontier.fromInclusive())
          : HOTIncrementalInsert.compressChildRange(node, frontier.fromInclusive(), frontier.toExclusive(), revision,
              pageKeyAllocator);
      if (sourceRef == null) {
        throw new IllegalStateException("HOT incremental frontier has a null source reference");
      }

      split = splitSubtreeBeforeKey(sourceRef, keySlice, replacedLeafRefs, 0);
      final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
      putFreshSingleEntryOrThrow(keyLeaf, keySlice, valueSlice);
      keyRef = swizzle(keyLeaf);
      replacementRef = joinOrderedAroundKey(split.left(), keyRef, split.right(), keySlice, revision);

      if (frontier.size() == 1) {
        candidateRef = replaceSingleChildAndReencode(node, frontier.fromInclusive(), replacementRef, revision);
      } else {
        candidateRef = HOTIncrementalInsert.replaceChildRangeAndCompress(node, frontier.fromInclusive(),
            frontier.toExclusive(), replacementRef, revision, pageKeyAllocator);
      }
      candidatePage = candidateRef.getPage();
      if (candidatePage == null || freshStructuralPagesMalformed(candidateRef, 0)
          || !routedDescentContains(candidateRef, keySlice)
          || !canPropagateIncrementalSplice(navResult, nodeDepth, candidateRef)) {
        discardUnpublishedStructuralCandidateOrThrow(candidateRef);
        return false;
      }

      publicationRef = navResult.pathRefs()[nodeDepth];
      publicationRef.setPage(candidatePage);
      published = true;
      lastDispatchHandler = "h:complete-frontier-splice";
      registerFreshSubtree(publicationRef);
      registrationCompleted = true;
      if (nodeDepth > 0) {
        propagateStructuralSpliceUpSpine(navResult, nodeDepth, keySlice);
      }
      if (!replacedLeafRefs.isEmpty()) {
        storageEngineWriter.getLog()
                           .releaseOrphanedHOTLeaves(indexScope(), publicationRef, replacedLeafRefs,
                               TransactionIntentLog.RELEASE_SITE_FRONTIER_SPLICE);
      }
      COMPLETE_STRUCTURAL_FRONTIER_SPLICE.incrementAndGet();
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
        closeLocallyOwnedFreshSubtree(publicationRef, candidatePage, registrationCompleted, failure);
      } else if (candidateRef != null) {
        closeUnregisteredFreshSubtree(candidateRef, failure);
      } else if (replacementRef != null) {
        closeUnregisteredFreshSubtree(replacementRef, failure);
      } else {
        if (split != null) {
          closeUnregisteredFreshSubtree(split.left(), failure);
          closeUnregisteredFreshSubtree(split.right(), failure);
        }
        closeUnregisteredFreshSubtree(keyRef, failure);
      }
      throw failure;
    }
  }

  /** Completed insertions discharged by the persistent complete-frontier primitive. */
  public static final AtomicLong COMPLETE_STRUCTURAL_FRONTIER_SPLICE = new AtomicLong();

  /**
   * Start with the smallest complete flattened-BiNode range containing both K's sparse-routing slot
   * and its lexicographic insertion position. The two differ in the Direction-1 cases this total path
   * exists to handle; treating the routed slot alone as the split boundary would merely repeat the
   * failed approximate descent.
   */
  private StructuralFrontier structuralFrontierForKey(final HOTIndirectPage node, final byte[] keySlice) {
    final int childCount = node.getNumChildren();
    final int routedSlot = node.findChildIndex(keySlice);
    if (childCount < 1 || routedSlot < 0 || routedSlot >= childCount) {
      throw new IllegalStateException("HOT incremental frontier cannot resolve the key's sparse-routing slot");
    }
    final int lexicographicSlot = lexicographicBoundaryChild(node, keySlice);
    if (routedSlot == lexicographicSlot) {
      return new StructuralFrontier(routedSlot, routedSlot + 1);
    }
    final HOTIncrementalInsert.ChildRange complete = HOTIncrementalInsert.minimalBiNodeRangeContaining(node,
        Math.min(routedSlot, lexicographicSlot), Math.max(routedSlot, lexicographicSlot));
    return new StructuralFrontier(complete.fromInclusive(), complete.toExclusive());
  }

  /**
   * Locate the one child whose ordered key range precedes or contains {@code keySlice}. Child ranges
   * are disjoint and ascending (I8/I12), so binary search over their first keys touches only
   * {@code O(log fanout)} extreme paths and never enumerates a posting payload.
   */
  private int lexicographicBoundaryChild(final HOTIndirectPage node, final byte[] keySlice) {
    int low = 0;
    int high = node.getNumChildren();
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final byte[] first = firstKeyOfSubtree(node.getChildReference(middle));
      if (first == null) {
        throw new IllegalStateException("HOT incremental frontier cannot resolve child " + middle + "'s first key");
      }
      if (Arrays.compareUnsigned(first, keySlice) <= 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low == 0
        ? 0
        : low - 1;
  }

  /**
   * Persistent lexicographic split of a canonical subtree immediately before an absent key. Only the
   * boundary path is copied; an interior boundary leaf is split into two fresh leaves.
   */
  private StructuralKeySplit splitSubtreeBeforeKey(final PageReference sourceRef, final byte[] keySlice,
      final List<PageReference> replacedLeafRefs, final int depth) {
    if (depth > MAX_PATH_DEPTH) {
      throw new IllegalStateException("HOT persistent key split exceeded " + MAX_PATH_DEPTH + " levels");
    }
    final Page source = resolveHOTPageForTraversal(sourceRef);
    if (source instanceof HOTLeafPage leaf) {
      final int search = leaf.findEntry(keySlice);
      if (search >= 0) {
        throw new IllegalStateException(
            "HOT incremental frontier found the supposedly absent key in leaf " + leaf.getPageKey());
      }
      final int insertionPoint = -search - 1;
      if (insertionPoint == 0) {
        return new StructuralKeySplit(null, sourceRef);
      }
      if (insertionPoint == leaf.getEntryCount()) {
        return new StructuralKeySplit(sourceRef, null);
      }

      HOTLeafPage left = null;
      HOTLeafPage right = null;
      try {
        left = copyLeafRange(leaf, 0, insertionPoint);
        right = copyLeafRange(leaf, insertionPoint, leaf.getEntryCount());
        rehomeSplitLeafSideReferences(leaf, left, right);
        final StructuralKeySplit result = new StructuralKeySplit(swizzle(left), swizzle(right));
        if (sourceRef.getKey() >= 0 || sourceRef.getLogKey() >= 0) {
          replacedLeafRefs.add(sourceRef);
        } else if (!leaf.isClosed()) {
          // A frontier can consume an unregistered speculative leaf. That source has no durable/TIL
          // owner and is no longer reachable from either persistent half.
          leaf.close();
        }
        return result;
      } catch (final RuntimeException | Error failure) {
        if (left != null) {
          closeSpeculativeLeafIfOpen(left, failure);
        }
        if (right != left) {
          if (right != null) {
            closeSpeculativeLeafIfOpen(right, failure);
          }
        }
        throw failure;
      }
    }
    if (!(source instanceof HOTIndirectPage indirect)) {
      throw new IllegalStateException("HOT persistent key split encountered a non-HOT page");
    }
    ensureNodeChildrenLoaded(indirect);
    final int childCount = indirect.getNumChildren();
    // A complete-frontier splice is entered precisely when approximate sparse routing may disagree
    // with physical order. Descend through the lexicographic boundary child, not findChildIndex(K),
    // otherwise Direction 1 would recreate the same malformed placement at every wider frontier.
    final int target = lexicographicBoundaryChild(indirect, keySlice);

    final int revision = storageEngineWriter.getRevisionNumber();
    final StructuralKeySplit childSplit =
        splitSubtreeBeforeKey(indirect.getChildReference(target), keySlice, replacedLeafRefs, depth + 1);
    PageReference left = null;
    PageReference right = null;
    try {
      left = HOTIncrementalInsert.compressChildSliceReplacing(indirect, 0, target + 1, target, childSplit.left(),
          revision, pageKeyAllocator);
      right = HOTIncrementalInsert.compressChildSliceReplacing(indirect, target, childCount, target, childSplit.right(),
          revision, pageKeyAllocator);
      return new StructuralKeySplit(left, right);
    } catch (final RuntimeException | Error failure) {
      closeUnregisteredFreshSubtree(left, failure);
      closeUnregisteredFreshSubtree(right, failure);
      closeUnregisteredFreshSubtree(childSplit.left(), failure);
      closeUnregisteredFreshSubtree(childSplit.right(), failure);
      throw failure;
    }
  }

  /** Copy one non-empty contiguous range out of a leaf without changing any value bytes. */
  private HOTLeafPage copyLeafRange(final HOTLeafPage source, final int fromInclusive, final int toExclusive) {
    final HOTLeafPage copy =
        new HOTLeafPage(pageKeyAllocator.getAsLong(), storageEngineWriter.getRevisionNumber(), indexType);
    try {
      for (int slot = fromInclusive; slot < toExclusive; slot++) {
        final byte[] key = source.getKey(slot);
        if (key == null || !copy.put(key, source.copyStoredValue(slot))) {
          throw new IllegalStateException(
              "HOT persistent key split could not copy source leaf " + source.getPageKey() + " slot " + slot);
        }
      }
      return copy;
    } catch (final RuntimeException | Error failure) {
      closeSpeculativeLeafIfOpen(copy, failure);
      throw failure;
    }
  }

  /** Preserve projection side-map ownership when the one boundary leaf is split. */
  private void rehomeSplitLeafSideReferences(final HOTLeafPage source, final HOTLeafPage left,
      final HOTLeafPage right) {
    if (source.segmentRefCount() == 0) {
      return;
    }
    final byte[] ownerKey = new byte[Long.BYTES];
    for (final long refKey : source.overflowPageRefKeysSorted()) {
      final PageReference sideRef = source.getPageReference(refKey);
      if (sideRef == null) {
        throw new IllegalStateException("HOT boundary leaf side reference " + refKey + " has no owner reference");
      }
      PathKeySerializer.INSTANCE.serialize(HOTLeafPage.overflowPageRefOwnerSlot(refKey), ownerKey, 0);
      final HOTLeafPage owner = left.findEntry(ownerKey) >= 0
          ? left
          : right.findEntry(ownerKey) >= 0
              ? right
              : null;
      if (owner == null) {
        throw new IllegalStateException("HOT boundary leaf split lost side-reference owner for refKey " + refKey);
      }
      owner.setPageReference(refKey, sideRef);
    }
  }

  /** Join {@code <K}, {@code K}, {@code >K} as one canonical one- or two-bit HOT block. */
  private PageReference joinOrderedAroundKey(final @Nullable PageReference left, final PageReference keyRef,
      final @Nullable PageReference right, final byte[] keySlice, final int revision) {
    final PageReference[] children = new PageReference[3];
    final byte[][] first = new byte[3][];
    final byte[][] last = new byte[3][];
    int count = 0;
    if (left != null) {
      children[count] = left;
      first[count] = requireNonNull(firstKeyOfSubtree(left), "left frontier first key");
      last[count++] = requireNonNull(lastKeyOfSubtree(left), "left frontier last key");
    }
    children[count] = keyRef;
    first[count] = keySlice;
    last[count++] = keySlice;
    if (right != null) {
      children[count] = right;
      first[count] = requireNonNull(firstKeyOfSubtree(right), "right frontier first key");
      last[count++] = requireNonNull(lastKeyOfSubtree(right), "right frontier last key");
    }
    for (int i = 1; i < count; i++) {
      if (Arrays.compareUnsigned(last[i - 1], first[i]) >= 0) {
        throw new IllegalStateException("HOT incremental frontier inputs overlap at ordered child " + i);
      }
    }
    if (count == 1) {
      return keyRef;
    }

    final int rootBit = HOTBulkBuilder.msdb(first[0], last[count - 1]);
    int split = 1;
    while (split < count && !HOTBulkBuilder.bitAt(first[split], rootBit)) {
      split++;
    }
    if (split == count) {
      throw new IllegalStateException("HOT incremental frontier has no Patricia transition at bit " + rootBit);
    }
    for (int i = 0; i < split; i++) {
      if (HOTBulkBuilder.bitAt(first[i], rootBit) || HOTBulkBuilder.bitAt(last[i], rootBit)) {
        throw new IllegalStateException("HOT left frontier child straddles Patricia bit " + rootBit);
      }
    }
    for (int i = split; i < count; i++) {
      if (!HOTBulkBuilder.bitAt(first[i], rootBit) || !HOTBulkBuilder.bitAt(last[i], rootBit)) {
        throw new IllegalStateException("HOT right frontier child straddles Patricia bit " + rootBit);
      }
    }

    int maxChildHeight = 0;
    for (int i = 0; i < count; i++) {
      maxChildHeight = Math.max(maxChildHeight, structuralHeight(children[i]));
    }
    if (count == 2) {
      return swizzle(HOTIndirectPage.createBiNode(pageKeyAllocator.getAsLong(), revision, rootBit, children[0],
          children[1], maxChildHeight + 1));
    }

    final int nestedBit = split == 1
        ? HOTBulkBuilder.msdb(first[1], last[2])
        : HOTBulkBuilder.msdb(first[0], last[1]);
    if (nestedBit <= rootBit) {
      throw new IllegalStateException("HOT nested frontier bit " + nestedBit + " is not below root bit " + rootBit);
    }
    final int[] discBits = {rootBit, nestedBit};
    final int[] partials = split == 1
        ? new int[] {0, 2, 3}
        : new int[] {0, 1, 2};
    return swizzle(HOTBulkBuilder.assembleIndirect(discBits, partials, Arrays.copyOf(children, count),
        maxChildHeight + 1, revision, pageKeyAllocator));
  }

  /** Re-encode one parent with a single child replacement, preserving its sparse coordinates. */
  private PageReference replaceSingleChildAndReencode(final HOTIndirectPage node, final int childSlot,
      final PageReference replacement, final int revision) {
    final int childCount = node.getNumChildren();
    final PageReference[] children = new PageReference[childCount];
    int maxChildHeight = 0;
    for (int slot = 0; slot < childCount; slot++) {
      final PageReference child = slot == childSlot
          ? replacement
          : node.getChildReference(slot);
      children[slot] = child;
      maxChildHeight = Math.max(maxChildHeight, structuralHeight(child));
    }
    final HOTIndirectPage candidate = HOTBulkBuilder.assembleIndirect(HOTIncrementalInsert.discriminativeBits(node),
        Arrays.copyOf(node.getPartialKeysRef(), childCount), children, maxChildHeight + 1, revision, pageKeyAllocator);
    return swizzle(candidate);
  }

  private int structuralHeight(final PageReference reference) {
    final Page page = resolveHOTPageForTraversal(reference);
    if (page instanceof HOTIndirectPage indirect) {
      return indirect.getHeight();
    }
    if (page instanceof HOTLeafPage) {
      return 0;
    }
    throw new IllegalStateException("HOT structural candidate has an unresolvable/non-HOT child");
  }

  /** Validate only newly allocated structural pages; durable/TIL children are trusted unchanged. */
  private boolean freshStructuralPagesMalformed(final PageReference reference, final int depth) {
    if (depth > MAX_PATH_DEPTH) {
      return true;
    }
    final Page page = resolveHOTPageForTraversal(reference);
    if (!(page instanceof HOTIndirectPage indirect)) {
      return !(page instanceof HOTLeafPage);
    }
    if (nodeStructurallyMalformed(indirect)) {
      return true;
    }
    for (int slot = 0; slot < indirect.getNumChildren(); slot++) {
      final PageReference child = indirect.getChildReference(slot);
      if (child == null) {
        return true;
      }
      if (child.getKey() < 0 && child.getLogKey() < 0 && freshStructuralPagesMalformed(child, depth + 1)) {
        return true;
      }
    }
    return false;
  }

  /** Exact lookup through the unpublished candidate; proves the new key's parent partials route. */
  private boolean routedDescentContains(final PageReference root, final byte[] keySlice) {
    PageReference current = root;
    for (int depth = 0; depth <= MAX_PATH_DEPTH; depth++) {
      final Page page = resolveHOTPageForTraversal(current);
      if (page instanceof HOTLeafPage leaf) {
        return leaf.findEntry(keySlice) >= 0;
      }
      if (!(page instanceof HOTIndirectPage indirect)) {
        return false;
      }
      final int childSlot = indirect.findChildIndex(keySlice);
      if (childSlot < 0 || childSlot >= indirect.getNumChildren()) {
        return false;
      }
      current = indirect.getChildReference(childSlot);
      if (current == null) {
        return false;
      }
    }
    return false;
  }

  /**
   * Close every locally owned leaf under a rejected candidate, preserving the first cleanup fault.
   */
  private void discardUnpublishedStructuralCandidateOrThrow(final @Nullable PageReference reference) {
    final IllegalStateException cleanupCollector =
        new IllegalStateException("HOT could not retire a rejected unpublished structural candidate");
    closeUnregisteredFreshSubtree(reference, cleanupCollector);
    final Throwable[] cleanupFailures = cleanupCollector.getSuppressed();
    if (cleanupFailures.length == 0) {
      return;
    }
    for (int index = 1; index < cleanupFailures.length; index++) {
      addSuppressedSafely(cleanupFailures[0], cleanupFailures[index]);
    }
    if (cleanupFailures[0] instanceof RuntimeException runtimeFailure) {
      throw runtimeFailure;
    }
    throw (Error) cleanupFailures[0];
  }

  /**
   * Validate the exact old-to-candidate reference insertion and return the complete affected leaf
   * frontier. Existing-node I8/I12 is an inductive premise; it is checked on this rare failure path
   * before endpoint-local classification. Any unrelated or non-ordering violation fails closed.
   */
  private HOTIncrementalInsert.@Nullable ChildRange malformedComboLeafFrontier(final HOTIndirectPage oldNode,
      final HOTIndirectPage candidate, final HOTLeafPage insertedLeaf, final byte[] keySlice) {
    final int oldCount = oldNode.getNumChildren();
    final int candidateCount = candidate.getNumChildren();
    if (oldCount < 1 || candidateCount != oldCount + 1 || nodeStructurallyMalformed(oldNode)) {
      return null;
    }

    int insertedSlot = -1;
    int oldSlot = 0;
    for (int candidateSlot = 0; candidateSlot < candidateCount; candidateSlot++) {
      final PageReference candidateRef = candidate.getChildReference(candidateSlot);
      if (candidateRef == null) {
        return null;
      }
      final Page candidatePage = resolveHOTPageForTraversal(candidateRef);
      if (candidatePage == insertedLeaf) {
        if (insertedSlot >= 0) {
          return null;
        }
        insertedSlot = candidateSlot;
        continue;
      }
      if (oldSlot >= oldCount || candidateRef != oldNode.getChildReference(oldSlot++)) {
        return null;
      }
    }
    if (insertedSlot < 0 || oldSlot != oldCount || candidate.findChildIndex(keySlice) != insertedSlot
        || insertedLeaf.getEntryCount() != 1 || !Arrays.equals(insertedLeaf.getFirstKey(), keySlice)
        || !hasStrictlyAscendingPartials(candidate) || candidate.getPartialKey(0) != 0) {
      return null;
    }

    final int parentMsb = candidate.getMostSignificantBitIndex();
    int firstInvolved = insertedSlot;
    int lastInvolved = insertedSlot;
    int violations = 0;
    byte[] previousFirst = null;
    byte[] previousLast = null;
    for (int slot = 0; slot < candidateCount; slot++) {
      final PageReference childRef = candidate.getChildReference(slot);
      final Page childPage = resolveHOTPageForTraversal(childRef);
      if (childPage == null) {
        return null;
      }
      if (parentMsb >= 0 && childPage instanceof HOTIndirectPage childIndirect
          && childIndirect.getMostSignificantBitIndex() >= 0
          && childIndirect.getMostSignificantBitIndex() <= parentMsb) {
        return null;
      }
      final byte[] first = firstKeyOfSubtree(childRef);
      if (first == null) {
        return null;
      }
      if (previousFirst != null && previousLast == null) {
        return null;
      }
      if (previousFirst != null
          && (Arrays.compareUnsigned(previousFirst, first) >= 0 || Arrays.compareUnsigned(previousLast, first) >= 0)) {
        if (slot != insertedSlot && slot - 1 != insertedSlot) {
          return null;
        }
        firstInvolved = Math.min(firstInvolved, slot - 1);
        lastInvolved = Math.max(lastInvolved, slot);
        violations++;
      }
      previousFirst = first;
      previousLast = slot + 1 < candidateCount
          ? lastKeyOfSubtree(childRef)
          : null;
    }
    if (violations == 0 || firstInvolved < 0 || lastInvolved >= candidateCount) {
      return null;
    }
    final HOTIncrementalInsert.ChildRange frontier =
        HOTIncrementalInsert.minimalBiNodeRangeContaining(candidate, firstInvolved, lastInvolved);
    return frontier.size() >= 2 && frontier.size() <= HOTIndirectPage.MAX_NODE_ENTRIES
        ? frontier
        : null;
  }

  /**
   * Resolve the Direction-1 shape in which {@code keySlice} belongs lexicographically inside the
   * preceding child even though exact sparse-partial routing selected {@code affectedSlot}. The
   * smallest complete flattened-BiNode range containing those adjacent slots is derived from the
   * parent's partial trie. Every member must be a direct leaf. That bounded frontier (plus the new
   * key) is rebuilt as a canonical mini-HOT, then the complete range is replaced by one mini-root.
   *
   * <p>
   * This is a leaf-unit structural splice, not an arbitrary subtree rebuild: it never descends an
   * indirect child, is hard-capped by one HOT block's fanout, preserves every side-map reference, and
   * re-encodes only the direct parent plus height-changed ancestors. The retained parent partial is
   * the complete range's lower partial; all entries are checked against the recompressed candidate's
   * actual coordinates and router before publication.
   * </p>
   */
  private boolean tryDirectionOneLeafPairSplice(LeafNavigationResult navResult, HOTIndirectPage node, int nodeDepth,
      int collisionSlot, int affectedSlot, byte[] keySlice, byte[] valueSlice) {
    final int numChildren = node.getNumChildren();
    if (nodeDepth < 0 || nodeDepth >= navResult.pathDepth() || numChildren < 2 || collisionSlot < 0
        || affectedSlot != collisionSlot + 1 || affectedSlot >= numChildren) {
      return false;
    }
    final HOTIncrementalInsert.ChildRange frontier =
        HOTIncrementalInsert.minimalBiNodeRangeContaining(node, collisionSlot, affectedSlot);
    if (frontier.size() < 2 || frontier.size() > HOTIndirectPage.MAX_NODE_ENTRIES) {
      return false;
    }
    return tryCanonicalLeafFrontierSplice(navResult, node, nodeDepth, frontier, keySlice, valueSlice, null, true);
  }

  /**
   * Reference-only Direction-1 splice for a complete frontier containing indirect children. When both
   * physical extrema differ from K exactly at beta, inductively maintained I8/I12 proves the entire
   * closed frontier range lies on beta's opposite side. The frontier can therefore be extracted from
   * one parent block, wrapped with K under a BiNode, and spliced back without reading or copying any
   * descendant entry. Uncertain endpoints or malformed local structure fail closed and leave the
   * source untouched.
   */
  private boolean tryDirectionOneOppositeFrontierWrap(final LeafNavigationResult navResult, final HOTIndirectPage node,
      final int nodeDepth, final int collisionSlot, final int affectedSlot, final int beta, final int betaValue,
      final byte[] keySlice, final byte[] valueSlice) {
    final int childCount = node.getNumChildren();
    final int[] nodeBits = HOTIncrementalInsert.discriminativeBits(node);
    if (nodeDepth < 0 || nodeDepth >= navResult.pathDepth() || collisionSlot < 0 || affectedSlot != collisionSlot + 1
        || affectedSlot >= childCount || nodeStructurallyMalformed(node) || node.getPartialKey(0) != 0
        || !partialsFitDiscriminativeWidth(node, nodeBits)) {
      return false;
    }
    // Both range compression primitives derive height from childRef.getPage(). Resolve this one
    // fixed-fanout block before either is called; otherwise a durable/TIL-only indirect child is
    // silently counted as a height-zero leaf and the candidate can be published stale-low.
    ensureNodeChildrenLoaded(node);
    final HOTIncrementalInsert.ChildRange frontier =
        HOTIncrementalInsert.minimalBiNodeRangeContaining(node, collisionSlot, affectedSlot);
    final PageReference firstRef = node.getChildReference(frontier.fromInclusive());
    final PageReference lastRef = node.getChildReference(frontier.toExclusive() - 1);
    final Page firstPage = resolveHOTPageForTraversal(firstRef);
    final Page lastPage = resolveHOTPageForTraversal(lastRef);
    if (firstPage == null || lastPage == null
        || extremeRelationToKeyAtBit(firstPage, false, keySlice, beta, 1) != EXTREME_RELATION_OPPOSITE_AT_BETA
        || extremeRelationToKeyAtBit(lastPage, true, keySlice, beta, 1) != EXTREME_RELATION_OPPOSITE_AT_BETA) {
      return false;
    }

    final byte[] frontierFirst = firstKeyOfSubtree(firstRef);
    final byte[] frontierLast = lastKeyOfSubtree(lastRef);
    if (frontierFirst == null || frontierLast == null) {
      return false;
    }
    final byte[] replacementFirst = betaValue == 0
        ? keySlice
        : frontierFirst;
    final byte[] replacementLast = betaValue == 0
        ? frontierLast
        : keySlice;
    if (frontier.fromInclusive() > 0) {
      final byte[] previousLast = lastKeyOfSubtree(node.getChildReference(frontier.fromInclusive() - 1));
      if (previousLast == null || Arrays.compareUnsigned(previousLast, replacementFirst) >= 0) {
        return false;
      }
    }
    if (frontier.toExclusive() < childCount) {
      final byte[] nextFirst = firstKeyOfSubtree(node.getChildReference(frontier.toExclusive()));
      if (nextFirst == null || Arrays.compareUnsigned(replacementLast, nextFirst) >= 0) {
        return false;
      }
    }

    final int revision = storageEngineWriter.getRevisionNumber();
    final PageReference rangeRef = HOTIncrementalInsert.compressChildRange(node, frontier.fromInclusive(),
        frontier.toExclusive(), revision, pageKeyAllocator);
    final Page rangePage = rangeRef.getPage();
    if (rangePage == null || rangePage instanceof HOTIndirectPage rangeIndirect
        && (rangeIndirect.getMostSignificantBitIndex() <= beta || nodeStructurallyMalformed(rangeIndirect))) {
      return false;
    }

    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    putFreshSingleEntryOrThrow(keyLeaf, keySlice, valueSlice);
    final PageReference keyRef;
    try {
      keyRef = swizzle(keyLeaf);
    } catch (final RuntimeException | Error constructionFailure) {
      closeSpeculativeLeaf(keyLeaf, constructionFailure);
      throw constructionFailure;
    }
    final int rangeHeight = rangePage instanceof HOTIndirectPage rangeIndirect
        ? rangeIndirect.getHeight()
        : 0;
    final PageReference leftRef = betaValue == 0
        ? keyRef
        : rangeRef;
    final PageReference rightRef = betaValue == 0
        ? rangeRef
        : keyRef;
    HOTIndirectPage miniRoot = null;
    Page candidateRoot = null;
    PageReference publicationRef = null;
    boolean published = false;
    boolean registrationCompleted = false;
    try {
      miniRoot = HOTIndirectPage.createBiNode(pageKeyAllocator.getAsLong(), revision, beta, leftRef, rightRef,
          rangeHeight + 1);
      final PageReference replacementRef = swizzle(miniRoot);
      final PageReference candidateRef = HOTIncrementalInsert.replaceChildRangeAndCompress(node,
          frontier.fromInclusive(), frontier.toExclusive(), replacementRef, revision, pageKeyAllocator);
      final Page candidatePage = candidateRef.getPage();
      candidateRoot = candidatePage;
      if (candidatePage == null || nodeStructurallyMalformed(miniRoot)) {
        keyLeaf.close();
        return false;
      }
      if (candidateRef != replacementRef) {
        if (!(candidatePage instanceof HOTIndirectPage candidate)
            || candidate.findChildIndex(keySlice) != frontier.fromInclusive()
            || candidate.findChildIndex(frontierFirst) != frontier.fromInclusive()
            || candidate.findChildIndex(frontierLast) != frontier.fromInclusive()
            || nodeStructurallyMalformed(candidate)) {
          keyLeaf.close();
          return false;
        }
      }
      if (!canPropagateIncrementalSplice(navResult, nodeDepth, candidateRef)) {
        keyLeaf.close();
        return false;
      }

      publicationRef = navResult.pathRefs()[nodeDepth];
      publicationRef.setPage(candidatePage);
      published = true;
      lastDispatchHandler = "h:d1-opposite-frontier-wrap";
      registerFreshSubtree(publicationRef);
      registrationCompleted = true;
      if (nodeDepth > 0) {
        propagateStructuralSpliceUpSpine(navResult, nodeDepth, keySlice);
      }
      DIRECTION_ONE_OPPOSITE_FRONTIER_WRAP.incrementAndGet();
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
        closeLocallyOwnedFreshSubtree(publicationRef, candidateRoot, registrationCompleted, failure);
      } else {
        closeSpeculativeLeafIfOpen(keyLeaf, failure);
      }
      throw failure;
    }
  }

  /**
   * Canonicalize one complete direct-leaf frontier and publish the recompressed parent atomically.
   * The source is bounded by one HOT block; indirect children are rejected rather than traversed. An
   * optional extra entry supports Direction-1, while {@code disposableSourceLeaf} denotes an
   * already-present speculative leaf owned by an unpublished combo candidate.
   */
  private boolean tryCanonicalLeafFrontierSplice(final LeafNavigationResult navResult, final HOTIndirectPage node,
      final int nodeDepth, final HOTIncrementalInsert.ChildRange frontier, final byte @Nullable [] additionalKey,
      final byte @Nullable [] additionalValue, final @Nullable HOTLeafPage disposableSourceLeaf,
      final boolean directionOne) {
    if ((additionalKey == null) != (additionalValue == null)) {
      throw new IllegalArgumentException("additional frontier key and value must be both present or both absent");
    }
    if (additionalKey == null && disposableSourceLeaf == null) {
      throw new IllegalArgumentException("a frontier splice needs either an added entry or a speculative source leaf");
    }
    final int numChildren = node.getNumChildren();
    // The recompression primitive derives the replacement parent's exact height without an I/O
    // callback. Swizzle this one bounded block first; these are cache pointers on writer-private
    // PageReferences, not a structural publication.
    for (int i = 0; i < numChildren; i++) {
      final PageReference childRef = node.getChildReference(i);
      if (childRef == null) {
        return false;
      }
      if (childRef.getPage() == null) {
        final Page resolved = resolveHOTPageForTraversal(childRef);
        if (resolved == null) {
          return false;
        }
        childRef.setPage(resolved);
      }
    }

    int entryCapacity = additionalKey == null
        ? 0
        : 1;
    int segmentRefCapacity = 0;
    final List<PageReference> orphanedLeafRefs = new ArrayList<>(frontier.size());
    for (int slot = frontier.fromInclusive(); slot < frontier.toExclusive(); slot++) {
      final PageReference childRef = node.getChildReference(slot);
      if (!(childRef.getPage() instanceof HOTLeafPage leaf)) {
        return false; // hard guard: never turn this into an indirect-subtree reconstruction
      }
      entryCapacity += leaf.getEntryCount();
      segmentRefCapacity += leaf.segmentRefCount();
      if (leaf != disposableSourceLeaf) {
        orphanedLeafRefs.add(childRef);
      }
    }
    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>(entryCapacity);
    final List<CapturedSegmentRef> segmentRefs = new ArrayList<>(segmentRefCapacity);
    for (int slot = frontier.fromInclusive(); slot < frontier.toExclusive(); slot++) {
      collectLeafEntries((HOTLeafPage) node.getChildReference(slot).getPage(), collected, segmentRefs);
    }
    if (additionalKey != null) {
      collected.add(new HOTBulkBuilder.Entry(additionalKey, additionalValue));
    }
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> entries = dedupMergeEntries(collected);

    final int revision = storageEngineWriter.getRevisionNumber();
    Page miniRoot = null;
    Page candidateRoot = null;
    PageReference publicationRef = null;
    boolean published = false;
    boolean registrationCompleted = false;
    try {
      miniRoot = HOTBulkBuilder.build(entries, revision, indexType, pageKeyAllocator).rootPage();
      final PageReference replacementRef = swizzle(miniRoot);
      final PageReference candidateRef = HOTIncrementalInsert.replaceChildRangeAndCompress(node,
          frontier.fromInclusive(), frontier.toExclusive(), replacementRef, revision, pageKeyAllocator);
      final Page candidatePage = candidateRef.getPage();
      candidateRoot = candidatePage;
      if (candidatePage == null) {
        closeFreshHOTSubtree(miniRoot);
        return false;
      }
      final boolean retainedParent = candidateRef != replacementRef;
      if (retainedParent) {
        if (!(candidatePage instanceof HOTIndirectPage candidate)) {
          closeFreshHOTSubtree(miniRoot);
          return false;
        }
        final int candidateParentMSB = candidate.getMostSignificantBitIndex();
        if (miniRoot instanceof HOTIndirectPage miniIndirect && candidateParentMSB >= 0
            && miniIndirect.getMostSignificantBitIndex() <= candidateParentMSB) {
          closeFreshHOTSubtree(miniRoot);
          return false; // I11: replacement must branch strictly below its recompressed parent
        }
        final int[] candidatePartials = candidate.getPartialKeysRef();
        final int replacementSlot = frontier.fromInclusive();
        if (candidatePartials == null || candidatePartials.length < candidate.getNumChildren()
            || replacementSlot >= candidate.getNumChildren()) {
          closeFreshHOTSubtree(miniRoot);
          return false;
        }
        final int replacementPartial = candidatePartials[replacementSlot];
        for (int i = 0; i < entries.size(); i++) {
          final byte[] entryKey = entries.get(i).key();
          final int densePartial = candidate.computeDensePartialKey(entryKey);
          if ((replacementPartial & ~densePartial) != 0 || candidate.findChildIndex(entryKey) != replacementSlot) {
            closeFreshHOTSubtree(miniRoot);
            return false; // I5/routing in the candidate's final compressed coordinates
          }
        }
      }
      if (candidatePage instanceof HOTIndirectPage candidate && nodeStructurallyMalformed(candidate)) {
        closeFreshHOTSubtree(miniRoot);
        return false;
      }
      if (!canPropagateIncrementalSplice(navResult, nodeDepth, candidateRef)) {
        closeFreshHOTSubtree(miniRoot);
        return false;
      }

      // Reattachment first validates every owner, then publishes every side-map reference into the
      // fresh mini-HOT. Closing an unpublished mini-root merely clears those shared references; it
      // never retires their overflow pages. The path reference is the sole publication boundary.
      reattachSegmentRefs(miniRoot, segmentRefs);
      publicationRef = navResult.pathRefs()[nodeDepth];
      publicationRef.setPage(candidatePage);
      published = true;
      lastDispatchHandler = directionOne
          ? "h:d1-leaf-frontier-splice"
          : "h:combo-leaf-frontier-splice";
      registerFreshSubtree(publicationRef);
      registrationCompleted = true;
      if (nodeDepth > 0) {
        final byte[] propagationKey = additionalKey != null
            ? additionalKey
            : disposableSourceLeaf.getFirstKey();
        propagateStructuralSpliceUpSpine(navResult, nodeDepth, propagationKey);
      }
      storageEngineWriter.getLog()
                         .releaseOrphanedHOTLeaves(indexScope(), navResult.pathRefs()[nodeDepth], orphanedLeafRefs,
                             TransactionIntentLog.RELEASE_SITE_FRONTIER_SPLICE);
      if (disposableSourceLeaf != null) {
        disposableSourceLeaf.close();
      }
      if (directionOne) {
        DIRECTION_ONE_LEAF_FRONTIER_SPLICE.incrementAndGet();
      }
      if (directionOne && frontier.size() == 2) {
        DIRECTION_ONE_LEAF_PAIR_SPLICE.incrementAndGet();
      } else if (directionOne) {
        DIRECTION_ONE_MULTI_LEAF_FRONTIER_SPLICE.incrementAndGet();
      }
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        markTransactionRollbackOnly(failure);
        // The published candidate, rather than miniRoot alone, is the cleanup scope. It carries the
        // exact references registration visited, so TIL/durable children remain owned while a hook
        // failure before the first registration still releases every locally owned replacement.
        closeLocallyOwnedFreshSubtree(publicationRef, candidateRoot, registrationCompleted, failure);
        if (disposableSourceLeaf != null) {
          closeSpeculativeLeafIfOpen(disposableSourceLeaf, failure);
        }
      } else {
        closeFreshHOTSubtree(miniRoot, failure);
        // The caller transfers ownership of this already-speculative leaf for the duration of the
        // frontier attempt. A normal false return preserves caller ownership; an exception cannot
        // resume that caller, so this catch must retire it even before publication.
        if (disposableSourceLeaf != null) {
          closeSpeculativeLeafIfOpen(disposableSourceLeaf, failure);
        }
      }
      throw failure;
    }
  }

  /** Release the off-heap leaves of a disposable tree produced entirely by {@link HOTBulkBuilder}. */
  private static void closeFreshHOTSubtree(Page page) {
    if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference childRef = indirect.getChildReference(i);
        if (childRef != null && childRef.getPage() != null) {
          closeFreshHOTSubtree(childRef.getPage());
        }
      }
      return;
    }
    page.close();
  }

  /** Retire a provably unpublished fresh HOT without replacing the primary failure. */
  private static void closeFreshHOTSubtree(final @Nullable Page page, final Throwable failure) {
    if (page == null) {
      return;
    }
    try {
      closeFreshHOTSubtree(page);
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(failure, cleanupFailure);
    }
  }

  /**
   * Retire only the still-local leaves of a fresh subtree after its publication reference was
   * changed.
   *
   * <p>
   * The publication reference is commonly an existing durable/TIL reference. Its key fields therefore
   * cannot by themselves say whether they name the old page or the fresh replacement. Exact
   * {@link PageContainer} identity is the ownership boundary. If the first complete
   * {@link #registerFreshSubtree} call returned, the whole replacement has crossed that boundary; a
   * later ancestor-registration failure must retain it even if an ownership probe itself is no longer
   * conclusive (for example, an asynchronous flush already made the reference durable). During a
   * failure inside the first post-order registration, the root has not transferred yet, so descend
   * and close only child references which carry neither a durable nor a live log identity.
   * </p>
   */
  private void closeLocallyOwnedFreshSubtree(final @Nullable PageReference publicationRef,
      final @Nullable Page freshRoot, final boolean registrationCompleted, final Throwable primaryFailure) {
    if (freshRoot == null) {
      return;
    }
    try {
      if (registrationCompleted) {
        return;
      }
      if (publicationRef != null && publicationRef.getLogKey() >= 0) {
        final PageContainer owner;
        try {
          owner = requireNonNull(storageEngineWriter.getLog(), "transaction intent log").get(publicationRef);
        } catch (final RuntimeException | Error ownershipFailure) {
          // Ownership is uncertain. Retain rather than free a frame which may already be reachable
          // from the TIL; the transaction is poisoned by every caller of this failure-only helper.
          addSuppressedSafely(primaryFailure, ownershipFailure);
          return;
        }
        if (containerOwnsPage(owner, freshRoot)) {
          return;
        }
      }
      closeLocallyOwnedFreshPage(freshRoot, primaryFailure);
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(primaryFailure, cleanupFailure);
    }
  }

  /** Failure-only recursive half of {@link #closeLocallyOwnedFreshSubtree}. */
  private void closeLocallyOwnedFreshPage(final Page page, final Throwable primaryFailure) {
    if (page instanceof HOTIndirectPage indirect) {
      closeUnregisteredFreshChildren(indirect, 0, primaryFailure);
    } else if (!page.isClosed()) {
      page.close();
    }
  }

  static void setStructuralPublicationTestHook(final @Nullable Runnable hook) {
    structuralPublicationTestHook = hook;
  }

  static void setTwoLeafMigrationAfterPublicationTestHook(final @Nullable Runnable hook) {
    twoLeafMigrationAfterPublicationTestHook = hook;
  }

  static void setTwoLeafMigrationAfterReattachTestHook(final @Nullable Consumer<HOTIndirectPage> hook) {
    twoLeafMigrationAfterReattachTestHook = hook;
  }

  /**
   * Inspect one leaf from the already-bounded route-feasible scan plan. The plan carries the leaf's
   * candidate-node owner slot, so finding strandable keys in two physical leaves or two owner slots
   * rejects the two-leaf primitive without another tree walk.
   */
  private void collectMigratableLeafKeys(final HOTLeafPage leaf, final HOTIndirectPage newNode, final int comboSlot,
      final byte[] excludeKey, final int ownerSlot, final long[] info) {
    if (info[2] != 1L) {
      return;
    }
    boolean leafHasStrand = false;
    for (int i = 0; i < leaf.getEntryCount(); i++) {
      final byte[] key = leaf.getKey(i);
      if (key == null) {
        throw refuseMutationTraversal("two-leaf strand source identification",
            "unreadable key at leaf " + leaf.getPageKey() + " slot " + i);
      }
      if (Arrays.equals(key, excludeKey) || newNode.findChildIndex(key) != comboSlot) {
        continue;
      }
      leafHasStrand = true;
      info[3]++;
    }
    if (leafHasStrand) {
      if (ownerSlot < 0 || info[0] >= 0 && (info[0] != ownerSlot || info[1] != leaf.getPageKey())) {
        info[2] = 0L; // strandable keys span >1 candidate slot or >1 physical leaf
      } else {
        info[0] = ownerSlot;
        info[1] = leaf.getPageKey();
      }
    }
  }

  /** Apply the index's value semantics while de-duplicating a bounded leaf-frontier input. */
  private List<HOTBulkBuilder.Entry> dedupMergeEntries(final List<HOTBulkBuilder.Entry> sorted) {
    final List<HOTBulkBuilder.Entry> out = new ArrayList<>(sorted.size());
    for (final HOTBulkBuilder.Entry entry : sorted) {
      final int last = out.size() - 1;
      if (last >= 0 && Arrays.equals(out.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = out.get(last);
        // PATH/CAS/NAME values are NodeReferences bitmaps and duplicate inserts union their node
        // sets. A PROJECTION key is a physical slot: its value is arbitrary descriptor/segment bytes
        // (the zero-length value is a tombstone), so bitmap decoding is both semantically wrong and
        // unsafe. Sort is stable and newly inserted (K,V) is appended after collected state; keeping
        // the last duplicate therefore implements projection's last-write slot semantics byte-for-byte.
        final byte[] mergedValue = indexType == IndexType.PROJECTION
            ? entry.value()
            : HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value());
        out.set(last, new HOTBulkBuilder.Entry(prev.key(), mergedValue));
      } else {
        out.add(entry);
      }
    }
    return out;
  }

  /**
   * Returns {@code true} iff at least one key under {@code oldNode} would strand to {@code newSlot}
   * on {@code newNode} and <em>every</em> such key lives in the leaf with page key {@code
   * leafPageKey}. Used to gate the bounded leaf/frontier handlers.
   */
  final boolean strandConfinedToLeaf(HOTIndirectPage oldNode, HOTIndirectPage newNode, int newSlot, byte[] excludeKey,
      long leafPageKey) {
    // branchAddStrandsExisting has just run the same route-feasibility preflight. Re-run that
    // allocation-free classifier here instead of walking the whole old node: the shared scratch is
    // reset into an exact bounded scan plan containing only children which can route to newSlot.
    final RebuildFootprint footprint = requireBoundedExistingKeyRoutingTraversal(oldNode, newNode, newSlot, excludeKey);
    return strandConfinedToLeaf(footprint, newNode, newSlot, excludeKey, leafPageKey);
  }

  private boolean strandConfinedToLeaf(final RebuildFootprint footprint, final HOTIndirectPage newNode,
      final int newSlot, final byte[] excludeKey, final long leafPageKey) {
    boolean found = false;
    for (int pageIndex = 0; pageIndex < footprint.pages; pageIndex++) {
      if (!(footprint.visitedPages[pageIndex] instanceof HOTLeafPage leaf)) {
        continue;
      }
      final int entryCount = leaf.getEntryCount();
      for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
        final byte[] key = leaf.getKey(entryIndex);
        if (key == null) {
          throw refuseMutationTraversal("strand confinement to descended leaf",
              "unreadable key at leaf " + leaf.getPageKey() + " slot " + entryIndex);
        }
        if (Arrays.equals(key, excludeKey) || newNode.findChildIndex(key) != newSlot) {
          continue;
        }
        found = true;
        if (leaf.getPageKey() != leafPageKey) {
          return false;
        }
      }
    }
    return found;
  }

  /**
   * Preflight every deterministic condition {@link #propagateStructuralSpliceUpSpine} can encounter
   * after an incremental splice. The live path still points at the old subtree, so this walk
   * substitutes the candidate's exact key range and height in registers while resolving every
   * unaffected sibling. No PageReference is changed and no page is allocated.
   *
   * <p>
   * A {@code false} result leaves the caller free to discard the unpublished mini-HOT. Once this
   * returns {@code true}, propagation can fail only through an unexpected allocation/TIL/runtime
   * fault; the caller therefore poisons the transaction if such a failure occurs after publication.
   */
  private boolean canPropagateIncrementalSplice(final LeafNavigationResult navResult, final int rebuiltDepth,
      final PageReference candidateRef) {
    final Page candidatePage = resolveHOTPageForTraversal(candidateRef);
    if (candidatePage == null) {
      return false;
    }
    byte[] propagatedFirst = firstKeyOfSubtree(candidateRef);
    byte[] propagatedLast = lastKeyOfSubtree(candidateRef);
    if (propagatedFirst == null || propagatedLast == null) {
      return false;
    }
    int propagatedHeight = candidatePage instanceof HOTIndirectPage indirect
        ? indirect.getHeight()
        : 0;
    boolean boundaryRelevant = true;

    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int[] childSlots = navResult.pathChildIndices();
    if (rebuiltDepth > 0 && candidatePage instanceof HOTIndirectPage candidateIndirect) {
      final HOTIndirectPage directParent = pathNodes[rebuiltDepth - 1];
      if (directParent.getMostSignificantBitIndex() >= 0
          && candidateIndirect.getMostSignificantBitIndex() <= directParent.getMostSignificantBitIndex()) {
        return false; // I11 must hold before the replacement becomes the parent's live child
      }
    }
    for (int ancestorDepth = rebuiltDepth - 1; ancestorDepth >= 0; ancestorDepth--) {
      final HOTIndirectPage ancestor = pathNodes[ancestorDepth];
      final int numChildren = ancestor.getNumChildren();
      final int rebuiltSlot = childSlots[ancestorDepth];
      if (rebuiltSlot < 0 || rebuiltSlot >= numChildren) {
        return false;
      }

      if (boundaryRelevant) {
        if (rebuiltSlot > 0) {
          final byte[] previousLast = lastKeyOfSubtree(ancestor.getChildReference(rebuiltSlot - 1));
          if (previousLast == null || Arrays.compareUnsigned(previousLast, propagatedFirst) >= 0) {
            return false;
          }
        }
        if (rebuiltSlot + 1 < numChildren) {
          final byte[] nextFirst = firstKeyOfSubtree(ancestor.getChildReference(rebuiltSlot + 1));
          if (nextFirst == null || Arrays.compareUnsigned(propagatedLast, nextFirst) >= 0) {
            return false;
          }
        }
        if (rebuiltSlot != 0) {
          propagatedFirst = firstKeyOfSubtree(ancestor.getChildReference(0));
          if (propagatedFirst == null) {
            return false;
          }
        }
        if (rebuiltSlot != numChildren - 1) {
          propagatedLast = lastKeyOfSubtree(ancestor.getChildReference(numChildren - 1));
          if (propagatedLast == null) {
            return false;
          }
        }
        boundaryRelevant = rebuiltSlot == 0 || rebuiltSlot == numChildren - 1;
      }

      int maxChildHeight = 0;
      for (int i = 0; i < numChildren; i++) {
        final int childHeight;
        if (i == rebuiltSlot) {
          childHeight = propagatedHeight;
        } else {
          final Page childPage = resolveHOTPageForTraversal(ancestor.getChildReference(i));
          if (childPage == null) {
            return false;
          }
          childHeight = childPage instanceof HOTIndirectPage childIndirect
              ? childIndirect.getHeight()
              : 0;
        }
        maxChildHeight = Math.max(maxChildHeight, childHeight);
      }
      propagatedHeight = maxChildHeight + 1;
      if (!boundaryRelevant && propagatedHeight == ancestor.getHeight()) {
        return true;
      }
    }
    return true;
  }

  /**
   * Propagate one preflighted structural splice up the spine via in-place height re-encoding. At each
   * ancestor from {@code rebuiltDepth - 1} down to 0:
   *
   * <ul>
   * <li>Recompute the ancestor's height as {@code 1 + max(child.height)} -- HOT heights are max-based
   * ({@link HOTBulkBuilder#assembleIndirect}); a single rebuilt slot's new height only matters if
   * it's the (possibly tied) maximum.</li>
   * <li>Keep every stored partial verbatim -- a sparse partial is the slot's position in the
   * ancestor's block trie (off-path bits zero by convention), invariant under content growth of the
   * subtree behind it. Recomputing it from the subtree's new first key stamps off-path bits and was
   * the historical mass producer of I4/I5 violations (plan doc, Stage 5).</li>
   * <li>Check the one property a content change can break: sibling key ORDER (I8/I12). Compare the
   * rebuilt slot's extremes against its neighbours'. The mandatory preflight proves every boundary; a
   * post-publication mismatch is transaction-fatal. The check continues upward only while the changed
   * slot sits at its block's edge.</li>
   * <li>Stop early once neither the height nor an edge boundary can change further up.</li>
   * <li>On a height change re-encode the ancestor with the same children + disc bits + partials, just
   * an updated height. The ancestor's child references are shared with the prior version; only the
   * rebuilt slot already points at fresh content via the swizzled {@link PageReference}.</li>
   * </ul>
   *
   * <p>
   * The propagation does not orphan any leaves. Re-encoded ancestors replace their TIL entries at the
   * same {@link PageReference}, dropping the prior in-memory page.
   */
  private void propagateStructuralSpliceUpSpine(LeafNavigationResult navResult, int rebuiltDepth, byte[] keySlice) {
    try {
      propagateStructuralSpliceAfterPublication(navResult, rebuiltDepth, keySlice);
    } catch (final RuntimeException | Error failure) {
      // Every caller has already published the rebuilt child/splice. Keep the poison boundary local
      // so a new caller cannot accidentally turn an unresolved sibling or registration failure into
      // a catchable fallback over a partially updated graph.
      markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  private void propagateStructuralSpliceAfterPublication(final LeafNavigationResult navResult, final int rebuiltDepth,
      final byte[] keySlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final PageReference[] pathRefs = navResult.pathRefs();
    final int[] childSlots = navResult.pathChildIndices();
    final int revision = storageEngineWriter.getRevisionNumber();

    boolean boundaryRelevant = true;
    for (int ancestorDepth = rebuiltDepth - 1; ancestorDepth >= 0; ancestorDepth--) {
      final HOTIndirectPage ancestor = pathNodes[ancestorDepth];
      final int rebuiltSlot = childSlots[ancestorDepth];
      final int numChildren = ancestor.getNumChildren();

      // 1 + max(child.height) -- HOTBulkBuilder.build uses the same formula.
      int maxChildHeight = 0;
      for (int i = 0; i < numChildren; i++) {
        final PageReference childRef = ancestor.getChildReference(i);
        if (childRef == null) {
          throw new IllegalStateException(
              "HOT propagation cannot resolve null child " + i + " of ancestor " + ancestor.getPageKey());
        }
        final Page childPage = resolveHOTPageForTraversal(childRef);
        if (childPage == null) {
          throw new IllegalStateException(
              "HOT propagation cannot resolve child " + i + " of ancestor " + ancestor.getPageKey());
        }
        final int h = childPage instanceof HOTIndirectPage hi
            ? hi.getHeight()
            : 0;
        if (h > maxChildHeight) {
          maxChildHeight = h;
        }
      }
      final int newAncestorHeight = maxChildHeight + 1;

      // The rebuilt slot's PageReference is the same instance the ancestor holds in its
      // children array, so ancestor.getChildReference(rebuiltSlot) already sees the fresh
      // subtree. The slot's STORED PARTIAL is deliberately left untouched: a sparse partial
      // encodes the slot's path through the ancestor's block trie — a position, not a content
      // fingerprint — and the rebuilt subtree holds the same key set (plus a key that
      // subset-routed through this very slot), so its position is unchanged. The previous
      // shape recomputed the partial as densePK(new firstKey), which stamps the first key's
      // values at OFF-PATH mask bits into an encoding whose off-path bits must be zero
      // (Binna's sparse-path convention) — on this branch's attribution run that single line
      // manufactured 1,151 I5 and 80 I4 violations per 36K-insert shred.
      //
      // What a content change CAN legitimately break is sibling ORDER (I8/I12): the rebuilt
      // subtree's minimum may have dropped below the previous sibling's maximum (the
      // Direction-1 shape) — subset routing admits keys the lex order does not. That is a
      // boundary property preflighted before publication; observing it here is transaction-fatal.
      final boolean heightChanged = newAncestorHeight != ancestor.getHeight();

      if (boundaryRelevant) {
        final byte[] slotFirst = firstKeyOfSubtree(ancestor.getChildReference(rebuiltSlot));
        if (slotFirst == null) {
          throw new IllegalStateException(
              "HOT propagation cannot resolve rebuilt slot minimum at ancestor " + ancestor.getPageKey());
        }
        byte[] prevLast = null;
        if (rebuiltSlot > 0) {
          prevLast = lastKeyOfSubtree(ancestor.getChildReference(rebuiltSlot - 1));
          if (prevLast == null) {
            throw new IllegalStateException(
                "HOT propagation cannot resolve preceding sibling maximum at ancestor " + ancestor.getPageKey());
          }
        }
        byte[] slotLast = null;
        byte[] nextFirst = null;
        if (rebuiltSlot + 1 < numChildren) {
          slotLast = lastKeyOfSubtree(ancestor.getChildReference(rebuiltSlot));
          nextFirst = firstKeyOfSubtree(ancestor.getChildReference(rebuiltSlot + 1));
          if (slotLast == null || nextFirst == null) {
            throw new IllegalStateException(
                "HOT propagation cannot resolve rebuilt/next sibling boundary at ancestor " + ancestor.getPageKey());
          }
        }
        final boolean leftViolated = prevLast != null && Arrays.compareUnsigned(prevLast, slotFirst) >= 0;
        final boolean rightViolated = slotLast != null && Arrays.compareUnsigned(slotLast, nextFirst) >= 0;
        if (leftViolated || rightViolated) {
          STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.incrementAndGet();
          throw new IllegalStateException("HOT structural splice crossed a sibling boundary after its successful "
              + "pre-publication propagation proof at ancestor depth " + ancestorDepth);
        }
        // The slot's extremes can influence the NEXT ancestor's boundaries only while the
        // changed slot is the edge of this block (the subtree minimum propagates from slot 0,
        // the maximum from the last slot); interior slots absorb the change here.
        boundaryRelevant = rebuiltSlot == 0 || rebuiltSlot == numChildren - 1;
      }

      if (!heightChanged && !boundaryRelevant) {
        return; // Stable -- propagation complete.
      }
      if (!heightChanged) {
        continue; // Order verified at this level; only the edge-propagation continues upward.
      }

      // Re-encode the ancestor: same disc bits + children + partials, updated height only.
      // assembleIndirect picks the SingleMask/MultiMask layout to match the disc bits exactly
      // as the original encoding -- the new page's mask is identical so routing is
      // invariant-preserving.
      final int[] discBits = HOTIncrementalInsert.discriminativeBits(ancestor);
      final int[] partials = ancestor.getPartialKeysRef().clone();
      final PageReference[] children = new PageReference[numChildren];
      for (int i = 0; i < numChildren; i++) {
        children[i] = ancestor.getChildReference(i);
      }
      final HOTIndirectPage rebuiltAncestor =
          HOTBulkBuilder.assembleIndirect(discBits, partials, children, newAncestorHeight, revision, pageKeyAllocator);
      pathRefs[ancestorDepth].setPage(rebuiltAncestor);
      registerFreshSubtree(pathRefs[ancestorDepth]);
      STRUCTURAL_HEIGHT_REENCODE.incrementAndGet();
    }
  }

  /** Copy the bounded direct-leaf frontier; this helper never descends an indirect subtree. */
  private void collectLeafEntries(final HOTLeafPage leaf, final List<HOTBulkBuilder.Entry> out,
      final List<CapturedSegmentRef> segmentRefsOut) {
    for (final long refKey : leaf.overflowPageRefKeysSorted()) {
      segmentRefsOut.add(new CapturedSegmentRef(refKey, leaf.getPageReference(refKey)));
    }
    final int count = leaf.getEntryCount();
    for (int i = 0; i < count; i++) {
      final byte[] key = leaf.getKey(i);
      if (key == null) {
        throw new IllegalStateException(
            "HOT leaf-frontier splice cannot read leaf " + leaf.getPageKey() + " entry " + i);
      }
      out.add(new HOTBulkBuilder.Entry(key, leaf.copyStoredValue(i)));
    }
  }

  /** A side-map entry captured off a leaf that a bounded frontier splice is about to replace. */
  private record CapturedSegmentRef(long refKey, PageReference reference) {
  }

  /**
   * Re-home captured side-map references into the freshly built subtree: for each reference, descend
   * from {@code newRoot} to the leaf now holding its owning slot and re-attach there. Mirrors
   * {@link HOTLeafPage#overflowPageRefKey}'s contract (owner slot = {@code refKey >>> 16}, stored-key
   * encoding = {@link PathKeySerializer}) — the same owner-slot-residency routing the leaf split
   * paths apply via {@code moveOverflowPageRefsAfterSplit}.
   *
   * <p>
   * The bulk-built subtree contains every collected entry, so the owning slot MUST be found; anything
   * else is data loss and fails loudly.
   */
  private void reattachSegmentRefs(final Page newRoot, final List<CapturedSegmentRef> refs) {
    if (refs.isEmpty()) {
      return;
    }
    final HOTLeafPage[] owners = new HOTLeafPage[refs.size()];
    final LongOpenHashSet uniqueRefKeys = new LongOpenHashSet(refs.size());
    final byte[] ownerKey = new byte[8];
    // Pass 1 is deliberately side-effect free. A missing owner must be discovered before ANY
    // captured reference is attached, so an unpublished bulk-built root always remains safely
    // discardable as one ownership unit.
    for (int i = 0; i < refs.size(); i++) {
      final CapturedSegmentRef captured = refs.get(i);
      if (captured.reference() == null) {
        throw new IllegalStateException(
            "Segment-ref reattach after frontier splice: refKey " + captured.refKey() + " has no PageReference");
      }
      if (!uniqueRefKeys.add(captured.refKey())) {
        throw new IllegalStateException("Segment-ref reattach after frontier splice: duplicate refKey "
            + captured.refKey() + " was captured from more than one source leaf");
      }
      final long ownerSlot = HOTLeafPage.overflowPageRefOwnerSlot(captured.refKey());
      PathKeySerializer.INSTANCE.serialize(ownerSlot, ownerKey, 0);
      Page current = newRoot;
      while (current instanceof HOTIndirectPage indirect) {
        final int childIndex = indirect.findChildIndex(ownerKey);
        if (childIndex < 0) {
          current = null;
          break;
        }
        current = resolveHOTPageForTraversal(indirect.getChildReference(childIndex));
      }
      if (!(current instanceof HOTLeafPage leaf) || leaf.findEntry(ownerKey) < 0) {
        throw new IllegalStateException("Segment-ref reattach after frontier splice: owning slot " + ownerSlot
            + " (refKey=" + captured.refKey() + ") not found in the replacement subtree — an entry was lost.");
      }
      owners[i] = leaf;
    }
    // Pass 2 cannot fail for a fresh active leaf under the single-writer discipline. If an
    // unexpected lifecycle/runtime fault does occur, the caller closes the still-unpublished root;
    // HOTLeafPage teardown severs (but does not retire) these shared PageReference objects.
    for (int i = 0; i < refs.size(); i++) {
      final CapturedSegmentRef captured = refs.get(i);
      owners[i].setPageReference(captured.refKey(), captured.reference());
    }
  }

  /**
   * Build the {@code spineRefs} array {@link HOTIncrementalInsert#integrate} expects: the descent
   * path's compound-node references followed by the leaf's reference ({@code pathDepth + 1} entries).
   */
  private static PageReference[] buildSpineRefs(LeafNavigationResult navResult) {
    final int pathDepth = navResult.pathDepth();
    final PageReference[] spineRefs = new PageReference[pathDepth + 1];
    System.arraycopy(navResult.pathRefs(), 0, spineRefs, 0, pathDepth);
    spineRefs[pathDepth] = navResult.leafRef();
    return spineRefs;
  }

  /**
   * Resolve and swizzle every child page of every path compound node, so that
   * {@link HOTIncrementalInsert}'s split / {@code addEntry} height accounting reads real pages
   * instead of {@code null}. Runs once per structural overflow (rare), never on the merge fast path;
   * a child already in memory is left untouched.
   */
  private void ensurePathChildrenLoaded(final HOTIndirectPage[] pathNodes, final int pathDepth) {
    for (int depth = 0; depth < pathDepth; depth++) {
      ensureNodeChildrenLoaded(pathNodes[depth]);
    }
  }

  /** Resolve one compound node's direct children for exact local height accounting. */
  private void ensureNodeChildrenLoaded(final HOTIndirectPage node) {
    for (int i = 0; i < node.getNumChildren(); i++) {
      final PageReference childRef = node.getChildReference(i);
      if (childRef == null) {
        throw new IllegalStateException(
            "HOT direct child " + i + " of page " + node.getPageKey() + " has no reference");
      }
      if (childRef.getPage() == null) {
        final Page child = resolveHOTPageForTraversal(childRef);
        if (child == null) {
          throw new IllegalStateException(
              "HOT cannot resolve direct child " + i + " of page " + node.getPageKey() + " for exact height");
        }
        childRef.setPage(child);
      }
    }
  }

  /**
   * Register the fresh subtree {@link HOTIncrementalInsert#integrate} produced into the
   * transaction-intent log. {@code touchedRef} is the single spine reference {@code integrate}
   * re-pointed; its TIL entry still holds the stale pre-integration page, and every page strictly
   * below it is swizzled in memory but unlogged.
   *
   * <p>
   * The walk is post-order — {@code TransactionIntentLog.put} nulls a reference's in-memory page, so
   * children are registered before their parent — and stops at shared subtrees: a reference that
   * already carries an on-disk key or a TIL log-key roots an unchanged subtree that {@code integrate}
   * merely re-used by reference.
   */
  private void registerFreshSubtree(PageReference touchedRef) {
    // Every structural mutation publishes exactly one touched reference before entering this
    // registration choke point. Keeping the deterministic fault seam here tests the atomicity of
    // the shared writer instead of depending on one rare repair shape to occur by chance.
    final Runnable publicationTestHook = structuralPublicationTestHook;
    if (publicationTestHook != null) {
      publicationTestHook.run();
    }
    structuralValidationScope = touchedRef;
    registerFreshPage(touchedRef, true);
  }

  /**
   * Retire the old leaf entry after a split/pair published at an ancestor reference. A root-leaf
   * replacement reuses the source reference (or its shared TIL handle), so the ordinary
   * {@link TransactionIntentLog#put} replacement path already retired it and no forwarding is needed.
   * The TIL's identity owner index makes the remaining path O(1), independent of transaction size,
   * and preserves a leaf instance that the replacement subtree deliberately reuses.
   */
  private void retireReplacedLeaf(final PageReference sourceRef, final PageReference replacementRef,
      final int releaseSite) {
    final PageReference.TransactionLogReference sourceIdentity = sourceRef.transactionLogReference();
    if (sourceRef == replacementRef
        || (sourceIdentity != null && sourceIdentity == replacementRef.transactionLogReference())) {
      return;
    }
    requireNonNull(storageEngineWriter.getLog(), "transaction intent log").releaseOrphanedHOTLeaves(indexScope(),
        replacementRef, List.of(sourceRef), releaseSite);
  }

  private void registerFreshPage(PageReference ref, boolean touched) {
    if (ref == null) {
      return;
    }
    if (!touched && (ref.getLogKey() >= 0 || ref.getKey() >= 0)) {
      return; // a shared subtree — already in the TIL or on disk; nothing fresh hangs below it
    }
    final Page page = ref.getPage();
    if (page == null) {
      return;
    }
    if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        try {
          registerFreshPage(indirect.getChildReference(i), false);
        } catch (final RuntimeException | Error failure) {
          // Registration is post-order. Earlier siblings are already TIL-owned and the failing
          // child's registered refs are skipped by the cleanup guard; the failing child and later
          // siblings may still be locally owned. Retire that fresh suffix before unwinding.
          closeUnregisteredFreshChildren(indirect, i, failure);
          throw failure;
        }
      }
    } else if (page instanceof HOTLeafPage freshLeaf) {
      // A freshly created leaf has no on-disk predecessor — mark it a complete dump so commit
      // emits it as a full first fragment and later readers never chase a fragment chain.
      freshLeaf.setCompleteDump(true);
    }
    // Register a PageContainer so the page is persisted: a fresh page is its own complete and
    // modified view; an indirect carries no version chain, and a fresh leaf is full-emitted at
    // commit because its completePageRef is null (see PageKind.HOT_LEAF_PAGE.serializePage).
    PageContainer container = null;
    TransactionIntentLog log = null;
    boolean putStarted = false;
    try {
      container = PageContainer.getInstance(page, page);
      log = requireNonNull(storageEngineWriter.getLog(), "transaction intent log");
      putStarted = true;
      log.put(ref, container);
    } catch (final RuntimeException | Error failure) {
      recoverFromRegistrationFailure(failure, ref, page, container, log, putStarted);
      throw failure;
    }
  }

  /**
   * Settle ownership of a fresh page whose transaction-log registration threw.
   *
   * <p>
   * {@code put()} clears {@code ref.page} before it publishes a new log slot. Retain the local page
   * so a failure between those operations cannot strand an off-heap leaf outside both the tree and
   * the TIL. Before {@code put} begins, ownership is known to remain local. Once it begins, exact
   * container identity is the boundary; if that probe itself fails, retain rather than risk closing a
   * log-owned page. Any failure here dooms the transaction because descendant pages may already have
   * crossed their ownership boundary.
   * </p>
   *
   * <p>
   * Every step absorbs its own secondary failure into {@code failure}'s suppressed list, so the
   * caller always rethrows the original cause.
   * </p>
   */
  private void recoverFromRegistrationFailure(final Throwable failure, final PageReference ref, final Page page,
      final @Nullable PageContainer container, final @Nullable TransactionIntentLog log, final boolean putStarted) {
    boolean ownershipKnown = !putStarted;
    boolean logOwnsContainer = false;
    if (putStarted) {
      try {
        logOwnsContainer = log.get(ref) == container;
        ownershipKnown = true;
      } catch (final RuntimeException | Error ownershipFailure) {
        addSuppressedSafely(failure, ownershipFailure);
      }
    }
    try {
      storageEngineWriter.markTransactionRollbackOnly(failure);
    } catch (final RuntimeException | Error poisonFailure) {
      addSuppressedSafely(failure, poisonFailure);
    }
    if (ownershipKnown && !logOwnsContainer && page instanceof HOTLeafPage freshLeaf) {
      try {
        freshLeaf.close();
      } catch (final RuntimeException | Error cleanupFailure) {
        addSuppressedSafely(failure, cleanupFailure);
      }
    }
  }

  /**
   * Best-effort cleanup for a fresh subtree that registration has not visited. A disk key is a
   * durable shared boundary and a log key is a TIL-owned boundary. Each recursive call absorbs its
   * own cleanup failure so siblings are still examined.
   */
  private void closeUnregisteredFreshSubtree(final @Nullable PageReference ref, final Throwable primaryFailure) {
    try {
      if (ref == null || ref.getKey() >= 0) {
        return;
      }
      final Page page = ref.getPage();
      if (page == null) {
        return;
      }
      if (ref.getLogKey() >= 0) {
        // Unlike the reused publication root, structural builders never replace a shared child in
        // place. Its live log identity is therefore already the exact ownership boundary.
        return;
      }
      if (page instanceof HOTIndirectPage indirect) {
        closeUnregisteredFreshChildren(indirect, 0, primaryFailure);
      } else if (page instanceof HOTLeafPage leaf && !leaf.isClosed()) {
        leaf.close();
      }
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(primaryFailure, cleanupFailure);
    }
  }

  private void closeUnregisteredFreshChildren(final HOTIndirectPage indirect, final int fromInclusive,
      final Throwable primaryFailure) {
    final int numChildren;
    try {
      numChildren = indirect.getNumChildren();
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(primaryFailure, cleanupFailure);
      return;
    }
    for (int i = fromInclusive; i < numChildren; i++) {
      final PageReference childRef;
      try {
        childRef = indirect.getChildReference(i);
      } catch (final RuntimeException | Error cleanupFailure) {
        addSuppressedSafely(primaryFailure, cleanupFailure);
        continue;
      }
      closeUnregisteredFreshSubtree(childRef, primaryFailure);
    }
  }

  /**
   * The least significant (largest absolute index) discriminative bit of a compound node — the
   * deepest bit it branches on. Computed allocation-free: {@code discriminativeBits} returns the bits
   * sorted ascending by absolute position, so the maximum is the highest extraction byte's
   * lowest-order on-path bit (MULTI_MASK) or {@code initialBytePos*8 + (63 - ntz(bitMask))}
   * (single-mask). This is on the per-insert merge-vs-branch decision path, so it must not allocate
   * the {@code int[]} that {@link HOTIncrementalInsert#discriminativeBits} would.
   */
  private static int leastSignificantDiscBit(HOTIndirectPage node) {
    if (node.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
      final int last = node.getNumExtractionBytes() - 1; // highest key-byte position
      final int bytePos = node.getExtractionPositions()[last] & 0xFF;
      final long[] masks = node.getExtractionMasks();
      final int byteMask = (int) ((masks[last / 8] >>> ((7 - last % 8) * 8)) & 0xFFL);
      // Largest MSB-first bit-in-byte set = 7 - (trailing zeros of the byte mask).
      return bytePos * 8 + (7 - Integer.numberOfTrailingZeros(byteMask));
    }
    return node.getInitialBytePos() * 8 + (63 - Long.numberOfTrailingZeros(node.getBitMask()));
  }

  /**
   * Get value from a leaf page.
   *
   * @param leaf the leaf page
   * @param keyBuf the key buffer
   * @return the node references, or null if not found
   */
  protected @Nullable NodeReferences getFromLeaf(HOTLeafPage leaf, byte[] keyBuf) {
    int index = leaf.findEntry(keyBuf);
    if (index < 0) {
      return null;
    }

    byte[] valueBytes = leaf.getValue(index);
    if (NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
      return null; // Deleted entry
    }
    return NodeReferencesSerializer.deserializeChunk(valueBytes);
  }

  /**
   * Serialize value to the thread-local buffer, expanding if necessary.
   *
   * <p>
   * Results are stored in {@link #lastSerializedValueBuf} and {@link #lastSerializedValueLen} to
   * avoid the {@code Object[]} allocation and {@code int} boxing of the old return-value API. This is
   * safe because {@code AbstractHOTIndexWriter} is single-threaded per transaction.
   * </p>
   *
   * @param value the value to serialize
   */
  protected void serializeValueInto(NodeReferences value) {
    byte[] valueBuf = VALUE_BUFFER.get();
    final int requiredSize = NodeReferencesSerializer.computeSerializedSize(value);
    if (requiredSize > valueBuf.length) {
      valueBuf = new byte[requiredSize];
      VALUE_BUFFER.set(valueBuf);
    }
    final int valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    lastSerializedValueBuf = valueBuf;
    lastSerializedValueLen = valueLen;
  }

}
