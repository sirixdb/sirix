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
import io.sirix.access.trx.page.HOTTrieWriter;
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
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * <li>Pre-allocated traversal state via {@link HOTTrieWriter}</li>
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
   */
  private static final long HOT_LEAF_GUARD_RETRY_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(1L);

  /** Inserts between periodic leaf-consolidation sweeps ({@link #consolidateSubtree}). */
  private static final int CONSOLIDATION_INTERVAL = 4096;

  /**
   * The largest union a consolidation merge produces — kept below page capacity so a merged leaf has
   * room before it re-splits. {@code MAX_ENTRIES * 3/4} packs leaves toward well-filled.
   */
  private static final int CONSOLIDATION_TARGET = (HOTLeafPage.MAX_ENTRIES * 3) / 4;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHOTIndexWriter.class);

  /** Package-private deterministic fault seam; non-null only inside the atomicity regression test. */
  private static volatile @Nullable Runnable directionOneFrontierAfterPublicationTestHook;

  protected final StorageEngineWriter storageEngineWriter;
  protected final IndexType indexType;
  protected final int indexNumber;

  /**
   * Persistent page-key allocator for this index, used to stamp the pages
   * {@link HOTIncrementalInsert} creates on the live insert path.
   */
  protected final LongSupplier pageKeyAllocator;

  /** HOT trie writer for handling page splits. */
  protected final HOTTrieWriter trieWriter;

  /** Cached root page reference for the index. */
  protected PageReference rootReference;

  // ===== Pre-allocated path-tracking arrays — ZERO allocation per insert on hot path =====
  // These are overwritten on every prepareLeafOfTree() call; LeafNavigationResult stores
  // copies only when the path depth is non-zero (a small Arrays.copyOf of depth <= 64).
  private final HOTIndirectPage[] _pathNodes = new HOTIndirectPage[MAX_PATH_DEPTH];
  private final PageReference[] _pathRefs = new PageReference[MAX_PATH_DEPTH];
  private final int[] _pathChildIndices = new int[MAX_PATH_DEPTH];

  // ===== Last serialized value — replaces Object[] return from serializeValue =====
  /** The serialized value bytes from the most recent {@link #serializeValueInto} call. */
  protected byte[] lastSerializedValueBuf;
  /** The valid byte count in {@link #lastSerializedValueBuf}. */
  protected int lastSerializedValueLen;

  /** Inserts since the last {@link #consolidateSubtree} sweep — drives periodic consolidation. */
  private int insertsSinceConsolidation;

  /**
   * The reference a structural handler last spliced into the trie this dispatch — the root of the
   * subtree the mutation touched, captured at the {@link #registerFreshSubtree} choke point. The
   * post-dispatch full-invariant self-heal scopes its detector to exactly this subtree: a mutation
   * can only malform nodes it touched, so verifying this subtree is sound, and far cheaper than a
   * from-root scan. {@code null} when no structural change occurred (the merge fast path).
   */
  private PageReference selfHealScope;

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
  protected record LeafNavigationResult(HOTLeafPage leaf, PageReference leafRef, HOTIndirectPage[] pathNodes,
      PageReference[] pathRefs, int[] pathChildIndices, int pathDepth) {
  }

  /**
   * Protected constructor.
   *
   * @param storageEngineWriter the storage engine writer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  protected AbstractHOTIndexWriter(StorageEngineWriter storageEngineWriter, IndexType indexType, int indexNumber) {
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
    this.pageKeyAllocator = createPageKeyAllocator(storageEngineWriter, indexType, indexNumber);
    this.trieWriter = new HOTTrieWriter(pageKeyAllocator);
  }

  protected boolean allowsSubtreeRebuild() {
    return true;
  }

  private void requireSubtreeRebuildAllowed() {
    if (indexType == IndexType.PROJECTION) {
      PROJECTION_REBUILD_SUBTREE_ATTEMPTED.incrementAndGet();
    }
    if (!allowsSubtreeRebuild()) {
      throw new IllegalStateException(indexType + " index " + indexNumber
          + " requires a subtree rebuild; refusing the transaction before publication");
    }
  }

  /**
   * Create a persistent page key allocator backed by the index page's maxHotPageKey counter.
   *
   * <p>
   * The returned {@link LongSupplier} allocates monotonically increasing page keys that are persisted
   * across transactions via the index page (PathPage/CASPage/NamePage). This replaces the old
   * hardcoded {@code nextPageKey = 1000000L} counter that restarted on every transaction.
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
      case PATH -> () -> {
        final RevisionRootPage rrp = writer.getActualRevisionRootPage();
        return writer.getPathPage(rrp).incrementAndGetMaxHotPageKey(indexNo);
      };
      case CAS -> () -> {
        final RevisionRootPage rrp = writer.getActualRevisionRootPage();
        return writer.getCASPage(rrp).incrementAndGetMaxHotPageKey(indexNo);
      };
      case NAME -> () -> {
        final RevisionRootPage rrp = writer.getActualRevisionRootPage();
        return writer.getNamePage(rrp).incrementAndGetMaxHotPageKey(indexNo);
      };
      case PROJECTION -> () -> {
        final RevisionRootPage rrp = writer.getActualRevisionRootPage();
        return writer.getProjectionIndexPage(rrp).incrementAndGetMaxHotPageKey(indexNo);
      };
      case VALIDTIME -> () -> {
        final RevisionRootPage rrp = writer.getActualRevisionRootPage();
        return writer.getValidTimeIndexPage(rrp).incrementAndGetMaxHotPageKey(indexNo);
      };
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
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = storageEngineWriter.getCASPage(revisionRootPage);
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = storageEngineWriter.getNamePage(revisionRootPage);
        yield namePage.getOrCreateReference(indexNumber);
      }
      case PROJECTION -> {
        final ProjectionIndexPage projPage = storageEngineWriter.getProjectionIndexPage(revisionRootPage);
        yield projPage.getOrCreateReference(indexNumber);
      }
      case VALIDTIME -> {
        final ValidTimeIndexPage vtPage = storageEngineWriter.getValidTimeIndexPage(revisionRootPage);
        yield vtPage.getOrCreateReference(indexNumber);
      }
      default -> throw new IllegalStateException("Unsupported index type for HOT: " + indexType);
    };
  }

  /**
   * Mark the index page as dirty so changes are persisted.
   */
  protected void prepareIndexPage() {
    final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    switch (indexType) {
      case PATH -> {
        final PageReference pathPageRef = revisionRootPage.getPathPageReference();
        PageContainer container = storageEngineWriter.getLog().get(pathPageRef);
        if (container == null) {
          PathPage pathPage = storageEngineWriter.getPathPage(revisionRootPage);
          storageEngineWriter.appendLogRecord(pathPageRef, PageContainer.getInstance(pathPage, pathPage));
        }
      }
      case CAS -> {
        final PageReference casPageRef = revisionRootPage.getCASPageReference();
        PageContainer container = storageEngineWriter.getLog().get(casPageRef);
        if (container == null) {
          CASPage casPage = storageEngineWriter.getCASPage(revisionRootPage);
          storageEngineWriter.appendLogRecord(casPageRef, PageContainer.getInstance(casPage, casPage));
        }
      }
      case NAME -> {
        final PageReference namePageRef = revisionRootPage.getNamePageReference();
        PageContainer container = storageEngineWriter.getLog().get(namePageRef);
        if (container == null) {
          NamePage namePage = storageEngineWriter.getNamePage(revisionRootPage);
          storageEngineWriter.appendLogRecord(namePageRef, PageContainer.getInstance(namePage, namePage));
        }
      }
      case PROJECTION -> {
        final PageReference projPageRef = revisionRootPage.getProjectionIndexPageReference();
        PageContainer container = storageEngineWriter.getLog().get(projPageRef);
        if (container == null) {
          // Top-down CoW: deep-copy the page so the writer-side mutates a private instance.
          // Without this the rev-(N-1) cached ProjectionIndexPage still aliases the same root
          // PageReference instance, and TIL.put / chain-bump mutations bleed into historical reads.
          ProjectionIndexPage projPage = storageEngineWriter.getProjectionIndexPage(revisionRootPage);
          ProjectionIndexPage modified = new ProjectionIndexPage(projPage);
          storageEngineWriter.appendLogRecord(projPageRef, PageContainer.getInstance(projPage, modified));
        }
      }
      case VALIDTIME -> {
        final PageReference vtPageRef = revisionRootPage.getValidTimeIndexPageReference();
        PageContainer container = storageEngineWriter.getLog().get(vtPageRef);
        if (container == null) {
          // Top-down CoW: deep-copy the page so the writer-side mutates a private instance.
          // Without this the rev-(N-1) cached ValidTimeIndexPage still aliases the same root
          // PageReference instance, and TIL.put / chain-bump mutations bleed into historical reads.
          ValidTimeIndexPage vtPage = storageEngineWriter.getValidTimeIndexPage(revisionRootPage);
          ValidTimeIndexPage modified = new ValidTimeIndexPage(vtPage);
          storageEngineWriter.appendLogRecord(vtPageRef, PageContainer.getInstance(vtPage, modified));
        }
      }
      default -> {
        /* ignore */ }
    }
  }

  /**
   * Navigate to the correct leaf page for a key, tracking the path from root.
   *
   * <p>
   * <b>Zero allocation design:</b> Path nodes/refs/indices are accumulated in pre-allocated instance
   * arrays ({@code _pathNodes}, {@code _pathRefs}, {@code _pathChildIndices}). Only shallow
   * {@link Arrays#copyOf} trims are done on return to give the caller independent arrays of exactly
   * the right depth. This eliminates {@code ArrayList} and {@code Integer} boxing that would
   * otherwise occur on every insert.
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
    // Any write-path navigation invalidates the read-side leaf cache —
    // splits/merges change key ranges and leaf identities, so the cached
    // firstKey/lastKey may no longer match the resident page.
    invalidateLeafCache();

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
    final byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);
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

      int childIndex = cowedIndirect.findChildIndex(keySlice);
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
          && !modifiedLeaf.isClosed()) {
        return buildNavigationResult(modifiedLeaf, currentRef, pathDepth);
      }

      final HOTLeafPage cowedLeaf = cowHOTLeafForModification(currentRef, hotLeaf);
      return buildNavigationResult(cowedLeaf, currentRef, pathDepth);
    }

    // Empty tree path: create a new leaf at currentRef (root or missing child).
    // currentRef here is owned by the CoW'd parent's children array (top-down CoW above).
    final HOTLeafPage newLeaf = new HOTLeafPage(currentRef.getKey() >= 0
        ? currentRef.getKey()
        : 0, storageEngineWriter.getRevisionNumber(), indexType);
    final PageContainer container = PageContainer.getInstance(newLeaf, newLeaf);
    storageEngineWriter.getLog().put(currentRef, container);

    return buildNavigationResult(newLeaf, currentRef, pathDepth);
  }

  /**
   * Resolve the root reference of this HOT sub-tree from the CoW'd index page now in the transaction
   * log. Required because the cached {@link #rootReference} field points at the pre-CoW index page's
   * slot — that instance is shared with the historical revision's view. After
   * {@link #prepareIndexPage()} has put a deep-copied page in the log, the slot returned by
   * {@code getOrCreateReference(indexNumber)} on the CoW'd page is a fresh {@link PageReference}
   * owned exclusively by this writer's transaction.
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
      case PATH -> ((PathPage) modified).getOrCreateReference(indexNumber);
      case CAS -> ((CASPage) modified).getOrCreateReference(indexNumber);
      case NAME -> ((NamePage) modified).getOrCreateReference(indexNumber);
      case PROJECTION -> ((ProjectionIndexPage) modified).getOrCreateReference(indexNumber);
      case VALIDTIME -> ((ValidTimeIndexPage) modified).getOrCreateReference(indexNumber);
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
   * Get the HOT leaf page for reading.
   *
   * <p>
   * Uses the storage engine's versioning-aware page loading. Navigates through the tree structure
   * when splits have occurred.
   * </p>
   *
   * @param keyBuf the key buffer
   * @return the HOT leaf page, or null if not found
   */
  protected @Nullable HOTLeafPage getLeafForRead(byte[] keyBuf) {
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
      int childIndex = indirectPage.findChildIndex(keyBuf);
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
   */
  private @Nullable Page resolveHOTPageForTraversal(final PageReference ref) {
    final PageContainer container = storageEngineWriter.getLog().get(ref);
    if (container != null) {
      final Page modified = container.getModified();
      if (modified != null && !modified.isClosed()) {
        return modified;
      }
      final Page complete = container.getComplete();
      if (complete != null && !complete.isClosed()) {
        return complete;
      }
    }

    final Page swizzled = ref.getPage();
    if (swizzled != null && !swizzled.isClosed()) {
      return swizzled;
    }

    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null;
    }

    return storageEngineWriter.loadHOTPage(ref);
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
   * leaf {@code createHOT*IndexTree} planted. An indirect root only exists once a leaf has split, so
   * it always covers at least one entry.
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
   * Splices exactly like the scoped {@link #rebuildExistingSubtree} does, but at the root and from
   * externally supplied entries: {@link HOTBulkBuilder} output is invariant-clean by construction
   * (foundation Theorem 1), so no self-heal is needed afterwards. Registering the fresh subtree also
   * re-puts the root reference, which closes the empty leaf it displaces.
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
    final HOTBulkBuilder.BuildResult built =
        HOTBulkBuilder.build(sortedEntries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    rootReference.setPage(built.rootPage());
    registerFreshSubtree(rootReference);
  }

  /**
   * Build immutable navigation result by trimming reusable path buffers.
   */
  private LeafNavigationResult buildNavigationResult(final HOTLeafPage leaf, final PageReference leafRef,
      final int pathDepth) {
    final HOTIndirectPage[] pathNodes = pathDepth == 0
        ? new HOTIndirectPage[0]
        : Arrays.copyOf(_pathNodes, pathDepth);
    final PageReference[] pathRefs = pathDepth == 0
        ? new PageReference[0]
        : Arrays.copyOf(_pathRefs, pathDepth);
    final int[] pathChildIndices = pathDepth == 0
        ? new int[0]
        : Arrays.copyOf(_pathChildIndices, pathDepth);
    return new LeafNavigationResult(leaf, leafRef, pathNodes, pathRefs, pathChildIndices, pathDepth);
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
      if (!(reloaded instanceof HOTLeafPage reloadedLeaf)) {
        throw new IllegalStateException("HOT leaf disappeared while acquiring a copy-on-write guard");
      }
      sourceLeaf = reloadedLeaf;
    }
  }

  /**
   * Pace one lost guard race and enforce the retry deadline.
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
    Thread.yield();
    return deadlineNanos;
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

  private static boolean containerOwnsPage(final @Nullable PageContainer container, final HOTLeafPage page) {
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
      final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();

      // CRITICAL: Check if PathPage is already in the transaction log first
      final PageReference pathPageRef = revisionRootPage.getPathPageReference();
      PageContainer pathContainer = storageEngineWriter.getLog().get(pathPageRef);
      final PathPage pathPage;
      if (pathContainer != null && pathContainer.getModified() instanceof PathPage modifiedPath) {
        pathPage = modifiedPath;
      } else {
        pathPage = storageEngineWriter.getPathPage(revisionRootPage);
        storageEngineWriter.appendLogRecord(pathPageRef, PageContainer.getInstance(pathPage, pathPage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = pathPage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        pathPage.createHOTPathIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = pathPage.getOrCreateReference(indexNumber);
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
      final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();

      // CRITICAL: Check if CASPage is already in the transaction log first
      final PageReference casPageRef = revisionRootPage.getCASPageReference();
      PageContainer casContainer = storageEngineWriter.getLog().get(casPageRef);
      final CASPage casPage;
      if (casContainer != null && casContainer.getModified() instanceof CASPage modifiedCAS) {
        casPage = modifiedCAS;
      } else {
        casPage = storageEngineWriter.getCASPage(revisionRootPage);
        storageEngineWriter.appendLogRecord(casPageRef, PageContainer.getInstance(casPage, casPage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = casPage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        casPage.createHOTCASIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = casPage.getOrCreateReference(indexNumber);
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
      final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();

      // CRITICAL: Check if NamePage is already in the transaction log first
      final PageReference namePageRef = revisionRootPage.getNamePageReference();
      PageContainer nameContainer = storageEngineWriter.getLog().get(namePageRef);
      final NamePage namePage;
      if (nameContainer != null && nameContainer.getModified() instanceof NamePage modifiedName) {
        namePage = modifiedName;
      } else {
        namePage = storageEngineWriter.getNamePage(revisionRootPage);
        storageEngineWriter.appendLogRecord(namePageRef, PageContainer.getInstance(namePage, namePage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = namePage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        namePage.createHOTNameIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
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
      final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();

      // CRITICAL: Check if ValidTimeIndexPage is already in the transaction log first
      final PageReference vtPageRef = revisionRootPage.getValidTimeIndexPageReference();
      PageContainer vtContainer = storageEngineWriter.getLog().get(vtPageRef);
      final ValidTimeIndexPage vtPage;
      if (vtContainer != null && vtContainer.getModified() instanceof ValidTimeIndexPage modifiedVt) {
        vtPage = modifiedVt;
      } else {
        vtPage = storageEngineWriter.getValidTimeIndexPage(revisionRootPage);
        storageEngineWriter.appendLogRecord(vtPageRef, PageContainer.getInstance(vtPage, vtPage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = vtPage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        vtPage.createValidTimeIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
      }
      rootReference = vtPage.getOrCreateReference(indexNumber);
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
  protected void doIndex(byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {
    if (rootReference == null) {
      throw new SirixIOException("HOT index not initialized for " + indexType);
    }

    // Trim the 4KB thread-local key buffer to its real length ONCE, then reuse the slice for
    // navigation too: passing keySlice (whose length == keyLen) makes prepareLeafOfTree's own
    // trim a no-op, eliminating one redundant per-insert copy on the dominant churn path.
    final byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keySlice, keySlice.length);

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
    dispatchInsert(navResult, keyBuf, keyLen, valueBuf, valueLen, keySlice);

    if (localize && i8Before == null) {
      i8ProbeReport("dispatch(" + (i8ProbeMerge
          ? "merge"
          : "branch") + ")", keySlice, cntBefore);
    }

    // Periodic leaf consolidation (the thesis's underflow rule). The incremental insert leaves
    // the trie over-partitioned — under-full frozen leaves an insert never re-routes to — so a
    // per-insert trigger cannot reach them; a periodic sweep does. Amortized: one O(index) sweep
    // per CONSOLIDATION_INTERVAL inserts.
    if (navResult.pathDepth() > 0 && ++insertsSinceConsolidation >= CONSOLIDATION_INTERVAL) {
      insertsSinceConsolidation = 0;
      final String consBefore = localize
          ? firstStructuralViolationFromRoot()
          : null;
      final long[] consCntBefore = localize
          ? i8ProbeSnapshot()
          : null;
      consolidateSubtree(navResult.pathRefs()[0]);
      // Defense-in-depth: consolidation is a whole-subtree post-order sweep (it merges under-full
      // sibling leaves), so unlike a dispatch fold it can touch nodes OFF the inserted key's path
      // — the path self-heal above cannot see those. Run the FULL-invariant detector over the
      // consolidated subtree (incl. I5 = routing soundness) and discharge every malformed node via
      // a scoped rebuild. Amortized cheap: runs once per CONSOLIDATION_INTERVAL inserts, the same
      // O(subtree) cadence as the sweep it guards.
      if (SELFHEAL_STRUCTURAL) {
        lastDispatchHandler = "h:consolidate-sweep";
        detectAndHeal(navResult.pathRefs()[0], keySlice);
      }
      if (localize && consBefore == null) {
        i8ProbeReport("consolidate", keySlice, consCntBefore);
      }
    }
  }

  // ===== I8-onset localizer helpers (diagnostic; see field declarations). =====

  private long[] i8ProbeSnapshot() {
    return new long[] {OFF_PATH_OVERFLOW_OK.get(), OFF_PATH_OVERFLOW_FALLBACK.get(), DIRECTION_ONE_SUBINSERT.get(),
        DIRECTION_ONE_FALLBACK.get(), STRAND_LEAF_REBUILD.get(), STRAND_FULL_FALLBACK.get(),
        STRAND_TWO_LEAF_MIGRATE.get(), REBUILD_SUBTREE_CALLED.get()};
  }

  private void i8ProbeReport(String phase, byte[] keySlice, long[] before) {
    final String viol = firstStructuralViolationFromRoot();
    if (viol == null) {
      return;
    }
    i8ProbeReports++;
    final long[] after = i8ProbeSnapshot();
    final String[] names = {"offPathOk", "offPathFallback", "dir1Subinsert", "dir1Fallback", "strandLeaf", "strandFull",
        "strandMigrate", "rebuild"};
    final StringBuilder deltas = new StringBuilder();
    for (int i = 0; i < names.length; i++) {
      if (after[i] != before[i]) {
        deltas.append(names[i]).append('+').append(after[i] - before[i]).append(' ');
      }
    }
    System.err.println("[I8-LOCALIZE] rev=" + storageEngineWriter.getRevisionNumber() + " phase=" + phase + " key="
        + HexFormat.of().formatHex(keySlice, 0, Math.min(keySlice.length, 22)) + " handlers={"
        + deltas.toString().trim() + "} onset=" + viol);
  }

  /**
   * First cheap structural violation (I4 first-partial-zero, I7 partials-ascending, I8
   * children-by-firstKey) reachable from the index root, or {@code null}. These are the
   * O(children)/O(children×height) invariants — the expensive I5 constancy walk is left to the
   * per-revision {@code HOTInvariantValidator}. Diagnostic only (localizer).
   */
  private @Nullable String firstStructuralViolationFromRoot() {
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
  private void dispatchInsert(LeafNavigationResult navResult, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen,
      byte[] keySlice) {
    final int pathDepth = navResult.pathDepth();
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final HOTIncrementalInsert.DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(pathNodes,
        navResult.pathChildIndices(), pathDepth, navResult.leaf(), keySlice);

    // Merge-vs-branch: the key merges into the routed leaf when there is no compound ancestor,
    // when it is already present or the leaf is empty (beta < 0), or when the mismatch bit beta
    // is strictly less significant than every ancestor discriminative bit -- i.e. beta lies
    // inside the leaf's R(S)-subtree (I5 holds). The bound is the deepest compound node's least
    // significant disc bit (I11 dominates the shallower bits). Larger absolute bit index = less
    // significant.
    final int beta = analysis.mismatchBit();
    final boolean merge = beta < 0 || pathDepth == 0 || beta > leastSignificantDiscBit(pathNodes[pathDepth - 1]);
    if (LOCALIZE_I8) {
      i8ProbeMerge = merge;
    }

    selfHealScope = null; // set by registerFreshSubtree iff this dispatch splices a subtree
    lastDispatchHandler = merge
        ? "merge"
        : "branch";
    final boolean structurallyChanged = merge
        ? mergeIntoLeaf(navResult, keyBuf, keyLen, valueBuf, valueLen, keySlice)
        : branchAboveLeaf(navResult, analysis, keySlice, valueBuf, valueLen);

    // Defense-in-depth (full-invariant). A structural fold (off-path-overflow / integrate / a
    // combo-add the pre-commit guards don't fully cover) can, in rare multi-value-leaf shapes at
    // high chunkIdx, leave the touched subtree malformed. Two scoped, complementary checks, both
    // discharging via the Theorem-4 scoped rebuild and both skipped on the fast merge (no
    // structural change) and after a rebuild (already canonical):
    // (1) detectAndHeal on the touched subtree runs the FULL invariant set — crucially I5
    // (leaf-constancy), which is routing-soundness (foundation Theorem 2), so it transitively
    // covers I6 (mis-route) and I1 (cross-leaf dup) without separate machinery — plus
    // I3/I4/I7/I8/I11. A mutation only malforms nodes it touched, so scoping the O(subtree)
    // walk to selfHealScope is sound and bounded (not a from-root scan).
    // (2) healStructuralViolationOnPath covers the ANCESTORS above the touched subtree: their
    // blocks are unmodified (I5 preserved), but the touched subtree's key range may have
    // shifted, so re-verify the cheap ordering invariants (I4/I7/I8/I12) up the spine.
    if (structurallyChanged && SELFHEAL_STRUCTURAL) {
      detectAndHeal(selfHealScope, keySlice);
      healStructuralViolationOnPath(keySlice);
    }
  }

  /**
   * Branch-escape guard for subclasses that run their own merge path (multi-entry slot stores such as
   * the projection index, whose slot semantics are replace-not-OR-merge): decide whether
   * {@code (keySlice, value)} may be MERGED into the routed leaf, and if not, perform the branch
   * insert here.
   *
   * <p>
   * This is the merge-vs-branch dispatch of {@link #dispatchInsert} made available to callers that
   * bypass {@link #doIndex}. Skipping it is not an optimisation but a correctness bug: subset-match
   * routing ({@link HOTIndirectPage#findChildIndex}) can land a key in a leaf whose
   * {@code R(S)}-subtree it does not belong to (mismatch bit β at or above an ancestor's
   * discriminative bit). Absorbing there makes the leaf's key range NON-CONTIGUOUS — point lookups
   * keep working (routing stays self-consistent), but leaves stop being lex-ordered, and every
   * bounded range scan that ends at the first key past its upper bound silently truncates. Measured
   * on a 196-row-group projection index: one absorbed boundary key left a leaf holding row groups
   * {@code 128..159} AND {@code 192..196} while its right sibling held {@code 160..191}, and the
   * index silently stopped serving.
   *
   * @return {@code true} when the key branched (it is fully inserted — the caller must NOT also
   *         merge); {@code false} when the key belongs in the routed leaf and the caller merges
   */
  protected final boolean branchIfEscapesRoutedLeaf(final LeafNavigationResult navResult, final byte[] keySlice,
      final byte[] valueBuf, final int valueLen) {
    final int pathDepth = navResult.pathDepth();
    if (pathDepth == 0) {
      return false; // the root is the leaf — nothing to escape from
    }
    final HOTIncrementalInsert.DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(navResult.pathNodes(),
        navResult.pathChildIndices(), pathDepth, navResult.leaf(), keySlice);
    final int beta = analysis.mismatchBit();
    if (beta < 0 || beta > leastSignificantDiscBit(navResult.pathNodes()[pathDepth - 1])) {
      return false; // present/empty, or β inside the leaf's R(S)-subtree
    }
    selfHealScope = null;
    final boolean structurallyChanged = branchAboveLeaf(navResult, analysis, keySlice, valueBuf, valueLen);
    if (structurallyChanged && SELFHEAL_STRUCTURAL) {
      detectAndHeal(selfHealScope, keySlice);
      healStructuralViolationOnPath(keySlice);
    }
    return true;
  }

  /**
   * Full-invariant self-heal scoped to {@code scope}'s subtree: run the executable invariant spec
   * ({@link HOTMalformedSubtreeDetector}, I3/I4/I5/I7/I8/I11) and discharge every highest malformed
   * indirect via a canonical scoped rebuild ({@link #rebuildExistingSubtree}). Because I5 is
   * routing-soundness (foundation Theorem 2), this is the runtime guarantee that the touched subtree
   * routes correctly (I6) and holds no cross-leaf duplicate (I1) — not merely the cheap structural
   * invariants. {@code scope} is the just-spliced subtree root, so the detector cost is bounded by
   * the mutation's footprint, and the rebuild is Θ(n)-optimal (foundation Theorem 4).
   */
  private void detectAndHeal(@Nullable PageReference scope, byte[] keySlice) {
    if (scope == null) {
      return;
    }
    // Iterate to a fixed point. The detector's I8/I12 checks read a child's extremes off its edge
    // descents, which only report the truth once the child itself is clean — so a node judged in
    // the same round as a malformed descendant may have passed on stale extremes. A discharge is a
    // canonical rebuild (never itself malformed), so each round only ever surfaces nodes ABOVE the
    // previous round's repairs: the loop terminates within the scope's height, and the common case
    // — detect finds nothing — still runs exactly one pass, as before.
    for (int round = 0; round <= MAX_PATH_DEPTH; round++) {
      final var malformed = HOTMalformedSubtreeDetector.detect(scope, this::resolveHOTPageForTraversal);
      if (malformed.isEmpty()) {
        return;
      }
      for (final HOTMalformedSubtreeDetector.MalformedSubtree m : malformed) {
        STRUCTURAL_SELFHEAL_REBUILD.incrementAndGet();
        HEAL_TALLY.computeIfAbsent(m.invariant() + "|" + lastDispatchHandler, k -> new AtomicLong()).incrementAndGet();
        if (Boolean.getBoolean("hot.diag.healDump")) {
          final Page malformedPage = resolveHOTPageForTraversal(m.reference());
          final int height = malformedPage instanceof HOTIndirectPage hi
              ? hi.getHeight()
              : 0;
          System.err.println("[healdump] inv=" + m.invariant() + " handler=" + lastDispatchHandler + " round=" + round
              + " height=" + height + " atScopeRoot=" + (m.reference() == scope) + " K="
              + HexFormat.of().formatHex(keySlice) + " detail=" + m.detail());
        }
        rebuildExistingSubtree(m.reference());
      }
    }
    // Every round is spent, so the LAST round's rebuilds have not been re-checked yet. Verify them
    // before declaring failure: reporting from inside the loop marked a run unresolved that the very
    // rebuilds it was about to perform went on to fix.
    if (HOTMalformedSubtreeDetector.detect(scope, this::resolveHOTPageForTraversal).isEmpty()) {
      return;
    }

    // Still malformed after every round. The per-defect discharge is converging too slowly (or
    // oscillating, if a child page resolves in one round and not the next), so escalate to the one
    // operation that cannot itself be malformed: rebuild the WHOLE scope canonically.
    LOG.warn("HOT self-heal did not converge in {} rounds; rebuilding the whole scope canonically.",
        MAX_PATH_DEPTH + 1);
    STRUCTURAL_SELFHEAL_REBUILD.incrementAndGet();
    rebuildExistingSubtree(scope);

    final var residual = HOTMalformedSubtreeDetector.detect(scope, this::resolveHOTPageForTraversal);
    if (residual.isEmpty()) {
      return;
    }

    // A canonical rebuild left defects behind, so the trie cannot be repaired by this machinery at
    // all. Committing anyway would persist an index whose bounded range scans truncate at the first
    // out-of-order leaf and silently return partial answers — for good, since committed pages are
    // never revisited. Fail the transaction instead; every caller runs inside doIndex, before commit.
    SELFHEAL_UNRESOLVED.incrementAndGet();
    final String detail = String.format(
        "HOT self-heal could not repair the index: %d subtree(s) still malformed after %d rounds and a "
            + "canonical scope rebuild (first: %s — %s).",
        residual.size(), MAX_PATH_DEPTH + 1, residual.getFirst().invariant(), residual.getFirst().detail());
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
   * @return {@code true} iff the insert succeeded incrementally; {@code false} on defensive failure
   *         (unresolvable descent / depth overflow) -- caller falls back to its scoped rebuild.
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

    dispatchInsert(subNav, keyBuf, keyLen, valueBuf, valueLen, keySlice);
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
   * the caller abandons the incremental branch and returns {@code false}, falling back to the
   * canonical {@link #rebuildSubtree} at the insert depth — straddle-free and I5/I6/I8-clean by
   * construction. (An earlier merge-into-descended-leaf shortcut was abandoned: it left a straddling
   * leaf that a later branch could mis-encode, surfacing as I8/I5/I6 under fuzzing.)
   */
  private boolean branchAddStrandsExisting(HOTIndirectPage oldNode, HOTIndirectPage newNode, byte[] newKey) {
    final int newSlot = newNode.findChildIndex(newKey);
    return newSlot >= 0 && existingKeyRoutesToSlot(oldNode, newNode, newSlot, newKey);
  }

  /**
   * Stranding check for adding a combo child to {@code oldNode}. Returns {@code true} iff some
   * physical key currently stored under {@code oldNode} (other than {@code excludeKey}) would, on the
   * candidate {@code newNode}, route to {@code newSlot} — the freshly added child that holds only the
   * new key. Such a key would be silently re-routed to the new child without being migrated into it
   * (PEXT routing is equality-/most-specific-preferred), i.e. it would become a cross-leaf duplicate.
   * Resolves pages writer-side ({@link #resolveHOTPageForTraversal}) so it sees the in-progress (TIL)
   * subtree. Short-circuits on the first captured key. O(subtree keys).
   */
  private boolean existingKeyRoutesToSlot(HOTIndirectPage oldNode, HOTIndirectPage newNode, int newSlot,
      byte[] excludeKey) {
    for (int i = 0; i < oldNode.getNumChildren(); i++) {
      if (subtreeHasKeyRoutingToSlot(oldNode.getChildReference(i), newNode, newSlot, excludeKey, 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Cheap single-node structural check covering the O(children)-class invariants that a combo-add /
   * fold can break under multi-value leaves: I4 (smallest stored partial must be 0 — Binna's "first
   * mask always zero"), I7 (stored partials strictly ascending), I8 (children ordered by ascending
   * subtree first-key), and I12 (consecutive children's key RANGES must not interleave — first-key
   * order alone misses a preceding sibling whose subtree spans past the next child's start, the shape
   * every residual combo-fold heal turned out to be). Returns {@code true} on the first violation.
   * The expensive I5 constancy walk is intentionally excluded here (the post-dispatch
   * {@link #detectAndHeal} runs the full detector incl. I5); these are O(children) /
   * O(children×height). Used as a pre-commit combo-add guard (the ordering complement to the
   * routing-only {@link #branchAddStrandsExisting}, discharging via the I8-clean canonical
   * {@link #rebuildSubtree}) and as the post-dispatch path probe
   * ({@link #healStructuralViolationOnPath}). Sufficient as a single-node scan because a fold leaves
   * every existing child's subtree untouched.
   */
  private boolean nodeStructurallyMalformed(HOTIndirectPage candidate) {
    final int n = candidate.getNumChildren();
    final int[] partials = candidate.getPartialKeysRef();
    if (partials != null && partials.length >= n && n > 0) {
      int minPartial = partials[0];
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          return true; // I7: partials not strictly ascending
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
    for (int i = 0; i < n; i++) {
      final PageReference childRef = candidate.getChildReference(i);
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
  private boolean subtreeHasKeyWithBit(@Nullable PageReference ref, int beta, int bitValue, byte[] excludeKey) {
    if (ref == null) {
      return false;
    }
    final Page page = resolveHOTPageForTraversal(ref);
    if (page instanceof HOTLeafPage leaf) {
      final int n = leaf.getEntryCount();
      final int bytePos = beta / 8;
      final int mask = 1 << (7 - (beta % 8));
      for (int i = 0; i < n; i++) {
        final byte[] k = leaf.getKey(i);
        if (k == null || Arrays.equals(k, excludeKey)) {
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
        if (subtreeHasKeyWithBit(indirect.getChildReference(i), beta, bitValue, excludeKey)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Recursive helper for {@link #existingKeyRoutesToSlot}; short-circuits on the first match. */
  private boolean subtreeHasKeyRoutingToSlot(@Nullable PageReference ref, HOTIndirectPage newNode, int newSlot,
      byte[] excludeKey, int depth) {
    if (ref == null || depth > MAX_PATH_DEPTH + 2) {
      return false;
    }
    final Page page = resolveHOTPageForTraversal(ref);
    if (page instanceof HOTLeafPage leaf) {
      final int n = leaf.getEntryCount();
      for (int i = 0; i < n; i++) {
        final byte[] k = leaf.getKey(i);
        if (k == null || Arrays.equals(k, excludeKey)) {
          continue;
        }
        if (newNode.findChildIndex(k) == newSlot) {
          return true;
        }
      }
      return false;
    }
    if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        if (subtreeHasKeyRoutingToSlot(indirect.getChildReference(i), newNode, newSlot, excludeKey, depth + 1)) {
          return true;
        }
      }
    }
    return false;
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
  /** C2 continuations performed inside a freshly split full-node half. */
  public static final AtomicLong FULL_EXISTING_BIT_DIRECTION_ONE_SUBINSERT = new AtomicLong();

  /**
   * Issue B outcome counters -- how often handleOffPathOverflow succeeds vs falls back to the
   * caller's whole-index self-heal. Plan §4.3.
   */
  public static final AtomicLong OFF_PATH_OVERFLOW_OK = new AtomicLong();
  public static final AtomicLong OFF_PATH_OVERFLOW_FALLBACK = new AtomicLong();

  /**
   * Stage 3c (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §12) -- how often the scoped
   * {@link #rebuildSubtree} avoided a height-escalation by re-encoding an ancestor in place (one
   * increment per ancestor refreshed). A high count means Stage 3c stopped a cascade that the
   * original behaviour would have grown into a depth-0 whole rebuild.
   */
  public static final AtomicLong REBUILD_HEIGHT_ESCALATION_AVOIDED = new AtomicLong();
  /**
   * Stage 3c defensive arm -- the rebuilt slot's key range collides with a sibling's (the Direction-1
   * shape: subset routing admitted a key the lex order does not), so the propagation falls back to a
   * scoped rebuild at the ancestor's depth instead of an in-place re-encode. Stored partials are
   * never touched by the propagation (Stage 5: a sparse partial is the slot's position in the block
   * trie, invariant under content growth), so sibling ORDER is the only property a content change can
   * break on the way up. Should stay near zero in practice.
   */
  public static final AtomicLong REBUILD_PROPAGATION_ORDER_FALLBACK = new AtomicLong();
  /**
   * Total invocations of {@link #rebuildSubtree} (any depth, any caller). With
   * {@link #REBUILD_HEIGHT_ESCALATION_AVOIDED} reports both how often a rebuild occurred and how
   * often Stage 3c's propagation re-encoded at least one ancestor.
   */
  public static final AtomicLong REBUILD_SUBTREE_CALLED = new AtomicLong();
  public static final AtomicLong PROJECTION_REBUILD_SUBTREE_ATTEMPTED = new AtomicLong();


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

    System.err.println("[D1-FALLBACK " + site + "] K=" + hexKey + " (lenK=" + keySlice.length + ")" + " pathDepth="
        + navResult.pathDepth() + " insertDepth=" + insertDepth + " dStar.children=" + n + " dStar.height="
        + dStar.getHeight() + " affectedIdx=" + affectedIdx + " spine=" + spine + " beta=" + beta + " betaValue="
        + betaValue + " comboPartial=0x" + Integer.toHexString(comboPartial) + " affected.fk=" + hexAffected + " (lenA="
        + (affectedFirstKey == null
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
        + " class=" + carveClass + " prevOrderOk=" + prevOrderOk);
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
   * Falls back ({@code return false}) when β is not in D(N), L's β-column is already 1 (not the
   * off-path-straddle case), addChildAtCombination throws C2 collision, or any defensive failure.
   * Caller then proceeds with standard integrate (which will throw and land in the self-heal
   * whole-rebuild).
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
      // rebuildSubtree(insertDepth) path that escalated to depth 0 mid-revision -- producing
      // a freshly canonical h=1 root that competed with the handler's h=1→h=2 growth and
      // surfaced as I1+I6 corruption at rev 9 of interleavedInsertDeleteMultiRev. Plan §12
      // Stage 3c (in-spine height/partial propagation) removed the escalation, eliminating
      // the structural divergence. The handler now applies at pathDepth==1 too.
      return handleOffPathOverflowFullN(navResult, biNode, slotOfL, comboPartial);
    }

    // Step 1: slot-replace L → L₀ in N's children array (in-place on the CoW'd N).
    // The partial at slotOfL is unchanged -- it still has β-column-0, which matches
    // L₀'s β=0 keys. The follow-on addChildAtCombination snapshots the mutated children.
    parentN.setChildReference(slotOfL, biNode.left());

    // Step 2: add L₁ at comboPartial. addChildAtCombination throws on C2 collision (existing
    // sibling at comboPartial).
    final HOTIndirectPage newN;
    try {
      newN = HOTIncrementalInsert.addChildAtCombination(parentN, comboPartial, biNode.right(), parentN.getHeight(),
          storageEngineWriter.getRevisionNumber(), pageKeyAllocator);
    } catch (IllegalArgumentException c2Collision) {
      // C2: comboPartial collides with an existing c'. Direction-1-style sub-insert
      // of L₁'s keys into c' is the future iteration; for now, restore N's slot and
      // fall back to the caller's standard integrate path.
      parentN.setChildReference(slotOfL, navResult.leafRef());
      OFF_PATH_OVERFLOW_FALLBACK.incrementAndGet();
      return false;
    }

    // Step 3: re-point N's reference at its parent + register fresh subtree.
    navResult.pathRefs()[pathDepth - 1].setPage(newN);
    lastDispatchHandler = "h:merge-integrate";
    registerFreshSubtree(navResult.pathRefs()[pathDepth - 1]);
    OFF_PATH_OVERFLOW_OK.incrementAndGet();
    return true;
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
    final HOTIncrementalInsert.BiNode parentSplit;
    try {
      parentSplit = HOTIncrementalInsert.splitIndirectWithSlotReplaceAndInsertion(parentN, slotOfL, biNode.left(),
          comboPartial, biNode.right(), revision, pageKeyAllocator);
    } catch (IllegalArgumentException | IllegalStateException ex) {
      // C2 collision or other structural mismatch -- fall back to caller's standard integrate.
      OFF_PATH_OVERFLOW_FALLBACK.incrementAndGet();
      return false;
    }

    final int currentDepth = pathDepth - 1;
    final HOTIncrementalInsert.IntegrationResult result;
    try {
      result = HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult),
          navResult.pathChildIndices(), currentDepth, parentSplit, revision, pageKeyAllocator);
    } catch (IllegalArgumentException | IllegalStateException ex) {
      OFF_PATH_OVERFLOW_FALLBACK.incrementAndGet();
      return false;
    }

    lastDispatchHandler = "h:merge-offpath";
    registerFreshSubtree(result.touchedRef());
    if (Boolean.getBoolean("hot.diag.postHandlerValidate")) {
      final List<HOTMalformedSubtreeDetector.MalformedSubtree> defects =
          HOTMalformedSubtreeDetector.detect(navResult.pathRefs()[0], this::resolveHOTPageForTraversal);
      final HashSet<String> seen = new HashSet<>(4096);
      final ArrayList<String> duplicates = new ArrayList<>();
      collectKeysForI1(navResult.pathRefs()[0].getPage(), seen, duplicates);
      System.err.println("[POST-HANDLER-FULL-N] depth=" + pathDepth + " defects=" + defects + " duplicates="
          + duplicates.size() + (duplicates.isEmpty()
              ? ""
              : " (first: " + duplicates.get(0) + ")"));
    }
    OFF_PATH_OVERFLOW_OK.incrementAndGet();
    return true;
  }

  /**
   * Diagnostic helper — walk the subtree rooted at {@code page} and collect duplicate stored keys.
   */
  private void collectKeysForI1(Page page, HashSet<String> seen, ArrayList<String> duplicates) {
    if (page instanceof HOTLeafPage leaf) {
      final int count = leaf.getEntryCount();
      for (int i = 0; i < count; i++) {
        final String h = HexFormat.of().formatHex(leaf.getKey(i));
        if (!seen.add(h)) {
          duplicates.add(h);
        }
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference ref = indirect.getChildReference(i);
        if (ref == null)
          continue;
        final Page child = resolveHOTPageForTraversal(ref);
        if (child != null) {
          collectKeysForI1(child, seen, duplicates);
        }
      }
    }
  }

  /**
   * The merge outcome of {@link #doIndex}: the key belongs inside the routed leaf/bucket. Merges it
   * in; on bucket overflow defragments and retries once, then splits the leaf page and integrates the
   * resulting {@link HOTIncrementalInsert.BiNode} at the leaf's depth.
   */
  private boolean mergeIntoLeaf(LeafNavigationResult navResult, byte[] keyBuf, int keyLen, byte[] valueBuf,
      int valueLen, byte[] keySlice) {
    final HOTLeafPage leaf = navResult.leaf();
    // Fast path: the entry fits the bucket. The leaf is mutated in place — already in the TIL.
    // No indirect structure changes, so no structural self-heal is needed (return false).
    // keySlice is already trimmed to the real key length, so passing it (length == keySlice.length)
    // skips mergeWithNodeRefs's internal copyOf — one fewer per-insert allocation on the hot path.
    if (leaf.mergeWithNodeRefs(keySlice, keySlice.length, valueBuf, valueLen)) {
      return false;
    }
    // The bucket is full. compact() repacks live entries without dropping tombstones (it is
    // versioning-safe, unlike compactTombstones) — retry the merge once if it reclaimed space.
    if (leaf.compact() > 0 && leaf.mergeWithNodeRefs(keySlice, keySlice.length, valueBuf, valueLen)) {
      return false;
    }
    // Genuine overflow: split the leaf page at its key-set MSDB and integrate the BiNode.
    if (!leaf.canSplit()) {
      throw new SirixIOException(
          "HOT leaf page cannot store the entry and cannot split — a " + "single value exceeds page capacity. index="
              + indexType + ", entries=" + leaf.getEntryCount() + ", remaining=" + leaf.getRemainingSpace());
    }
    final int revision = storageEngineWriter.getRevisionNumber();
    final byte[] valueSlice = valueLen == valueBuf.length
        ? valueBuf
        : Arrays.copyOf(valueBuf, valueLen);
    final HOTIncrementalInsert.BiNode biNode =
        HOTIncrementalInsert.splitLeafPage(leaf, keySlice, valueSlice, revision, indexType, pageKeyAllocator);
    ensurePathChildrenLoaded(navResult.pathNodes());

    // Issue B (plan §4.3): if β = msdb(L ∪ {K}) is already a disc bit of L's parent N,
    // standard addEntry will reject. Apply the incremental off-path-overflow handler before
    // calling integrate -- it slot-replaces L with L₀ and adds L₁ at comboPartial, sidestepping
    // the rebuild-fallback over-partitioning observed in iterations 3/5/6/7/8.
    if (handleOffPathOverflow(navResult, biNode, keySlice, valueSlice)) {
      return true;
    }

    // Plan §12 Stage 3b: the structural-inconsistency self-heal arm (rebuildWholeIndex on
    // IllegalArgument/IllegalStateException) is gone -- every overflow case is now handled
    // incrementally by handleOffPathOverflow or its handleOffPathOverflowFullN variant. An
    // exception escaping integrate at this point is a real bug, not a tolerable structural
    // drift, so it propagates.
    final HOTIncrementalInsert.IntegrationResult result =
        HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
            navResult.pathDepth(), biNode, revision, pageKeyAllocator);
    lastDispatchHandler = "h:merge-offpath-fullN";
    registerFreshSubtree(result.touchedRef());
    return true;
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
   * The {@link #tryBranchIncremental} false return -- the I8-unsafe Direction 1 case where
   * sub-inserting K would violate sibling ordering -- still falls back to a scoped
   * {@link #rebuildSubtree} at the insert depth (now non-escalating per plan §12 Stage 3c).
   */
  private boolean branchAboveLeaf(LeafNavigationResult navResult, HOTIncrementalInsert.DescentAnalysis analysis,
      byte[] keySlice, byte[] valueBuf, int valueLen) {
    final byte[] valueSlice = valueLen == valueBuf.length
        ? valueBuf
        : Arrays.copyOf(valueBuf, valueLen);
    if (!tryBranchIncremental(navResult, analysis, keySlice, valueSlice)) {
      // I8-unsafe Direction 1 (the only remaining false return from tryBranchIncremental).
      // Recanonicalize, but scoped to the insert-depth subtree: the key branches inside it,
      // so its ancestors are unaffected -- the rebuild stays bounded and Stage 3c's
      // propagation handles ancestor height/partial refreshes without escalating.
      rebuildSubtree(navResult, analysis.insertDepth(), keySlice, valueSlice);
      return false; // rebuildSubtree output is canonical — no structural self-heal needed
    }
    return true; // incremental branch — verify the path structurally
  }

  /**
   * Attempt the incremental branch insert — Binna's {@code insertNewValueIntoNode}. Returns
   * {@code false} (caller recanonicalizes) when the case needs a path not yet ported: {@code beta}
   * colliding with an existing discriminative bit, or a full node that would have to split.
   *
   * @return {@code true} iff the key was inserted incrementally
   */
  private boolean tryBranchIncremental(LeafNavigationResult navResult, HOTIncrementalInsert.DescentAnalysis analysis,
      byte[] keySlice, byte[] valueSlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final PageReference[] pathRefs = navResult.pathRefs();
    final int[] childSlots = navResult.pathChildIndices();
    final int pathDepth = navResult.pathDepth();
    final int beta = analysis.mismatchBit();
    final int betaValue = HOTBulkBuilder.bitAt(keySlice, beta)
        ? 1
        : 0;
    final int revision = storageEngineWriter.getRevisionNumber();

    final int insertDepth = analysis.insertDepth();
    final HOTIndirectPage node = pathNodes[insertDepth];
    final HOTIncrementalInsert.InsertInfo info =
        HOTIncrementalInsert.getInsertInformation(node, analysis.affectedChildIndex(), beta);
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
        if (!comboLeaf.put(keySlice, valueSlice)) {
          throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
        }
        try {
          final HOTIndirectPage newNode = HOTIncrementalInsert.addChildAtCombination(node, comboPartial,
              swizzle(comboLeaf), node.getHeight(), revision, pageKeyAllocator);
          if (branchAddStrandsExisting(node, newNode, keySlice)) {
            comboLeaf.close();
            return dischargeStrandViaLeafRebuild(navResult, node, newNode, insertDepth, keySlice, valueSlice);
          }
          if (nodeStructurallyMalformed(newNode)) {
            comboLeaf.close();
            BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
            return false; // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
          }
          pathRefs[insertDepth].setPage(newNode);
          lastDispatchHandler = "h:combo-site1";
          registerFreshSubtree(pathRefs[insertDepth]);
          return true;
        } catch (IllegalArgumentException c2Collision) {
          // C2 -- comboPartial coincides with an existing child of d*. Direction 1 sub-insert
          // into affected (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §11) is routing-correct
          // by the descent tautology; the only remaining risk is I8 (range-scan ordering) when
          // K becomes affected's new firstKey and the trie has an MSDB-closure gap at some
          // ancestor's mask. Pre-check via isDirectionOneI8Safe; if safe, sub-insert; else
          // fall back to a scoped rebuildSubtree at insertDepth (cheaper than the baseline's
          // whole-index self-heal).
          comboLeaf.close();
          if (isDirectionOneI8Safe(navResult, insertDepth, analysis.affectedChildIndex(), keySlice)) {
            lastDispatchHandler = "h:d1-subinsert";
            DIRECTION_ONE_SUBINSERT.incrementAndGet();
            return subInsertAt(node.getChildReference(analysis.affectedChildIndex()), keySlice, keySlice.length,
                valueSlice, valueSlice.length);
          }
          final int collisionSlot = findChildSlotByPartial(node, comboPartial);
          if (tryDirectionOneLeafPairSplice(navResult, node, insertDepth, collisionSlot, analysis.affectedChildIndex(),
              keySlice, valueSlice)) {
            return true;
          }
          DIRECTION_ONE_FALLBACK.incrementAndGet();
          if (Boolean.getBoolean("hot.diag.directionOneFallback")) {
            dumpDirectionOneFallback("site1", navResult, analysis.affectedChildIndex(), analysis.insertDepth(), beta,
                betaValue, comboPartial, keySlice);
          }
          return false;
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
        if (!pullUpLeaf.put(keySlice, valueSlice)) {
          throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
        }
        if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, insertDepth, beta)) {
          pullUpLeaf.close();
          return false;
        }
        // Stranding guard: the whole node goes on beta's (1-betaValue) side. If its subtree holds
        // a key with beta==betaValue, that key would strand under the pull-up leaf. Rebuild instead.
        if (subtreeHasKeyWithBit(pathRefs[insertDepth], beta, betaValue, keySlice)) {
          pullUpLeaf.close();
          STRAND_FULL_FALLBACK.incrementAndGet();
          return false; // BiNode-wrap strand (whole-subtree source): canonical rebuildSubtree
        }
        final PageReference pullUpLeafRef = swizzle(pullUpLeaf);
        final PageReference wrappedNodeRef = swizzle(node);
        final int biHeight = node.getHeight() + 1;
        final HOTIncrementalInsert.BiNode biNode = betaValue == 1
            ? new HOTIncrementalInsert.BiNode(beta, biHeight, wrappedNodeRef, pullUpLeafRef)
            : new HOTIncrementalInsert.BiNode(beta, biHeight, pullUpLeafRef, wrappedNodeRef);
        ensurePathChildrenLoaded(pathNodes);
        final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(pathNodes,
            buildSpineRefs(navResult), childSlots, insertDepth, biNode, revision, pageKeyAllocator);
        lastDispatchHandler = "h:integrate-existing-bit";
        registerFreshSubtree(result.touchedRef());
        return true;
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
        if (!comboLeaf.put(keySlice, valueSlice)) {
          throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
        }
        try {
          final HOTIndirectPage newChild = HOTIncrementalInsert.addChildAtCombination(child, comboPartial,
              swizzle(comboLeaf), child.getHeight(), revision, pageKeyAllocator);
          if (branchAddStrandsExisting(child, newChild, keySlice)) {
            comboLeaf.close();
            return dischargeStrandViaLeafRebuild(navResult, child, newChild, insertDepth + 1, keySlice, valueSlice);
          }
          if (nodeStructurallyMalformed(newChild)) {
            comboLeaf.close();
            BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
            return false; // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
          }
          pathRefs[insertDepth + 1].setPage(newChild);
          lastDispatchHandler = "h:combo-site3";
          registerFreshSubtree(pathRefs[insertDepth + 1]);
          return true;
        } catch (IllegalArgumentException collisionOrPrecondition) {
          // Site 3 C2 -- comboPartial collides with an existing child of the boundary node.
          // Apply Direction 1 at the boundary level: sub-insert K into the boundary child's
          // affected slot if I8-safe (the routing tautology holds at depth+1 just as at d*),
          // else fall back to the caller's scoped rebuildSubtree
          // (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §11 iteration 4).
          comboLeaf.close();
          if (isDirectionOneI8Safe(navResult, insertDepth + 1, childEntryIndex, keySlice)) {
            lastDispatchHandler = "h:d1-subinsert";
            DIRECTION_ONE_SUBINSERT.incrementAndGet();
            return subInsertAt(child.getChildReference(childEntryIndex), keySlice, keySlice.length, valueSlice,
                valueSlice.length);
          }
          final int collisionSlot = findChildSlotByPartial(child, comboPartial);
          if (tryDirectionOneLeafPairSplice(navResult, child, insertDepth + 1, collisionSlot, childEntryIndex, keySlice,
              valueSlice)) {
            return true;
          }
          DIRECTION_ONE_FALLBACK.incrementAndGet();
          if (Boolean.getBoolean("hot.diag.directionOneFallback")) {
            dumpDirectionOneFallback("site3", navResult, childEntryIndex, insertDepth + 1, beta, betaValue,
                comboPartial, keySlice);
          }
          return false;
        }
      }
    }

    // K's fresh single-entry leaf page — its own R(S)-subtree root.
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    if (!keyLeaf.put(keySlice, valueSlice)) {
      throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
    }
    final PageReference newLeafRef = swizzle(keyLeaf);

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
        lastDispatchHandler = "h:strand-pairleaf";
        leafScopedRebuild(navResult, keySlice, valueSlice); // strandable keys are the descended leaf's
        STRAND_LEAF_REBUILD.incrementAndGet();
        return true;
      }
      if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, pathDepth, beta)) {
        keyLeaf.close();
        return false;
      }
      // Direction-1-dual pre-guard: the pair keeps the leaf's slot, so K becomes the slot's new
      // minimum (betaValue == 0) or maximum (betaValue == 1). Subset routing brought K here, but
      // subset routing does not imply lex position — if K falls outside the slot's boundary with
      // its neighbour, the pairing would break I8/I12 (the shape the impossibility analysis
      // proves no local primitive fixes). Detect it BEFORE splicing and take the canonical
      // rebuild instead of manufacturing a violation for the detector to repair.
      if (pathDepth > 0) {
        final HOTIndirectPage pairParent = pathNodes[pathDepth - 1];
        final int leafSlot = childSlots[pathDepth - 1];
        if (betaValue == 0 && leafSlot > 0) {
          final byte[] prevLast = lastKeyOfSubtree(pairParent.getChildReference(leafSlot - 1));
          if (prevLast != null && Arrays.compareUnsigned(prevLast, keySlice) >= 0) {
            keyLeaf.close();
            return false; // K sorts at or below the previous sibling: canonical rebuildSubtree
          }
        } else if (betaValue == 1 && leafSlot + 1 < pairParent.getNumChildren()) {
          final byte[] nextFirst = firstKeyOfSubtree(pairParent.getChildReference(leafSlot + 1));
          if (nextFirst != null && Arrays.compareUnsigned(keySlice, nextFirst) >= 0) {
            keyLeaf.close();
            return false; // K sorts at or above the next sibling: canonical rebuildSubtree
          }
        }
      }
      final PageReference leafRef = swizzle(navResult.leaf());
      final HOTIncrementalInsert.BiNode biNode = betaValue == 1
          ? new HOTIncrementalInsert.BiNode(beta, 1, leafRef, newLeafRef)
          : new HOTIncrementalInsert.BiNode(beta, 1, newLeafRef, leafRef);
      ensurePathChildrenLoaded(pathNodes);
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(pathNodes,
          buildSpineRefs(navResult), childSlots, pathDepth, biNode, revision, pageKeyAllocator);
      lastDispatchHandler = "h:pair-leaf";
      registerFreshSubtree(result.touchedRef());
      return true;
    }

    if (singleEntry) {
      // Binna's "false positive": the single affected entry is a boundary node, not the leaf —
      // the MSB-stack insert depth was one level too shallow. beta is more significant than every
      // discriminative bit of that child, so K joins it as a new partition root.
      final int childDepth = insertDepth + 1;
      final HOTIndirectPage child = pathNodes[childDepth];
      if (child.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
        final HOTIndirectPage newChild = HOTIncrementalInsert.addEntryWithInsertInfo(child, beta, betaValue, 0,
            child.getNumChildren(), 0, newLeafRef, child.getHeight(), revision, pageKeyAllocator);
        if (branchAddStrandsExisting(child, newChild, keySlice)) {
          keyLeaf.close();
          return dischargeStrandViaLeafRebuild(navResult, child, newChild, childDepth, keySlice, valueSlice);
        }
        if (nodeStructurallyMalformed(newChild)) {
          keyLeaf.close();
          BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
          return false; // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
        }
        pathRefs[childDepth].setPage(newChild);
        lastDispatchHandler = "h:boundary-addentry";
        registerFreshSubtree(pathRefs[childDepth]);
        return true;
      }
      // The boundary node is full — wrap it whole under a BiNode on beta and integrate. It needs
      // a fresh reference (integrate may re-point the boundary node's own spine slot).
      if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, childDepth, beta)) {
        keyLeaf.close();
        return false;
      }
      // Stranding guard: the whole boundary child goes on beta's (1-betaValue) side; if its
      // subtree holds a key with beta==betaValue, that key would strand. Rebuild instead.
      if (subtreeHasKeyWithBit(pathRefs[childDepth], beta, betaValue, keySlice)) {
        keyLeaf.close();
        STRAND_FULL_FALLBACK.incrementAndGet();
        return false; // BiNode-wrap strand (whole-subtree source): canonical rebuildSubtree
      }
      final PageReference childRef = swizzle(child);
      final int biHeight = child.getHeight() + 1;
      final HOTIncrementalInsert.BiNode biNode = betaValue == 1
          ? new HOTIncrementalInsert.BiNode(beta, biHeight, childRef, newLeafRef)
          : new HOTIncrementalInsert.BiNode(beta, biHeight, newLeafRef, childRef);
      ensurePathChildrenLoaded(pathNodes);
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(pathNodes,
          buildSpineRefs(navResult), childSlots, childDepth, biNode, revision, pageKeyAllocator);
      lastDispatchHandler = "h:wrap-full";
      registerFreshSubtree(result.touchedRef());
      return true;
    }

    // affectedCount > 1 — K's leaf is folded into d*'s block beside the affected subtree. beta
    // becomes a new discriminative bit; the node keeps its height (a leaf child never raises it).
    final HOTIndirectPage newNode =
        HOTIncrementalInsert.addEntryWithInsertInfo(node, beta, betaValue, info.firstAffected(), info.affectedCount(),
            info.subtreePrefix(), newLeafRef, node.getHeight(), revision, pageKeyAllocator);
    if (branchAddStrandsExisting(node, newNode, keySlice)) {
      keyLeaf.close();
      return dischargeStrandViaLeafRebuild(navResult, node, newNode, insertDepth, keySlice, valueSlice);
    }
    if (nodeStructurallyMalformed(newNode)) {
      keyLeaf.close();
      BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
      return false; // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
    }
    pathRefs[insertDepth].setPage(newNode);
    lastDispatchHandler = "h:fold-multi";
    registerFreshSubtree(pathRefs[insertDepth]);
    return true;
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
    // un-mergeable cross-level overlap and bail to the caller's scoped rebuild if found.
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(), insertDepth,
        node.getMostSignificantBitIndex())) {
      return false;
    }
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    if (!keyLeaf.put(keySlice, valueSlice)) {
      throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
    }
    ensurePathChildrenLoaded(navResult.pathNodes());
    final HOTIncrementalInsert.BiNode biNode = HOTIncrementalInsert.splitIndirectWithEntry(node, info, beta, betaValue,
        swizzle(keyLeaf), revision, pageKeyAllocator);
    final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(navResult.pathNodes(),
        buildSpineRefs(navResult), navResult.pathChildIndices(), insertDepth, biNode, revision, pageKeyAllocator);
    lastDispatchHandler = "h:branch-integrate";
    registerFreshSubtree(result.touchedRef());
    return true;
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
   * Out-of-scope corner cases (§6 C1 / C2) fall back to the existing rebuild — not yet
   * probe-verified: C1 (1:31 lone-child half) and C2 ({@code comboPartial} collision = a descent
   * imprecision). On either, this method returns {@code false} and the caller's scoped
   * {@code rebuildSubtree} handles it (no regression vs. the prior {@code return false}).
   *
   * @return {@code true} iff the key was inserted incrementally
   */
  /**
   * Pre-check whether {@link HOTIncrementalInsert#integrate}'s cascade — starting at
   * {@code currentDepth} with a BiNode on {@code biNodeBeta} — will fold cleanly, or whether any
   * level requires an un-mergeable cross-level-overlap fold (which would otherwise throw out of
   * integrate). Returns {@code false} to signal the caller should fall back to a scoped
   * {@link #rebuildSubtree} instead of attempting the incremental integrate.
   *
   * <p>
   * <b>Crash-safety.</b> The walk is conservative: it never returns {@code true} when integrate would
   * throw. It checks {@link HOTIncrementalInsert#canMergeBiNodeAtExistingDiscBit} at every level
   * whose mask contains the running β. The β evolution exactly matches integrate's full-node cascade
   * (β becomes {@code parent.MSB} after a split). It does not model integrate's
   * intermediate-placement short-circuit (a height comparison) — skipping it can only cause an
   * occasional *unnecessary* rebuild (integrate would have succeeded via intermediate placement),
   * never a missed crash, because integrate never folds at an intermediate level.
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
    // un-mergeable cross-level overlap and bail to the caller's scoped rebuild if found.
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(), insertDepth,
        node.getMostSignificantBitIndex())) {
      return false;
    }
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    if (!keyLeaf.put(keySlice, valueSlice)) {
      throw new SirixIOException("HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
    }
    ensurePathChildrenLoaded(navResult.pathNodes());

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
      // Not yet probe-verified; fall back to the caller's scoped rebuildSubtree.
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
    try {
      if (betaCol >= 0) {
        // beta survived as a disc bit of the half — still betaIsDiscBit for the half.
        final int comboPartial = halfInfo.subtreePrefix() | (betaValue == 1
            ? 1 << (halfDiscBits.length - 1 - betaCol)
            : 0);
        if (findChildSlotByPartial(half, comboPartial) >= 0) {
          // C2: the split did not make K a new combination. K belongs below the child selected by
          // the half's own descent; finish that descent, then rebuild only the two freshly split
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
    } catch (IllegalArgumentException collisionOrPrecondition) {
      // C2 — comboPartial collides with an existing child (the descent stopped one level too
      // shallow), or another fold precondition fails. Not yet probe-verified; fall back to
      // the caller's scoped rebuildSubtree.
      keyLeaf.close();
      return false;
    }
    // Multi-entry-leaf stranding guard ([[hot-multientry-leaf-quirks]] #1): the fold added K's
    // single-key leaf to the half; if an existing key in the half would now route to it, the half
    // straddles the fold bit. Discard the (uncommitted) split and fall back to a canonical rebuild.
    if (branchAddStrandsExisting(half, foldedHalf, keySlice)) {
      keyLeaf.close();
      return dischargeStrandViaLeafRebuild(navResult, half, foldedHalf, -1, keySlice, valueSlice);
    }
    if (nodeStructurallyMalformed(foldedHalf)) {
      keyLeaf.close();
      BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
      return false; // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
    }
    halfRef.setPage(foldedHalf);

    // 4. Integrate the split BiNode at insertDepth — the standard capacity cascade.
    final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(navResult.pathNodes(),
        buildSpineRefs(navResult), navResult.pathChildIndices(), insertDepth, split, revision, pageKeyAllocator);
    lastDispatchHandler = "h:combo-site2-fold";
    registerFreshSubtree(result.touchedRef());
    return true;
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

    if (!subInsertAt(half.getChildReference(affectedIdx), keySlice, keySlice.length, valueSlice, valueSlice.length)) {
      return false;
    }

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
  }

  /**
   * Leaf-consolidation sweep — the thesis's underflow rule (§3.3.2) applied across the index. The
   * incremental insert over-partitions: a faithful leaf split at the key-set MSDB is uneven and
   * freezes a small half, a branch starts a single-entry leaf, and ascending workloads never re-route
   * to those frozen leaves — so they drift to a fraction of capacity. This post-order walk merges
   * every adjacent BiNode-paired leaf-child pair whose union still fits a page
   * ({@link HOTIncrementalInsert#consolidateNodeLeaves}), packing the leaves back toward full.
   *
   * <p>
   * Copy-on-write: each visited indirect is CoW'd into the transaction-intent log
   * ({@link #prepareIndirectPage}, idempotent), and a node whose leaves were merged is re-pointed and
   * registered. A merge never changes a node's height (a leaf child carries height 0), so ancestors
   * are structurally unaffected.
   *
   * <p>
   * The child pages are swizzled onto their references before {@code consolidateNodeLeaves} runs: a
   * page already flushed to the transaction-intent log has a {@code null} in-memory page on its
   * reference, and the consolidation reads child pages through {@code getPage()}.
   *
   * <p>
   * Every merged-away leaf across the whole sweep is collected and released in one batch — the
   * transaction-intent log's sharing check is a full-log scan, so a per-leaf release would be
   * quadratic in the transaction's entry count.
   */
  private void consolidateSubtree(PageReference ref) {
    final List<PageReference> orphanedLeaves = new ArrayList<>();
    consolidateSubtree(ref, orphanedLeaves);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(orphanedLeaves);
  }

  private void consolidateSubtree(PageReference ref, List<PageReference> orphanedLeaves) {
    final Page page = resolveHOTPageForTraversal(ref);
    if (!(page instanceof HOTIndirectPage indirect)) {
      return;
    }
    final HOTIndirectPage cowed = prepareIndirectPage(ref, indirect);
    for (int i = 0; i < cowed.getNumChildren(); i++) {
      final PageReference childRef = cowed.getChildReference(i);
      if (childRef.getPage() == null) {
        final Page child = resolveHOTPageForTraversal(childRef);
        if (child != null) {
          childRef.setPage(child);
        }
      }
      if (childRef.getPage() instanceof HOTIndirectPage) {
        consolidateSubtree(childRef, orphanedLeaves);
      }
    }
    final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(cowed, CONSOLIDATION_TARGET,
        storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator, orphanedLeaves);
    if (consolidated != cowed) {
      ref.setPage(consolidated);
      registerFreshSubtree(ref);
    }
  }

  /** Wrap a freshly created page in a new {@link PageReference} with the page swizzled in. */
  private static PageReference swizzle(Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  /**
   * Recanonicalize the subtree rooted at {@code pathNodes[depth]}: rebuild it as a canonical HOT
   * holding every entry it currently contains plus {@code (keySlice, valueSlice)}, and re-point its
   * spine slot. {@link HOTBulkBuilder} produces a compression of {@code R(S)} by construction
   * (Theorem 1), so the result is invariant-clean and routing is exact again — this places a branched
   * (misrouted) key correctly and heals any pre-existing inconsistency in the subtree.
   *
   * <p>
   * Rebuilding the <em>insert-depth</em> subtree rather than the whole index bounds the work and the
   * pages orphaned: the key branches strictly inside {@code pathNodes[depth]}, so its ancestors keep
   * routing to it unchanged. The one ancestor-visible property is height — if the rebuilt subtree is
   * taller than the old node, the ancestors' height accounting is stale, so the rebuild escalates one
   * level shallower (terminating at the root, which has no ancestor).
   *
   * <p>
   * The collected entries are explicitly sorted and de-duplicated before the build: a rebuild
   * recanonicalizes a possibly-corrupt subtree, so it must not assume the trie's traversal order is
   * already a valid (strictly ascending, distinct) {@link HOTBulkBuilder} input.
   */
  private void rebuildSubtree(LeafNavigationResult navResult, int depth, byte[] keySlice, byte[] valueSlice) {
    requireSubtreeRebuildAllowed();
    REBUILD_SUBTREE_CALLED.incrementAndGet();
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int safeDepth = Math.max(0, Math.min(depth, navResult.pathDepth() - 1));
    final HOTIndirectPage subtreeRoot = pathNodes[safeDepth];

    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>();
    final List<CapturedSegmentRef> segmentRefs = new ArrayList<>();
    collectSubtreeEntries(subtreeRoot, collected, segmentRefs);
    collected.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));

    // Collapse duplicate keys (a re-insert over an existing key) by OR-merging their values.
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(collected.size());
    for (final HOTBulkBuilder.Entry entry : collected) {
      final int last = entries.size() - 1;
      if (last >= 0 && Arrays.equals(entries.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = entries.get(last);
        entries.set(last,
            new HOTBulkBuilder.Entry(prev.key(), HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value())));
      } else {
        entries.add(entry);
      }
    }

    final HOTBulkBuilder.BuildResult built =
        HOTBulkBuilder.build(entries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    final Page rebuilt = built.rootPage();
    reattachSegmentRefs(rebuilt, segmentRefs);
    final PageReference subtreeRef = navResult.pathRefs()[safeDepth];
    subtreeRef.setPage(rebuilt);
    lastDispatchHandler = "h:rebuildSubtree";
    registerFreshSubtree(subtreeRef);

    // Plan §12 Stage 3c (A): propagate the rebuilt subtree's height + (if firstKey changed)
    // sparse partial up the spine instead of escalating to a shallower rebuild. The original
    // escalation cascaded to depth 0 on the leftmost-chain firings -- silently producing a
    // freshly canonical whole index mid-revision -- which iter-19 pinned as the root cause of
    // the rev-9 corruption that competes with iter-18's pathDepth==1 N-full Issue B handler.
    // The in-place propagation is bounded by spine depth and avoids the depth-0 whole rebuild
    // on every path tested. The defensive fallback on an I7 collision is the prior scoped
    // rebuild at the ancestor's depth (still strictly shallower than the original cascade).
    if (safeDepth > 0) {
      propagateRebuildUpSpine(navResult, safeDepth, keySlice, valueSlice);
    }

    // HOTBulkBuilder.build produced an all-new subtree from the collected entries — every leaf
    // page of the replaced subtree is now unreachable; release their off-heap slots in one batch
    // instead of pinning the 64KB segments in the transaction-intent log until commit.
    final List<PageReference> staleLeafRefs = new ArrayList<>();
    collectSubtreeLeafRefs(subtreeRoot, staleLeafRefs);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(staleLeafRefs);
  }

  /**
   * Heal attribution: per-{@code (invariant|dispatch handler)} tally of detector heals, keyed
   * {@code "I5|h:merge-integrate"}-style. Only touched when a heal actually fires (rare by design),
   * so the hot path pays nothing beyond the {@link #lastDispatchHandler} field store. This is the
   * instrumentation that attributed 91% of all heals to the spine propagation's dense-partial
   * recompute (plan doc, Stage 5) — kept so a future regression is attributable the same day it
   * lands.
   */
  public static final ConcurrentHashMap<String, AtomicLong> HEAL_TALLY = new ConcurrentHashMap<>();

  /**
   * The structural handler that produced the tree state a subsequent heal repairs — the attribution
   * key half of {@link #HEAL_TALLY}. Written as a constant-string field store at each dispatch site;
   * read only when a heal fires.
   */
  protected String lastDispatchHandler = "?";

  /** Disable hook for the post-dispatch structural self-heal (default ON — correctness first). */
  private static final boolean SELFHEAL_STRUCTURAL = !Boolean.getBoolean("hot.selfheal.structural.disable");
  /**
   * Post-dispatch structural self-heals: a structural fold (combo-add / integrate / off-path-
   * overflow) left a node on the insert path malformed (I4/I7/I8/I12) and was discharged by a scoped
   * canonical rebuild. Distinct from {@link #BRANCH_I8_UNSAFE_REBUILD} (the pre-commit combo-add
   * guard) — this is the defense-in-depth backstop that covers the merge/integrate handlers too.
   */
  public static final AtomicLong STRUCTURAL_SELFHEAL_REBUILD = new AtomicLong();

  /**
   * Times {@link #detectAndHeal} exhausted its round budget with defects still standing. Must stay
   * zero; a non-zero value means an index was committed that the detector still considers malformed,
   * and the accompanying ERROR log names the first offender.
   */
  public static final AtomicLong SELFHEAL_UNRESOLVED = new AtomicLong();

  /**
   * Defense-in-depth backstop after a structural change: walk {@code keySlice}'s <em>current</em>
   * descent path from the root and, at the shallowest indirect that is structurally malformed
   * (I4/I7/I8/I12 — {@link #nodeStructurallyMalformed}), discharge by a canonical scoped rebuild of
   * that node's subtree ({@link #rebuildExistingSubtree}). Rebuilding the shallowest violator
   * subsumes any malformed descendant (Binna Lemma 3). A fold can only malform nodes on the inserted
   * key's path, so this O(height × children) walk is necessary and sufficient — and far cheaper than
   * a from-root scan or the corruption-prone whole-index rebuild (Stage 3c).
   */
  private void healStructuralViolationOnPath(byte[] keySlice) {
    PageReference cur = rootReference;
    for (int depth = 0; depth <= MAX_PATH_DEPTH; depth++) {
      if (!(resolveHOTPageForTraversal(cur) instanceof HOTIndirectPage indirect)) {
        return; // reached the leaf — nothing malformed
      }
      if (nodeStructurallyMalformed(indirect)) {
        STRUCTURAL_SELFHEAL_REBUILD.incrementAndGet();
        rebuildExistingSubtree(cur);
        return;
      }
      final int childIndex = indirect.findChildIndex(keySlice);
      if (childIndex < 0) {
        return; // defensive: descent failed
      }
      cur = indirect.getChildReference(childIndex);
      if (cur == null) {
        return;
      }
    }
  }

  /**
   * Canonical scoped rebuild of the <em>existing</em> subtree at {@code ref} from its current entries
   * (no extra key — the inserted key is already present after dispatch). Mirrors
   * {@link #rebuildSubtree} but reads the post-dispatch tree directly and re-points {@code ref} in
   * place, so it can heal whatever a structural fold produced. {@link HOTBulkBuilder} output is
   * invariant-clean by construction (Theorem 1).
   */
  private void rebuildExistingSubtree(PageReference ref) {
    requireSubtreeRebuildAllowed();
    final Page page = resolveHOTPageForTraversal(ref);
    if (!(page instanceof HOTIndirectPage subtreeRoot)) {
      return; // a leaf root has no indirect invariant
    }
    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>();
    final List<CapturedSegmentRef> segmentRefs = new ArrayList<>();
    collectSubtreeEntries(subtreeRoot, collected, segmentRefs);
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> entries = dedupMergeEntries(collected);

    final List<PageReference> staleLeafRefs = new ArrayList<>();
    collectSubtreeLeafRefs(subtreeRoot, staleLeafRefs);

    final HOTBulkBuilder.BuildResult built =
        HOTBulkBuilder.build(entries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    reattachSegmentRefs(built.rootPage(), segmentRefs);
    ref.setPage(built.rootPage());
    registerFreshSubtree(ref);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(staleLeafRefs);
  }

  /**
   * Strand-discharge observability. {@link #STRAND_LEAF_REBUILD} counts strands resolved by the
   * surgical {@code O(one leaf + path)} {@link #leafScopedRebuild} (the on-path single-source-leaf
   * case); {@link #STRAND_FULL_FALLBACK} counts strands that fall back to {@link #rebuildSubtree} at
   * the insert depth (off-path / multi-leaf / BiNode-wrap source — the minimal correct scope when K
   * and the strandable keys occupy different node slots).
   */
  public static final AtomicLong STRAND_LEAF_REBUILD = new AtomicLong();
  /**
   * Strands discharged canonically: the descended leaf held keys on both sides of the branch bit, so
   * the union {@code leaf ∪ {K}} was split at its own key-set MSDB and the BiNode integrated at the
   * leaf's depth — Binna's insert, leaving no straddled leaf behind (unlike the
   * {@link #leafScopedRebuild} splice, which keeps the union in the leaf's slot and thereby
   * re-attracts the strand guard on later inserts).
   */
  public static final AtomicLong STRAND_SPLIT_INTEGRATE = new AtomicLong();
  public static final AtomicLong STRAND_FULL_FALLBACK = new AtomicLong();
  /** Off-path strands discharged by the two-leaf migration ({@link #tryTwoLeafMigration}). */
  public static final AtomicLong STRAND_TWO_LEAF_MIGRATE = new AtomicLong();
  /**
   * Branch combo-adds discharged by the canonical {@link #rebuildSubtree} because the partial-sorted
   * new slot is structurally malformed ({@link #nodeStructurallyMalformed} — typically the I7≡I8
   * first-key-order divergence under multi-value leaves). Bounded like the Direction-1 fallback; a
   * runaway count signals a workload stressing off-path-bit reordering.
   */
  public static final AtomicLong BRANCH_I8_UNSAFE_REBUILD = new AtomicLong();

  /**
   * Surgical strand discharge ({@code O(one leaf + path)}). When a branch-add stranding guard fires
   * and <em>all</em> strandable keys are confined to the descended leaf {@code
   * navResult.leaf()}, rebuild just that leaf together with the new key {@code K} into a canonical
   * mini-HOT ({@link HOTBulkBuilder}) and splice it into the leaf's slot, propagating height/partial
   * up the spine. Returns {@code true} when so handled; {@code false} when the strand is not confined
   * to the descended leaf (multi-leaf or off-path source — the rare case), leaving the caller to fall
   * back to the canonical {@link #rebuildSubtree} at the insert depth.
   *
   * <p>
   * Correctness: K and the strandable keys all route (via {@code node.findChildIndex}) to the
   * descended leaf's slot, so rebuilding {@code leaf ∪ {K}} and re-splicing there preserves routing
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
   * Contrast with {@link #leafScopedRebuild}: that splice keeps the whole union in the leaf's slot,
   * which routes correctly (the slot's sparse partial is untouched and every key subset-matches it)
   * but leaves a leaf spanning a bit above its cut point — a shape the strand guard fires on again
   * for every later key branching at that bit. The split removes the shape, so the guard goes quiet.
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
    ensurePathChildrenLoaded(navResult.pathNodes());
    lastDispatchHandler = "h:strand-split-integrate";
    final HOTIncrementalInsert.IntegrationResult result =
        HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
            navResult.pathDepth(), biNode, revision, pageKeyAllocator);
    registerFreshSubtree(result.touchedRef());
    STRAND_SPLIT_INTEGRATE.incrementAndGet();
    return true;
  }

  private boolean dischargeStrandViaLeafRebuild(LeafNavigationResult navResult, HOTIndirectPage oldNode,
      HOTIndirectPage newNode, int nodeDepth, byte[] keySlice, byte[] valueSlice) {
    final int newSlot = newNode.findChildIndex(keySlice);
    if (newSlot < 0 || navResult.pathDepth() < 1) {
      STRAND_FULL_FALLBACK.incrementAndGet();
      return false;
    }
    // (a) On-path: strandable keys confined to the descended leaf -> O(one leaf + path).
    if (strandConfinedToLeaf(oldNode, newNode, newSlot, keySlice, navResult.leaf().getPageKey())) {
      if (strandDischargeSplitIntegrate(navResult, keySlice, valueSlice)) {
        return true;
      }
      lastDispatchHandler = "h:strand-leafrebuild-b";
      leafScopedRebuild(navResult, keySlice, valueSlice);
      STRAND_LEAF_REBUILD.incrementAndGet();
      return true;
    }
    // (b) Off-path: strandable keys in a single sibling leaf -> two-leaf migration (split that
    // leaf, fold its matching keys + K into the new child), validated, else full rebuild.
    if (tryTwoLeafMigration(navResult, newNode, newSlot, nodeDepth, keySlice, valueSlice)) {
      STRAND_TWO_LEAF_MIGRATE.incrementAndGet();
      return true;
    }
    STRAND_FULL_FALLBACK.incrementAndGet();
    return false;
  }

  /**
   * Off-path strand discharge ({@code O(two leaves + node re-encode + path)}). When the strandable
   * keys are confined to a <em>single sibling leaf</em> {@code L_src} (a different node slot than
   * where K descended) and all share {@code densePK == comboPartial} exactly, migrate: build the new
   * child as {@code bulk-build(K ∪ strandable)}, replace {@code L_src} with
   * {@code bulk-build(L_src \ strandable)}, re-encode {@code newNode} with recomputed partials, and —
   * only if the result passes {@link HOTMalformedSubtreeDetector} — splice it at {@code nodeDepth}
   * and propagate up the spine. Returns {@code false} (caller does the canonical full rebuild) when
   * the source is not a single exact-densePK leaf, the rebuilt child overflows, or the candidate is
   * malformed. The detector backstop makes this safe by construction: any I3/I4/I5/I7/I8/I11 defect
   * triggers the fallback, and the end-to-end fuzz validates I1/I6.
   */
  private boolean tryTwoLeafMigration(LeafNavigationResult navResult, HOTIndirectPage newNode, int comboSlot,
      int nodeDepth, byte[] keySlice, byte[] valueSlice) {
    if (nodeDepth < 0 || nodeDepth >= navResult.pathDepth()) {
      return false; // node is not a spliceable path node
    }
    // Identify the unique source slot/leaf and collect the strandable keys; require a single
    // source leaf (so the migration touches exactly one sibling leaf). Strandable keys all have
    // comboPartial ⊆ densePK, so the new child is I5-clean; bulk-build discriminates the rest.
    final List<byte[]> strandKeys = new ArrayList<>();
    final long[] info = {-1L, -1L, 1L}; // {sourceSlot, sourceLeafPageKey, ok}
    for (int i = 0; i < newNode.getNumChildren() && info[2] == 1L; i++) {
      if (i == comboSlot) {
        continue;
      }
      collectMigratableKeys(newNode.getChildReference(i), newNode, comboSlot, keySlice, i, strandKeys, info, 0);
    }
    if (info[2] != 1L || strandKeys.isEmpty() || info[0] < 0) {
      return false; // not a single source leaf
    }
    final int sourceSlot = (int) info[0];
    final long sourceLeafPageKey = info[1];
    final int revision = storageEngineWriter.getRevisionNumber();

    // Build the migrated child = bulk-build(K ∪ strandable). All keys have comboPartial ⊆ densePK,
    // so the child is I5-clean under newNode's mask; bulk-build discriminates them internally.
    final List<HOTBulkBuilder.Entry> childEntries = new ArrayList<>(strandKeys.size() + 1);
    childEntries.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    final Page sourceLeafPage = resolveHOTPageForTraversal(newNode.getChildReference(sourceSlot));
    if (!(sourceLeafPage instanceof HOTLeafPage sourceLeaf) || sourceLeaf.getPageKey() != sourceLeafPageKey) {
      return false; // source slot is not the single source leaf
    }
    final HashSet<String> strandSet = new HashSet<>(strandKeys.size() * 2);
    for (final byte[] k : strandKeys) {
      strandSet.add(HexFormat.of().formatHex(k));
    }
    // This two-leaf migration extracts keys/values only and discards the source leaf. Rather
    // than route the side map across the two rebuilt leaves, decline: the caller's canonical
    // {@link #rebuildSubtree} fallback carries segment refs (collect + reattach), so falling
    // back is correct and this rare path stays simple.
    if (sourceLeaf.segmentRefCount() > 0) {
      return false;
    }
    final List<HOTBulkBuilder.Entry> remaining = new ArrayList<>(sourceLeaf.getEntryCount());
    for (int i = 0; i < sourceLeaf.getEntryCount(); i++) {
      final byte[] k = sourceLeaf.getKey(i);
      if (strandSet.contains(HexFormat.of().formatHex(k))) {
        childEntries.add(new HOTBulkBuilder.Entry(k, sourceLeaf.getValue(i)));
      } else {
        remaining.add(new HOTBulkBuilder.Entry(k, sourceLeaf.getValue(i)));
      }
    }
    if (remaining.isEmpty()) {
      return false; // source leaf would empty -> slot removal; rebuild
    }
    childEntries.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> childDeduped = dedupMergeEntries(childEntries);

    try {
      final HOTBulkBuilder.BuildResult childBuilt =
          HOTBulkBuilder.build(childDeduped, revision, indexType, pageKeyAllocator);
      final HOTBulkBuilder.BuildResult srcBuilt =
          HOTBulkBuilder.build(remaining, revision, indexType, pageKeyAllocator);

      // Re-encode newNode: same disc bits, children with comboSlot/sourceSlot replaced, partials
      // recomputed from the children's first keys.
      final int n = newNode.getNumChildren();
      final PageReference[] children = new PageReference[n];
      for (int i = 0; i < n; i++) {
        children[i] = newNode.getChildReference(i);
      }
      children[comboSlot] = swizzle(childBuilt.rootPage());
      children[sourceSlot] = swizzle(srcBuilt.rootPage());
      // Keep newNode's original SPARSE partials: the new children's keys still route by them
      // (comboPartial ⊆ migrated densePK; s_src.partial ⊆ remaining densePK). Recomputing from
      // firstKeys would yield dense PEXT values that break Binna's I4 (leftmost partial = 0).
      final int[] partials = newNode.getPartialKeysRef().clone();
      final int[] discBits = HOTIncrementalInsert.discriminativeBits(newNode);
      final HOTIndirectPage candidate = HOTBulkBuilder.assembleIndirect(discBits, partials, children,
          newNode.getHeight(), revision, pageKeyAllocator);

      // Safety net: only commit if the candidate subtree is structurally clean; else full rebuild.
      final PageReference candidateRef = swizzle(candidate);
      // Safety net: an I8-unsafe off-path strand (the Class-1 firing Theorems 1-4 prove no
      // localized primitive resolves) yields an I8/I7-malformed candidate here -> full rebuild.
      if (!HOTMalformedSubtreeDetector.detect(candidateRef, this::resolveHOTPageForTraversal).isEmpty()) {
        return false;
      }

      navResult.pathRefs()[nodeDepth].setPage(candidate);
      lastDispatchHandler = "h:twoleaf-migrate";
      registerFreshSubtree(navResult.pathRefs()[nodeDepth]);
      if (nodeDepth > 0) {
        propagateRebuildUpSpine(navResult, nodeDepth, keySlice, valueSlice);
      }
      return true;
    } catch (RuntimeException defensiveFallback) {
      // Any unexpected edge (build/assemble/propagate) -> the canonical full rebuild, which
      // re-derives structure from the collected keys regardless of any partial migration state.
      return false;
    }
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

    int entryCapacity = 1;
    int segmentRefCapacity = 0;
    final List<PageReference> orphanedLeafRefs = new ArrayList<>(frontier.size());
    for (int slot = frontier.fromInclusive(); slot < frontier.toExclusive(); slot++) {
      final PageReference childRef = node.getChildReference(slot);
      if (!(childRef.getPage() instanceof HOTLeafPage leaf)) {
        return false; // hard guard: never turn this into an indirect-subtree reconstruction
      }
      entryCapacity += leaf.getEntryCount();
      segmentRefCapacity += leaf.segmentRefCount();
      orphanedLeafRefs.add(childRef);
    }
    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>(entryCapacity);
    final List<CapturedSegmentRef> segmentRefs = new ArrayList<>(segmentRefCapacity);
    for (int slot = frontier.fromInclusive(); slot < frontier.toExclusive(); slot++) {
      collectSubtreeEntries(node.getChildReference(slot).getPage(), collected, segmentRefs);
    }
    collected.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> entries = dedupMergeEntries(collected);

    final int revision = storageEngineWriter.getRevisionNumber();
    Page miniRoot = null;
    boolean published = false;
    try {
      miniRoot = HOTBulkBuilder.build(entries, revision, indexType, pageKeyAllocator).rootPage();
      final PageReference replacementRef = swizzle(miniRoot);
      final PageReference candidateRef = HOTIncrementalInsert.replaceChildRangeAndCompress(node,
          frontier.fromInclusive(), frontier.toExclusive(), replacementRef, revision, pageKeyAllocator);
      final Page candidatePage = candidateRef.getPage();
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
      navResult.pathRefs()[nodeDepth].setPage(candidatePage);
      published = true;
      final Runnable afterPublicationTestHook = directionOneFrontierAfterPublicationTestHook;
      if (afterPublicationTestHook != null) {
        afterPublicationTestHook.run();
      }
      lastDispatchHandler = "h:d1-leaf-frontier-splice";
      registerFreshSubtree(navResult.pathRefs()[nodeDepth]);
      if (nodeDepth > 0) {
        propagateRebuildUpSpine(navResult, nodeDepth, keySlice, valueSlice);
      }
      storageEngineWriter.getLog().releaseOrphanedHOTLeaves(orphanedLeafRefs);
      DIRECTION_ONE_LEAF_FRONTIER_SPLICE.incrementAndGet();
      if (frontier.size() == 2) {
        DIRECTION_ONE_LEAF_PAIR_SPLICE.incrementAndGet();
      } else {
        DIRECTION_ONE_MULTI_LEAF_FRONTIER_SPLICE.incrementAndGet();
      }
      return true;
    } catch (final RuntimeException | Error failure) {
      if (published) {
        try {
          storageEngineWriter.markTransactionRollbackOnly(failure);
        } catch (final RuntimeException | Error poisonFailure) {
          addSuppressedSafely(failure, poisonFailure);
        }
        // The publication hook deliberately runs before TIL registration. A failure there leaves
        // the fresh mini leaves reachable from a transaction that can only roll back, but not yet
        // owned by the log. Release their off-heap frames here. The same cleanup is safe after a
        // later registration/propagation failure: leaf close is idempotent and the poisoned graph
        // can never be committed.
        if (miniRoot != null) {
          try {
            closeFreshHOTSubtree(miniRoot);
          } catch (final RuntimeException | Error cleanupFailure) {
            addSuppressedSafely(failure, cleanupFailure);
          }
        }
      } else if (miniRoot != null) {
        try {
          closeFreshHOTSubtree(miniRoot);
        } catch (final RuntimeException | Error cleanupFailure) {
          addSuppressedSafely(failure, cleanupFailure);
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

  static void setDirectionOneFrontierAfterPublicationTestHook(final @Nullable Runnable hook) {
    directionOneFrontierAfterPublicationTestHook = hook;
  }

  /**
   * Collect strandable keys (route to {@code comboSlot}) under {@code ref}; gate single-source +
   * exact.
   */
  private void collectMigratableKeys(@Nullable PageReference ref, HOTIndirectPage newNode, int comboSlot,
      byte[] excludeKey, int slot, List<byte[]> out, long[] info, int depth) {
    if (ref == null || depth > MAX_PATH_DEPTH + 2 || info[2] != 1L) {
      return;
    }
    final Page page = resolveHOTPageForTraversal(ref);
    if (page instanceof HOTLeafPage leaf) {
      boolean leafHasStrand = false;
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        final byte[] k = leaf.getKey(i);
        if (k == null || Arrays.equals(k, excludeKey) || newNode.findChildIndex(k) != comboSlot) {
          continue;
        }
        // Any key routing to comboSlot has comboPartial ⊆ densePK (I5-clean under node's mask);
        // bulk-build discriminates their below-comboPartial differences into the new child.
        leafHasStrand = true;
        out.add(k);
      }
      if (leafHasStrand) {
        if (info[0] >= 0 && (info[0] != slot || info[1] != leaf.getPageKey())) {
          info[2] = 0L; // strandable keys span >1 slot or >1 leaf
          return;
        }
        info[0] = slot;
        info[1] = leaf.getPageKey();
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren() && info[2] == 1L; i++) {
        collectMigratableKeys(indirect.getChildReference(i), newNode, comboSlot, excludeKey, slot, out, info,
            depth + 1);
      }
    }
  }

  /**
   * OR-merge duplicate keys in a sorted entry list (shared by the scoped/leaf/migration rebuilds).
   */
  private static List<HOTBulkBuilder.Entry> dedupMergeEntries(List<HOTBulkBuilder.Entry> sorted) {
    final List<HOTBulkBuilder.Entry> out = new ArrayList<>(sorted.size());
    for (final HOTBulkBuilder.Entry entry : sorted) {
      final int last = out.size() - 1;
      if (last >= 0 && Arrays.equals(out.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = out.get(last);
        out.set(last,
            new HOTBulkBuilder.Entry(prev.key(), HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value())));
      } else {
        out.add(entry);
      }
    }
    return out;
  }

  /**
   * Rebuild only {@code navResult.leaf()}'s entries together with {@code (keySlice, valueSlice)} into
   * a canonical mini-HOT and splice it into the leaf's slot of {@code pathNodes[pathDepth-1]}, then
   * propagate height/partial changes up the spine. {@code O(leaf entries + path depth)}.
   */
  private void leafScopedRebuild(LeafNavigationResult navResult, byte[] keySlice, byte[] valueSlice) {
    final int pathDepth = navResult.pathDepth();
    final HOTLeafPage oldLeaf = navResult.leaf();

    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>(oldLeaf.getEntryCount() + 1);
    final List<CapturedSegmentRef> segmentRefs = new ArrayList<>();
    collectSubtreeEntries(oldLeaf, collected, segmentRefs); // preserves tombstone entries
    collected.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(collected.size());
    for (final HOTBulkBuilder.Entry entry : collected) { // OR-merge duplicate keys
      final int last = entries.size() - 1;
      if (last >= 0 && Arrays.equals(entries.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = entries.get(last);
        entries.set(last,
            new HOTBulkBuilder.Entry(prev.key(), HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value())));
      } else {
        entries.add(entry);
      }
    }

    final HOTBulkBuilder.BuildResult built =
        HOTBulkBuilder.build(entries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    final Page miniRoot = built.rootPage();
    reattachSegmentRefs(miniRoot, segmentRefs);

    if (pathDepth == 0) { // the leaf is the whole index root
      navResult.leafRef().setPage(miniRoot);
      lastDispatchHandler = "h:leafrebuild-root";
      registerFreshSubtree(navResult.leafRef());
      return;
    }
    final int leafSlot = navResult.pathChildIndices()[pathDepth - 1];
    final HOTIndirectPage parent = navResult.pathNodes()[pathDepth - 1];
    final PageReference oldLeafRef = navResult.leafRef();
    final PageReference newRef = swizzle(miniRoot);
    parent.setChildReference(leafSlot, newRef);
    lastDispatchHandler = "h:leafrebuild-splice";
    registerFreshSubtree(newRef);
    // Reuse the scoped-rebuild spine propagation: treat the leaf level as rebuiltDepth=pathDepth so
    // it refreshes the parent's height + the leaf-slot partial (and recurses on an I7 collision).
    propagateRebuildUpSpine(navResult, pathDepth, keySlice, valueSlice);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(List.of(oldLeafRef));
  }

  /**
   * Returns {@code true} iff at least one key under {@code oldNode} would strand to {@code newSlot}
   * on {@code newNode} and <em>every</em> such key lives in the leaf with page key {@code
   * leafPageKey}. Used to gate {@link #leafScopedRebuild}.
   */
  private boolean strandConfinedToLeaf(HOTIndirectPage oldNode, HOTIndirectPage newNode, int newSlot, byte[] excludeKey,
      long leafPageKey) {
    final boolean[] state = {false, true}; // {found a strandable key, all so far confined}
    for (int i = 0; i < oldNode.getNumChildren() && state[1]; i++) {
      strandConfinedRec(oldNode.getChildReference(i), newNode, newSlot, excludeKey, leafPageKey, state, 0);
    }
    return state[0] && state[1];
  }

  private void strandConfinedRec(@Nullable PageReference ref, HOTIndirectPage newNode, int newSlot, byte[] excludeKey,
      long leafPageKey, boolean[] state, int depth) {
    if (ref == null || depth > MAX_PATH_DEPTH + 2 || !state[1]) {
      return;
    }
    final Page page = resolveHOTPageForTraversal(ref);
    if (page instanceof HOTLeafPage leaf) {
      final int n = leaf.getEntryCount();
      for (int i = 0; i < n; i++) {
        final byte[] k = leaf.getKey(i);
        if (k == null || Arrays.equals(k, excludeKey)) {
          continue;
        }
        if (newNode.findChildIndex(k) == newSlot) {
          state[0] = true;
          if (leaf.getPageKey() != leafPageKey) {
            state[1] = false; // a strandable key lives elsewhere
            return;
          }
        }
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren() && state[1]; i++) {
        strandConfinedRec(indirect.getChildReference(i), newNode, newSlot, excludeKey, leafPageKey, state, depth + 1);
      }
    }
  }

  /**
   * Preflight every deterministic condition {@link #propagateRebuildUpSpine} can encounter after an
   * incremental splice. The live path still points at the old subtree, so this walk substitutes the
   * candidate's exact key range and height in registers while resolving every unaffected sibling. No
   * PageReference is changed and no page is allocated.
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
   * Plan §12 Stage 3c (partials-verbatim since Stage 5) -- propagate a scoped
   * {@link #rebuildSubtree}'s effects up the spine via in-place re-encoding. At each ancestor from
   * {@code rebuiltDepth - 1} down to 0:
   *
   * <ul>
   * <li>Recompute the ancestor's height as {@code 1 + max(child.height)} -- HOT heights are max-based
   * ({@link HOTBulkBuilder#assembleIndirect}); a single rebuilt slot's new height only matters if
   * it's the (possibly tied) maximum.</li>
   * <li>Keep every stored partial verbatim -- a sparse partial is the slot's position in the
   * ancestor's block trie (off-path bits zero by convention), invariant under content growth of the
   * subtree behind it. Recomputing it from the subtree's new first key stamps off-path bits and was
   * the mass producer of I4/I5 heals (plan doc, Stage 5).</li>
   * <li>Check the one property a content change can break: sibling key ORDER (I8/I12). Compare the
   * rebuilt slot's extremes against its neighbours'; on a violation fall back to a scoped
   * {@link #rebuildSubtree} at this ancestor's depth (the recursive call re-enters this propagation;
   * the cascade is at most {@code rebuiltDepth} levels). The check runs at the immediate parent and
   * continues upward only while the changed slot sits at its block's edge -- interior slots absorb
   * the change locally.</li>
   * <li>Stop early once neither the height nor an edge boundary can change further up.</li>
   * <li>On a height change re-encode the ancestor with the same children + disc bits + partials, just
   * an updated height. The ancestor's child references are shared with the prior version; only the
   * rebuilt slot already points at fresh content via the swizzled {@link PageReference}.</li>
   * </ul>
   *
   * <p>
   * The propagation does not orphan any leaves -- only the originally rebuilt subtree's leaves are
   * released by the caller. Re-encoded ancestors replace their TIL entries at the same
   * {@link PageReference}, dropping the prior in-memory page.
   */
  private void propagateRebuildUpSpine(LeafNavigationResult navResult, int rebuiltDepth, byte[] keySlice,
      byte[] valueSlice) {
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
        final Page childPage = resolveHOTPageForTraversal(childRef);
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
      // manufactured 1,151 I5 and 80 I4 violations per 36K-insert shred, ~91% of every
      // self-heal the detector had to discharge.
      //
      // What a content change CAN legitimately break is sibling ORDER (I8/I12): the rebuilt
      // subtree's minimum may have dropped below the previous sibling's maximum (the
      // Direction-1 shape) — subset routing admits keys the lex order does not. That is a
      // boundary property, checked as such; a violation falls back to the canonical scoped
      // rebuild at this ancestor's depth, the Theorem-4 discharge for genuine firings.
      final boolean heightChanged = newAncestorHeight != ancestor.getHeight();

      if (boundaryRelevant) {
        final byte[] slotFirst = firstKeyOfSubtree(ancestor.getChildReference(rebuiltSlot));
        final byte[] prevLast = rebuiltSlot > 0
            ? lastKeyOfSubtree(ancestor.getChildReference(rebuiltSlot - 1))
            : null;
        final byte[] slotLast = rebuiltSlot + 1 < numChildren
            ? lastKeyOfSubtree(ancestor.getChildReference(rebuiltSlot))
            : null;
        final byte[] nextFirst = rebuiltSlot + 1 < numChildren
            ? firstKeyOfSubtree(ancestor.getChildReference(rebuiltSlot + 1))
            : null;
        final boolean leftViolated =
            prevLast != null && slotFirst != null && Arrays.compareUnsigned(prevLast, slotFirst) >= 0;
        final boolean rightViolated =
            slotLast != null && nextFirst != null && Arrays.compareUnsigned(slotLast, nextFirst) >= 0;
        if (leftViolated || rightViolated) {
          REBUILD_PROPAGATION_ORDER_FALLBACK.incrementAndGet();
          rebuildSubtree(navResult, ancestorDepth, keySlice, valueSlice);
          return;
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
      REBUILD_HEIGHT_ESCALATION_AVOIDED.incrementAndGet();
    }
  }

  /**
   * Depth-first gather of every reference pointing at a leaf page in {@code indirect}'s subtree. Used
   * by {@link #rebuildSubtree} to release the off-heap slots of a subtree that a canonical rebuild
   * replaced wholesale. Pages are resolved through the transaction-intent log so the walk sees
   * in-transaction modifications.
   */
  private void collectSubtreeLeafRefs(HOTIndirectPage indirect, List<PageReference> out) {
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef == null) {
        continue;
      }
      final Page child = resolveHOTPageForTraversal(childRef);
      if (child instanceof HOTIndirectPage childIndirect) {
        collectSubtreeLeafRefs(childIndirect, out);
      } else if (child instanceof HOTLeafPage) {
        out.add(childRef);
      }
    }
  }

  /**
   * Depth-first gather of every {@code (key, value)} entry in {@code page}'s subtree into
   * {@code out}. The traversal order follows the trie's child arrays, which equals key order only for
   * a canonical trie — {@link #rebuildSubtree} sorts the result, so this method does not rely on it.
   * Pages are resolved through the transaction-intent log so in-transaction modifications are seen.
   */
  private void collectSubtreeEntries(Page page, List<HOTBulkBuilder.Entry> out) {
    collectSubtreeEntries(page, out, null);
  }

  /**
   * Depth-first gather of every {@code (key, value)} entry — and, when {@code segmentRefsOut} is
   * non-null, every segment-reference side-map entry — in {@code page}'s subtree.
   *
   * <p>
   * A side map (projection index segment references,
   * {@code docs/PROJECTION_INDEX_STORAGE_REDESIGN.md} §2.3) must ride whichever leaf holds its OWNING
   * SLOT; a rebuild that reconstructs leaves from {@code (key, value)} pairs alone would silently
   * orphan the committed segment pages. Callers that reattach — via {@link #reattachSegmentRefs}
   * after the bulk build — pass a sink; callers that cannot pass {@code null} and keep the loud
   * backstop, failing attributably instead of losing data.
   */
  private void collectSubtreeEntries(Page page, List<HOTBulkBuilder.Entry> out,
      @Nullable List<CapturedSegmentRef> segmentRefsOut) {
    if (page instanceof HOTLeafPage leaf) {
      if (leaf.segmentRefCount() > 0) {
        if (segmentRefsOut == null) {
          throw new IllegalStateException(
              "Subtree rebuild would drop " + leaf.segmentRefCount() + " segment reference(s) on leaf pageKey="
                  + leaf.getPageKey() + " — this rebuild path is not instrumented for segment-ref routing.");
        }
        for (final long refKey : leaf.overflowPageRefKeysSorted()) {
          segmentRefsOut.add(new CapturedSegmentRef(refKey, leaf.getPageReference(refKey)));
        }
      }
      final int count = leaf.getEntryCount();
      for (int i = 0; i < count; i++) {
        out.add(new HOTBulkBuilder.Entry(leaf.getKey(i), leaf.getValue(i)));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final Page child = resolveHOTPageForTraversal(indirect.getChildReference(i));
        if (child == null) {
          throw new SirixIOException("HOT: unresolvable child page during subtree rebuild");
        }
        collectSubtreeEntries(child, out, segmentRefsOut);
      }
    }
  }

  /** A side-map entry captured off a leaf that a rebuild is about to discard. */
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
            "Segment-ref reattach after rebuild: refKey " + captured.refKey() + " has no PageReference");
      }
      if (!uniqueRefKeys.add(captured.refKey())) {
        throw new IllegalStateException("Segment-ref reattach after rebuild: duplicate refKey " + captured.refKey()
            + " was captured from more than one source leaf");
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
        throw new IllegalStateException(
            "Segment-ref reattach after rebuild: owning slot " + ownerSlot + " (refKey=" + captured.refKey()
                + ") not found in the rebuilt subtree" + " — the rebuild dropped an entry it collected.");
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
  private void ensurePathChildrenLoaded(HOTIndirectPage[] pathNodes) {
    for (final HOTIndirectPage node : pathNodes) {
      for (int i = 0; i < node.getNumChildren(); i++) {
        final PageReference childRef = node.getChildReference(i);
        if (childRef != null && childRef.getPage() == null) {
          final Page child = resolveHOTPageForTraversal(childRef);
          if (child != null) {
            childRef.setPage(child);
          }
        }
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
    selfHealScope = touchedRef; // root of the just-spliced subtree — scope for the self-heal
    registerFreshPage(touchedRef, true);
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
      // put() clears ref.page before it publishes a new log slot. Retain the local page so a
      // failure between those operations cannot strand an off-heap leaf outside both the tree and
      // the TIL. Before put begins, ownership is known to remain local. Once it begins, exact
      // container identity is the boundary; if that probe itself fails, retain rather than risk
      // closing a log-owned page. Any failure here dooms the transaction because descendant pages
      // may already have crossed their ownership boundary.
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
      throw failure;
    }
  }

  /**
   * Best-effort, allocation-free cleanup for a fresh subtree that registration has not visited. A
   * disk key or log key marks a shared/TIL-owned boundary and is never crossed. Each recursive call
   * absorbs its own cleanup failure so siblings are still examined.
   */
  private static void closeUnregisteredFreshSubtree(final @Nullable PageReference ref, final Throwable primaryFailure) {
    try {
      if (ref == null || ref.getLogKey() >= 0 || ref.getKey() >= 0) {
        return;
      }
      final Page page = ref.getPage();
      if (page instanceof HOTIndirectPage indirect) {
        closeUnregisteredFreshChildren(indirect, 0, primaryFailure);
      } else if (page instanceof HOTLeafPage leaf) {
        leaf.close();
      }
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(primaryFailure, cleanupFailure);
    }
  }

  private static void closeUnregisteredFreshChildren(final HOTIndirectPage indirect, final int fromInclusive,
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
    return NodeReferencesSerializer.deserialize(valueBytes);
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

  /**
   * Phase 7d — Populate the leaf's ancestor-owned bits from the path's ancestor disc bits. For each
   * absolute bit position β captured by some ancestor's mask, query the leaf's β-constancy: if all
   * existing keys agree, record β with the constant value. Mixed bits are NOT recorded (= leaf is
   * already β-mixed and no further constraint can be added).
   *
   * <p>
   * Called BEFORE strict merge so {@code mergeWithNodeRefsStrict} has the metadata to detect
   * β-breaks. Idempotent: replaces any previous owned bits.
   *
   * <p>
   * HFT-grade: at most O(pathDepth * pathMaskBits * leafEntries) per call. Single allocation per call
   * (the owned-bits and values arrays).
   */
  protected void populateLeafOwnedBitsFromPath(final HOTLeafPage leaf, final HOTIndirectPage[] pathNodes,
      final int pathDepth) {
    if (leaf == null) {
      return; // nothing to populate — the guard used to dereference the very null it tested
    }
    if (pathDepth <= 0) {
      leaf.setAncestorOwnedBits(new int[0], new byte[0]);
      return;
    }
    final int[] ancestorBits = trieWriter.collectAncestorDiscBits(pathNodes, pathDepth);
    if (ancestorBits.length == 0) {
      leaf.setAncestorOwnedBits(new int[0], new byte[0]);
      return;
    }
    final int[] tempBits = new int[ancestorBits.length];
    final byte[] tempValues = new byte[ancestorBits.length];
    int n = 0;
    for (final int beta : ancestorBits) {
      final int v = leaf.isBitConstantAtAbsBit(beta);
      if (v < 0)
        continue; // β-mixed — cannot constrain
      tempBits[n] = beta;
      tempValues[n] = (byte) v;
      n++;
    }
    if (n == ancestorBits.length) {
      leaf.setAncestorOwnedBits(tempBits, tempValues);
    } else {
      final int[] finalBits = new int[n];
      final byte[] finalValues = new byte[n];
      System.arraycopy(tempBits, 0, finalBits, 0, n);
      System.arraycopy(tempValues, 0, finalValues, 0, n);
      leaf.setAncestorOwnedBits(finalBits, finalValues);
    }
  }
}
