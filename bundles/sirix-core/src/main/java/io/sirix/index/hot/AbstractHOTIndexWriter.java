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
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
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

  /** Inserts between periodic leaf-consolidation sweeps ({@link #consolidateSubtree}). */
  private static final int CONSOLIDATION_INTERVAL = 4096;

  /**
   * The largest union a consolidation merge produces — kept below page capacity so a merged leaf
   * has room before it re-splits. {@code MAX_ENTRIES * 3/4} packs leaves toward well-filled.
   */
  private static final int CONSOLIDATION_TARGET = (HOTLeafPage.MAX_ENTRIES * 3) / 4;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHOTIndexWriter.class);

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

  /**
   * Create a persistent page key allocator backed by the index page's maxHotPageKey counter.
   *
   * <p>The returned {@link LongSupplier} allocates monotonically increasing page keys that are
   * persisted across transactions via the index page (PathPage/CASPage/NamePage). This replaces
   * the old hardcoded {@code nextPageKey = 1000000L} counter that restarted on every transaction.</p>
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
      default -> {
        /* ignore */ }
    }
  }

  /**
   * Navigate to the correct leaf page for a key, tracking the path from root.
   *
   * <p><b>Zero allocation design:</b> Path nodes/refs/indices are accumulated in pre-allocated
   * instance arrays ({@code _pathNodes}, {@code _pathRefs}, {@code _pathChildIndices}).
   * Only shallow {@link Arrays#copyOf} trims are done on return to give the caller independent
   * arrays of exactly the right depth. This eliminates {@code ArrayList} and {@code Integer}
   * boxing that would otherwise occur on every insert.</p>
   *
   * <p><b>Thread safety:</b> {@code AbstractHOTIndexWriter} is per-transaction (single-threaded),
   * so the pre-allocated arrays are safe.</p>
   *
   * @param rootRef the root reference (must be obtained ONCE and reused)
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return navigation result with leaf and path
   */
  protected LeafNavigationResult prepareLeafOfTree(PageReference rootRef, byte[] keyBuf, int keyLen) {
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
    final byte[] keySlice = keyLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, keyLen);
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
      if (existingLeafContainer != null
          && existingLeafContainer.getModified() instanceof HOTLeafPage modifiedLeaf
          && !modifiedLeaf.isClosed()) {
        return buildNavigationResult(modifiedLeaf, currentRef, pathDepth);
      }

      final ResourceConfiguration resourceConfig = storageEngineWriter.getResourceSession().getResourceConfig();
      final VersioningType versioningType = resourceConfig.versioningType;
      final int revsToRestore = resourceConfig.maxNumberOfRevisionsToRestore;
      final boolean forceFullEmit =
          versioningType.bumpHOTPageFragmentChain(currentRef, hotLeaf.getRevision(), revsToRestore,
              storageEngineWriter.getDatabaseId(), storageEngineWriter.getResourceId());

      final HOTLeafPage modifiedLeaf = hotLeaf.copy();
      if (versioningType == VersioningType.FULL || forceFullEmit) {
        modifiedLeaf.markAllEntriesDirty();
      }
      final PageContainer leafContainer = PageContainer.getInstance(hotLeaf, modifiedLeaf);
      storageEngineWriter.getLog().put(currentRef, leafContainer);

      return buildNavigationResult(modifiedLeaf, currentRef, pathDepth);
    }

    // Empty tree path: create a new leaf at currentRef (root or missing child).
    // currentRef here is owned by the CoW'd parent's children array (top-down CoW above).
    final HOTLeafPage newLeaf = new HOTLeafPage(currentRef.getKey() >= 0 ? currentRef.getKey() : 0,
        storageEngineWriter.getRevisionNumber(), indexType);
    final PageContainer container = PageContainer.getInstance(newLeaf, newLeaf);
    storageEngineWriter.getLog().put(currentRef, container);

    return buildNavigationResult(newLeaf, currentRef, pathDepth);
  }

  /**
   * Resolve the root reference of this HOT sub-tree from the CoW'd index page now in the
   * transaction log. Required because the cached {@link #rootReference} field points at the
   * pre-CoW index page's slot — that instance is shared with the historical revision's view.
   * After {@link #prepareIndexPage()} has put a deep-copied page in the log, the slot returned
   * by {@code getOrCreateReference(indexNumber)} on the CoW'd page is a fresh
   * {@link PageReference} owned exclusively by this writer's transaction.
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
      default -> null;
    };
    if (indexPageRef == null) return fallbackRef;
    final PageContainer container = storageEngineWriter.getLog().get(indexPageRef);
    if (container == null) return fallbackRef;
    final Page modified = container.getModified();
    final PageReference cowed = switch (indexType) {
      case PATH -> ((PathPage) modified).getOrCreateReference(indexNumber);
      case CAS -> ((CASPage) modified).getOrCreateReference(indexNumber);
      case NAME -> ((NamePage) modified).getOrCreateReference(indexNumber);
      case PROJECTION -> ((ProjectionIndexPage) modified).getOrCreateReference(indexNumber);
      default -> fallbackRef;
    };
    return cowed != null ? cowed : fallbackRef;
  }

  /**
   * Top-down CoW for a HOT indirect page on the write path. Mirrors
   * {@link io.sirix.access.trx.page.KeyedTrieWriter#prepareIndirectPage} for the document trie:
   * if not already in the transaction log this trx, deep-copy the page via
   * {@link HOTIndirectPage#HOTIndirectPage(HOTIndirectPage)} — the copy ctor allocates a fresh
   * children array and a fresh {@link PageReference} per occupied slot, so subsequent mutations
   * to a child reference (its key, pageFragments, swizzled page) cannot bleed back to the
   * historical revision's view of the parent indirect through cache aliasing. Idempotent within
   * a transaction: subsequent calls return the same in-log copy.
   *
   * @param reference the reference whose page is to be CoW'd into the log
   * @param indirectPage the resolved indirect page (must not be {@code null})
   * @return the CoW'd indirect page (newly created or already in log)
   */
  private HOTIndirectPage prepareIndirectPage(final PageReference reference,
      final HOTIndirectPage indirectPage) {
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
    if (currentRef == null) return null;

    Page page = resolveHOTPageForTraversal(currentRef);
    while (page instanceof HOTIndirectPage indirectPage) {
      int childIndex = indirectPage.findChildIndex(keyBuf);
      if (childIndex < 0) childIndex = 0;
      final PageReference childRef = indirectPage.getChildReference(childIndex);
      if (childRef == null) {
        LOG.warn("HOT navigation: null child ref at index {} in indirect page {}", childIndex, indirectPage.getPageKey());
        return null;
      }
      currentRef = childRef;
      page = resolveHOTPageForTraversal(currentRef);
      if (page == null) {
        LOG.warn("HOT navigation: unresolvable page for ref key={}", currentRef.getKey());
        return null;
      }
    }
    return page instanceof HOTLeafPage hotLeaf ? hotLeaf : null;
  }


  /** No-op: leaf cache was removed (unsafe for HOT's PEXT-based partitioning). */
  protected final void invalidateLeafCache() {
    // kept as a public hook in case callers rely on it; nothing to invalidate.
  }

  /**
   * Resolve a HOT page from TIL/swizzled/storage for traversal.
   *
   * <p>Prefers the modified TIL page so in-transaction reads see latest writes.</p>
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
   * Build immutable navigation result by trimming reusable path buffers.
   */
  private LeafNavigationResult buildNavigationResult(final HOTLeafPage leaf, final PageReference leafRef, final int pathDepth) {
    final HOTIndirectPage[] pathNodes = pathDepth == 0 ? new HOTIndirectPage[0] : Arrays.copyOf(_pathNodes, pathDepth);
    final PageReference[] pathRefs = pathDepth == 0 ? new PageReference[0] : Arrays.copyOf(_pathRefs, pathDepth);
    final int[] pathChildIndices = pathDepth == 0 ? new int[0] : Arrays.copyOf(_pathChildIndices, pathDepth);
    return new LeafNavigationResult(leaf, leafRef, pathNodes, pathRefs, pathChildIndices, pathDepth);
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
   * Insert a {@code (key, value)} pair into the HOT secondary index — the live driver of the
   * faithful incremental port ({@code docs/HOT_INCREMENTAL_PORT_PLAN.md} step 5).
   *
   * <p>{@link #prepareLeafOfTree} copy-on-writes the descent path to a leaf page;
   * {@link HOTIncrementalInsert#analyzeDescent} then locates the mismatch bit {@code beta}
   * between the new key and the routed leaf. Two outcomes follow (plan §1.2):
   * <ul>
   *   <li><b>merge</b> — {@code beta} lies inside the leaf's {@code R(S)}-subtree (or the index
   *       has no compound node yet): the entry is merged into the leaf bucket. On bucket
   *       overflow the leaf page is split ({@link HOTIncrementalInsert#splitLeafPage}) and the
   *       resulting {@code BiNode} is integrated at the leaf's depth.</li>
   *   <li><b>branch</b> — {@code beta} is at or above an ancestor's discriminative bit: HOT's
   *       subset-match routing landed the key in a leaf it does not fully belong to, so the index
   *       is rebuilt canonically with the key included ({@link #branchAboveLeaf}).</li>
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
    final byte[] keySlice = keyLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, keyLen);
    final LeafNavigationResult navResult =
        prepareLeafOfTree(rootReference, keySlice, keySlice.length);

    final boolean localize = LOCALIZE_I8
        && storageEngineWriter.getRevisionNumber() >= LOCALIZE_I8_FROM_REV && i8ProbeReports < 60;
    final String i8Before = localize ? firstStructuralViolationFromRoot() : null;
    final long[] cntBefore = localize ? i8ProbeSnapshot() : null;

    // Factored merge-vs-branch dispatch — re-used by {@link #subInsertAt} on a C2 re-descend
    // (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §4.1).
    dispatchInsert(navResult, keyBuf, keyLen, valueBuf, valueLen, keySlice);

    if (localize && i8Before == null) {
      i8ProbeReport("dispatch(" + (i8ProbeMerge ? "merge" : "branch") + ")", keySlice, cntBefore);
    }

    // Periodic leaf consolidation (the thesis's underflow rule). The incremental insert leaves
    // the trie over-partitioned — under-full frozen leaves an insert never re-routes to — so a
    // per-insert trigger cannot reach them; a periodic sweep does. Amortized: one O(index) sweep
    // per CONSOLIDATION_INTERVAL inserts.
    if (navResult.pathDepth() > 0 && ++insertsSinceConsolidation >= CONSOLIDATION_INTERVAL) {
      insertsSinceConsolidation = 0;
      final String consBefore = localize ? firstStructuralViolationFromRoot() : null;
      final long[] consCntBefore = localize ? i8ProbeSnapshot() : null;
      consolidateSubtree(navResult.pathRefs()[0]);
      // Defense-in-depth: consolidation is a whole-subtree post-order sweep (it merges under-full
      // sibling leaves), so unlike a dispatch fold it can touch nodes OFF the inserted key's path
      // — the path self-heal above cannot see those. Run the FULL-invariant detector over the
      // consolidated subtree (incl. I5 = routing soundness) and discharge every malformed node via
      // a scoped rebuild. Amortized cheap: runs once per CONSOLIDATION_INTERVAL inserts, the same
      // O(subtree) cadence as the sweep it guards.
      if (SELFHEAL_STRUCTURAL) {
        detectAndHeal(navResult.pathRefs()[0]);
      }
      if (localize && consBefore == null) {
        i8ProbeReport("consolidate", keySlice, consCntBefore);
      }
    }
  }

  // ===== I8-onset localizer helpers (diagnostic; see field declarations). =====

  private long[] i8ProbeSnapshot() {
    return new long[] {OFF_PATH_OVERFLOW_OK.get(), OFF_PATH_OVERFLOW_FALLBACK.get(),
        DIRECTION_ONE_SUBINSERT.get(), DIRECTION_ONE_FALLBACK.get(), STRAND_LEAF_REBUILD.get(),
        STRAND_FULL_FALLBACK.get(), STRAND_TWO_LEAF_MIGRATE.get(), REBUILD_SUBTREE_CALLED.get()};
  }

  private void i8ProbeReport(String phase, byte[] keySlice, long[] before) {
    final String viol = firstStructuralViolationFromRoot();
    if (viol == null) {
      return;
    }
    i8ProbeReports++;
    final long[] after = i8ProbeSnapshot();
    final String[] names = {"offPathOk", "offPathFallback", "dir1Subinsert", "dir1Fallback",
        "strandLeaf", "strandFull", "strandMigrate", "rebuild"};
    final StringBuilder deltas = new StringBuilder();
    for (int i = 0; i < names.length; i++) {
      if (after[i] != before[i]) {
        deltas.append(names[i]).append('+').append(after[i] - before[i]).append(' ');
      }
    }
    System.err.println("[I8-LOCALIZE] rev=" + storageEngineWriter.getRevisionNumber() + " phase="
        + phase + " key=" + HexFormat.of().formatHex(keySlice, 0, Math.min(keySlice.length, 22))
        + " handlers={" + deltas.toString().trim() + "} onset=" + viol);
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
    final int[] partials = indirect.getPartialKeys();
    if (partials != null && partials.length >= n && n > 0) {
      int minPartial = partials[0];
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], minPartial) < 0) {
          minPartial = partials[i];
        }
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          return "I7 node=" + indirect.getPageKey() + " nChildren=" + n + " partial[" + (i - 1)
              + "]=0x" + Integer.toHexString(partials[i - 1]) + " >= partial[" + i + "]=0x"
              + Integer.toHexString(partials[i]);
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
   * The merge-vs-branch dispatch core of {@link #doIndex}: run {@code analyzeDescent}, decide
   * between merge and branch via the merge-vs-branch bound (Â§1.2 of the port plan), invoke the
   * corresponding handler. Factored out so {@link #subInsertAt} can re-use it on a C2
   * re-descend ({@code docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md} Â§4.1).
   */
  private void dispatchInsert(LeafNavigationResult navResult, byte[] keyBuf, int keyLen,
      byte[] valueBuf, int valueLen, byte[] keySlice) {
    final int pathDepth = navResult.pathDepth();
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final HOTIncrementalInsert.DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(
        pathNodes, navResult.pathChildIndices(), pathDepth, navResult.leaf(), keySlice);

    // Merge-vs-branch: the key merges into the routed leaf when there is no compound ancestor,
    // when it is already present or the leaf is empty (beta < 0), or when the mismatch bit beta
    // is strictly less significant than every ancestor discriminative bit -- i.e. beta lies
    // inside the leaf's R(S)-subtree (I5 holds). The bound is the deepest compound node's least
    // significant disc bit (I11 dominates the shallower bits). Larger absolute bit index = less
    // significant.
    final int beta = analysis.mismatchBit();
    final boolean merge = beta < 0 || pathDepth == 0
        || beta > leastSignificantDiscBit(pathNodes[pathDepth - 1]);
    if (LOCALIZE_I8) {
      i8ProbeMerge = merge;
    }

    selfHealScope = null;     // set by registerFreshSubtree iff this dispatch splices a subtree
    final boolean structurallyChanged = merge
        ? mergeIntoLeaf(navResult, keyBuf, keyLen, valueBuf, valueLen, keySlice)
        : branchAboveLeaf(navResult, analysis, keySlice, valueBuf, valueLen);

    // Defense-in-depth (full-invariant). A structural fold (off-path-overflow / integrate / a
    // combo-add the pre-commit guards don't fully cover) can, in rare multi-value-leaf shapes at
    // high chunkIdx, leave the touched subtree malformed. Two scoped, complementary checks, both
    // discharging via the Theorem-4 scoped rebuild and both skipped on the fast merge (no
    // structural change) and after a rebuild (already canonical):
    //   (1) detectAndHeal on the touched subtree runs the FULL invariant set — crucially I5
    //       (leaf-constancy), which is routing-soundness (foundation Theorem 2), so it transitively
    //       covers I6 (mis-route) and I1 (cross-leaf dup) without separate machinery — plus
    //       I3/I4/I7/I8/I11. A mutation only malforms nodes it touched, so scoping the O(subtree)
    //       walk to selfHealScope is sound and bounded (not a from-root scan).
    //   (2) healStructuralViolationOnPath covers the ANCESTORS above the touched subtree: their
    //       blocks are unmodified (I5 preserved), but the touched subtree's firstKey may have
    //       shifted, so re-verify the cheap ordering invariants (I4/I7/I8) up the spine.
    if (structurallyChanged && SELFHEAL_STRUCTURAL) {
      detectAndHeal(selfHealScope);
      healStructuralViolationOnPath(keySlice);
    }
  }

  /**
   * Full-invariant self-heal scoped to {@code scope}'s subtree: run the executable invariant spec
   * ({@link HOTMalformedSubtreeDetector}, I3/I4/I5/I7/I8/I11) and discharge every highest malformed
   * indirect via a canonical scoped rebuild ({@link #rebuildExistingSubtree}). Because I5 is
   * routing-soundness (foundation Theorem 2), this is the runtime guarantee that the touched
   * subtree routes correctly (I6) and holds no cross-leaf duplicate (I1) — not merely the cheap
   * structural invariants. {@code scope} is the just-spliced subtree root, so the detector cost is
   * bounded by the mutation's footprint, and the rebuild is Θ(n)-optimal (foundation Theorem 4).
   */
  private void detectAndHeal(@Nullable PageReference scope) {
    if (scope == null) {
      return;
    }
    final var malformed = HOTMalformedSubtreeDetector.detect(scope, this::resolveHOTPageForTraversal);
    for (final HOTMalformedSubtreeDetector.MalformedSubtree m : malformed) {
      STRUCTURAL_SELFHEAL_REBUILD.incrementAndGet();
      rebuildExistingSubtree(m.reference());
    }
  }

  /**
   * Insert {@code (key, value)} into the subtree rooted at {@code subtreeRef}. Used by the
   * C2-collision handlers: when {@code addChildAtCombination}'s {@code comboPartial} coincides
   * with an existing child of d* (or of the boundary node), K structurally belongs INSIDE that
   * child's subtree -- the descent stopped one level too shallow. This method extends the
   * descent through {@code subtreeRef} and runs the standard merge-vs-branch dispatch at the
   * deeper depth ({@code docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md} Â§4.1).
   *
   * <p>Uses local descent arrays (not the shared {@code _pathNodes} field) so it is safe under
   * recursive invocation (a sub-insert that itself triggers another C2). Bounded by tree depth
   * ({@code MAX_PATH_DEPTH}).
   *
   * @return {@code true} iff the insert succeeded incrementally; {@code false} on defensive
   *         failure (unresolvable descent / depth overflow) -- caller falls back to its scoped
   *         rebuild.
   */
  private boolean subInsertAt(PageReference subtreeRef, byte[] keyBuf, int keyLen,
      byte[] valueBuf, int valueLen) {
    if (subtreeRef == null) {
      return false;
    }
    final byte[] keySlice = keyLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, keyLen);

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
        return false;                          // defensive: tree-depth overflow
      }
      final HOTIndirectPage cowedIndirect = prepareIndirectPage(currentRef, indirectPage);
      subPathNodes[subPathDepth] = cowedIndirect;
      subPathRefs[subPathDepth] = currentRef;
      final int childIndex = cowedIndirect.findChildIndex(keySlice);
      if (childIndex < 0) {
        return false;                          // defensive: descent failed
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
      return false;                            // defensive: expected a leaf
    }

    // CoW the leaf into the TIL (mirrors prepareLeafOfTree's leaf handling).
    final HOTLeafPage modifiedLeaf;
    final PageContainer existing = storageEngineWriter.getLog().get(currentRef);
    if (existing != null
        && existing.getModified() instanceof HOTLeafPage existingModified
        && !existingModified.isClosed()) {
      modifiedLeaf = existingModified;
    } else {
      final ResourceConfiguration cfg = storageEngineWriter.getResourceSession().getResourceConfig();
      final VersioningType versioningType = cfg.versioningType;
      final boolean forceFullEmit = versioningType.bumpHOTPageFragmentChain(currentRef,
          hotLeaf.getRevision(), cfg.maxNumberOfRevisionsToRestore,
          storageEngineWriter.getDatabaseId(), storageEngineWriter.getResourceId());
      modifiedLeaf = hotLeaf.copy();
      if (versioningType == VersioningType.FULL || forceFullEmit) {
        modifiedLeaf.markAllEntriesDirty();
      }
      storageEngineWriter.getLog().put(currentRef,
          PageContainer.getInstance(hotLeaf, modifiedLeaf));
    }

    final LeafNavigationResult subNav = new LeafNavigationResult(modifiedLeaf, currentRef,
        Arrays.copyOf(subPathNodes, subPathDepth),
        Arrays.copyOf(subPathRefs, subPathDepth),
        Arrays.copyOf(subPathChildIndices, subPathDepth),
        subPathDepth);

    dispatchInsert(subNav, keyBuf, keyLen, valueBuf, valueLen, keySlice);
    return true;
  }

  /**
   * Find the slot of {@code node}'s child whose stored partial equals {@code partial}, or
   * {@code -1} if none. Used by the C2-collision handlers to find the colliding child for
   * {@link #subInsertAt}.
   */
  private static int findChildSlotByPartial(HOTIndirectPage node, int partial) {
    final int[] partials = node.getPartialKeys();
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
   * Walk the leftmost path from {@code ref} to its leaf and return that leaf's first key --
   * the smallest key contained in the subtree rooted at {@code ref}. Bounded by tree height
   * ({@link #MAX_PATH_DEPTH}); returns {@code null} on an empty subtree or an unresolvable
   * descent (defensive). Used by the Direction 1 I8-safety pre-check to compare K's lex
   * position against {@code affected}'s neighbouring siblings.
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
   * Multi-entry-leaf stranding guard ([[hot-multientry-leaf-quirks]] #1). Returns {@code true} iff
   * folding the new key {@code newKey} into {@code oldNode} — producing the candidate {@code
   * newNode} where {@code newKey} routes to a freshly created single-key child — would re-route an
   * EXISTING key to that child without migrating it (a cross-leaf duplicate / I6 misroute).
   *
   * <p>The faithful HOT port assumes the affected subtree is one-sided on the split bit (Binna's
   * single-TID leaves trivially satisfy this). Sirix's multi-entry leaves can straddle it, so a
   * sibling subtree may already hold keys captured by the new child's partial. PEXT routing is
   * equality-/most-specific-preferred, so the new child silently steals them. On a detected strand
   * the caller abandons the incremental branch and returns {@code false}, falling back to the
   * canonical {@link #rebuildSubtree} at the insert depth — straddle-free and I5/I6/I8-clean by
   * construction. (An earlier merge-into-descended-leaf shortcut was abandoned: it left a
   * straddling leaf that a later branch could mis-encode, surfacing as I8/I5/I6 under fuzzing.)
   */
  private boolean branchAddStrandsExisting(HOTIndirectPage oldNode, HOTIndirectPage newNode,
      byte[] newKey) {
    final int newSlot = newNode.findChildIndex(newKey);
    return newSlot >= 0 && existingKeyRoutesToSlot(oldNode, newNode, newSlot, newKey);
  }

  /**
   * Stranding check for adding a combo child to {@code oldNode}. Returns {@code true} iff some
   * physical key currently stored under {@code oldNode} (other than {@code excludeKey}) would, on
   * the candidate {@code newNode}, route to {@code newSlot} — the freshly added child that holds
   * only the new key. Such a key would be silently re-routed to the new child without being
   * migrated into it (PEXT routing is equality-/most-specific-preferred), i.e. it would become a
   * cross-leaf duplicate. Resolves pages writer-side ({@link #resolveHOTPageForTraversal}) so it
   * sees the in-progress (TIL) subtree. Short-circuits on the first captured key. O(subtree keys).
   */
  private boolean existingKeyRoutesToSlot(HOTIndirectPage oldNode, HOTIndirectPage newNode,
      int newSlot, byte[] excludeKey) {
    for (int i = 0; i < oldNode.getNumChildren(); i++) {
      if (subtreeHasKeyRoutingToSlot(oldNode.getChildReference(i), newNode, newSlot, excludeKey, 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Cheap single-node structural check covering the three O(children)-class invariants that a
   * combo-add / fold can break under multi-value leaves: I4 (smallest stored partial must be 0 —
   * Binna's "first mask always zero"), I7 (stored partials strictly ascending), and I8 (children
   * ordered by ascending subtree first-key). Returns {@code true} on the first violation. The
   * expensive I5 constancy walk is intentionally excluded here (the post-dispatch
   * {@link #detectAndHeal} runs the full detector incl. I5); these three are O(children) /
   * O(children×height). Used as a pre-commit combo-add guard (the first-key-order complement to the
   * routing-only {@link #branchAddStrandsExisting}, discharging via the I8-clean canonical
   * {@link #rebuildSubtree}) and as the post-dispatch path probe ({@link #healStructuralViolationOnPath}).
   * Sufficient as a single-node scan because a fold leaves every existing child's subtree untouched.
   */
  private boolean nodeStructurallyMalformed(HOTIndirectPage candidate) {
    final int n = candidate.getNumChildren();
    final int[] partials = candidate.getPartialKeys();
    if (partials != null && partials.length >= n && n > 0) {
      int minPartial = partials[0];
      for (int i = 1; i < n; i++) {
        if (Integer.compareUnsigned(partials[i], partials[i - 1]) <= 0) {
          return true;                                   // I7: partials not strictly ascending
        }
        if (Integer.compareUnsigned(partials[i], minPartial) < 0) {
          minPartial = partials[i];
        }
      }
      if (minPartial != 0) {
        return true;                                     // I4: smallest stored partial must be 0
      }
    }
    byte[] previousFirstKey = null;
    for (int i = 0; i < n; i++) {
      final byte[] firstKey = firstKeyOfSubtree(candidate.getChildReference(i));
      if (firstKey == null) {
        continue;
      }
      if (previousFirstKey != null && Arrays.compareUnsigned(previousFirstKey, firstKey) >= 0) {
        return true;                                     // I8: children not ordered by first-key
      }
      previousFirstKey = firstKey;
    }
    return false;
  }

  /**
   * Returns {@code true} iff some physical key under the subtree at {@code ref} (other than
   * {@code excludeKey}) has bit {@code beta} (MSB-first absolute position) equal to {@code
   * bitValue}. Used by the BiNode-wrap stranding guards: wrapping a whole subtree on one side of
   * {@code beta} strands any key inside it that sits on the opposite ({@code bitValue}) side.
   */
  private boolean subtreeHasKeyWithBit(@Nullable PageReference ref, int beta, int bitValue,
      byte[] excludeKey) {
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
        final int bit = (bytePos < k.length) && ((k[bytePos] & mask) != 0) ? 1 : 0;
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
  private boolean subtreeHasKeyRoutingToSlot(@Nullable PageReference ref, HOTIndirectPage newNode,
      int newSlot, byte[] excludeKey, int depth) {
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
        if (subtreeHasKeyRoutingToSlot(indirect.getChildReference(i), newNode, newSlot, excludeKey,
            depth + 1)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * I8 (children-sorted-by-firstkey) safety predicate for sub-inserting {@code K} into the
   * {@code affected} subtree at {@code insertDepth}. Direction 1 sub-insert
   * ({@code docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md} §11) is routing-correct by the
   * descent tautology -- but if K becomes the new {@code firstKey} of {@code affected}, that
   * change PROPAGATES up the spine through every ancestor where {@code affected}'s slot at
   * that level is 0 (the leftmost child). At each such ancestor, I8 demands {@code K} also
   * fits between the left and right siblings' first keys. An MSDB-closure gap in the
   * ancestor's mask can put K outside that interval -- a real failure mode (a regression
   * surfaced by HOTVersionedLeafStressTest's interleavedInsertDeleteMultiRev).
   *
   * <p>Returns {@code true} iff sub-inserting K is safe at every affected level. The cost is
   * O(height) per check (leftmost-walk per inspected sibling, capped at {@link #MAX_PATH_DEPTH}).
   *
   * <p><b>Short-circuit.</b> When {@code K >= affected.firstKey}, K cannot become the new
   * leftmost key of {@code affected}, so no firstKey changes on the spine -- I8 is trivially
   * preserved.
   */
  private boolean isDirectionOneI8Safe(LeafNavigationResult navResult, int insertDepth,
      int affectedIdx, byte[] keySlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int[] childSlots = navResult.pathChildIndices();
    final HOTIndirectPage dStar = pathNodes[insertDepth];

    final byte[] affectedFirstKey = firstKeyOfSubtree(dStar.getChildReference(affectedIdx));
    if (affectedFirstKey == null) {
      return false;                              // defensive: unresolvable subtree
    }
    if (Arrays.compareUnsigned(keySlice, affectedFirstKey) >= 0) {
      return true;                               // K >= affected.firstKey: no firstKey change.
    }

    // K < affected.firstKey -> K becomes new firstKey of affected. Check I8 at d*.
    if (!isI8SafeAtSlot(dStar, affectedIdx, keySlice)) {
      return false;
    }
    // K's firstKey-change propagates upward as long as the current slot is 0 (leftmost).
    int currentSlot = affectedIdx;
    for (int depth = insertDepth - 1; depth >= 0 && currentSlot == 0; depth--) {
      final int parentSlot = childSlots[depth];
      if (!isI8SafeAtSlot(pathNodes[depth], parentSlot, keySlice)) {
        return false;
      }
      currentSlot = parentSlot;
    }
    return true;
  }

  /**
   * Check I8 around {@code slot} of {@code node} given {@code keySlice} as the slot's new
   * (smaller) firstKey: {@code prev.firstKey < keySlice < next.firstKey} must hold. Helper
   * for {@link #isDirectionOneI8Safe}.
   */
  private boolean isI8SafeAtSlot(HOTIndirectPage node, int slot, byte[] keySlice) {
    final int n = node.getNumChildren();
    if (slot > 0) {
      final byte[] prevFirstKey = firstKeyOfSubtree(node.getChildReference(slot - 1));
      if (prevFirstKey == null || Arrays.compareUnsigned(keySlice, prevFirstKey) <= 0) {
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
   * Direction 1 outcome counter -- how often the C2 catch sub-inserts vs falls back to
   * scoped rebuild. Useful for empirical hit-rate measurement; never read by the writer.
   */
  public static final java.util.concurrent.atomic.AtomicLong DIRECTION_ONE_SUBINSERT =
      new java.util.concurrent.atomic.AtomicLong();
  public static final java.util.concurrent.atomic.AtomicLong DIRECTION_ONE_FALLBACK =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Issue B outcome counters -- how often handleOffPathOverflow succeeds vs falls back to
   * the caller's whole-index self-heal. Plan §4.3.
   */
  public static final java.util.concurrent.atomic.AtomicLong OFF_PATH_OVERFLOW_OK =
      new java.util.concurrent.atomic.AtomicLong();
  public static final java.util.concurrent.atomic.AtomicLong OFF_PATH_OVERFLOW_FALLBACK =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Stage 3c (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §12) -- how often the scoped
   * {@link #rebuildSubtree} avoided a height-escalation by re-encoding an ancestor in place
   * (one increment per ancestor refreshed). A high count means Stage 3c stopped a cascade
   * that the original behaviour would have grown into a depth-0 whole rebuild.
   */
  public static final java.util.concurrent.atomic.AtomicLong REBUILD_HEIGHT_ESCALATION_AVOIDED =
      new java.util.concurrent.atomic.AtomicLong();
  /**
   * Stage 3c defensive arm -- the new partial would break I7 (ascending, distinct partials);
   * the propagation falls back to a scoped rebuild at the ancestor's depth instead of an
   * in-place re-encode. Should stay near zero in practice (the C2-firing descent already
   * picked a slot for K, so the new partial slots into the same ordering).
   */
  public static final java.util.concurrent.atomic.AtomicLong REBUILD_PROPAGATION_I7_FALLBACK =
      new java.util.concurrent.atomic.AtomicLong();
  /**
   * Total invocations of {@link #rebuildSubtree} (any depth, any caller). With
   * {@link #REBUILD_HEIGHT_ESCALATION_AVOIDED} reports both how often a rebuild occurred
   * and how often Stage 3c's propagation re-encoded at least one ancestor.
   */
  public static final java.util.concurrent.atomic.AtomicLong REBUILD_SUBTREE_CALLED =
      new java.util.concurrent.atomic.AtomicLong();


  /**
   * Characterize an I8-unsafe Direction 1 fallback (Stage 4b iter-3 diagnostic). Gated on
   * {@code -Dhot.diag.directionOneFallback=true}. Dumps the trigger key, d*'s shape, the
   * affected slot's lex position vs. K, and -- as a routing-encoding-rewrite Phase 1 probe
   * (docs/HOT_ROUTING_ENCODING_REWRITE.md) -- the candidate disc bit β'' = MSDB(K XOR
   * affected.firstKey) AND β''' = MSDB(K XOR prev.firstKey), plus whether each is fresh to
   * d*'s current mask. The Phase 1 hypothesis: β'' (and ideally β''') is always present +
   * fresh, so a proactive mask extension at d* can fix the ambiguity that drove the
   * I8-unsafe fallback.
   */
  private void dumpDirectionOneFallback(String site, LeafNavigationResult navResult,
      int affectedIdx, int insertDepth, int beta, int betaValue, int comboPartial,
      byte[] keySlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int[] childSlots = navResult.pathChildIndices();
    final HOTIndirectPage dStar = pathNodes[insertDepth];
    final int n = dStar.getNumChildren();
    final byte[] affectedFirstKey = firstKeyOfSubtree(dStar.getChildReference(affectedIdx));
    final byte[] prevFirstKey = affectedIdx > 0
        ? firstKeyOfSubtree(dStar.getChildReference(affectedIdx - 1)) : null;
    final byte[] nextFirstKey = affectedIdx + 1 < n
        ? firstKeyOfSubtree(dStar.getChildReference(affectedIdx + 1)) : null;
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
    final String hexAffected = affectedFirstKey == null ? "null"
        : hex.formatHex(affectedFirstKey, 0, Math.min(affectedFirstKey.length, 22));
    final String hexPrev = prevFirstKey == null ? "<none>"
        : hex.formatHex(prevFirstKey, 0, Math.min(prevFirstKey.length, 22));
    final String hexNext = nextFirstKey == null ? "<none>"
        : hex.formatHex(nextFirstKey, 0, Math.min(nextFirstKey.length, 22));

    // Routing-encoding-rewrite Phase 1 probe (docs/HOT_ROUTING_ENCODING_REWRITE.md):
    // compute candidate disc bits + freshness. Empirically (2026-05-20) all 4 canary
    // firings have β'' (= MSDB(K XOR affected.fk)) IN d*'s mask -- the bit is there but
    // off-path-straddled at affected's slot. The §2.2 "proactive mask extension"
    // hypothesis is therefore refuted: the right Phase 2 primitive is to SPLIT
    // affected on β'' (force the straddled bit onto path), not add the bit to d*.
    final int[] dStarDiscBits = HOTIncrementalInsert.discriminativeBits(dStar);
    final int betaPrimePrime = affectedFirstKey == null
        ? -1 : msdbOfKeyXor(keySlice, affectedFirstKey);
    final int betaTriple = prevFirstKey == null
        ? -1 : msdbOfKeyXor(keySlice, prevFirstKey);
    final boolean bppFresh = betaPrimePrime >= 0
        && Arrays.binarySearch(dStarDiscBits, betaPrimePrime) < 0;
    final boolean btFresh = betaTriple >= 0
        && Arrays.binarySearch(dStarDiscBits, betaTriple) < 0;

    // Paper-grade single-entry-leaf-for-K probe (2026-05-20). Classifies whether
    // each firing's K can be carved out as its own slot at d* with partial = K's
    // densePK without colliding with affected's stored partial. Two cases:
    //   (a) K's densePK == affected's stored        -> COLLISION (cannot give K its
    //       own slot under d*'s current mask; would need mask extension or other
    //       structural change first).
    //   (b) K's densePK is a strict superset       -> CARVABLE (K's densePK is
    //       unique to its slot; adding a new slot with that partial preserves I7
    //       AND I8 because K's densePK < prev's stored as integers at the β'''
    //       packed position where K=0, prev=1 and they agree above).
    // If ALL firings are case (b), the localized fix is theoretically viable
    // (still needs to verify routing doesn't break for other keys); if ALL are
    // case (a), the impossibility evidence strengthens.
    final int densePkK = dStar.computeDensePartialKey(keySlice);
    final int affectedStored = dStar.getPartialKey(affectedIdx);
    final int prevStored = affectedIdx > 0 ? dStar.getPartialKey(affectedIdx - 1) : -1;
    final boolean subsetOk = (densePkK & affectedStored) == affectedStored;
    final boolean strictSuperset = subsetOk && densePkK != affectedStored;
    final String carveClass = !subsetOk ? "ROUTING-BUG"
        : (densePkK == affectedStored ? "COLLISION" : "CARVABLE");
    // For CARVABLE cases, verify K's densePK sorts BEFORE prev's stored (so K's
    // new slot lands at I7 position < prev's, satisfying I8 with K's firstKey <
    // prev.firstKey). When prev is absent (affectedIdx=0) the firing must still
    // respect d*'s outer ancestors -- record as N/A here.
    final String prevOrderOk;
    if (prevStored < 0) {
      prevOrderOk = "n/a";
    } else if (strictSuperset) {
      prevOrderOk = Integer.compareUnsigned(densePkK, prevStored) < 0 ? "yes" : "NO";
    } else {
      prevOrderOk = "skip";
    }

    System.err.println("[D1-FALLBACK " + site + "] K=" + hexKey
        + " (lenK=" + keySlice.length + ")"
        + " pathDepth=" + navResult.pathDepth() + " insertDepth=" + insertDepth
        + " dStar.children=" + n + " dStar.height=" + dStar.getHeight()
        + " affectedIdx=" + affectedIdx + " spine=" + spine
        + " beta=" + beta + " betaValue=" + betaValue
        + " comboPartial=0x" + Integer.toHexString(comboPartial)
        + " affected.fk=" + hexAffected + " (lenA="
        + (affectedFirstKey == null ? "n/a" : Integer.toString(affectedFirstKey.length)) + ")"
        + " prev.fk=" + hexPrev + " (lenP="
        + (prevFirstKey == null ? "n/a" : Integer.toString(prevFirstKey.length)) + ")"
        + " next.fk=" + hexNext
        + " // Phase1-probe: beta''=" + betaPrimePrime + (bppFresh ? "(fresh)" : "(IN-MASK)")
        + " beta'''=" + betaTriple + (btFresh ? "(fresh)" : "(IN-MASK)")
        + " mask=" + Arrays.toString(dStarDiscBits)
        + " // CarveProbe: densePK_K=0x" + Integer.toHexString(densePkK)
        + " affectedStored=0x" + Integer.toHexString(affectedStored)
        + " prevStored=" + (prevStored < 0 ? "<none>" : "0x" + Integer.toHexString(prevStored))
        + " class=" + carveClass + " prevOrderOk=" + prevOrderOk);
  }

  /**
   * Most-significant differing bit between two byte arrays (MSB-first absolute index). The
   * routing-encoding-rewrite candidate bit for closing a MSDB gap at an ancestor's mask is
   * always the MSDB of the trigger key XOR'd with the lex-correct neighbour's first key.
   */
  private static int msdbOfKeyXor(byte[] a, byte[] b) {
    final int len = Math.min(a.length, b.length);
    for (int i = 0; i < len; i++) {
      final int diff = (a[i] ^ b[i]) & 0xFF;
      if (diff != 0) {
        return i * 8 + Integer.numberOfLeadingZeros(diff) - 24;
      }
    }
    return a.length == b.length ? -1 : len * 8;
  }

  /**
   * Plan §4.3 -- Issue B incremental off-path-overflow handler. Called from
   * {@link #mergeIntoLeaf} BEFORE {@link HOTIncrementalInsert#integrate}, when
   * {@link HOTIncrementalInsert#splitLeafPage} produces a {@link HOTIncrementalInsert.BiNode}
   * whose split bit β coincides with an already-existing discriminative bit of L's parent N.
   *
   * <p>The standard {@code addEntry} fold rejects β-already-disc-bit. The incremental fix
   * (when applicable): slot-replace L → L₀ in L's slot (β-column-0 partial unchanged) and
   * add L₁ at {@code comboPartial = L.partial | β-bit} via
   * {@link HOTIncrementalInsert#addChildAtCombination}. β is NOT added as a new disc bit
   * (it was already one); the structure is invariant-clean by Stage 0's off-path-straddle
   * canonicity finding.
   *
   * <p>Falls back ({@code return false}) when β is not in D(N), L's β-column is already 1
   * (not the off-path-straddle case), addChildAtCombination throws C2 collision, or any
   * defensive failure. Caller then proceeds with standard integrate (which will throw and
   * land in the self-heal whole-rebuild).
   *
   * @return {@code true} if the off-path-overflow was handled incrementally
   */
  private boolean handleOffPathOverflow(LeafNavigationResult navResult,
      HOTIncrementalInsert.BiNode biNode, byte[] keySlice, byte[] valueSlice) {
    final int pathDepth = navResult.pathDepth();
    if (pathDepth == 0) {
      return false;                              // L is the root; no parent to fold into
    }
    final HOTIndirectPage parentN = navResult.pathNodes()[pathDepth - 1];
    final int beta = biNode.discriminativeBitIndex();
    final int[] discBits = HOTIncrementalInsert.discriminativeBits(parentN);
    final int betaCol = Arrays.binarySearch(discBits, beta);
    if (betaCol < 0) {
      return false;                              // β fresh to N -- standard integrate handles
    }
    final int slotOfL = navResult.pathChildIndices()[pathDepth - 1];
    final int[] oldPartials = parentN.getPartialKeys();
    if (oldPartials == null || slotOfL >= oldPartials.length) {
      return false;                              // defensive: malformed partial array
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
      newN = HOTIncrementalInsert.addChildAtCombination(parentN, comboPartial,
          biNode.right(), parentN.getHeight(),
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
    registerFreshSubtree(navResult.pathRefs()[pathDepth - 1]);
    OFF_PATH_OVERFLOW_OK.incrementAndGet();
    return true;
  }

  /**
   * The full-N counterpart of {@link #handleOffPathOverflow}'s not-full path. When N (= L's
   * parent) already has {@link HOTIndirectPage#MAX_NODE_ENTRIES} children, the not-full strategy
   * (slot-replace + {@link HOTIncrementalInsert#addChildAtCombination}) cannot fit L₁ — N has no
   * room for a new child. The standard {@link HOTIncrementalInsert#integrate} capacity cascade
   * would then split N at {@code N.MSB} and call {@link HOTIncrementalInsert#addEntry} on the
   * half that holds L's slot — but {@code addEntry} rejects when β ∈ D(half), which holds
   * whenever the half retains β as a discriminative bit (= some half-children have β=0 and some
   * have β=1; the common non-1:31 case).
   *
   * <p>The fix: do the slot-replace + insertion of {@code (comboPartial, L₁)} in N's coordinate
   * space FIRST, then split the resulting (n+1)-wide virtual node at {@code N.MSB} via
   * {@link HOTIncrementalInsert#splitIndirectWithSlotReplaceAndInsertion}. The half containing the
   * modified slot retains β as a disc bit (L₀ has β=0, L₁ has β=1 — varies ⟹ live), so the half
   * is canonical without needing a separate β-fold step.
   *
   * <p>The {@link HOTIncrementalInsert.BiNode} produced is on {@code N.MSB}; we then call
   * {@link HOTIncrementalInsert#integrate} at {@code currentDepth = pathDepth - 1} to splice it
   * where N sat in the spine. When N is the root, that grows the tree by one level (the new root
   * is a 2-entry compound at {@code N.MSB}, height = N.height + 1).
   *
   * @return {@code true} if the N-full off-path-overflow was handled incrementally
   */
  private boolean handleOffPathOverflowFullN(LeafNavigationResult navResult,
      HOTIncrementalInsert.BiNode biNode, int slotOfL, int comboPartial) {
    final int pathDepth = navResult.pathDepth();
    final HOTIndirectPage parentN = navResult.pathNodes()[pathDepth - 1];
    final int revision = storageEngineWriter.getRevisionNumber();
    final HOTIncrementalInsert.BiNode parentSplit;
    try {
      parentSplit = HOTIncrementalInsert.splitIndirectWithSlotReplaceAndInsertion(
          parentN, slotOfL, biNode.left(), comboPartial, biNode.right(),
          revision, pageKeyAllocator);
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

    registerFreshSubtree(result.touchedRef());
    if (Boolean.getBoolean("hot.diag.postHandlerValidate")) {
      final java.util.List<HOTMalformedSubtreeDetector.MalformedSubtree> defects =
          HOTMalformedSubtreeDetector.detect(navResult.pathRefs()[0], this::resolveHOTPageForTraversal);
      final java.util.HashSet<String> seen = new java.util.HashSet<>(4096);
      final java.util.ArrayList<String> duplicates = new java.util.ArrayList<>();
      collectKeysForI1(navResult.pathRefs()[0].getPage(), seen, duplicates);
      System.err.println("[POST-HANDLER-FULL-N] depth=" + pathDepth + " defects=" + defects
          + " duplicates=" + duplicates.size()
          + (duplicates.isEmpty() ? "" : " (first: " + duplicates.get(0) + ")"));
    }
    OFF_PATH_OVERFLOW_OK.incrementAndGet();
    return true;
  }

  /** Diagnostic helper — walk the subtree rooted at {@code page} and collect duplicate stored keys. */
  private void collectKeysForI1(io.sirix.page.interfaces.Page page, java.util.HashSet<String> seen,
      java.util.ArrayList<String> duplicates) {
    if (page instanceof io.sirix.page.HOTLeafPage leaf) {
      final int count = leaf.getEntryCount();
      for (int i = 0; i < count; i++) {
        final String h = java.util.HexFormat.of().formatHex(leaf.getKey(i));
        if (!seen.add(h)) {
          duplicates.add(h);
        }
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final io.sirix.page.PageReference ref = indirect.getChildReference(i);
        if (ref == null) continue;
        final io.sirix.page.interfaces.Page child = resolveHOTPageForTraversal(ref);
        if (child != null) {
          collectKeysForI1(child, seen, duplicates);
        }
      }
    }
  }

  /**
   * The merge outcome of {@link #doIndex}: the key belongs inside the routed leaf/bucket.
   * Merges it in; on bucket overflow defragments and retries once, then splits the leaf page and
   * integrates the resulting {@link HOTIncrementalInsert.BiNode} at the leaf's depth.
   */
  private boolean mergeIntoLeaf(LeafNavigationResult navResult, byte[] keyBuf, int keyLen,
      byte[] valueBuf, int valueLen, byte[] keySlice) {
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
      throw new SirixIOException("HOT leaf page cannot store the entry and cannot split — a "
          + "single value exceeds page capacity. index=" + indexType + ", entries="
          + leaf.getEntryCount() + ", remaining=" + leaf.getRemainingSpace());
    }
    final int revision = storageEngineWriter.getRevisionNumber();
    final byte[] valueSlice =
        valueLen == valueBuf.length ? valueBuf : Arrays.copyOf(valueBuf, valueLen);
    final HOTIncrementalInsert.BiNode biNode = HOTIncrementalInsert.splitLeafPage(
        leaf, keySlice, valueSlice, revision, indexType, pageKeyAllocator);
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
    final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(
        navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
        navResult.pathDepth(), biNode, revision, pageKeyAllocator);
    registerFreshSubtree(result.touchedRef());
    return true;
  }

  /**
   * The branch outcome of {@link #doIndex} — Binna's {@code insertNewValueIntoNode}
   * ({@code HOTSingleThreaded.hpp:413}). HOT's subset-match descent landed the new key in a leaf
   * it does not fully belong to: its mismatch bit {@code beta} is at or above an ancestor's
   * discriminative bit, so the key must branch off as its own subtree.
   *
   * <p>The faithful port computes {@code beta} (the genuine first-differing bit, never an
   * existing discriminative bit of the branch node) and lets {@link HOTIncrementalInsert#getInsertInformation}
   * locate the affected subtree at the insert-depth node {@code d*}; one of three outcomes
   * follows:
   * <ul>
   *   <li><b>leaf pair</b> — the affected subtree is the descended leaf itself: pair it with the
   *       new key's single-entry leaf under a {@code BiNode} on {@code beta} and integrate at the
   *       leaf's depth (Binna's {@code createFromExistingAndNewEntry} + {@code integrateBiNodeIntoTree}).</li>
   *   <li><b>new partition root</b> — the affected subtree is a single boundary <em>node</em>
   *       (the MSB-stack insert depth was one level too shallow — Binna's "false positive"): the
   *       new key joins that child node as a new partition root.</li>
   *   <li><b>add entry</b> — the affected subtree spans several children: the new key's leaf is
   *       folded into {@code d*}'s block beside it ({@link HOTIncrementalInsert#addEntryWithInsertInfo}).</li>
   * </ul>
   * The {@link #tryBranchIncremental} false return -- the I8-unsafe Direction 1 case where
   * sub-inserting K would violate sibling ordering -- still falls back to a scoped
   * {@link #rebuildSubtree} at the insert depth (now non-escalating per plan §12 Stage 3c).
   */
  private boolean branchAboveLeaf(LeafNavigationResult navResult,
      HOTIncrementalInsert.DescentAnalysis analysis, byte[] keySlice, byte[] valueBuf,
      int valueLen) {
    final byte[] valueSlice =
        valueLen == valueBuf.length ? valueBuf : Arrays.copyOf(valueBuf, valueLen);
    if (!tryBranchIncremental(navResult, analysis, keySlice, valueSlice)) {
      // I8-unsafe Direction 1 (the only remaining false return from tryBranchIncremental).
      // Recanonicalize, but scoped to the insert-depth subtree: the key branches inside it,
      // so its ancestors are unaffected -- the rebuild stays bounded and Stage 3c's
      // propagation handles ancestor height/partial refreshes without escalating.
      rebuildSubtree(navResult, analysis.insertDepth(), keySlice, valueSlice);
      return false;     // rebuildSubtree output is canonical — no structural self-heal needed
    }
    return true;        // incremental branch — verify the path structurally
  }

  /**
   * Attempt the incremental branch insert — Binna's {@code insertNewValueIntoNode}. Returns
   * {@code false} (caller recanonicalizes) when the case needs a path not yet ported: {@code beta}
   * colliding with an existing discriminative bit, or a full node that would have to split.
   *
   * @return {@code true} iff the key was inserted incrementally
   */
  private boolean tryBranchIncremental(LeafNavigationResult navResult,
      HOTIncrementalInsert.DescentAnalysis analysis, byte[] keySlice, byte[] valueSlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final PageReference[] pathRefs = navResult.pathRefs();
    final int[] childSlots = navResult.pathChildIndices();
    final int pathDepth = navResult.pathDepth();
    final int beta = analysis.mismatchBit();
    final int betaValue = HOTBulkBuilder.bitAt(keySlice, beta) ? 1 : 0;
    final int revision = storageEngineWriter.getRevisionNumber();

    final int insertDepth = analysis.insertDepth();
    final HOTIndirectPage node = pathNodes[insertDepth];
    final HOTIncrementalInsert.InsertInfo info = HOTIncrementalInsert.getInsertInformation(
        node, analysis.affectedChildIndex(), beta);
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
        return branchFullNodeAtExistingBit(navResult, node, insertDepth, beta, betaValue,
            keySlice, valueSlice);
      }
      final int[] nodeDiscBits = HOTIncrementalInsert.discriminativeBits(node);
      final int betaColumn = Arrays.binarySearch(nodeDiscBits, beta);
      final int comboPartial = info.subtreePrefix()
          | (betaValue == 1 ? 1 << (nodeDiscBits.length - 1 - betaColumn) : 0);
      final HOTLeafPage comboLeaf =
          new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
      if (!comboLeaf.put(keySlice, valueSlice)) {
        throw new SirixIOException(
            "HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
      }
      try {
        final HOTIndirectPage newNode = HOTIncrementalInsert.addChildAtCombination(node,
            comboPartial, swizzle(comboLeaf), node.getHeight(), revision, pageKeyAllocator);
        if (branchAddStrandsExisting(node, newNode, keySlice)) {
          comboLeaf.close();
          return dischargeStrandViaLeafRebuild(navResult, node, newNode, insertDepth, keySlice, valueSlice);
        }
        if (nodeStructurallyMalformed(newNode)) {
          comboLeaf.close();
          BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
          return false;   // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
        }
        pathRefs[insertDepth].setPage(newNode);
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
        if (isDirectionOneI8Safe(navResult, insertDepth, analysis.affectedChildIndex(),
            keySlice)) {
          DIRECTION_ONE_SUBINSERT.incrementAndGet();
          return subInsertAt(node.getChildReference(analysis.affectedChildIndex()), keySlice,
              keySlice.length, valueSlice, valueSlice.length);
        }
        DIRECTION_ONE_FALLBACK.incrementAndGet();
        if (Boolean.getBoolean("hot.diag.directionOneFallback")) {
          dumpDirectionOneFallback("site1", navResult, analysis.affectedChildIndex(),
              analysis.insertDepth(), beta, betaValue, comboPartial, keySlice);
        }
        return false;
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
        final HOTLeafPage pullUpLeaf =
            new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
        if (!pullUpLeaf.put(keySlice, valueSlice)) {
          throw new SirixIOException(
              "HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
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
        final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(
            pathNodes, buildSpineRefs(navResult), childSlots, insertDepth, biNode, revision,
            pageKeyAllocator);
        registerFreshSubtree(result.touchedRef());
        return true;
      }
      return branchSplitFullNode(navResult, info, node, insertDepth, beta, betaValue, keySlice,
          valueSlice);
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
          return branchFullNodeAtExistingBit(navResult, child, insertDepth + 1, beta, betaValue,
              keySlice, valueSlice);
        }
        // Not-full boundary child + betaIsDiscBit — addChildAtCombination on the child (the
        // Q1-verified not-full pattern, applied at depth+1).
        final int childEntryIndex = childSlots[insertDepth + 1];
        final HOTIncrementalInsert.InsertInfo childInfo = HOTIncrementalInsert.getInsertInformation(
            child, childEntryIndex, beta);
        final int comboPartial = childInfo.subtreePrefix()
            | (betaValue == 1 ? 1 << (childDiscBits.length - 1 - betaColAtChild) : 0);
        final HOTLeafPage comboLeaf =
            new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
        if (!comboLeaf.put(keySlice, valueSlice)) {
          throw new SirixIOException(
              "HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
        }
        try {
          final HOTIndirectPage newChild = HOTIncrementalInsert.addChildAtCombination(child,
              comboPartial, swizzle(comboLeaf), child.getHeight(), revision, pageKeyAllocator);
          if (branchAddStrandsExisting(child, newChild, keySlice)) {
            comboLeaf.close();
            return dischargeStrandViaLeafRebuild(navResult, child, newChild, insertDepth + 1, keySlice, valueSlice);
          }
          if (nodeStructurallyMalformed(newChild)) {
            comboLeaf.close();
            BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
            return false;   // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
          }
          pathRefs[insertDepth + 1].setPage(newChild);
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
            DIRECTION_ONE_SUBINSERT.incrementAndGet();
            return subInsertAt(child.getChildReference(childEntryIndex), keySlice,
                keySlice.length, valueSlice, valueSlice.length);
          }
          DIRECTION_ONE_FALLBACK.incrementAndGet();
          if (Boolean.getBoolean("hot.diag.directionOneFallback")) {
            dumpDirectionOneFallback("site3", navResult, childEntryIndex, insertDepth + 1, beta,
                betaValue, comboPartial, keySlice);
          }
          return false;
        }
      }
    }

    // K's fresh single-entry leaf page — its own R(S)-subtree root.
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    if (!keyLeaf.put(keySlice, valueSlice)) {
      throw new SirixIOException(
          "HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
    }
    final PageReference newLeafRef = swizzle(keyLeaf);

    if (singleEntry && leafEntry) {
      // The affected subtree is the descended leaf page itself — pair it with K's leaf under a
      // BiNode on beta and integrate at the leaf's depth. The leaf needs a fresh reference:
      // integrate's materialize cases re-point the leaf's own spine slot, and aliasing it would
      // make a page its own descendant (a cycle).
      if (!canIntegrateBiNodeCleanly(pathNodes, childSlots, pathDepth, beta)) {
        keyLeaf.close();
        return false;
      }
      // Multi-entry-leaf stranding guard ([[hot-multientry-leaf-quirks]] #1): pairing puts the
      // whole descended leaf on beta's (1-betaValue) side. If the leaf straddles beta (holds a
      // key with beta==betaValue), that key would route to K's leaf without migrating -> dup.
      // Fall back to the canonical rebuild instead of the lossy pairing.
      if (navResult.leaf().isBitConstantAtAbsBit(beta) != (1 - betaValue)) {
        keyLeaf.close();
        leafScopedRebuild(navResult, keySlice, valueSlice);   // strandable keys are the descended leaf's
        STRAND_LEAF_REBUILD.incrementAndGet();
        return true;
      }
      final PageReference leafRef = swizzle(navResult.leaf());
      final HOTIncrementalInsert.BiNode biNode = betaValue == 1
          ? new HOTIncrementalInsert.BiNode(beta, 1, leafRef, newLeafRef)
          : new HOTIncrementalInsert.BiNode(beta, 1, newLeafRef, leafRef);
      ensurePathChildrenLoaded(pathNodes);
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(
          pathNodes, buildSpineRefs(navResult), childSlots, pathDepth, biNode, revision,
          pageKeyAllocator);
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
        final HOTIndirectPage newChild = HOTIncrementalInsert.addEntryWithInsertInfo(child, beta,
            betaValue, 0, child.getNumChildren(), 0, newLeafRef, child.getHeight(), revision,
            pageKeyAllocator);
        if (branchAddStrandsExisting(child, newChild, keySlice)) {
          keyLeaf.close();
          return dischargeStrandViaLeafRebuild(navResult, child, newChild, childDepth, keySlice, valueSlice);
        }
        if (nodeStructurallyMalformed(newChild)) {
          keyLeaf.close();
          BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
          return false;   // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
        }
        pathRefs[childDepth].setPage(newChild);
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
      final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(
          pathNodes, buildSpineRefs(navResult), childSlots, childDepth, biNode, revision,
          pageKeyAllocator);
      registerFreshSubtree(result.touchedRef());
      return true;
    }

    // affectedCount > 1 — K's leaf is folded into d*'s block beside the affected subtree. beta
    // becomes a new discriminative bit; the node keeps its height (a leaf child never raises it).
    final HOTIndirectPage newNode = HOTIncrementalInsert.addEntryWithInsertInfo(node, beta,
        betaValue, info.firstAffected(), info.affectedCount(), info.subtreePrefix(), newLeafRef,
        node.getHeight(), revision, pageKeyAllocator);
    if (branchAddStrandsExisting(node, newNode, keySlice)) {
      keyLeaf.close();
      return dischargeStrandViaLeafRebuild(navResult, node, newNode, insertDepth, keySlice, valueSlice);
    }
    if (nodeStructurallyMalformed(newNode)) {
      keyLeaf.close();
      BRANCH_I8_UNSAFE_REBUILD.incrementAndGet();
      return false;   // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
    }
    pathRefs[insertDepth].setPage(newNode);
    registerFreshSubtree(pathRefs[insertDepth]);
    return true;
  }

  /**
   * Branch insert into a <em>full</em> compound node — Binna's {@code insertNewValue} full-node
   * {@code split} ({@code HOTSingleThreaded.hpp:475}). {@code beta} is a genuinely new
   * discriminative bit (the descent reached this node via {@code !betaIsDiscBit}) and the
   * affected subtree spans more than one child but not the whole node, so Binna's {@code split}
   * applies: {@link HOTIncrementalInsert#splitIndirectWithEntry} partitions the node at its own
   * MSB while folding the new key's leaf into the affected half, and the resulting {@code BiNode}
   * on the node's MSB is integrated where the node sat (the integration may cascade further up).
   */
  private boolean branchSplitFullNode(LeafNavigationResult navResult,
      HOTIncrementalInsert.InsertInfo info, HOTIndirectPage node, int insertDepth, int beta,
      int betaValue, byte[] keySlice, byte[] valueSlice) {
    final int revision = storageEngineWriter.getRevisionNumber();
    // splitIndirectWithEntry returns a BiNode on node.MSB; pre-check the integrate cascade for an
    // un-mergeable cross-level overlap and bail to the caller's scoped rebuild if found.
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(),
        insertDepth, node.getMostSignificantBitIndex())) {
      return false;
    }
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    if (!keyLeaf.put(keySlice, valueSlice)) {
      throw new SirixIOException(
          "HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
    }
    ensurePathChildrenLoaded(navResult.pathNodes());
    final HOTIncrementalInsert.BiNode biNode = HOTIncrementalInsert.splitIndirectWithEntry(node,
        info, beta, betaValue, swizzle(keyLeaf), revision, pageKeyAllocator);
    final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(
        navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
        insertDepth, biNode, revision, pageKeyAllocator);
    registerFreshSubtree(result.touchedRef());
    return true;
  }

  /**
   * Branch insert into a <em>full</em> compound node at an <em>existing</em> discriminative bit
   * — Binna's {@code betaIsDiscBit + full d*} case
   * ({@code docs/HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md} §4.1). The case decomposes into
   * already-verified primitives:
   * <ol>
   *   <li>{@link HOTIncrementalInsert#splitIndirect} the full node at its {@code node.MSB} into
   *       a {@code BiNode} of two not-full halves.</li>
   *   <li>K routes (by {@code node.MSB}) into one half.</li>
   *   <li>In that half, dispatch on whether {@code beta} survived {@code compressHalf} (the
   *       crux the prior attempts missed):
   *     <ul>
   *       <li>{@code beta} survived (still a disc bit of the half) →
   *           {@link HOTIncrementalInsert#addChildAtCombination} (still
   *           {@code betaIsDiscBit} for the half — Q1-verified routing-correct).</li>
   *       <li>{@code beta} dropped (constant across the half) → {@code beta} is a genuinely
   *           new disc bit for the half →
   *           {@link HOTIncrementalInsert#addEntryWithInsertInfo} (the existing
   *           multi-affected branch primitive).</li>
   *     </ul>
   *   </li>
   *   <li>{@link HOTIncrementalInsert#integrate} the {@code BiNode} at {@code insertDepth} —
   *       the standard capacity cascade.</li>
   * </ol>
   *
   * <p>{@code BetaIsDiscBitRoutingProbe} Q4 verified 74/74 cases route strictly correctly,
   * including 40-byte MultiMask {@code widespan} keys. The prior 7 decomposition attempts
   * failed by using {@code addChildAtCombination} unconditionally — the β-survival dispatch
   * is mandatory.
   *
   * <p>Out-of-scope corner cases (§6 C1 / C2) fall back to the existing rebuild — not yet
   * probe-verified: C1 (1:31 lone-child half) and C2 ({@code comboPartial} collision = a
   * descent imprecision). On either, this method returns {@code false} and the caller's
   * scoped {@code rebuildSubtree} handles it (no regression vs. the prior {@code return false}).
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
   * <p><b>Crash-safety.</b> The walk is conservative: it never returns {@code true} when integrate
   * would throw. It checks {@link HOTIncrementalInsert#canMergeBiNodeAtExistingDiscBit} at every
   * level whose mask contains the running β. The β evolution exactly matches integrate's
   * full-node cascade (β becomes {@code parent.MSB} after a split). It does not model integrate's
   * intermediate-placement short-circuit (a height comparison) — skipping it can only cause an
   * occasional *unnecessary* rebuild (integrate would have succeeded via intermediate placement),
   * never a missed crash, because integrate never folds at an intermediate level.
   *
   * @param pathNodes    the spine, root-to-leaf
   * @param childSlots   the child slot taken at each spine node
   * @param currentDepth the depth at which the initial BiNode integrates
   * @param biNodeBeta   the initial BiNode's discriminative bit
   * @return {@code true} iff the integrate cascade folds without an un-mergeable overlap
   */
  private boolean canIntegrateBiNodeCleanly(HOTIndirectPage[] pathNodes, int[] childSlots,
      int currentDepth, int biNodeBeta) {
    int beta = biNodeBeta;
    int depth = currentDepth;
    while (depth > 0) {
      final HOTIndirectPage parent = pathNodes[depth - 1];
      if (parent.isDiscriminativeBit(beta)
          && !HOTIncrementalInsert.canMergeBiNodeAtExistingDiscBit(parent, beta,
              childSlots[depth - 1])) {
        return false;
      }
      if (parent.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES) {
        return true;                              // addEntry/merge fits; cascade terminates
      }
      beta = parent.getMostSignificantBitIndex(); // parent full → split → cascade with parent.MSB
      depth--;
    }
    return true;                                  // reached the root
  }

  private boolean branchFullNodeAtExistingBit(LeafNavigationResult navResult,
      HOTIndirectPage node, int insertDepth, int beta, int betaValue, byte[] keySlice,
      byte[] valueSlice) {
    final int revision = storageEngineWriter.getRevisionNumber();
    // splitIndirect produces a BiNode on node.MSB; pre-check the integrate cascade for an
    // un-mergeable cross-level overlap and bail to the caller's scoped rebuild if found.
    if (!canIntegrateBiNodeCleanly(navResult.pathNodes(), navResult.pathChildIndices(),
        insertDepth, node.getMostSignificantBitIndex())) {
      return false;
    }
    final HOTLeafPage keyLeaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
    if (!keyLeaf.put(keySlice, valueSlice)) {
      throw new SirixIOException(
          "HOT: a single index entry does not fit a fresh leaf page. index=" + indexType);
    }
    ensurePathChildrenLoaded(navResult.pathNodes());

    // 1. Split the full node at its own MSB into BiNode(node.MSB, leftHalf, rightHalf).
    final HOTIncrementalInsert.BiNode split = HOTIncrementalInsert.splitIndirect(node, revision,
        pageKeyAllocator);

    // 2. K routes by node.MSB into one half.
    final int nodeMsb = node.getMostSignificantBitIndex();
    final boolean kMsbBit = HOTBulkBuilder.bitAt(keySlice, nodeMsb);
    final PageReference halfRef = kMsbBit ? split.right() : split.left();
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
    final HOTIncrementalInsert.InsertInfo halfInfo = HOTIncrementalInsert.getInsertInformation(
        half, childIdx, beta);
    final PageReference keyLeafRef = swizzle(keyLeaf);
    final HOTIndirectPage foldedHalf;
    try {
      if (betaCol >= 0) {
        // beta survived as a disc bit of the half — still betaIsDiscBit for the half.
        final int comboPartial = halfInfo.subtreePrefix()
            | (betaValue == 1 ? 1 << (halfDiscBits.length - 1 - betaCol) : 0);
        foldedHalf = HOTIncrementalInsert.addChildAtCombination(half, comboPartial, keyLeafRef,
            half.getHeight(), revision, pageKeyAllocator);
      } else {
        // beta was dropped from the half (constant across it) — beta is genuinely new to the
        // half; addEntryWithInsertInfo folds it as a new disc bit.
        foldedHalf = HOTIncrementalInsert.addEntryWithInsertInfo(half, beta, betaValue,
            halfInfo.firstAffected(), halfInfo.affectedCount(), halfInfo.subtreePrefix(),
            keyLeafRef, half.getHeight(), revision, pageKeyAllocator);
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
      return false;   // I8-unsafe combo-add -> canonical rebuildSubtree(insertDepth)
    }
    halfRef.setPage(foldedHalf);

    // 4. Integrate the split BiNode at insertDepth — the standard capacity cascade.
    final HOTIncrementalInsert.IntegrationResult result = HOTIncrementalInsert.integrate(
        navResult.pathNodes(), buildSpineRefs(navResult), navResult.pathChildIndices(),
        insertDepth, split, revision, pageKeyAllocator);
    registerFreshSubtree(result.touchedRef());
    return true;
  }

  /**
   * Leaf-consolidation sweep — the thesis's underflow rule (§3.3.2) applied across the index. The
   * incremental insert over-partitions: a faithful leaf split at the key-set MSDB is uneven and
   * freezes a small half, a branch starts a single-entry leaf, and ascending workloads never
   * re-route to those frozen leaves — so they drift to a fraction of capacity. This post-order
   * walk merges every adjacent BiNode-paired leaf-child pair whose union still fits a page
   * ({@link HOTIncrementalInsert#consolidateNodeLeaves}), packing the leaves back toward full.
   *
   * <p>Copy-on-write: each visited indirect is CoW'd into the transaction-intent log
   * ({@link #prepareIndirectPage}, idempotent), and a node whose leaves were merged is re-pointed
   * and registered. A merge never changes a node's height (a leaf child carries height 0), so
   * ancestors are structurally unaffected.
   *
   * <p>The child pages are swizzled onto their references before {@code consolidateNodeLeaves}
   * runs: a page already flushed to the transaction-intent log has a {@code null} in-memory page
   * on its reference, and the consolidation reads child pages through {@code getPage()}.
   *
   * <p>Every merged-away leaf across the whole sweep is collected and released in one batch — the
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
    final HOTIndirectPage consolidated = HOTIncrementalInsert.consolidateNodeLeaves(cowed,
        CONSOLIDATION_TARGET, storageEngineWriter.getRevisionNumber(), indexType,
        pageKeyAllocator, orphanedLeaves);
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
   * holding every entry it currently contains plus {@code (keySlice, valueSlice)}, and re-point
   * its spine slot. {@link HOTBulkBuilder} produces a compression of {@code R(S)} by construction
   * (Theorem 1), so the result is invariant-clean and routing is exact again — this places a
   * branched (misrouted) key correctly and heals any pre-existing inconsistency in the subtree.
   *
   * <p>Rebuilding the <em>insert-depth</em> subtree rather than the whole index bounds the work
   * and the pages orphaned: the key branches strictly inside {@code pathNodes[depth]}, so its
   * ancestors keep routing to it unchanged. The one ancestor-visible property is height — if the
   * rebuilt subtree is taller than the old node, the ancestors' height accounting is stale, so
   * the rebuild escalates one level shallower (terminating at the root, which has no ancestor).
   *
   * <p>The collected entries are explicitly sorted and de-duplicated before the build: a rebuild
   * recanonicalizes a possibly-corrupt subtree, so it must not assume the trie's traversal order
   * is already a valid (strictly ascending, distinct) {@link HOTBulkBuilder} input.
   */
  private void rebuildSubtree(LeafNavigationResult navResult, int depth, byte[] keySlice,
      byte[] valueSlice) {
    REBUILD_SUBTREE_CALLED.incrementAndGet();
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final int safeDepth = Math.max(0, Math.min(depth, navResult.pathDepth() - 1));
    final HOTIndirectPage subtreeRoot = pathNodes[safeDepth];

    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>();
    collectSubtreeEntries(subtreeRoot, collected);
    collected.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));

    // Collapse duplicate keys (a re-insert over an existing key) by OR-merging their values.
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(collected.size());
    for (final HOTBulkBuilder.Entry entry : collected) {
      final int last = entries.size() - 1;
      if (last >= 0 && Arrays.equals(entries.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = entries.get(last);
        entries.set(last, new HOTBulkBuilder.Entry(prev.key(),
            HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value())));
      } else {
        entries.add(entry);
      }
    }

    final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(
        entries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    final Page rebuilt = built.rootPage();
    final PageReference subtreeRef = navResult.pathRefs()[safeDepth];
    subtreeRef.setPage(rebuilt);
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

  /** Disable hook for the post-dispatch structural self-heal (default ON — correctness first). */
  private static final boolean SELFHEAL_STRUCTURAL =
      !Boolean.getBoolean("hot.selfheal.structural.disable");
  /**
   * Post-dispatch structural self-heals: a structural fold (combo-add / integrate / off-path-
   * overflow) left a node on the insert path malformed (I4/I7/I8) and was discharged by a scoped
   * canonical rebuild. Distinct from {@link #BRANCH_I8_UNSAFE_REBUILD} (the pre-commit combo-add
   * guard) — this is the defense-in-depth backstop that covers the merge/integrate handlers too.
   */
  public static final java.util.concurrent.atomic.AtomicLong STRUCTURAL_SELFHEAL_REBUILD =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Defense-in-depth backstop after a structural change: walk {@code keySlice}'s <em>current</em>
   * descent path from the root and, at the shallowest indirect that is structurally malformed
   * (I4/I7/I8 — {@link #nodeStructurallyMalformed}), discharge by a canonical scoped rebuild of
   * that node's subtree ({@link #rebuildExistingSubtree}). Rebuilding the shallowest violator
   * subsumes any malformed descendant (Binna Lemma 3). A fold can only malform nodes on the
   * inserted key's path, so this O(height × children) walk is necessary and sufficient — and far
   * cheaper than a from-root scan or the corruption-prone whole-index rebuild (Stage 3c).
   */
  private void healStructuralViolationOnPath(byte[] keySlice) {
    PageReference cur = rootReference;
    for (int depth = 0; depth <= MAX_PATH_DEPTH; depth++) {
      if (!(resolveHOTPageForTraversal(cur) instanceof HOTIndirectPage indirect)) {
        return;                                          // reached the leaf — nothing malformed
      }
      if (nodeStructurallyMalformed(indirect)) {
        STRUCTURAL_SELFHEAL_REBUILD.incrementAndGet();
        rebuildExistingSubtree(cur);
        return;
      }
      final int childIndex = indirect.findChildIndex(keySlice);
      if (childIndex < 0) {
        return;                                          // defensive: descent failed
      }
      cur = indirect.getChildReference(childIndex);
      if (cur == null) {
        return;
      }
    }
  }

  /**
   * Canonical scoped rebuild of the <em>existing</em> subtree at {@code ref} from its current
   * entries (no extra key — the inserted key is already present after dispatch). Mirrors
   * {@link #rebuildSubtree} but reads the post-dispatch tree directly and re-points {@code ref}
   * in place, so it can heal whatever a structural fold produced. {@link HOTBulkBuilder} output
   * is invariant-clean by construction (Theorem 1).
   */
  private void rebuildExistingSubtree(PageReference ref) {
    final Page page = resolveHOTPageForTraversal(ref);
    if (!(page instanceof HOTIndirectPage subtreeRoot)) {
      return;                                            // a leaf root has no indirect invariant
    }
    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>();
    collectSubtreeEntries(subtreeRoot, collected);
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> entries = dedupMergeEntries(collected);

    final List<PageReference> staleLeafRefs = new ArrayList<>();
    collectSubtreeLeafRefs(subtreeRoot, staleLeafRefs);

    final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(
        entries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    ref.setPage(built.rootPage());
    registerFreshSubtree(ref);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(staleLeafRefs);
  }

  /**
   * Strand-discharge observability. {@link #STRAND_LEAF_REBUILD} counts strands resolved by the
   * surgical {@code O(one leaf + path)} {@link #leafScopedRebuild} (the on-path single-source-leaf
   * case); {@link #STRAND_FULL_FALLBACK} counts strands that fall back to {@link #rebuildSubtree}
   * at the insert depth (off-path / multi-leaf / BiNode-wrap source — the minimal correct scope
   * when K and the strandable keys occupy different node slots).
   */
  public static final java.util.concurrent.atomic.AtomicLong STRAND_LEAF_REBUILD =
      new java.util.concurrent.atomic.AtomicLong();
  public static final java.util.concurrent.atomic.AtomicLong STRAND_FULL_FALLBACK =
      new java.util.concurrent.atomic.AtomicLong();
  /** Off-path strands discharged by the two-leaf migration ({@link #tryTwoLeafMigration}). */
  public static final java.util.concurrent.atomic.AtomicLong STRAND_TWO_LEAF_MIGRATE =
      new java.util.concurrent.atomic.AtomicLong();
  /**
   * Branch combo-adds discharged by the canonical {@link #rebuildSubtree} because the partial-sorted
   * new slot is structurally malformed ({@link #nodeStructurallyMalformed} — typically the I7≡I8
   * first-key-order divergence under multi-value leaves). Bounded like the Direction-1 fallback; a
   * runaway count signals a workload stressing off-path-bit reordering.
   */
  public static final java.util.concurrent.atomic.AtomicLong BRANCH_I8_UNSAFE_REBUILD =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Surgical strand discharge ({@code O(one leaf + path)}). When a branch-add stranding guard
   * fires and <em>all</em> strandable keys are confined to the descended leaf {@code
   * navResult.leaf()}, rebuild just that leaf together with the new key {@code K} into a canonical
   * mini-HOT ({@link HOTBulkBuilder}) and splice it into the leaf's slot, propagating height/partial
   * up the spine. Returns {@code true} when so handled; {@code false} when the strand is not
   * confined to the descended leaf (multi-leaf or off-path source — the rare case), leaving the
   * caller to fall back to the canonical {@link #rebuildSubtree} at the insert depth.
   *
   * <p>Correctness: K and the strandable keys all route (via {@code node.findChildIndex}) to the
   * descended leaf's slot, so rebuilding {@code leaf ∪ {K}} and re-splicing there preserves routing
   * and re-discriminates them straddle-free (Fact R1). 99%+ of strands (empirically) hit this path.
   */
  private boolean dischargeStrandViaLeafRebuild(LeafNavigationResult navResult,
      HOTIndirectPage oldNode, HOTIndirectPage newNode, int nodeDepth, byte[] keySlice,
      byte[] valueSlice) {
    final int newSlot = newNode.findChildIndex(keySlice);
    if (newSlot < 0 || navResult.pathDepth() < 1) {
      STRAND_FULL_FALLBACK.incrementAndGet();
      return false;
    }
    // (a) On-path: strandable keys confined to the descended leaf -> O(one leaf + path).
    if (strandConfinedToLeaf(oldNode, newNode, newSlot, keySlice, navResult.leaf().getPageKey())) {
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
   * where K descended) and all share {@code densePK == comboPartial} exactly, migrate: build the
   * new child as {@code bulk-build(K ∪ strandable)}, replace {@code L_src} with
   * {@code bulk-build(L_src \ strandable)}, re-encode {@code newNode} with recomputed partials, and
   * — only if the result passes {@link HOTMalformedSubtreeDetector} — splice it at {@code nodeDepth}
   * and propagate up the spine. Returns {@code false} (caller does the canonical full rebuild) when
   * the source is not a single exact-densePK leaf, the rebuilt child overflows, or the candidate is
   * malformed. The detector backstop makes this safe by construction: any I3/I4/I5/I7/I8/I11 defect
   * triggers the fallback, and the end-to-end fuzz validates I1/I6.
   */
  private boolean tryTwoLeafMigration(LeafNavigationResult navResult, HOTIndirectPage newNode,
      int comboSlot, int nodeDepth, byte[] keySlice, byte[] valueSlice) {
    if (nodeDepth < 0 || nodeDepth >= navResult.pathDepth()) {
      return false;                                  // node is not a spliceable path node
    }
    // Identify the unique source slot/leaf and collect the strandable keys; require a single
    // source leaf (so the migration touches exactly one sibling leaf). Strandable keys all have
    // comboPartial ⊆ densePK, so the new child is I5-clean; bulk-build discriminates the rest.
    final List<byte[]> strandKeys = new ArrayList<>();
    final long[] info = {-1L, -1L, 1L};              // {sourceSlot, sourceLeafPageKey, ok}
    for (int i = 0; i < newNode.getNumChildren() && info[2] == 1L; i++) {
      if (i == comboSlot) {
        continue;
      }
      collectMigratableKeys(newNode.getChildReference(i), newNode, comboSlot, keySlice,
          i, strandKeys, info, 0);
    }
    if (info[2] != 1L || strandKeys.isEmpty() || info[0] < 0) {
      return false;                                  // not a single source leaf
    }
    final int sourceSlot = (int) info[0];
    final long sourceLeafPageKey = info[1];
    final int revision = storageEngineWriter.getRevisionNumber();

    // Build the migrated child = bulk-build(K ∪ strandable). All keys have comboPartial ⊆ densePK,
    // so the child is I5-clean under newNode's mask; bulk-build discriminates them internally.
    final List<HOTBulkBuilder.Entry> childEntries = new ArrayList<>(strandKeys.size() + 1);
    childEntries.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    final Page sourceLeafPage = resolveHOTPageForTraversal(newNode.getChildReference(sourceSlot));
    if (!(sourceLeafPage instanceof HOTLeafPage sourceLeaf)
        || sourceLeaf.getPageKey() != sourceLeafPageKey) {
      return false;                                  // source slot is not the single source leaf
    }
    final java.util.HashSet<String> strandSet = new java.util.HashSet<>(strandKeys.size() * 2);
    for (final byte[] k : strandKeys) {
      strandSet.add(java.util.HexFormat.of().formatHex(k));
    }
    final List<HOTBulkBuilder.Entry> remaining = new ArrayList<>(sourceLeaf.getEntryCount());
    for (int i = 0; i < sourceLeaf.getEntryCount(); i++) {
      final byte[] k = sourceLeaf.getKey(i);
      if (strandSet.contains(java.util.HexFormat.of().formatHex(k))) {
        childEntries.add(new HOTBulkBuilder.Entry(k, sourceLeaf.getValue(i)));
      } else {
        remaining.add(new HOTBulkBuilder.Entry(k, sourceLeaf.getValue(i)));
      }
    }
    if (remaining.isEmpty()) {
      return false;                                  // source leaf would empty -> slot removal; rebuild
    }
    childEntries.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> childDeduped = dedupMergeEntries(childEntries);

    try {
      final HOTBulkBuilder.BuildResult childBuilt = HOTBulkBuilder.build(
          childDeduped, revision, indexType, pageKeyAllocator);
      final HOTBulkBuilder.BuildResult srcBuilt = HOTBulkBuilder.build(
          remaining, revision, indexType, pageKeyAllocator);

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
      final int[] partials = newNode.getPartialKeys().clone();
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

  /** Collect strandable keys (route to {@code comboSlot}) under {@code ref}; gate single-source + exact. */
  private void collectMigratableKeys(@Nullable PageReference ref, HOTIndirectPage newNode,
      int comboSlot, byte[] excludeKey, int slot, List<byte[]> out, long[] info,
      int depth) {
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
          info[2] = 0L;                               // strandable keys span >1 slot or >1 leaf
          return;
        }
        info[0] = slot;
        info[1] = leaf.getPageKey();
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren() && info[2] == 1L; i++) {
        collectMigratableKeys(indirect.getChildReference(i), newNode, comboSlot,
            excludeKey, slot, out, info, depth + 1);
      }
    }
  }

  /** OR-merge duplicate keys in a sorted entry list (shared by the scoped/leaf/migration rebuilds). */
  private static List<HOTBulkBuilder.Entry> dedupMergeEntries(List<HOTBulkBuilder.Entry> sorted) {
    final List<HOTBulkBuilder.Entry> out = new ArrayList<>(sorted.size());
    for (final HOTBulkBuilder.Entry entry : sorted) {
      final int last = out.size() - 1;
      if (last >= 0 && Arrays.equals(out.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = out.get(last);
        out.set(last, new HOTBulkBuilder.Entry(prev.key(),
            HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value())));
      } else {
        out.add(entry);
      }
    }
    return out;
  }

  /**
   * Rebuild only {@code navResult.leaf()}'s entries together with {@code (keySlice, valueSlice)}
   * into a canonical mini-HOT and splice it into the leaf's slot of {@code pathNodes[pathDepth-1]},
   * then propagate height/partial changes up the spine. {@code O(leaf entries + path depth)}.
   */
  private void leafScopedRebuild(LeafNavigationResult navResult, byte[] keySlice, byte[] valueSlice) {
    final int pathDepth = navResult.pathDepth();
    final HOTLeafPage oldLeaf = navResult.leaf();

    final List<HOTBulkBuilder.Entry> collected = new ArrayList<>(oldLeaf.getEntryCount() + 1);
    collectSubtreeEntries(oldLeaf, collected);              // preserves tombstone entries
    collected.add(new HOTBulkBuilder.Entry(keySlice, valueSlice));
    collected.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
    final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(collected.size());
    for (final HOTBulkBuilder.Entry entry : collected) {   // OR-merge duplicate keys
      final int last = entries.size() - 1;
      if (last >= 0 && Arrays.equals(entries.get(last).key(), entry.key())) {
        final HOTBulkBuilder.Entry prev = entries.get(last);
        entries.set(last, new HOTBulkBuilder.Entry(prev.key(),
            HOTIncrementalInsert.mergeIndexValues(prev.value(), entry.value())));
      } else {
        entries.add(entry);
      }
    }

    final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(
        entries, storageEngineWriter.getRevisionNumber(), indexType, pageKeyAllocator);
    final Page miniRoot = built.rootPage();

    if (pathDepth == 0) {                                   // the leaf is the whole index root
      navResult.leafRef().setPage(miniRoot);
      registerFreshSubtree(navResult.leafRef());
      return;
    }
    final int leafSlot = navResult.pathChildIndices()[pathDepth - 1];
    final HOTIndirectPage parent = navResult.pathNodes()[pathDepth - 1];
    final PageReference oldLeafRef = navResult.leafRef();
    final PageReference newRef = swizzle(miniRoot);
    parent.setChildReference(leafSlot, newRef);
    registerFreshSubtree(newRef);
    // Reuse the scoped-rebuild spine propagation: treat the leaf level as rebuiltDepth=pathDepth so
    // it refreshes the parent's height + the leaf-slot partial (and recurses on an I7 collision).
    propagateRebuildUpSpine(navResult, pathDepth, keySlice, valueSlice);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(java.util.List.of(oldLeafRef));
  }

  /**
   * Returns {@code true} iff at least one key under {@code oldNode} would strand to {@code newSlot}
   * on {@code newNode} and <em>every</em> such key lives in the leaf with page key {@code
   * leafPageKey}. Used to gate {@link #leafScopedRebuild}.
   */
  private boolean strandConfinedToLeaf(HOTIndirectPage oldNode, HOTIndirectPage newNode, int newSlot,
      byte[] excludeKey, long leafPageKey) {
    final boolean[] state = {false, true}; // {found a strandable key, all so far confined}
    for (int i = 0; i < oldNode.getNumChildren() && state[1]; i++) {
      strandConfinedRec(oldNode.getChildReference(i), newNode, newSlot, excludeKey, leafPageKey,
          state, 0);
    }
    return state[0] && state[1];
  }

  private void strandConfinedRec(@Nullable PageReference ref, HOTIndirectPage newNode, int newSlot,
      byte[] excludeKey, long leafPageKey, boolean[] state, int depth) {
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
            state[1] = false;                              // a strandable key lives elsewhere
            return;
          }
        }
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren() && state[1]; i++) {
        strandConfinedRec(indirect.getChildReference(i), newNode, newSlot, excludeKey, leafPageKey,
            state, depth + 1);
      }
    }
  }

  /**
   * Plan §12 Stage 3c -- propagate a scoped {@link #rebuildSubtree}'s effects up the spine
   * via in-place re-encoding. At each ancestor from {@code rebuiltDepth - 1} down to 0:
   *
   * <ul>
   *   <li>Recompute the ancestor's height as {@code 1 + max(child.height)} -- HOT heights
   *       are max-based ({@link HOTBulkBuilder#assembleIndirect}); a single rebuilt slot's
   *       new height only matters if it's the (possibly tied) maximum.</li>
   *   <li>If the rebuilt subtree's leftmost key changed (only when the rebuilt slot is 0),
   *       recompute the slot's sparse partial via
   *       {@link HOTIndirectPage#computeDensePartialKey}.</li>
   *   <li>Stop early if both are unchanged -- the propagation hit a stable ancestor.</li>
   *   <li>If the new partial would break I7 (must stay strictly between the prev/next
   *       sibling partials), fall back to a scoped {@link #rebuildSubtree} at this
   *       ancestor's depth. The recursive call re-enters this propagation; the cascade is
   *       at most {@code rebuiltDepth} levels.</li>
   *   <li>Otherwise re-encode the ancestor with the same children + disc bits, just an
   *       updated height (and partial for the rebuilt slot if changed). The ancestor's
   *       child references are shared with the prior version; only the rebuilt slot
   *       already points at fresh content via the swizzled {@link PageReference}.</li>
   * </ul>
   *
   * <p>The propagation does not orphan any leaves -- only the originally rebuilt subtree's
   * leaves are released by the caller. Re-encoded ancestors replace their TIL entries at
   * the same {@link PageReference}, dropping the prior in-memory page.
   */
  private void propagateRebuildUpSpine(LeafNavigationResult navResult, int rebuiltDepth,
      byte[] keySlice, byte[] valueSlice) {
    final HOTIndirectPage[] pathNodes = navResult.pathNodes();
    final PageReference[] pathRefs = navResult.pathRefs();
    final int[] childSlots = navResult.pathChildIndices();
    final int revision = storageEngineWriter.getRevisionNumber();

    for (int ancestorDepth = rebuiltDepth - 1; ancestorDepth >= 0; ancestorDepth--) {
      final HOTIndirectPage ancestor = pathNodes[ancestorDepth];
      final int rebuiltSlot = childSlots[ancestorDepth];
      final int numChildren = ancestor.getNumChildren();

      // 1 + max(child.height) -- HOTBulkBuilder.build uses the same formula.
      int maxChildHeight = 0;
      for (int i = 0; i < numChildren; i++) {
        final PageReference childRef = ancestor.getChildReference(i);
        final Page childPage = resolveHOTPageForTraversal(childRef);
        final int h = childPage instanceof HOTIndirectPage hi ? hi.getHeight() : 0;
        if (h > maxChildHeight) {
          maxChildHeight = h;
        }
      }
      final int newAncestorHeight = maxChildHeight + 1;

      // The rebuilt slot's PageReference is the same instance the ancestor holds in its
      // children array, so ancestor.getChildReference(rebuiltSlot) already sees the fresh
      // subtree -- only the encoded partial may need refreshing.
      final byte[] newSlotFirstKey = firstKeyOfSubtree(ancestor.getChildReference(rebuiltSlot));
      final int oldSlotPartial = ancestor.getPartialKey(rebuiltSlot);
      final int newSlotPartial = newSlotFirstKey != null
          ? ancestor.computeDensePartialKey(newSlotFirstKey)
          : oldSlotPartial;

      final boolean heightChanged = newAncestorHeight != ancestor.getHeight();
      final boolean partialChanged = newSlotPartial != oldSlotPartial;

      if (!heightChanged && !partialChanged) {
        return;                                  // Stable -- propagation complete.
      }

      // I7 (partials strictly ascending) safety: a new partial must stay between the prev/next
      // sibling partials. A violation falls back to a scoped rebuild at this ancestor's depth
      // -- still smaller than the original always-cascade-when-height-changes behaviour.
      if (partialChanged) {
        final boolean leftViolated = rebuiltSlot > 0
            && Integer.compareUnsigned(ancestor.getPartialKey(rebuiltSlot - 1),
                newSlotPartial) >= 0;
        final boolean rightViolated = rebuiltSlot + 1 < numChildren
            && Integer.compareUnsigned(newSlotPartial,
                ancestor.getPartialKey(rebuiltSlot + 1)) >= 0;
        if (leftViolated || rightViolated) {
          REBUILD_PROPAGATION_I7_FALLBACK.incrementAndGet();
          rebuildSubtree(navResult, ancestorDepth, keySlice, valueSlice);
          return;
        }
      }

      // Re-encode the ancestor: same disc bits + children, updated height + possibly one
      // partial. assembleIndirect picks the SingleMask/MultiMask layout to match the disc
      // bits exactly as the original encoding -- the new page's mask is identical so routing
      // is invariant-preserving.
      final int[] discBits = HOTIncrementalInsert.discriminativeBits(ancestor);
      final int[] partials = ancestor.getPartialKeys().clone();
      if (partialChanged) {
        partials[rebuiltSlot] = newSlotPartial;
      }
      final PageReference[] children = new PageReference[numChildren];
      for (int i = 0; i < numChildren; i++) {
        children[i] = ancestor.getChildReference(i);
      }
      final HOTIndirectPage rebuiltAncestor = HOTBulkBuilder.assembleIndirect(discBits, partials,
          children, newAncestorHeight, revision, pageKeyAllocator);
      pathRefs[ancestorDepth].setPage(rebuiltAncestor);
      registerFreshSubtree(pathRefs[ancestorDepth]);
      REBUILD_HEIGHT_ESCALATION_AVOIDED.incrementAndGet();
    }
  }

  /**
   * Depth-first gather of every reference pointing at a leaf page in {@code indirect}'s subtree.
   * Used by {@link #rebuildSubtree} to release the off-heap slots of a subtree that a canonical
   * rebuild replaced wholesale. Pages are resolved through the transaction-intent log so the
   * walk sees in-transaction modifications.
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
   * {@code out}. The traversal order follows the trie's child arrays, which equals key order
   * only for a canonical trie — {@link #rebuildSubtree} sorts the result, so this method does
   * not rely on it. Pages are resolved through the transaction-intent log so in-transaction
   * modifications are seen.
   */
  private void collectSubtreeEntries(Page page, List<HOTBulkBuilder.Entry> out) {
    if (page instanceof HOTLeafPage leaf) {
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
        collectSubtreeEntries(child, out);
      }
    }
  }

  /**
   * Build the {@code spineRefs} array {@link HOTIncrementalInsert#integrate} expects: the
   * descent path's compound-node references followed by the leaf's reference
   * ({@code pathDepth + 1} entries).
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
   * instead of {@code null}. Runs once per structural overflow (rare), never on the merge fast
   * path; a child already in memory is left untouched.
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
   * <p>The walk is post-order — {@code TransactionIntentLog.put} nulls a reference's in-memory
   * page, so children are registered before their parent — and stops at shared subtrees: a
   * reference that already carries an on-disk key or a TIL log-key roots an unchanged subtree
   * that {@code integrate} merely re-used by reference.
   */
  private void registerFreshSubtree(PageReference touchedRef) {
    selfHealScope = touchedRef;   // root of the just-spliced subtree — scope for the self-heal
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
        registerFreshPage(indirect.getChildReference(i), false);
      }
    } else if (page instanceof HOTLeafPage freshLeaf) {
      // A freshly created leaf has no on-disk predecessor — mark it a complete dump so commit
      // emits it as a full first fragment and later readers never chase a fragment chain.
      freshLeaf.setCompleteDump(true);
    }
    // Register a PageContainer so the page is persisted: a fresh page is its own complete and
    // modified view; an indirect carries no version chain, and a fresh leaf is full-emitted at
    // commit because its completePageRef is null (see PageKind.HOT_LEAF_PAGE.serializePage).
    storageEngineWriter.getLog().put(ref, PageContainer.getInstance(page, page));
  }

  /**
   * The least significant (largest absolute index) discriminative bit of a compound node — the
   * deepest bit it branches on. Computed allocation-free: {@code discriminativeBits} returns the
   * bits sorted ascending by absolute position, so the maximum is the highest extraction byte's
   * lowest-order on-path bit (MULTI_MASK) or {@code initialBytePos*8 + (63 - ntz(bitMask))}
   * (single-mask). This is on the per-insert merge-vs-branch decision path, so it must not
   * allocate the {@code int[]} that {@link HOTIncrementalInsert#discriminativeBits} would.
   */
  private static int leastSignificantDiscBit(HOTIndirectPage node) {
    if (node.getLayoutType() == HOTIndirectPage.LayoutType.MULTI_MASK) {
      final int last = node.getNumExtractionBytes() - 1;     // highest key-byte position
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
   * <p>Results are stored in {@link #lastSerializedValueBuf} and {@link #lastSerializedValueLen}
   * to avoid the {@code Object[]} allocation and {@code int} boxing of the old return-value API.
   * This is safe because {@code AbstractHOTIndexWriter} is single-threaded per transaction.</p>
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
   * Phase 7d — Populate the leaf's ancestor-owned bits from the path's ancestor disc bits.
   * For each absolute bit position β captured by some ancestor's mask, query the leaf's
   * β-constancy: if all existing keys agree, record β with the constant value. Mixed bits
   * are NOT recorded (= leaf is already β-mixed and no further constraint can be added).
   *
   * <p>Called BEFORE strict merge so {@code mergeWithNodeRefsStrict} has the metadata to
   * detect β-breaks. Idempotent: replaces any previous owned bits.
   *
   * <p>HFT-grade: at most O(pathDepth * pathMaskBits * leafEntries) per call. Single
   * allocation per call (the owned-bits and values arrays).
   */
  protected void populateLeafOwnedBitsFromPath(io.sirix.page.HOTLeafPage leaf,
      io.sirix.page.HOTIndirectPage[] pathNodes, int pathDepth) {
    if (leaf == null || pathDepth <= 0) {
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
      if (v < 0) continue; // β-mixed — cannot constrain
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

