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

  /**
   * Diagnostic counter — structural-inconsistency self-heal firings (process-wide). On a tree
   * kept canonical by the incremental primitives this catch is unreachable; Stage 3 of
   * {@code docs/HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md} deletes the self-heal once a
   * canary run confirms zero firings.
   */
  public static final java.util.concurrent.atomic.AtomicLong SELF_HEAL_FIRINGS =
      new java.util.concurrent.atomic.AtomicLong();

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

    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
    final byte[] keySlice = keyLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, keyLen);

    // Factored merge-vs-branch dispatch — re-used by {@link #subInsertAt} on a C2 re-descend
    // (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §4.1).
    dispatchInsert(navResult, keyBuf, keyLen, valueBuf, valueLen, keySlice);

    // Periodic leaf consolidation (the thesis's underflow rule). The incremental insert leaves
    // the trie over-partitioned — under-full frozen leaves an insert never re-routes to — so a
    // per-insert trigger cannot reach them; a periodic sweep does. Amortized: one O(index) sweep
    // per CONSOLIDATION_INTERVAL inserts.
    if (navResult.pathDepth() > 0 && ++insertsSinceConsolidation >= CONSOLIDATION_INTERVAL) {
      insertsSinceConsolidation = 0;
      consolidateSubtree(navResult.pathRefs()[0]);
    }
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

    if (merge) {
      mergeIntoLeaf(navResult, keyBuf, keyLen, valueBuf, valueLen, keySlice);
    } else {
      branchAboveLeaf(navResult, analysis, keySlice, valueBuf, valueLen);
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
   * The merge outcome of {@link #doIndex}: the key belongs inside the routed leaf's bucket.
   * Merges it in; on bucket overflow defragments and retries once, then splits the leaf page and
   * integrates the resulting {@link HOTIncrementalInsert.BiNode} at the leaf's depth.
   */
  private void mergeIntoLeaf(LeafNavigationResult navResult, byte[] keyBuf, int keyLen,
      byte[] valueBuf, int valueLen, byte[] keySlice) {
    final HOTLeafPage leaf = navResult.leaf();
    // Fast path: the entry fits the bucket. The leaf is mutated in place — already in the TIL.
    if (leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
      return;
    }
    // The bucket is full. compact() repacks live entries without dropping tombstones (it is
    // versioning-safe, unlike compactTombstones) — retry the merge once if it reclaimed space.
    if (leaf.compact() > 0 && leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
      return;
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
    final HOTIncrementalInsert.IntegrationResult result;
    try {
      result = HOTIncrementalInsert.integrate(navResult.pathNodes(), buildSpineRefs(navResult),
          navResult.pathChildIndices(), navResult.pathDepth(), biNode, revision, pageKeyAllocator);
    } catch (IllegalArgumentException | IllegalStateException structuralInconsistency) {
      // Issue B (docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md §3.2 / §4.3): the leaf split
      // bit β = msdb(L ∪ {K}) coincides with an OFF-path discriminative bit of N (= L's
      // parent). Self-heal via whole-index rebuild for now -- scoping the rebuild to N's
      // subtree (iteration 3 + iteration 5) was attempted and broke
      // oracleVerifiedMultiRevRangeQueries even with a K >= N.firstKey escalation guard.
      // Iteration 6 diagnostic confirmed: 2 firings during the test (pathDepth=1 + 2),
      // both kGteN=true, both throwing "split bit X is already a discriminative bit".
      // Scoped rebuild at pathDepth=1 (= whole-index for that firing) is fine; scoped at
      // pathDepth=2 (deeper) breaks the test even with firstKey preserved. Root cause:
      // unsettled, see plan §11.X for the three candidate hypotheses.
      SELF_HEAL_FIRINGS.incrementAndGet();
      rebuildWholeIndex(navResult, keySlice, valueSlice);
      return;
    }
    registerFreshSubtree(result.touchedRef());
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
   * The cases still needing the not-yet-ported {@code beta}-collision re-route or a full-node
   * split fall back to a canonical {@link #rebuildWholeIndex}; a structural inconsistency thrown
   * by an integration step self-heals the same way.
   */
  private void branchAboveLeaf(LeafNavigationResult navResult,
      HOTIncrementalInsert.DescentAnalysis analysis, byte[] keySlice, byte[] valueBuf,
      int valueLen) {
    final byte[] valueSlice =
        valueLen == valueBuf.length ? valueBuf : Arrays.copyOf(valueBuf, valueLen);
    try {
      if (!tryBranchIncremental(navResult, analysis, keySlice, valueSlice)) {
        // A case not yet ported (a full-node split during a branch insert). Recanonicalize, but
        // scoped to the insert-depth subtree: the key branches inside it, so its ancestors are
        // unaffected — this bounds the rebuild and the pages it orphans.
        rebuildSubtree(navResult, analysis.insertDepth(), keySlice, valueSlice);
      }
    } catch (IllegalArgumentException | IllegalStateException structuralInconsistency) {
      // Self-heal — see the mergeIntoLeaf twin. Stage 3a verification (2026-05-20) measured 103
      // firings here on the HOT test suite: "sparse partial key N is already a child of the
      // node" — addChildAtCombination C2 collision (plan §6 C2: K's comboPartial coincides with
      // an existing child of N; K actually belongs INSIDE that child's subtree, the descent
      // stopped one level too shallow). Cannot be deleted until the C2 re-descend is handled.
      SELF_HEAL_FIRINGS.incrementAndGet();
      rebuildWholeIndex(navResult, keySlice, valueSlice);
    }
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
        pathRefs[childDepth].setPage(newChild);
        registerFreshSubtree(pathRefs[childDepth]);
        return true;
      }
      // The boundary node is full — wrap it whole under a BiNode on beta and integrate. It needs
      // a fresh reference (integrate may re-point the boundary node's own spine slot).
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
  private boolean branchFullNodeAtExistingBit(LeafNavigationResult navResult,
      HOTIndirectPage node, int insertDepth, int beta, int betaValue, byte[] keySlice,
      byte[] valueSlice) {
    final int revision = storageEngineWriter.getRevisionNumber();
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
   * Rebuild the whole index as a canonical HOT holding every existing entry plus
   * {@code (keySlice, valueSlice)}, and install it as the index root — {@link #rebuildSubtree}
   * rooted at the index root.
   */
  private void rebuildWholeIndex(LeafNavigationResult navResult, byte[] keySlice,
      byte[] valueSlice) {
    rebuildSubtree(navResult, 0, keySlice, valueSlice);
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
    final int rebuiltHeight = rebuilt instanceof HOTIndirectPage hip ? hip.getHeight() : 0;
    if (safeDepth > 0 && rebuiltHeight != subtreeRoot.getHeight()) {
      // The rebuilt subtree changed height — its ancestors' height accounting is now stale.
      // Escalate to a shallower rebuild that re-derives those ancestors too.
      rebuildSubtree(navResult, safeDepth - 1, keySlice, valueSlice);
      return;
    }
    final PageReference subtreeRef = navResult.pathRefs()[safeDepth];
    subtreeRef.setPage(rebuilt);
    registerFreshSubtree(subtreeRef);
    // HOTBulkBuilder.build produced an all-new subtree from the collected entries — every leaf
    // page of the replaced subtree is now unreachable; release their off-heap slots in one batch
    // instead of pinning the 64KB segments in the transaction-intent log until commit.
    final List<PageReference> staleLeafRefs = new ArrayList<>();
    collectSubtreeLeafRefs(subtreeRoot, staleLeafRefs);
    storageEngineWriter.getLog().releaseOrphanedHOTLeaves(staleLeafRefs);
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
   * only for a canonical trie — {@link #rebuildWholeIndex} sorts the result, so this method does
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
   * deepest bit it branches on. {@link HOTIncrementalInsert#discriminativeBits} returns a node's
   * disc bits sorted ascending by absolute position, so the last entry is the least significant.
   */
  private static int leastSignificantDiscBit(HOTIndirectPage node) {
    final int[] discBits = HOTIncrementalInsert.discriminativeBits(node);
    return discBits[discBits.length - 1];
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

