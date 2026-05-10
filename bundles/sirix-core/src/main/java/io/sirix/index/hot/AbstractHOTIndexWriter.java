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

import java.util.Arrays;
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

  /** Maximum retry attempts for insert after split/compact. */
  protected static final int MAX_INSERT_RETRIES = 3;

  /**
   * Thread-local buffer for value serialization (4KB default).
   */
  protected static final ThreadLocal<byte[]> VALUE_BUFFER = ThreadLocal.withInitial(() -> new byte[4096]);

  /** Maximum navigable tree depth — pre-allocates path arrays at this depth. */
  private static final int MAX_PATH_DEPTH = 64;

  protected final StorageEngineWriter storageEngineWriter;
  protected final IndexType indexType;
  protected final int indexNumber;

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
    this.trieWriter = new HOTTrieWriter(createPageKeyAllocator(storageEngineWriter, indexType, indexNumber));
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
      if (existingLeafContainer != null && existingLeafContainer.getModified() instanceof HOTLeafPage modifiedLeaf) {
        return buildNavigationResult(modifiedLeaf, currentRef, pathDepth);
      }

      // CoW + fragment-chain bump: under non-FULL versioning the writer commits a sparse fragment
      // at a new disk offset, so the prior on-disk offset must be recorded on the reference's
      // pageFragments before the writer overwrites it. Mirrors KVLP's plumbing in
      // VersioningType.combineRecordPagesForModification. The bump returns true when the chain
      // would have overflowed: in that case the chain is reset and we force a full emit so the
      // soon-to-be-dropped fragment's entries do not become unreachable.
      // Safe under top-down CoW: currentRef is owned by the CoW'd parent's children array.
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
   * Handle insert failure by attempting split and/or compact operations.
   *
   * @param rootRef the root reference
   * @param navResult the navigation result with leaf and path
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @param valueBuf the value buffer
   * @param valueLen the value length
   * @return true if insert eventually succeeded
   */
  protected boolean handleInsertFailure(PageReference rootRef, LeafNavigationResult navResult, byte[] keyBuf,
      int keyLen, byte[] valueBuf, int valueLen) {

    HOTLeafPage leaf = navResult.leaf();

    for (int attempt = 0; attempt < MAX_INSERT_RETRIES; attempt++) {
      // Try compacting first — O(N) memcpy with no allocation, may free
      // enough fragmented space to avoid the more expensive split entirely.
      final int reclaimedSpace = leaf.compact();
      if (reclaimedSpace > 0) {
        storageEngineWriter.getLog().put(navResult.leafRef(), PageContainer.getInstance(leaf, leaf));

        if (leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
          return true;
        }
      }

      // MSDB-aware split+insert: split the leaf by the most significant
      // discriminative bit (including the new key's disc bits with its neighbors),
      // then insert the new key into the correct half atomically.
      //
      // This matches the C++ reference implementation (Binna Listing 3.1,
      // insertNewValue + integrateBiNodeIntoTree), adapted for COW semantics
      // and multi-entry leaf pages. No re-navigation needed — the MSDB
      // guarantees disc-bit routing correctness for all keys.
      if (leaf.canSplit()) {
        final boolean inserted = trieWriter.handleLeafSplitAndInsert(
            storageEngineWriter, storageEngineWriter.getLog(), leaf, navResult.leafRef(),
            rootRef, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
            navResult.pathDepth(), keyBuf, keyLen, valueBuf, valueLen);

        // CRITICAL: Mark index page dirty so updated root reference gets persisted
        prepareIndexPage();

        if (inserted) {
          return true;
        }
      }

      // If neither compact nor split helped, re-navigate for fresh state
      if (attempt < MAX_INSERT_RETRIES - 1) {
        navResult = prepareLeafOfTree(rootRef, keyBuf, keyLen);
        leaf = navResult.leaf();
      }
    }

    return false;
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
      if (childRef == null) return null;
      currentRef = childRef;
      page = resolveHOTPageForTraversal(currentRef);
      if (page == null) return null;
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
      if (modified != null) {
        return modified;
      }
      return container.getComplete();
    }

    final Page swizzled = ref.getPage();
    if (swizzled != null) {
      return swizzled;
    }

    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null;
    }

    return storageEngineWriter.loadHOTPage(ref);
  }

  /**
   * COW all ancestors from leaf parent up to root so updated child references are committed.
   */
  private void propagateCowPathToRoot(final int pathDepth, PageReference modifiedChildRef) {
    PageReference childRef = modifiedChildRef;

    for (int i = pathDepth - 1; i >= 0; i--) {
      final PageReference parentRef = _pathRefs[i];
      final int childIndex = _pathChildIndices[i];
      final PageContainer existingParentContainer = storageEngineWriter.getLog().get(parentRef);

      if (existingParentContainer != null && existingParentContainer.getModified() instanceof HOTIndirectPage parentInLog) {
        parentInLog.setChildReference(childIndex, childRef);
        _pathNodes[i] = parentInLog;
        childRef = parentRef;
        continue;
      }

      final HOTIndirectPage parentNode = _pathNodes[i];
      final HOTIndirectPage updatedParent = parentNode.copyWithUpdatedChild(childIndex, childRef);
      storageEngineWriter.getLog().put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
      _pathNodes[i] = updatedParent;
      childRef = parentRef;
    }
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
   * Perform the common index operation with the given key and value bytes.
   *
   * @param keyBuf the serialized key
   * @param keyLen the key length
   * @param valueBuf the serialized value
   * @param valueLen the value length
   * @throws SirixIOException if the insert fails after all retry attempts
   */
  protected void doIndex(byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {
    // IMPORTANT: Use the cached root reference to ensure consistency
    if (rootReference == null) {
      throw new SirixIOException("HOT index not initialized for " + indexType);
    }
    PageReference rootRef = rootReference;

    LeafNavigationResult navResult = prepareLeafOfTree(rootRef, keyBuf, keyLen);
    HOTLeafPage leaf = navResult.leaf();

    // Option B (Stage G.13) — insert-time re-route via Phase 4 subtree-merge logic.
    // When inserting newKey would break β-constancy at some ancestor A's mask bit β,
    // detect it BEFORE the leaf insert and re-route newKey to A's exact-XOR sibling
    // slot. The existing leaf stays β-constant; newKey gets routed to the right
    // subtree via PEXT descent from the sibling.
    //
    // This is the architectural fix from HOT_FIX_DESIGN_V2.md §3.2 Option B. It avoids
    // the multi-entry-leaf β-mixing pathology by routing the offending key away from
    // the "would-make-it-mixed" leaf, into the existing β-correct subtree.
    // Stage G.18+G.19 — ambiguous-routing detection + reactive split.
    // When PEXT-routing for newKey is ambiguous at ancestor A (= densePK(newKey, A.mask)
    // differs from chosen slot's stored), find the offending bit β and force a leaf
    // split-and-insert on β. The split-and-integrate path then routes the halves
    // correctly via the existing addEntry → Phase3/4 → fallback dispatch.
    if (Boolean.getBoolean("hot.strict.binna") && navResult.pathDepth() > 0
        && Boolean.getBoolean("hot.strict.ambig.split")) {
      final byte[] keyBytesG18 = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
      final int ambigIdx = trieWriter.findAmbiguousAncestor(navResult.pathNodes(),
          navResult.pathChildIndices(), navResult.pathDepth(), keyBytesG18);
      if (ambigIdx >= 0 && leaf.canSplit()) {
        final HOTIndirectPage A = navResult.pathNodes()[ambigIdx];
        final int chosenSlot = navResult.pathChildIndices()[ambigIdx];
        final int offendingBit = trieWriter.findOffendingBitAtAncestor(A, chosenSlot, keyBytesG18);
        if (offendingBit >= 0) {
          final byte[] valueBytes = valueLen == valueBuf.length ? valueBuf : java.util.Arrays.copyOf(valueBuf, valueLen);
          final boolean ok = trieWriter.handleLeafSplitAndInsert(
              storageEngineWriter, storageEngineWriter.getLog(), leaf, navResult.leafRef(),
              rootRef, navResult.pathNodes(), navResult.pathRefs(),
              navResult.pathChildIndices(), navResult.pathDepth(),
              keyBytesG18, keyLen, valueBytes, valueLen, offendingBit);
          prepareIndexPage();
          if (ok) return;
        }
      }
    } else if (Boolean.getBoolean("hot.strict.binna") && navResult.pathDepth() > 0) {
      final byte[] keyBytesG18 = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
      trieWriter.findAmbiguousAncestor(navResult.pathNodes(),
          navResult.pathChildIndices(), navResult.pathDepth(), keyBytesG18);

      final byte[] valueBytesG20 = valueLen == valueBuf.length ? valueBuf : java.util.Arrays.copyOf(valueBuf, valueLen);
      final int[] ancestorBits = trieWriter.collectAncestorDiscBits(
          navResult.pathNodes(), navResult.pathDepth());
      if (ancestorBits.length > 0 && ancestorBits.length <= 6) {
        final byte[][][] buckets = new byte[1 << ancestorBits.length][][];
        trieWriter.recursiveConstancyAwareSplit(leaf, keyBytesG18, valueBytesG20,
            ancestorBits, buckets);
      }

      // Stage G.21 — Wire constancy-aware split. When leaf would become β-mixed at
      // ancestor β, split on that β before inserting. Uses handleLeafSplitAndInsert's
      // explicit-bit overload. Gated on -Dhot.strict.constancy.split=true.
      if (Boolean.getBoolean("hot.strict.constancy.split") && ancestorBits.length > 0
          && leaf.canSplit()) {
        final int mixedBit = trieWriter.findFirstMixedAncestorBit(leaf, keyBytesG18, ancestorBits);
        if (mixedBit >= 0) {
          final boolean ok = trieWriter.handleLeafSplitAndInsert(
              storageEngineWriter, storageEngineWriter.getLog(), leaf, navResult.leafRef(),
              rootRef, navResult.pathNodes(), navResult.pathRefs(),
              navResult.pathChildIndices(), navResult.pathDepth(),
              keyBytesG18, keyLen, valueBytesG20, valueLen, mixedBit);
          prepareIndexPage();
          if (ok) return;
        }
      }
    }

    // Stage G.15 — I8 pre-check + reroute. When newKey would become the new deep-firstKey
    // of the leaf's slot AND that new firstKey would be less than the predecessor sibling's
    // deep-firstKey at some ancestor, route newKey to the predecessor sibling's subtree
    // instead. Force-leaf-split-on-offending-bit then integrates correctly.
    if (Boolean.getBoolean("hot.strict.binna") && navResult.pathDepth() > 0) {
      final byte[] keyBytes = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
      final int i8AncestorIdx = trieWriter.findI8OffendingAncestor(
          navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
          navResult.pathDepth(), leaf, keyBytes);
      if (i8AncestorIdx >= 0) {
        // newKey would break I8 at this ancestor. Force leaf split on the leaf's MSDB-with-K
        // (= the bit where newKey differs from existing leaf keys at most-significant pos).
        // The standard split-and-integrate path then routes the halves correctly.
        if (leaf.canSplit()) {
          final byte[] valueBytes = valueLen == valueBuf.length ? valueBuf : java.util.Arrays.copyOf(valueBuf, valueLen);
          final boolean ok = trieWriter.handleLeafSplitAndInsert(
              storageEngineWriter, storageEngineWriter.getLog(), leaf, navResult.leafRef(),
              rootRef, navResult.pathNodes(), navResult.pathRefs(),
              navResult.pathChildIndices(), navResult.pathDepth(),
              keyBytes, keyLen, valueBytes, valueLen);
          prepareIndexPage();
          if (ok) return;
        }
      }
    }

    // Option B (Stage G.13) — RE-ENABLED with diagnostic to trace cascade source.
    if (Boolean.getBoolean("hot.strict.binna") && navResult.pathDepth() > 0) {
      final byte[] keyBytes = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
      final int offendingBeta = trieWriter.findAnyOffendingAncestorBit(
          navResult.pathNodes(), navResult.pathDepth(), leaf, keyBytes);
      if (offendingBeta >= 0) {
        final byte[] valueBytes = valueLen == valueBuf.length ? valueBuf : java.util.Arrays.copyOf(valueBuf, valueLen);
        final boolean rerouted = trieWriter.tryReRouteOffendingKey(
            navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
            navResult.pathDepth(), offendingBeta, keyBytes, valueBytes,
            storageEngineWriter, storageEngineWriter.getLog());
        prepareIndexPage();
        if (rerouted) return;
      }
    }

    // Phase 2 (constancy-aware leaf split) — DISABLED 2026-05-09. Three attempts:
    //
    // Attempt 1 (eager, any non-constant ancestor β): 1 → 635 violations, height 6 → 12.
    // Attempt 2 (constrained: β must equal leaf MSDB-with-K): 1 → 645 violations.
    // Attempt 3 (Stage G.7 retry, with G.1/G.1b/G.3/G.5/G.6 in place): 1 → 517 violations
    // (512 I6, 2 I-Binna-sparse-path, 2 I5, 1 I8). The G-fixes' I4 self-checks didn't
    // prevent the cascade because the cascade source is leaf-level β-mixing AT INDIRECT
    // 26's child[0] slot — partial 0x0 ("don't care") matches keys with bit 0x8000=1
    // even though child[0]'s subtree was originally β=0-only (Stage G empirical trace).
    //
    // The fundamental issue: under sparse-path encoding, partial 0x0 means BOTH
    // "leftmost on every BiNode" AND "subtree has all bit-0 paths". Multi-entry-leaf
    // β-mixing breaks the second claim AT INSERT TIME, but routing soundness only
    // verifies the first. Phase 2 split-on-β fixes ONE leaf but the broken indirect
    // partial structure cascades through ancestor lookups.
    //
    // Required fix: split-on-β + REROUTE the misfit half to a new sibling slot at the
    // appropriate ancestor level (= Phase 4 subtree-merge for the leaf-insert case).
    // Phase 4 currently only fires for addEntryWithPDep-rejection cases, not for
    // pre-emptive leaf-level splits. Multi-week integration item.
    //
    // Helpers in place for future integration: {@link
    // HOTTrieWriter#findOffendingAncestorBit}, {@link HOTLeafPage#computeMsdbWithKey},
    // {@link HOTTrieWriter#handleLeafSplitAndInsert} explicit-split-bit overload.

    // Option B Phase 5 — constancy-aware insert. Gated on -Dhot.strict.option-b-phase-5=true.
    // BEFORE mergeWithNodeRefs: if inserting K into leaf would break β-constancy at any
    // ancestor β, split the leaf on β and reroute the wrong half to the ancestor's sibling
    // subtree. After Phase 5 succeeds, K has already been merged into the matching half;
    // caller skips the regular merge.
    boolean success;
    if (Boolean.getBoolean("hot.strict.option-b-phase-5")) {
      final boolean[] didMerge = new boolean[1];
      final boolean phase5Ok = trieWriter.applyConstancyAwareInsert(
          leaf, navResult.leafRef(), rootRef, navResult.pathNodes(), navResult.pathRefs(),
          navResult.pathChildIndices(), navResult.pathDepth(),
          keyBuf, keyLen, valueBuf, valueLen,
          storageEngineWriter, storageEngineWriter.getLog(), didMerge);
      if (phase5Ok && didMerge[0]) {
        prepareIndexPage();
        return;
      }
      if (!phase5Ok) {
        // Phase 5 reported infeasible reroute — fall through to standard merge.
        success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
      } else {
        // Phase 5 succeeded but no β-break detected; proceed with regular merge.
        success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
      }
    } else {
      success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
    }

    // Stage G.28 — post-insert mask-closure verification. Walk path from root, verify
    // each indirect's mask covers MSDB-closure of children's firstKeys. Extend if not.
    // Gated on -Dhot.strict.g28.closure=true.
    if (success && Boolean.getBoolean("hot.strict.g28.closure")) {
      final byte[] keyBytesG28 = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
      trieWriter.ensureMaskClosure(rootRef, keyBytesG28, storageEngineWriter,
          storageEngineWriter.getLog());
      prepareIndexPage();
    }

    // Stage G.32 — I11-safe root mask reconciliation. Gated on -Dhot.strict.g32=true.
    // Empirical finding: the closure-added bits aren't β-constant in children's subtrees
    // (= multi-entry leaves can hold any bit value at any position), so adding them to
    // root's mask makes subtree keys route to wrong leaves (I6 violations cascade). The
    // reconcile is structurally incompatible with multi-entry leaves at root level.
    if (success && Boolean.getBoolean("hot.strict.g32")) {
      trieWriter.reconcileRootMaskI11Safe(rootRef, storageEngineWriter,
          storageEngineWriter.getLog());
      prepareIndexPage();
    }

    // If merge failed, we need to split or compact
    if (!success) {
      success = handleInsertFailure(rootRef, navResult, keyBuf, keyLen, valueBuf, valueLen);
      // After failure-recovery insert, also run closure verification.
      if (success && Boolean.getBoolean("hot.strict.g28.closure")) {
        final byte[] keyBytesG28 = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
        trieWriter.ensureMaskClosure(rootRef, keyBytesG28, storageEngineWriter,
            storageEngineWriter.getLog());
        prepareIndexPage();
      }
      // G.32 reconcile also runs after failure-recovery insert (gated).
      if (success && Boolean.getBoolean("hot.strict.g32")) {
        trieWriter.reconcileRootMaskI11Safe(rootRef, storageEngineWriter,
            storageEngineWriter.getLog());
        prepareIndexPage();
      }

      if (!success) {
        // Last resort: check if this is a case of a single huge value
        int entryCount = navResult.leaf().getEntryCount();
        long remainingSpace = navResult.leaf().getRemainingSpace();
        int requiredSpace = 2 + keyLen + 2 + valueLen;

        throw new SirixIOException("Failed to insert entry after split/compact attempts. "
            + "This may indicate a single value that exceeds page capacity. " + "Index: " + indexType + ", entries: "
            + entryCount + ", remaining space: " + remainingSpace + ", required: " + requiredSpace
            + ". Consider limiting the number of identical keys or using overflow pages.");
      }
    }
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
}

