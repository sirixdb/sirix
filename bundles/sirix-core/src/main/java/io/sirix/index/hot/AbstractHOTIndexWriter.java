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
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.checkerframework.checker.nullness.qual.Nullable;

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
  // These are overwritten on every getLeafWithPath() call; LeafNavigationResult stores
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
      default -> null;
    };
  }

  /**
   * Mark the index page as dirty so changes are persisted.
   */
  protected void markIndexPageDirty() {
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
  protected LeafNavigationResult getLeafWithPath(PageReference rootRef, byte[] keyBuf, int keyLen) {
    if (rootRef == null) {
      throw new IllegalStateException("HOT index not initialized");
    }

    // Reset path depth counter — no allocation
    int pathDepth = 0;
    PageReference currentRef = rootRef;
    final byte[] keySlice = keyLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, keyLen);
    Page page = resolveHOTPageForTraversal(currentRef);

    // Navigate through indirect pages, tracking the path into pre-allocated arrays.
    while (page instanceof HOTIndirectPage indirectPage) {
      if (pathDepth >= MAX_PATH_DEPTH) {
        throw new IllegalStateException("HOT tree depth exceeds MAX_PATH_DEPTH=" + MAX_PATH_DEPTH);
      }
      _pathNodes[pathDepth] = indirectPage;
      _pathRefs[pathDepth] = currentRef;

      int childIndex = indirectPage.findChildIndex(keySlice);
      if (childIndex < 0) {
        childIndex = 0; // Default to first child
      }
      _pathChildIndices[pathDepth] = childIndex;
      pathDepth++;

      final PageReference childRef = indirectPage.getChildReference(childIndex);
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

      // COW leaf for write path and persist COW path to keep parent references in sync.
      final HOTLeafPage modifiedLeaf = hotLeaf.copy();
      final PageContainer leafContainer = PageContainer.getInstance(hotLeaf, modifiedLeaf);
      storageEngineWriter.getLog().put(currentRef, leafContainer);

      // If this is not the root leaf, COW all ancestors so child log keys are persisted.
      if (pathDepth > 0) {
        propagateCowPathToRoot(pathDepth, currentRef);
      }

      return buildNavigationResult(modifiedLeaf, currentRef, pathDepth);
    }

    // Empty tree path: create a new leaf at currentRef (root or missing child).
    final HOTLeafPage newLeaf = new HOTLeafPage(currentRef.getKey() >= 0 ? currentRef.getKey() : 0,
        storageEngineWriter.getRevisionNumber(), indexType);
    final PageContainer container = PageContainer.getInstance(newLeaf, newLeaf);
    storageEngineWriter.getLog().put(currentRef, container);

    if (pathDepth > 0) {
      propagateCowPathToRoot(pathDepth, currentRef);
    }

    return buildNavigationResult(newLeaf, currentRef, pathDepth);
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
      if (leaf.canSplit() || leaf.getEntryCount() >= 1) {
        final boolean inserted = trieWriter.handleLeafSplitAndInsert(
            storageEngineWriter, storageEngineWriter.getLog(), leaf, navResult.leafRef(),
            rootRef, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
            navResult.pathDepth(), keyBuf, keyLen, valueBuf, valueLen);

        // CRITICAL: Mark index page dirty so updated root reference gets persisted
        markIndexPageDirty();

        if (inserted) {
          return true;
        }
      }

      // If neither compact nor split helped, re-navigate for fresh state
      if (attempt < MAX_INSERT_RETRIES - 1) {
        navResult = getLeafWithPath(rootRef, keyBuf, keyLen);
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
    // Get fresh reference from the index page to ensure consistency
    PageReference currentRef = getRootReference();
    if (currentRef == null) {
      return null;
    }

    Page page = resolveHOTPageForTraversal(currentRef);
    while (page instanceof HOTIndirectPage indirectPage) {
      int childIndex = indirectPage.findChildIndex(keyBuf);
      if (childIndex < 0) {
        childIndex = 0;
      }

      final PageReference childRef = indirectPage.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      currentRef = childRef;
      page = resolveHOTPageForTraversal(currentRef);
      if (page == null) {
        return null;
      }
    }

    return page instanceof HOTLeafPage hotLeaf ? hotLeaf : null;
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

    LeafNavigationResult navResult = getLeafWithPath(rootRef, keyBuf, keyLen);
    HOTLeafPage leaf = navResult.leaf();

    // Merge entry
    boolean success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);

    // If merge failed, we need to split or compact
    if (!success) {
      success = handleInsertFailure(rootRef, navResult, keyBuf, keyLen, valueBuf, valueLen);

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
    int valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    if (valueLen > valueBuf.length) {
      valueBuf = new byte[valueLen];
      VALUE_BUFFER.set(valueBuf);
      valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    }
    lastSerializedValueBuf = valueBuf;
    lastSerializedValueLen = valueLen;
  }
}

