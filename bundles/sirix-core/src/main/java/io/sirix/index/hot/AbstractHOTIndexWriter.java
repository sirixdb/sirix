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

import java.util.ArrayList;
import java.util.Arrays;

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

  protected final StorageEngineWriter pageTrx;
  protected final IndexType indexType;
  protected final int indexNumber;

  /** HOT trie writer for handling page splits. */
  protected final HOTTrieWriter trieWriter;

  /** Cached root page reference for the index. */
  protected PageReference rootReference;

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
   * @param pageTrx the storage engine writer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  protected AbstractHOTIndexWriter(StorageEngineWriter pageTrx, IndexType indexType, int indexNumber) {
    this.pageTrx = requireNonNull(pageTrx);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
    this.trieWriter = new HOTTrieWriter();
  }

  /**
   * Get the storage engine writer.
   *
   * @return the storage engine writer
   */
  public StorageEngineWriter getPageTrx() {
    return pageTrx;
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
    final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = pageTrx.getCASPage(revisionRootPage);
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = pageTrx.getNamePage(revisionRootPage);
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
  }

  /**
   * Mark the index page as dirty so changes are persisted.
   */
  protected void markIndexPageDirty() {
    final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
    switch (indexType) {
      case PATH -> {
        final PageReference pathPageRef = revisionRootPage.getPathPageReference();
        PageContainer container = pageTrx.getLog().get(pathPageRef);
        if (container == null) {
          PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
          pageTrx.appendLogRecord(pathPageRef, PageContainer.getInstance(pathPage, pathPage));
        }
      }
      case CAS -> {
        final PageReference casPageRef = revisionRootPage.getCASPageReference();
        PageContainer container = pageTrx.getLog().get(casPageRef);
        if (container == null) {
          CASPage casPage = pageTrx.getCASPage(revisionRootPage);
          pageTrx.appendLogRecord(casPageRef, PageContainer.getInstance(casPage, casPage));
        }
      }
      case NAME -> {
        final PageReference namePageRef = revisionRootPage.getNamePageReference();
        PageContainer container = pageTrx.getLog().get(namePageRef);
        if (container == null) {
          NamePage namePage = pageTrx.getNamePage(revisionRootPage);
          pageTrx.appendLogRecord(namePageRef, PageContainer.getInstance(namePage, namePage));
        }
      }
      default -> {
        /* ignore */ }
    }
  }

  /**
   * Navigate to the correct leaf page for a key, tracking the path from root.
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

    // Use dynamic arrays for deep trees
    ArrayList<HOTIndirectPage> pathNodesList = new ArrayList<>();
    ArrayList<PageReference> pathRefsList = new ArrayList<>();
    ArrayList<Integer> pathChildIndicesList = new ArrayList<>();

    PageReference currentRef = rootRef;
    byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);

    // Check transaction log first (uncommitted modifications)
    PageContainer container = pageTrx.getLog().get(currentRef);
    if (container != null) {
      Page page = container.getModified();

      // Navigate through indirect pages, tracking the path
      while (page instanceof HOTIndirectPage indirectPage) {
        pathNodesList.add(indirectPage);
        pathRefsList.add(currentRef);

        int childIndex = indirectPage.findChildIndex(keySlice);
        if (childIndex < 0) {
          childIndex = 0; // Default to first child
        }
        pathChildIndicesList.add(childIndex);

        PageReference childRef = indirectPage.getChildReference(childIndex);
        if (childRef == null) {
          throw new IllegalStateException("Null child reference in indirect page");
        }
        currentRef = childRef;

        // Get child page from log or storage
        PageContainer childContainer = pageTrx.getLog().get(childRef);
        if (childContainer != null) {
          page = childContainer.getModified();
        } else {
          // Child not in log - check if the child reference has a page swizzled on it
          if (childRef.getPage() != null) {
            page = childRef.getPage();
          } else {
            // Load from storage and prepare for modification
            page = pageTrx.loadHOTPage(childRef);
            if (page instanceof HOTLeafPage loadedLeaf) {
              // Create COW copy for modifications and put in log
              HOTLeafPage modifiedLeaf = loadedLeaf.copy();
              childContainer = PageContainer.getInstance(loadedLeaf, modifiedLeaf);
              pageTrx.getLog().put(childRef, childContainer);
              page = modifiedLeaf;
            }
          }
        }
      }

      if (page instanceof HOTLeafPage hotLeaf) {
        // Convert lists to arrays for return
        int pathDepth = pathNodesList.size();
        HOTIndirectPage[] pathNodes = pathNodesList.toArray(new HOTIndirectPage[pathDepth]);
        PageReference[] pathRefs = pathRefsList.toArray(new PageReference[pathDepth]);
        int[] pathChildIndices = pathChildIndicesList.stream().mapToInt(Integer::intValue).toArray();
        return new LeafNavigationResult(hotLeaf, currentRef, pathNodes, pathRefs, pathChildIndices, pathDepth);
      }
    }

    // Use storage engine's versioning-aware page loading for committed data
    HOTLeafPage existingLeaf = pageTrx.getHOTLeafPage(indexType, indexNumber);
    if (existingLeaf != null) {
      // Create COW copy for modifications
      HOTLeafPage modifiedLeaf = existingLeaf.copy();
      container = PageContainer.getInstance(existingLeaf, modifiedLeaf);
      pageTrx.getLog().put(rootRef, container);
      return new LeafNavigationResult(modifiedLeaf, rootRef, new HOTIndirectPage[0], new PageReference[0], new int[0],
          0);
    }

    // Create new leaf page if none exists
    HOTLeafPage newLeaf = new HOTLeafPage(rootRef.getKey() >= 0
        ? rootRef.getKey()
        : 0, pageTrx.getRevisionNumber(), indexType);
    container = PageContainer.getInstance(newLeaf, newLeaf);
    pageTrx.getLog().put(rootRef, container);
    return new LeafNavigationResult(newLeaf, rootRef, new HOTIndirectPage[0], new PageReference[0], new int[0], 0);
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
      // Check if the page can be split
      if (leaf.canSplit()) {
        // Handle split using HOTTrieWriter with proper path information
        byte[] splitKey = trieWriter.handleLeafSplitWithPath(pageTrx, pageTrx.getLog(), leaf, navResult.leafRef(),
            rootRef, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(), navResult.pathDepth());

        // CRITICAL: Mark index page dirty so updated root reference gets persisted
        markIndexPageDirty();

        if (splitKey != null) {
          // Split succeeded - re-navigate and retry insert
          navResult = getLeafWithPath(rootRef, keyBuf, keyLen);
          leaf = navResult.leaf();

          if (leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
            return true;
          }
        }
        // Split returned null (couldn't split) - fall through to compact
      }

      // Try compacting the page to reclaim fragmented space
      int reclaimedSpace = leaf.compact();
      if (reclaimedSpace > 0) {
        // Update the page in the log after compaction
        pageTrx.getLog().put(navResult.leafRef(), PageContainer.getInstance(leaf, leaf));

        // Try inserting again after compaction
        if (leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
          return true;
        }
      }

      // If neither split nor compact helped on this attempt,
      // re-navigate to get fresh state for next attempt
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

    // Check transaction log first (uncommitted modifications)
    PageContainer container = pageTrx.getLog().get(currentRef);
    if (container != null) {
      Page page = container.getComplete();

      // Navigate through indirect pages
      while (page instanceof HOTIndirectPage indirectPage) {
        int childIndex = indirectPage.findChildIndex(keyBuf);
        if (childIndex < 0) {
          childIndex = 0;
        }

        PageReference childRef = indirectPage.getChildReference(childIndex);
        if (childRef == null) {
          return null;
        }
        currentRef = childRef;

        PageContainer childContainer = pageTrx.getLog().get(childRef);
        if (childContainer != null) {
          page = childContainer.getComplete();
        } else if (childRef.getPage() != null) {
          page = childRef.getPage();
        } else {
          page = pageTrx.loadHOTPage(childRef);
        }
      }

      if (page instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
    }

    // Use storage engine's versioning-aware page loading for committed data
    return pageTrx.getHOTLeafPage(indexType, indexNumber);
  }

  /**
   * Initialize the HOT index tree structure for PATH index.
   *
   * @throws SirixIOException if initialization fails
   */
  protected void initializePathIndex() {
    try {
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();

      // CRITICAL: Check if PathPage is already in the transaction log first
      final PageReference pathPageRef = revisionRootPage.getPathPageReference();
      PageContainer pathContainer = pageTrx.getLog().get(pathPageRef);
      final PathPage pathPage;
      if (pathContainer != null && pathContainer.getModified() instanceof PathPage modifiedPath) {
        pathPage = modifiedPath;
      } else {
        pathPage = pageTrx.getPathPage(revisionRootPage);
        pageTrx.appendLogRecord(pathPageRef, PageContainer.getInstance(pathPage, pathPage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = pathPage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        pathPage.createHOTPathIndexTree(pageTrx, indexNumber, pageTrx.getLog());
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
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();

      // CRITICAL: Check if CASPage is already in the transaction log first
      final PageReference casPageRef = revisionRootPage.getCASPageReference();
      PageContainer casContainer = pageTrx.getLog().get(casPageRef);
      final CASPage casPage;
      if (casContainer != null && casContainer.getModified() instanceof CASPage modifiedCAS) {
        casPage = modifiedCAS;
      } else {
        casPage = pageTrx.getCASPage(revisionRootPage);
        pageTrx.appendLogRecord(casPageRef, PageContainer.getInstance(casPage, casPage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = casPage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        casPage.createHOTCASIndexTree(pageTrx, indexNumber, pageTrx.getLog());
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
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();

      // CRITICAL: Check if NamePage is already in the transaction log first
      final PageReference namePageRef = revisionRootPage.getNamePageReference();
      PageContainer nameContainer = pageTrx.getLog().get(namePageRef);
      final NamePage namePage;
      if (nameContainer != null && nameContainer.getModified() instanceof NamePage modifiedName) {
        namePage = modifiedName;
      } else {
        namePage = pageTrx.getNamePage(revisionRootPage);
        pageTrx.appendLogRecord(namePageRef, PageContainer.getInstance(namePage, namePage));
      }

      // Get existing reference first to check if index already exists
      PageReference existingRef = namePage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && (existingRef.getKey() != Constants.NULL_ID_LONG
          || existingRef.getLogKey() != Constants.NULL_ID_INT || existingRef.getPage() != null);

      if (!indexExists) {
        namePage.createHOTNameIndexTree(pageTrx, indexNumber, pageTrx.getLog());
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
   * @param value the value to serialize
   * @return array with [valueBuf, valueLen]
   */
  protected Object[] serializeValue(NodeReferences value) {
    byte[] valueBuf = VALUE_BUFFER.get();
    int valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    if (valueLen > valueBuf.length) {
      valueBuf = new byte[valueLen];
      VALUE_BUFFER.set(valueBuf);
      valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    }
    return new Object[] {valueBuf, valueLen};
  }
}

