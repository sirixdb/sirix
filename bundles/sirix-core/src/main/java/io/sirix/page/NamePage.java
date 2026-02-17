/*
 * Copyright (c) 2023, Sirix Contributors
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

package io.sirix.page;

import com.google.common.base.MoreObjects;
import io.sirix.access.DatabaseType;
import io.sirix.node.NodeKind;
import io.sirix.page.delegates.BitmapReferencesPage;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Cache;
import io.sirix.cache.NamesCacheKey;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.name.Names;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

/**
 * The name page holds all names and their keys for a revision. Furthermore, it has references to
 * name indexes.
 */
public final class NamePage extends AbstractForwardingPage {

  /**
   * Offset of reference to attributes index-tree.
   */
  public static final int ATTRIBUTES_REFERENCE_OFFSET = 0;

  /**
   * Offset of reference to elements index-tree.
   */
  public static final int ELEMENTS_REFERENCE_OFFSET = 1;

  /**
   * Offset of reference to namespace index-tree.
   */
  public static final int NAMESPACE_REFERENCE_OFFSET = 2;

  /**
   * Offset of reference to processing instruction index-tree.
   */
  public static final int PROCESSING_INSTRUCTION_REFERENCE_OFFSET = 3;

  /**
   * Offset of reference to processing instruction index-tree.
   */
  public static final int JSON_OBJECT_KEY_REFERENCE_OFFSET = 0;

  /**
   * Attribute names.
   */
  private Names attributes;

  /**
   * Element names.
   */
  private Names elements;

  /**
   * Namespace URIs.
   */
  private Names namespaces;

  /**
   * Processing instruction names.
   */
  private Names processingInstructions;

  /**
   * JSON Object key names.
   */
  private Names jsonObjectKeys;

  /**
   * The references page delegate instance.
   */
  private Page delegate;

  /**
   * The number of arrays stored.
   */
  private final int numberOfArrays;


  /**
   * Maximum node keys.
   */
  private final Int2LongMap maxNodeKeys;

  /**
   * Current maximum levels of indirect pages in the tree.
   */
  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  /**
   * Create name page.
   */
  public NamePage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    attributes = Names.getInstance(ATTRIBUTES_REFERENCE_OFFSET);
    elements = Names.getInstance(ELEMENTS_REFERENCE_OFFSET);
    namespaces = Names.getInstance(NAMESPACE_REFERENCE_OFFSET);
    processingInstructions = Names.getInstance(PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
    jsonObjectKeys = Names.getInstance(JSON_OBJECT_KEY_REFERENCE_OFFSET);
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
    numberOfArrays = 0;
  }

  /**
   * Constructor when is deserialized data
   *
   * @param delegate The references page delegate instance.
   * @param maxNodeKeys Maximum node keys.
   * @param currentMaxLevelsOfIndirectPages Current maximum levels of indirect pages in the tree.
   * @param numberOfArrays The number of arrays stored.
   */
  NamePage(final Page delegate, final Int2LongMap maxNodeKeys, final Int2IntMap currentMaxLevelsOfIndirectPages,
      final int numberOfArrays) {
    this.delegate = delegate;
    this.maxNodeKeys = maxNodeKeys;
    this.currentMaxLevelsOfIndirectPages = currentMaxLevelsOfIndirectPages;
    this.numberOfArrays = numberOfArrays;
  }

  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key
   */
  public byte[] getRawName(final int key, final NodeKind nodeKind, final StorageEngineReader pageRtx) {
    final byte[] rawName;
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        rawName = elements.getRawName(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        rawName = namespaces.getRawName(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        rawName = attributes.getRawName(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        rawName = processingInstructions.getRawName(key);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        rawName = jsonObjectKeys.getRawName(key);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
    return rawName;
  }

  private Names getNames(StorageEngineReader pageRtx, int offset) {
    final var maxNodeKey = maxNodeKeys.getOrDefault(offset, 0L);
    if (pageRtx.hasTrxIntentLog()) {
      return Names.fromStorage(pageRtx, offset, maxNodeKey);
    }

    final Cache<NamesCacheKey, Names> namesCache = pageRtx.getBufferManager().getNamesCache();
    final NamesCacheKey namesCacheKey =
        new NamesCacheKey(pageRtx.getDatabaseId(), pageRtx.getResourceId(), pageRtx.getRevisionNumber(), offset);
    return namesCache.get(namesCacheKey, (_, _) -> Names.copy(Names.fromStorage(pageRtx, offset, maxNodeKey)));
  }

  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key, or {@code null} if not present
   */
  public String getName(final int key, @NonNull final NodeKind nodeKind, final StorageEngineReader pageRtx) {
    return switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        yield elements.getName(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        yield namespaces.getName(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        yield attributes.getName(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        yield processingInstructions.getName(key);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        yield jsonObjectKeys.getName(key);
      }
      case ARRAY -> "__array__";
      case OBJECT -> "__object__";
      default -> throw new IllegalStateException("No other node types supported!");
    };
  }

  /**
   * Get number of nodes with the given name key.
   *
   * @param key name key identifying name
   * @return number of nodes with the given name key
   */
  public int getCount(final int key, @NonNull final NodeKind nodeKind, final StorageEngineReader pageRtx) {
    return switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        yield elements.getCount(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        yield namespaces.getCount(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        yield attributes.getCount(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        yield processingInstructions.getCount(key);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        yield jsonObjectKeys.getCount(key);
      }
      case ARRAY -> numberOfArrays;
      default -> throw new IllegalStateException("No other node types supported!");
    };
  }

  /**
   * Create name key given a name.
   *
   * @param name name to create key for
   * @param nodeKind kind of node
   * @return the created key
   */
  public int setName(final String name, final NodeKind nodeKind, final StorageEngineWriter pageRtx) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        return elements.setName(name, pageRtx);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        return namespaces.setName(name, pageRtx);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        return attributes.setName(name, pageRtx);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        return processingInstructions.setName(name, pageRtx);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        return jsonObjectKeys.setName(name, pageRtx);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Get the size of CurrentMaxLevelOfIndirectPage to Serialize
   * 
   * @return int Size of CurrentMaxLevelOfIndirectPage
   */
  public int getCurrentMaxLevelOfIndirectPagesSize() {
    return currentMaxLevelsOfIndirectPages.size();
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("elements", elements)
                      .add("attributes", attributes)
                      .add("URIs", namespaces)
                      .add("PIs", processingInstructions)
                      .toString();
  }

  /**
   * Remove an attribute-name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key, final NodeKind nodeKind, final StorageEngineWriter pageRtx) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        elements.removeName(key, pageRtx);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        namespaces.removeName(key, pageRtx);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        attributes.removeName(key, pageRtx);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        processingInstructions.removeName(key, pageRtx);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        jsonObjectKeys.removeName(key, pageRtx);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Initialize name index tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx {@link StorageEngineReader} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createNameIndexTree(final DatabaseType databaseType, final StorageEngineReader pageReadTrx,
      final int index, final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.NAME, pageReadTrx, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
    }
  }

  /**
   * Initialize HOT (Height Optimized Trie) name index tree.
   *
   * <p>
   * Creates a cache-friendly HOT index instead of the traditional RBTree-based index.
   * </p>
   *
   * @param pageReadTrx {@link StorageEngineReader} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createHOTNameIndexTree(final StorageEngineReader pageReadTrx, final int index,
      final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createHOTTree(reference, IndexType.NAME, pageReadTrx, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
    }
  }

  /**
   * Get indirect page reference.
   *
   * @param offset the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(final int offset) {
    return getOrCreateReference(offset);
  }

  public int getNumberOfArrays() {
    return numberOfArrays;
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNumber the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNumber) {
    return maxNodeKeys.get(indexNumber);
  }

  /**
   * Get the size of MaxNodeKey to Serialize
   * 
   * @return int Size of MaxNodeKey
   */
  public int getMaxNodeKeySize() {
    return maxNodeKeys.size();
  }

  public long incrementAndGetMaxNodeKey(final int indexNumber) {
    final long newMaxNodeKey = maxNodeKeys.getOrDefault(indexNumber, 0L) + 1;
    maxNodeKeys.put(indexNumber, newMaxNodeKey);
    return newMaxNodeKey;
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);

    return false;
  }
}
