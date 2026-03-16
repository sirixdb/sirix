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

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.ToStringHelper;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

/**
 * Page to hold references to vector index trees for nearest-neighbor search on embeddings.
 *
 * <p>Follows the same delegation pattern as {@link CASPage}: starts with a
 * {@link ReferencesPage4} delegate and grows to a {@link BitmapReferencesPage}
 * when more than 4 index trees are needed.</p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class VectorPage extends AbstractForwardingPage {

  /**
   * The references page instance (starts as ReferencesPage4, may grow).
   */
  private Page delegate;

  /**
   * Maximum node keys per index number.
   */
  private final Int2LongMap maxNodeKeys;

  /**
   * Current maximum levels of indirect pages in the tree per index number.
   */
  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  /**
   * Default constructor — creates a fresh VectorPage with a ReferencesPage4 delegate.
   */
  public VectorPage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
  }

  /**
   * Constructor for deserialization.
   *
   * @param delegate the deserialized page delegate
   * @param maxNodeKeys the deserialized max node keys map
   * @param currentMaxLevelsOfIndirectPages the deserialized max levels map
   */
  VectorPage(final Page delegate, final Int2LongMap maxNodeKeys,
      final Int2IntMap currentMaxLevelsOfIndirectPages) {
    this.delegate = delegate;
    this.maxNodeKeys = maxNodeKeys;
    this.currentMaxLevelsOfIndirectPages = currentMaxLevelsOfIndirectPages;
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);
    return false;
  }

  /**
   * Get indirect page reference.
   *
   * @param index the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(final int index) {
    return getOrCreateReference(index);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("delegate", delegate).toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize vector index tree.
   *
   * @param databaseType the database type
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createVectorIndexTree(final DatabaseType databaseType,
      final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.VECTOR, storageEngineReader, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
    }
  }

  /**
   * Get the current maximum level of indirect pages for the given index.
   *
   * @param index the index number
   * @return the current maximum level
   */
  public int getCurrentMaxLevelOfIndirectPages(final int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }

  /**
   * Get the size of the currentMaxLevelsOfIndirectPages map for serialization.
   *
   * @return the size
   */
  public int getCurrentMaxLevelOfIndirectPagesSize() {
    return currentMaxLevelsOfIndirectPages.size();
  }

  /**
   * Increment and return the current maximum level of indirect pages for the given index.
   *
   * @param index the index number
   * @return the incremented maximum level
   */
  public int incrementAndGetCurrentMaxLevelOfIndirectPages(final int index) {
    return currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNo the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNo) {
    return maxNodeKeys.get(indexNo);
  }

  /**
   * Get the size of the maxNodeKeys map for serialization.
   *
   * @return the size
   */
  public int getMaxNodeKeySize() {
    return maxNodeKeys.size();
  }

  /**
   * Increment and return the maximum node key for the given index.
   *
   * @param indexNo the index number
   * @return the new maximum node key
   */
  public long incrementAndGetMaxNodeKey(final int indexNo) {
    final long newMaxNodeKey = maxNodeKeys.get(indexNo) + 1;
    maxNodeKeys.put(indexNo, newMaxNodeKey);
    return newMaxNodeKey;
  }
}
