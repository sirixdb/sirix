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
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

import java.util.concurrent.Semaphore;

/**
 * Page to hold references to a path summary.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class PathSummaryPage extends AbstractForwardingPage {

  private final Semaphore lock = new Semaphore(1);

  /**
   * The references page instance.
   */
  private Page delegate;

  /**
   * Maximum node keys.
   */
  private final Int2LongMap maxNodeKeys;

  /**
   * Current maximum levels of indirect pages in the tree.
   */
  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  /**
   * Constructor.
   */
  public PathSummaryPage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
  }

  /**
   * Clone/copy constructor
   */
  public PathSummaryPage(final PathSummaryPage pathSummaryPage) {
    var lock = pathSummaryPage.getLock();

    lock.acquireUninterruptibly();

    try {
      final Page pageDelegate = pathSummaryPage.delegate();

      if (pageDelegate instanceof ReferencesPage4) {
        delegate = new ReferencesPage4((ReferencesPage4) pageDelegate);
      } else if (pageDelegate instanceof BitmapReferencesPage) {
        delegate = new BitmapReferencesPage(pageDelegate, ((BitmapReferencesPage) pageDelegate).getBitmap());
      } else if (pageDelegate instanceof FullReferencesPage) {
        delegate = new FullReferencesPage((FullReferencesPage) pageDelegate);
      }
      this.maxNodeKeys = pathSummaryPage.maxNodeKeys;
      this.currentMaxLevelsOfIndirectPages = pathSummaryPage.currentMaxLevelsOfIndirectPages;
    } finally {
      lock.release();
    }
  }

  /**
   * Constructor to set deserialized values for PathSummaryPage
   *
   * @param delegate                        page
   * @param maxNodeKeys                     Hashmap deserialized
   * @param currentMaxLevelsOfIndirectPages Hashmap deserialized
   */
  PathSummaryPage(final Page delegate, final Int2LongMap maxNodeKeys,
      final Int2IntMap currentMaxLevelsOfIndirectPages) {
    this.delegate = delegate;
    this.maxNodeKeys = maxNodeKeys;
    this.currentMaxLevelsOfIndirectPages = currentMaxLevelsOfIndirectPages;
  }

  public Semaphore getLock() {
    return lock;
  }

  /**
   * Get indirect page reference.
   *
   * @param index the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(int index) {
    return getOrCreateReference(index);
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this).add("mDelegate", delegate).toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize path summary tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx  {@link PageReadOnlyTrx} instance
   * @param index        the index number
   * @param log          the transaction intent log
   */
  public void createPathSummaryTree(final DatabaseType databaseType, final PageReadOnlyTrx pageReadTrx, final int index,
      final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.PATH_SUMMARY, pageReadTrx, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
    }
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.getOrDefault(index, 0);
  }

  /**
   * Get the size of CurrentMaxLevelOfIndirectPage to Serialize
   *
   * @return int Size of CurrentMaxLevelOfIndirectPage
   */
  public int getCurrentMaxLevelOfIndirectPagesSize() {
    return currentMaxLevelsOfIndirectPages.size();
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
  }

  /**
   * Get the size of MaxNodeKey to Serialize
   *
   * @return int Size of MaxNodeKey
   */
  public int getMaxNodeKeySize() {
    return maxNodeKeys.size();
  }

  /**
   * Get the maximum node key of the specified index by its index number. The index number of the
   * PathSummary is 0.
   *
   * @param indexNo the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNo) {
    return maxNodeKeys.get(indexNo);
  }

  public long incrementAndGetMaxNodeKey(final int indexNo) {
    final long newMaxNodeKey = maxNodeKeys.get(indexNo) + 1;
    maxNodeKeys.put(indexNo, newMaxNodeKey);
    return newMaxNodeKey;
  }

}
