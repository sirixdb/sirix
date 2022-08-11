/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.settings;

import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageFragmentKeyImpl;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.PageFragmentKey;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Different versioning algorithms.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public enum VersioningType {

  /**
   * FullDump, just dumping the complete older revision.
   */
  FULL {
    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
        final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx) {
      assert pages.size() == 1 : "Only one version of the page!";
      return pages.get(0);
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() == 1;
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final List<T> returnVal = new ArrayList<>(2);
      returnVal.add(firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx));
      returnVal.add(firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx));

      for (final V record : pages.get(0).values()) {
        returnVal.get(0).setRecord(record);
        returnVal.get(1).setRecord(record);
      }

      return PageContainer.getInstance(returnVal.get(0), returnVal.get(1));
    }

    @Override
    public int[] getRevisionRoots(@NonNegative int previousRevision, @NonNegative int revsToRestore) {
      return new int[] { previousRevision };
    }
  },

  /**
   * Differential versioning. Pages are reconstructed reading the latest full dump as well as the
   * previous version.
   */
  DIFFERENTIAL {
    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
        final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx) {
      assert pages.size() <= 2;
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final T returnVal = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      final T latest = pages.get(0);
      T fullDump = pages.size() == 1 ? pages.get(0) : pages.get(1);

      assert latest.getPageKey() == recordPageKey;
      assert fullDump.getPageKey() == recordPageKey;

      for (final V record : latest.values()) {
        returnVal.setRecord(record);
      }
      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        returnVal.setPageReference(entry.getKey(), entry.getValue());
      }

      // Skip full dump if not needed (fulldump equals latest page).
      if (pages.size() == 2) {
        for (final V record : fullDump.values()) {
          final var recordKey = record.getNodeKey();
          if (returnVal.getValue(null, recordKey) == null) {
            returnVal.setRecord(record);
            if (returnVal.size() == Constants.NDP_NODE_COUNT) {
              break;
            }
          }
        }
        for (final Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (returnVal.getPageReference(entry.getKey()) == null) {
            returnVal.setPageReference(entry.getKey(), entry.getValue());
            if (returnVal.size() == Constants.NDP_NODE_COUNT) {
              break;
            }
          }
        }
      }

      return returnVal;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() <= 2;
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final int revision = pageReadTrx.getUberPage().getRevisionNumber();
      final List<T> returnVal = new ArrayList<>(2);

      reference.setPageFragments(List.of(new PageFragmentKeyImpl(pageReadTrx.getRevisionNumber(), reference.getKey())));

      returnVal.add(firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx));
      returnVal.add(firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx));

      @SuppressWarnings("UnnecessaryLocalVariable") final T latest = firstPage;
      T fullDump = pages.size() == 1 ? firstPage : pages.get(1);
      final boolean isFullDump = revision % revToRestore == 0;

      // Iterate through all nodes of the latest revision.
      for (final V record : latest.values()) {
        returnVal.get(0).setRecord(record);
        returnVal.get(1).setRecord(record);
      }
      // Iterate through all nodes of the latest revision.
      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        returnVal.get(0).setPageReference(entry.getKey(), entry.getValue());
        returnVal.get(1).setPageReference(entry.getKey(), entry.getValue());
      }

      // If not all entries are filled.
      if (latest.size() != Constants.NDP_NODE_COUNT) {
        // Iterate through the full dump.
        for (final V record : fullDump.values()) {
          final var nodeKey = record.getNodeKey();
          if (returnVal.get(0).getValue(null, nodeKey) == null) {
            returnVal.get(0).setRecord(record);
          }

          if (isFullDump && returnVal.get(1).getValue(null, nodeKey) == null) {
            returnVal.get(1).setRecord(record);
          }

          if (returnVal.get(0).size() == Constants.NDP_NODE_COUNT) {
            // Page is filled, thus skip all other entries of the full dump.
            break;
          }
        }
      }
      // If not all entries are filled.
      if (latest.size() != Constants.NDP_NODE_COUNT) {
        // Iterate through the full dump.
        for (final Map.Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (returnVal.get(0).getPageReference(entry.getKey()) == null) {
            returnVal.get(0).setPageReference(entry.getKey(), entry.getValue());
          }

          if (isFullDump && returnVal.get(1).getPageReference(entry.getKey()) == null) {
            returnVal.get(1).setPageReference(entry.getKey(), entry.getValue());
          }

          if (returnVal.get(0).size() == Constants.NDP_NODE_COUNT) {
            // Page is filled, thus skip all other entries of the full dump.
            break;
          }
        }
      }

      final var pageContainer = PageContainer.getInstance(returnVal.get(0), returnVal.get(1));
      log.put(reference, pageContainer);
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(@NonNegative int previousRevision, @NonNegative int revsToRestore) {
      final int revisionsToRestore = previousRevision % revsToRestore;
      final int lastFullDump = previousRevision - revisionsToRestore;
      if (lastFullDump == previousRevision) {
        return new int[] { lastFullDump };
      } else {
        return new int[] { previousRevision, lastFullDump };
      }
    }
  },

  /**
   * Incremental versioning. Each version is reconstructed through taking the last full-dump and all
   * incremental steps since that into account.
   */
  INCREMENTAL {
    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
        final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx) {
      assert pages.size() <= revToRestore;
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final T returnVal =
          firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      boolean filledPage = false;
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledPage) {
          break;
        }
        for (final V record : page.values()) {
          final long recordKey = record.getNodeKey();
          if (returnVal.getValue(null, recordKey) == null) {
            returnVal.setRecord(record);
            if (returnVal.size() == Constants.NDP_NODE_COUNT) {
              filledPage = true;
              break;
            }
          }
        }
        if (!filledPage) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long recordKey = entry.getKey();
            if (returnVal.getPageReference(recordKey) == null) {
              returnVal.setPageReference(recordKey, entry.getValue());
              if (returnVal.size() == Constants.NDP_NODE_COUNT) {
                filledPage = true;
                break;
              }
            }
          }
        }
      }

      return returnVal;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final PageReadOnlyTrx pageReadTrx, final PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final List<T> returnVal = new ArrayList<>(2);
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(pageReadTrx.getRevisionNumber(), reference.getKey()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      reference.setPageFragments(previousPageFragmentKeys);

      returnVal.add(firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx));
      returnVal.add(firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx));
      final boolean isFullDump = pages.size() == revToRestore;

      boolean filledPage = false;
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledPage) {
          break;
        }

        for (final V record : page.values()) {
          // Caching the complete page.
          final long recordKey = record.getNodeKey();
          if (returnVal.get(0).getValue(null, recordKey) == null) {
            returnVal.get(0).setRecord(record);

            if (returnVal.get(1).getValue(null, recordKey) == null && isFullDump) {
              returnVal.get(1).setRecord(record);
            }

            if (returnVal.get(0).size() == Constants.NDP_NODE_COUNT) {
              filledPage = true;
              break;
            }
          }
        }
        if (!filledPage) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            // Caching the complete page.
            final Long key = entry.getKey();
            assert key != null;
            if (returnVal.get(0).getPageReference(key) == null) {
              returnVal.get(0).setPageReference(key, entry.getValue());

              if (returnVal.get(1).getPageReference(entry.getKey()) == null && isFullDump) {
                returnVal.get(1).setPageReference(key, entry.getValue());
              }

              if (returnVal.get(0).size() == Constants.NDP_NODE_COUNT) {
                filledPage = true;
                break;
              }
            }
          }
        }
      }

      final var pageContainer = PageContainer.getInstance(returnVal.get(0), returnVal.get(1));
      log.put(reference, pageContainer);
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(final @NonNegative int previousRevision, final @NonNegative int revsToRestore) {
      final List<Integer> retVal = new ArrayList<>(revsToRestore);
      for (int i = previousRevision, until = previousRevision - revsToRestore; i > until && i >= 0; i--) {
        retVal.add(i);
      }
      assert retVal.size() <= revsToRestore;
      return convertIntegers(retVal);
    }

    // Convert integer list to primitive int-array.
    private int[] convertIntegers(final List<Integer> integers) {
      final int[] retVal = new int[integers.size()];
      final Iterator<Integer> iterator = integers.iterator();
      for (int i = 0; i < retVal.length; i++) {
        retVal[i] = iterator.next();
      }
      return retVal;
    }
  },

  /**
   * Sliding snapshot versioning using a window.
   */
  SLIDING_SNAPSHOT {
    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
        final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx) {
      assert pages.size() <= revToRestore;
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final T returnVal =
          firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      boolean filledPage = false;
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledPage) {
          break;
        }
        for (final V record : page.values()) {
          final long recordKey = record.getNodeKey();
          if (returnVal.getValue(null, recordKey) == null) {
            returnVal.setRecord(record);
            if (returnVal.size() == Constants.NDP_NODE_COUNT) {
              filledPage = true;
              break;
            }
          }
        }
        if (!filledPage) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long recordKey = entry.getKey();
            if (returnVal.getPageReference(recordKey) == null) {
              returnVal.setPageReference(recordKey, entry.getValue());
              if (returnVal.size() == Constants.NDP_NODE_COUNT) {
                filledPage = true;
                break;
              }
            }
          }
        }
      }

      return returnVal;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final PageReadOnlyTrx pageReadTrx, final PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(pageReadTrx.getRevisionNumber(), reference.getKey()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      reference.setPageFragments(previousPageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifyingPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      final T pageWithRecordsInSlidingWindow =
          firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      boolean filledPage = false;
      for (int i = 0; i < pages.size() && !filledPage; i++) {
        final T page = pages.get(i);
        assert page.getPageKey() == recordPageKey;

        final boolean isPageOutOfSlidingWindow = (i == pages.size() - 1 && revToRestore == pages.size());

        for (final V record : page.values()) {
          final long recordKey = record.getNodeKey();
          // Caching the complete page.
          if (!isPageOutOfSlidingWindow) {
            pageWithRecordsInSlidingWindow.setRecord(record);
          }

          if (completePage.getValue(null, recordKey) == null) {
            completePage.setRecord(record);
          }

          if (isPageOutOfSlidingWindow && pageWithRecordsInSlidingWindow.getValue(null, recordKey) == null) {
            modifyingPage.setRecord(record);
          }

          if (completePage.size() == Constants.NDP_NODE_COUNT) {
            filledPage = true;
            break;
          }
        }
        if (!filledPage) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            // Caching the complete page.
            final Long key = entry.getKey();
            assert key != null;

            if (!isPageOutOfSlidingWindow) {
              pageWithRecordsInSlidingWindow.setPageReference(key, entry.getValue());
            }

            if (completePage.getPageReference(key) == null) {
              completePage.setPageReference(key, entry.getValue());
            }

            if (isPageOutOfSlidingWindow && pageWithRecordsInSlidingWindow.getPageReference(key) == null) {
              modifyingPage.setPageReference(key, entry.getValue());
            }

            if (completePage.size() == Constants.NDP_NODE_COUNT) {
              filledPage = true;
              break;
            }
          }
        }
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifyingPage);
      log.put(reference, pageContainer);
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(final @NonNegative int previousRevision, final @NonNegative int revsToRestore) {
      final List<Integer> retVal = new ArrayList<>(revsToRestore);
      for (int i = previousRevision, until = previousRevision - revsToRestore; i > until && i >= 0; i--) {
        retVal.add(i);
      }
      assert retVal.size() <= revsToRestore;
      return convertIntegers(retVal);
    }

    // Convert integer list to primitive int-array.
    private int[] convertIntegers(final List<Integer> integers) {
      final int[] retVal = new int[integers.size()];
      final Iterator<Integer> iterator = integers.iterator();
      for (int i = 0; i < retVal.length; i++) {
        retVal[i] = iterator.next();
      }
      return retVal;
    }
  };

  /**
   * Method to reconstruct a complete {@link KeyValuePage} with the help of partly filled pages plus
   * a revision-delta which determines the necessary steps back.
   *
   * @param pages         the base of the complete {@link KeyValuePage}
   * @param revsToRestore the number of revisions needed to build the complete record page
   * @return the complete {@link KeyValuePage}
   */
  public abstract <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
      final @NonNegative int revsToRestore, final PageReadOnlyTrx pageReadTrx);

  /**
   * Method to reconstruct a complete {@link KeyValuePage} for reading as well as a
   * {@link KeyValuePage} for serializing with the nodes to write.
   *
   * @param pages         the base of the complete {@link KeyValuePage}
   * @param revsToRestore the revisions needed to build the complete record page
   * @return a {@link PageContainer} holding a complete {@link KeyValuePage} for reading and one for
   * writing
   */
  public abstract <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
      final List<T> pages, final @NonNegative int revsToRestore, final PageReadOnlyTrx pageReadTrx,
      final PageReference reference, final TransactionIntentLog log);

  /**
   * Get all revision root page numbers which are needed to restore a {@link KeyValuePage}.
   *
   * @param previousRevision the previous revision
   * @param revsToRestore    number of revisions to restore
   * @return revision root page numbers needed to restore a {@link KeyValuePage}
   */
  public abstract int[] getRevisionRoots(final @NonNegative int previousRevision, final @NonNegative int revsToRestore);
}
