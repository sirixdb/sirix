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

package io.sirix.settings;

import io.sirix.api.PageReadOnlyTrx;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import org.checkerframework.checker.index.qual.NonNegative;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.PageFragmentKey;

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

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      var slots = firstPage.slots();
      var deweyIds = firstPage.deweyIds();

      for (int i = 0; i < firstPage.size(); i++) {
        byte[] slot = slots[i];

        if (slot == null) {
          continue;
        }

        completePage.setSlot(slot, i);
        completePage.setDeweyId(deweyIds[i], i);

        modifiedPage.setSlot(slot, i);
        modifiedPage.setDeweyId(deweyIds[i], i);
      }

      return PageContainer.getInstance(completePage, modifiedPage);
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
      final T pageToReturn = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      final T latest = pages.get(0);
      T fullDump = pages.size() == 1 ? pages.get(0) : pages.get(1);

      assert latest.getPageKey() == recordPageKey;
      assert fullDump.getPageKey() == recordPageKey;

      byte[][] slots = firstPage.slots();
      byte[][] deweyIds = firstPage.deweyIds();
      for (int offset = 0; offset < slots.length; offset++) {
        pageToReturn.setSlot(slots[offset], offset);
        pageToReturn.setDeweyId(deweyIds[offset], offset);
      }
      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        pageToReturn.setPageReference(entry.getKey(), entry.getValue());
      }

      // Skip full dump if not needed (fulldump equals latest page).
      if (pages.size() == 2) {
        slots = firstPage.slots();
        deweyIds = firstPage.deweyIds();
        for (int offset = 0; offset < slots.length; offset++) {
          byte[] recordData = firstPage.getSlot(offset);
          if (recordData == null) {
            continue;
          }
          if (pageToReturn.getSlot(offset) == null) {
            pageToReturn.setSlot(slots[offset], offset);
          }
          final var deweyId = deweyIds[offset];
          if (deweyId != null && pageToReturn.getDeweyId(offset) == null) {
            pageToReturn.setDeweyId(deweyId, offset);
          }
        }

        for (final Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (pageToReturn.getPageReference(entry.getKey()) == null) {
            pageToReturn.setPageReference(entry.getKey(), entry.getValue());
            if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
              break;
            }
          }
        }
      }

      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final @NonNegative int revToRestore, final PageReadOnlyTrx pageReadTrx,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() <= 2;
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final int revision = pageReadTrx.getUberPage().getRevisionNumber();

      reference.setPageFragments(List.of(new PageFragmentKeyImpl(firstPage.getRevision(), reference.getKey())));

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      @SuppressWarnings("UnnecessaryLocalVariable") final T latest = firstPage;
      T fullDump = pages.size() == 1 ? firstPage : pages.get(1);
      final boolean isFullDump = revision % revToRestore == 0;

      // Iterate through all nodes of the latest revision.
      byte[][] slots = firstPage.slots();
      byte[][] deweyIds = firstPage.deweyIds();
      for (int offset = 0; offset < slots.length; offset++) {
        completePage.setSlot(slots[offset], offset);
        completePage.setDeweyId(deweyIds[offset], offset);

        modifiedPage.setSlot(slots[offset], offset);
        modifiedPage.setDeweyId(deweyIds[offset], offset);
      }

      // Iterate through all nodes of the latest revision.
      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        completePage.setPageReference(entry.getKey(), entry.getValue());
        modifiedPage.setPageReference(entry.getKey(), entry.getValue());
      }

      // If not all entries are filled.
      if (latest.size() != Constants.NDP_NODE_COUNT) {
        // Iterate through the full dump.
        slots = firstPage.slots();
        deweyIds = firstPage.deweyIds();
        for (int offset = 0; offset < slots.length; offset++) {
          var recordData = slots[offset];
          if (completePage.getSlot(offset) == null) {
            completePage.setSlot(slots[offset], offset);
          }
          if (isFullDump && modifiedPage.getSlot(offset) == null) {
            modifiedPage.setSlot(recordData, offset);
          }
          var deweyId = deweyIds[offset];
          if (completePage.getDeweyId(offset) == null) {
            completePage.setDeweyId(deweyId, offset);
          }
          if (isFullDump && modifiedPage.getDeweyId(offset) == null) {
            modifiedPage.setDeweyId(deweyId, offset);
          }

          if (completePage.size() == Constants.NDP_NODE_COUNT) {
            // Page is filled, thus skip all other entries of the full dump.
            break;
          }
        }
      }
      // If not all entries are filled.
      if (latest.size() != Constants.NDP_NODE_COUNT) {
        // Iterate through the full dump.
        for (final Map.Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (completePage.getPageReference(entry.getKey()) == null) {
            completePage.setPageReference(entry.getKey(), entry.getValue());
          }

          if (isFullDump && modifiedPage.getPageReference(entry.getKey()) == null) {
            modifiedPage.setPageReference(entry.getKey(), entry.getValue());
          }

          if (completePage.size() == Constants.NDP_NODE_COUNT) {
            // Page is filled, thus skip all other entries of the full dump.
            break;
          }
        }
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
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
      final T pageToReturn = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      boolean filledPage = false;
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledPage) {
          break;
        }

        final byte[][] slots = page.slots();
        final byte[][] deweyIds = page.deweyIds();

        for (int offset = 0; offset < slots.length; offset++) {
          final var recordData = slots[offset];

          if (recordData == null) {
            continue;
          }

          if (pageToReturn.getSlot(offset) == null) {
            pageToReturn.setSlot(recordData, offset);
          }
          final var deweyId = deweyIds[offset];
          if (pageToReturn.getDeweyId(offset) == null) {
            pageToReturn.setDeweyId(deweyId, offset);
          }

          if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
            filledPage = true;
            break;
          }
        }

        if (!filledPage) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long recordKey = entry.getKey();
            if (pageToReturn.getPageReference(recordKey) == null) {
              pageToReturn.setPageReference(recordKey, entry.getValue());
              if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
                filledPage = true;
                break;
              }
            }
          }
        }
      }

      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final PageReadOnlyTrx pageReadTrx, final PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.get(0);
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(firstPage.getRevision(), reference.getKey()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      reference.setPageFragments(previousPageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final boolean isFullDump = pages.size() == revToRestore;

      boolean filledPage = false;
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledPage) {
          break;
        }

        final V[] records = page.records();
        final byte[][] slots = page.slots();
        final byte[][] deweyIds = page.deweyIds();
        for (int offset = 0; offset < records.length; offset++) {
          final var recordData = slots[offset];

          if (recordData == null) {
            continue;
          }

          if (completePage.getSlot(offset) == null) {
            completePage.setSlot(recordData, offset);

            if (modifiedPage.getSlot(offset) == null && isFullDump) {
              modifiedPage.setSlot(recordData, offset);
            }
          }
          final var deweyId = deweyIds[offset];
          // Caching the complete page.
          if (completePage.getDeweyId(offset) == null) {
            completePage.setDeweyId(deweyId, offset);

            if (modifiedPage.getDeweyId(offset) == null && isFullDump) {
              modifiedPage.setDeweyId(deweyId, offset);
            }
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
            if (completePage.getPageReference(key) == null) {
              completePage.setPageReference(key, entry.getValue());

              if (modifiedPage.getPageReference(entry.getKey()) == null && isFullDump) {
                modifiedPage.setPageReference(key, entry.getValue());
              }

              if (completePage.size() == Constants.NDP_NODE_COUNT) {
                filledPage = true;
                break;
              }
            }
          }
        }
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
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
      final T returnVal = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      boolean filledPage = false;
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledPage) {
          break;
        }

        final byte[][] slots = page.slots();
        final byte[][] deweyIds = page.deweyIds();
        for (int offset = 0; offset < slots.length; offset++) {
          final var recordData = slots[offset];

          if (recordData == null) {
            continue;
          }

          if (returnVal.getSlot(offset) == null) {
            returnVal.setSlot(recordData, offset);
          }

          final var deweyId = deweyIds[offset];
          if (returnVal.getDeweyId(offset) == null) {
            returnVal.setDeweyId(deweyId, offset);
          }
        }

        if (returnVal.size() == Constants.NDP_NODE_COUNT) {
          filledPage = true;
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
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(firstPage.getRevision(), reference.getKey()));
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

        final byte[][] slots = page.slots();
        final byte[][] deweyIds = page.deweyIds();
        for (int offset = 0; offset < slots.length; offset++) {
          final var recordData = slots[offset];
          final var deweyId = deweyIds[offset];

          if (recordData == null) {
            continue;
          }

          if (!isPageOutOfSlidingWindow) {
            pageWithRecordsInSlidingWindow.setSlot(recordData, offset);
            pageWithRecordsInSlidingWindow.setDeweyId(deweyId, offset);
          }

          if (completePage.getSlot(offset) == null) {
            completePage.setSlot(recordData, offset);
          }
          if (isPageOutOfSlidingWindow && pageWithRecordsInSlidingWindow.getSlot(offset) == null) {
            modifyingPage.setSlot(recordData, offset);
          }

          if (completePage.getDeweyId(offset) == null) {
            completePage.setDeweyId(deweyId, offset);
          }
          if (isPageOutOfSlidingWindow && pageWithRecordsInSlidingWindow.getDeweyId(offset) == null) {
            modifyingPage.setDeweyId(deweyId, offset);
          }
        }

        if (completePage.size() == Constants.NDP_NODE_COUNT) {
          filledPage = true;
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

  private static <V extends DataRecord, T extends KeyValuePage<V>> void setSlots(T pageToReadFrom,
      T... pagesToSetSlots) {
    final byte[][] slots = pageToReadFrom.slots();
    for (int offset = 0; offset < slots.length; offset++) {
      final var recordData = slots[offset];
      for (T page : pagesToSetSlots) {
        page.setSlot(recordData, offset);
      }
    }
  }

  private static <V extends DataRecord, T extends KeyValuePage<V>> void setDeweyIds(T pageToReadFrom,
      T... pagesToSetDeweyIds) {
    final byte[][] deweyIds = pageToReadFrom.deweyIds();
    for (int offset = 0; offset < deweyIds.length; offset++) {
      final var deweyId = deweyIds[offset];
      for (T page : pagesToSetDeweyIds) {
        page.setDeweyId(deweyId, offset);
      }
    }
  }

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
