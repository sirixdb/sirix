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

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.PageFragmentKey;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        final @NonNegative int revToRestore, final StorageEngineReader pageReadTrx) {
      assert pages.size() == 1 : "Only one version of the page!";
      var firstPage = pages.getFirst();
      T completePage =  firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      for (int i = 0; i < firstPage.size(); i++) {
        var slot = firstPage.getSlot(i);

        if (slot == null) {
          continue;
        }

        completePage.setSlot(slot, i);
        completePage.setDeweyId(firstPage.getDeweyId(i), i);
      }

      // Propagate FSST symbol table for string compression
      propagateFsstSymbolTable(firstPage, completePage);

      return completePage;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final @NonNegative int revToRestore, final StorageEngineReader pageReadTrx,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() == 1;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();

      // OPTIMIZATION: Create only ONE page for modifications (not two)
      // FULL versioning stores complete pages, so both complete and modified can be the same
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      // Copy data once (not twice)
      for (int i = 0; i < firstPage.size(); i++) {
        var slot = firstPage.getSlot(i);
        if (slot == null) {
          continue;
        }
        modifiedPage.setSlot(slot, i);
        modifiedPage.setDeweyId(firstPage.getDeweyId(i), i);
      }

      // Propagate FSST symbol table from the original page
      propagateFsstSymbolTable(firstPage, modifiedPage);

      // Same page for both complete and modified:
      // - Writer reads from modifiedPage (sees own writes)
      // - Parallel readers have original from cache (isolation preserved via orphan tracking)
      final var pageContainer = PageContainer.getInstance(modifiedPage, modifiedPage);
      log.put(reference, pageContainer);
      return pageContainer;
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
        final @NonNegative int revToRestore, final StorageEngineReader pageReadTrx) {
      assert pages.size() <= 2;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T pageToReturn = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      final T latest = pages.get(0);
      final T fullDump = pages.size() == 1 ? pages.get(0) : pages.get(1);

      assert latest.getPageKey() == recordPageKey;
      assert fullDump.getPageKey() == recordPageKey;

      // Use bitmap iteration for O(k) instead of O(1024)
      final io.sirix.page.KeyValueLeafPage latestKvp = (io.sirix.page.KeyValueLeafPage) latest;
      final io.sirix.page.KeyValueLeafPage returnKvp = (io.sirix.page.KeyValueLeafPage) pageToReturn;
      
      // Copy all populated slots from latest page
      final int[] latestSlots = latestKvp.populatedSlots();
      for (int i = 0; i < latestSlots.length; i++) {
        final int offset = latestSlots[i];
        pageToReturn.setSlot(firstPage.getSlot(offset), offset);
        var deweyId = firstPage.getDeweyId(offset);
        if (deweyId != null) {
          pageToReturn.setDeweyId(deweyId, offset);
        }
      }
      
      // Copy references from latest
      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        pageToReturn.setPageReference(entry.getKey(), entry.getValue());
      }

      // Fill gaps from full dump if present
      if (pages.size() == 2 && returnKvp.populatedSlotCount() < Constants.NDP_NODE_COUNT) {
        final io.sirix.page.KeyValueLeafPage fullDumpKvp = (io.sirix.page.KeyValueLeafPage) fullDump;
        final long[] filledBitmap = returnKvp.getSlotBitmap();
        
        // Use bitmap iteration for O(k) on fullDump
        final int[] fullDumpSlots = fullDumpKvp.populatedSlots();
        for (int i = 0; i < fullDumpSlots.length; i++) {
          final int offset = fullDumpSlots[i];
          // Check if slot already filled using bitmap (O(1))
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;  // Already filled from latest
          }
          
          var recordData = fullDump.getSlot(offset);
          pageToReturn.setSlot(recordData, offset);
          
          var deweyId = fullDump.getDeweyId(offset);
          if (deweyId != null) {
            pageToReturn.setDeweyId(deweyId, offset);
          }
        }

        // Fill reference gaps
        for (final Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (pageToReturn.getPageReference(entry.getKey()) == null) {
            pageToReturn.setPageReference(entry.getKey(), entry.getValue());
            if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
              break;
            }
          }
        }
      }

      // Propagate FSST symbol table for string compression
      propagateFsstSymbolTable(firstPage, pageToReturn);

      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final @NonNegative int revToRestore, final StorageEngineReader pageReadTrx,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() <= 2;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final int revision = pageReadTrx.getUberPage().getRevisionNumber();

      // Update pageFragments on original reference
      final List<io.sirix.page.interfaces.PageFragmentKey> pageFragmentKeys = List.of(new PageFragmentKeyImpl(
          firstPage.getRevision(), 
          reference.getKey(),
          (int) pageReadTrx.getDatabaseId(),
          (int) pageReadTrx.getResourceId()));
      reference.setPageFragments(pageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);

      // DIAGNOSTIC
      if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
        LOGGER.debug("DIFFERENTIAL combineForMod created: complete=" + System.identityHashCode(completePage) +
            ", modified=" + System.identityHashCode(modifiedPage));
      }

      final T latest = firstPage;
      final T fullDump = pages.size() == 1 ? firstPage : pages.get(1);
      final boolean isFullDumpRevision = revision % revToRestore == 0;

      // Use bitmap iteration for O(k) instead of O(1024)
      final io.sirix.page.KeyValueLeafPage latestKvp = (io.sirix.page.KeyValueLeafPage) latest;
      final io.sirix.page.KeyValueLeafPage completeKvp = (io.sirix.page.KeyValueLeafPage) completePage;
      final io.sirix.page.KeyValueLeafPage modifiedKvp = (io.sirix.page.KeyValueLeafPage) modifiedPage;
      
      // Copy all populated slots from latest to completePage using bitmap iteration
      // For modifiedPage: use lazy copy - mark for preservation, actual copy deferred to commit time
      final int[] latestSlots = latestKvp.populatedSlots();
      for (int i = 0; i < latestSlots.length; i++) {
        final int offset = latestSlots[i];
        var recordData = firstPage.getSlot(offset);
        var deweyId = firstPage.getDeweyId(offset);
        
        completePage.setSlot(recordData, offset);
        if (deweyId != null) {
          completePage.setDeweyId(deweyId, offset);
        }
        
        // LAZY COPY: Mark slot for preservation instead of copying
        // Actual copy from completePage happens in addReferences() if records[offset] == null
        modifiedKvp.markSlotForPreservation(offset);
      }

      // Copy references from latest
      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        completePage.setPageReference(entry.getKey(), entry.getValue());
        modifiedPage.setPageReference(entry.getKey(), entry.getValue());
      }

      // Fill gaps from full dump if not all slots are filled
      if (completeKvp.populatedSlotCount() < Constants.NDP_NODE_COUNT && pages.size() == 2) {
        final io.sirix.page.KeyValueLeafPage fullDumpKvp = (io.sirix.page.KeyValueLeafPage) fullDump;
        final long[] filledBitmap = completeKvp.getSlotBitmap();
        
        // Use bitmap iteration on fullDump
        final int[] fullDumpSlots = fullDumpKvp.populatedSlots();
        for (int j = 0; j < fullDumpSlots.length; j++) {
          final int offset = fullDumpSlots[j];
          // Check if slot already filled using bitmap (O(1))
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;  // Already filled from latest
          }
          
          var recordData = fullDump.getSlot(offset);
          var deweyId = fullDump.getDeweyId(offset);
          
          completePage.setSlot(recordData, offset);
          if (deweyId != null) {
            completePage.setDeweyId(deweyId, offset);
          }
          
          if (isFullDumpRevision) {
            // LAZY COPY: Mark slot for preservation instead of copying
            modifiedKvp.markSlotForPreservation(offset);
          }
        }
        
        // Fill reference gaps from fullDump
        for (final Map.Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (completePage.getPageReference(entry.getKey()) == null) {
            completePage.setPageReference(entry.getKey(), entry.getValue());
          }

          if (isFullDumpRevision && modifiedPage.getPageReference(entry.getKey()) == null) {
            modifiedPage.setPageReference(entry.getKey(), entry.getValue());
          }

          if (completePage.size() == Constants.NDP_NODE_COUNT) {
            break;
          }
        }
      }

      // Set completePage reference for lazy copying at commit time
      modifiedKvp.setCompletePageRef(completeKvp);

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
      log.put(reference, pageContainer);  // TIL will remove from caches before mutating
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
        final @NonNegative int revToRestore, final StorageEngineReader pageReadTrx) {
      assert pages.size() <= revToRestore;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T pageToReturn = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      // Track which slots are already filled using bitmap from pageToReturn
      // This enables O(k) iteration instead of O(1024)
      final io.sirix.page.KeyValueLeafPage returnPage = (io.sirix.page.KeyValueLeafPage) pageToReturn;
      final long[] filledBitmap = returnPage.getSlotBitmap();
      
      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        // Use bitmap iteration for O(k) instead of O(1024)
        final io.sirix.page.KeyValueLeafPage kvPage = (io.sirix.page.KeyValueLeafPage) page;
        final int[] populatedSlots = kvPage.populatedSlots();
        
        for (final int offset : populatedSlots) {
          // Check if slot already filled using bitmap (O(1))
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;  // Already filled from newer fragment
          }
          
          final var recordData = page.getSlot(offset);
          pageToReturn.setSlot(recordData, offset);
          filledSlotCount++;
          
          final var deweyId = page.getDeweyId(offset);
          if (deweyId != null) {
            pageToReturn.setDeweyId(deweyId, offset);
          }

          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        if (filledSlotCount < Constants.NDP_NODE_COUNT) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long key = entry.getKey();
            if (pageToReturn.getPageReference(key) == null) {
              pageToReturn.setPageReference(key, entry.getValue());
              if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
                break;
              }
            }
          }
        }
      }

      // Propagate FSST symbol table for string compression
      propagateFsstSymbolTable(firstPage, pageToReturn);

      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader pageReadTrx, PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(
          firstPage.getRevision(), 
          reference.getKey(),
          (int) pageReadTrx.getDatabaseId(),
          (int) pageReadTrx.getResourceId()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      // Update pageFragments on original reference
      reference.setPageFragments(previousPageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final boolean isFullDump = pages.size() == revToRestore;
      
      // DIAGNOSTIC
      if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
        LOGGER.debug("INCREMENTAL combineForMod created: complete=" + System.identityHashCode(completePage) +
            ", modified=" + System.identityHashCode(modifiedPage));
      }

      // Use bitmap for O(k) iteration instead of O(1024)
      final io.sirix.page.KeyValueLeafPage completeKvp = (io.sirix.page.KeyValueLeafPage) completePage;
      final io.sirix.page.KeyValueLeafPage modifiedKvp = (io.sirix.page.KeyValueLeafPage) modifiedPage;
      final long[] filledBitmap = completeKvp.getSlotBitmap();
      
      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        // Use bitmap iteration for O(k) instead of O(1024)
        final io.sirix.page.KeyValueLeafPage kvPage = (io.sirix.page.KeyValueLeafPage) page;
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          // Check if slot already filled using bitmap (O(1))
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;  // Already filled from newer fragment
          }

          final var recordData = page.getSlot(offset);
          completePage.setSlot(recordData, offset);
          filledSlotCount++;

          if (isFullDump) {
            // LAZY COPY: Mark slot for preservation instead of copying
            // Actual copy from completePage happens in addReferences() if records[offset] == null
            modifiedKvp.markSlotForPreservation(offset);
          }
          
          final var deweyId = page.getDeweyId(offset);
          if (deweyId != null) {
            completePage.setDeweyId(deweyId, offset);
            // DeweyId will be lazily copied along with slot in addReferences()
          }
          
          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        if (filledSlotCount < Constants.NDP_NODE_COUNT) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long key = entry.getKey();
            assert key != null;
            if (completePage.getPageReference(key) == null) {
              completePage.setPageReference(key, entry.getValue());

              if (isFullDump && modifiedPage.getPageReference(key) == null) {
                modifiedPage.setPageReference(key, entry.getValue());
              }

              if (completePage.size() == Constants.NDP_NODE_COUNT) {
                break;
              }
            }
          }
        }
      }

      // Set completePage reference for lazy copying at commit time (only for full-dump)
      if (isFullDump) {
        modifiedKvp.setCompletePageRef(completeKvp);
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
      log.put(reference, pageContainer);  // TIL will remove from caches before mutating
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
        final @NonNegative int revToRestore, final StorageEngineReader pageReadTrx) {
      assert pages.size() <= revToRestore;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T returnVal = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), pageReadTrx);

      // Track which slots are already filled using bitmap from returnVal
      // This enables O(k) iteration instead of O(1024)
      final io.sirix.page.KeyValueLeafPage returnKvp = (io.sirix.page.KeyValueLeafPage) returnVal;
      final long[] filledBitmap = returnKvp.getSlotBitmap();
      
      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      
      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        // Use bitmap iteration for O(k) instead of O(1024)
        final io.sirix.page.KeyValueLeafPage kvPage = (io.sirix.page.KeyValueLeafPage) page;
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          // Check if slot already filled using bitmap (O(1))
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;  // Already filled from newer fragment
          }

          final var recordData = page.getSlot(offset);
          returnVal.setSlot(recordData, offset);
          filledSlotCount++;

          final var deweyId = page.getDeweyId(offset);
          if (deweyId != null) {
            returnVal.setDeweyId(deweyId, offset);
          }
          
          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        if (filledSlotCount < Constants.NDP_NODE_COUNT) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long key = entry.getKey();
            if (returnVal.getPageReference(key) == null) {
              returnVal.setPageReference(key, entry.getValue());
              if (returnVal.size() == Constants.NDP_NODE_COUNT) {
                break;
              }
            }
          }
        }
      }

      // Propagate FSST symbol table for string compression
      propagateFsstSymbolTable(firstPage, returnVal);

      return returnVal;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader pageReadTrx, final PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(
          firstPage.getRevision(), 
          reference.getKey(),
          (int) pageReadTrx.getDatabaseId(),
          (int) pageReadTrx.getResourceId()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      // Update pageFragments on original reference
      reference.setPageFragments(previousPageFragmentKeys);

      // Only create TWO pages instead of THREE - use bitmap instead of temp page
      // This saves 64KB allocation per combine operation
      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      final T modifyingPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), pageReadTrx);
      
      // OPTIMIZATION: Use bitmap (128 bytes) instead of temp page (64KB)
      // inWindowBitmap tracks which slots exist in the sliding window
      final long[] inWindowBitmap = new long[16];  // 16 * 64 = 1024 bits
      
      // DIAGNOSTIC
      if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
        LOGGER.debug("SLIDING_SNAPSHOT combineForMod created 2 pages + bitmap: complete=" + 
            System.identityHashCode(completePage) + ", modifying=" + System.identityHashCode(modifyingPage));
      }

      final io.sirix.page.KeyValueLeafPage completeKvp = (io.sirix.page.KeyValueLeafPage) completePage;
      final io.sirix.page.KeyValueLeafPage modifyingKvp = (io.sirix.page.KeyValueLeafPage) modifyingPage;
      final long[] filledBitmap = completeKvp.getSlotBitmap();
      
      final boolean hasOutOfWindowPage = (pages.size() == revToRestore);
      final int lastInWindowIndex = hasOutOfWindowPage ? pages.size() - 2 : pages.size() - 1;
      
      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      
      // Phase 1: Process in-window fragments, track populated slots in bitmap
      for (int i = 0; i <= lastInWindowIndex; i++) {
        final T page = pages.get(i);
        assert page.getPageKey() == recordPageKey;

        // Use bitmap iteration for O(k) instead of O(1024)
        final io.sirix.page.KeyValueLeafPage kvPage = (io.sirix.page.KeyValueLeafPage) page;
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          // Mark slot as in-window (for Phase 2 check)
          inWindowBitmap[offset >>> 6] |= (1L << (offset & 63));
          
          // Check if slot already filled in completePage using bitmap (O(1))
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;  // Already filled from newer fragment
          }

          final var recordData = page.getSlot(offset);
          completePage.setSlot(recordData, offset);
          filledSlotCount++;
          
          final var deweyId = page.getDeweyId(offset);
          if (deweyId != null) {
            completePage.setDeweyId(deweyId, offset);
          }
          
          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;  // Page is full
          }
        }

        // Handle references
        for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
          final Long key = entry.getKey();
          if (completePage.getPageReference(key) == null) {
            completePage.setPageReference(key, entry.getValue());
          }
        }
        
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;  // Page is full
        }
      }
      
      // Phase 2: Process out-of-window fragment if present
      // LAZY COPY: Mark slots for preservation instead of copying
      // Actual copy from completePage happens in addReferences() if records[offset] == null
      if (hasOutOfWindowPage) {
        final T outOfWindowPage = pages.get(pages.size() - 1);
        assert outOfWindowPage.getPageKey() == recordPageKey;
        
        final io.sirix.page.KeyValueLeafPage outOfWindowKvp = (io.sirix.page.KeyValueLeafPage) outOfWindowPage;
        final int[] populatedSlots = outOfWindowKvp.populatedSlots();
        
        for (final int offset : populatedSlots) {
          final var recordData = outOfWindowPage.getSlot(offset);
          final var deweyId = outOfWindowPage.getDeweyId(offset);
          
          // Add to completePage if not already filled
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
            completePage.setSlot(recordData, offset);
            if (deweyId != null) {
              completePage.setDeweyId(deweyId, offset);
            }
          }
          
          // If slot is NOT in the sliding window, mark for preservation in modifyingPage
          // (these are records falling out of the window that need to be written)
          if ((inWindowBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
            // LAZY COPY: Mark slot for preservation instead of copying
            modifyingKvp.markSlotForPreservation(offset);
          }
        }
        
        // Handle references from out-of-window page
        for (final Entry<Long, PageReference> entry : outOfWindowPage.referenceEntrySet()) {
          final Long key = entry.getKey();
          if (completePage.getPageReference(key) == null) {
            completePage.setPageReference(key, entry.getValue());
          }
          // References falling out of window - check if not in window
          if (modifyingPage.getPageReference(key) == null) {
            // Add to modifying if needed (reference handling simplified)
            modifyingPage.setPageReference(key, entry.getValue());
          }
        }
        
        // Set completePage reference for lazy copying at commit time
        modifyingKvp.setCompletePageRef(completeKvp);
      }

      // Propagate FSST symbol tables
      propagateFsstSymbolTable(firstPage, completePage);
      propagateFsstSymbolTable(firstPage, modifyingPage);

      final var pageContainer = PageContainer.getInstance(completePage, modifyingPage);
      log.put(reference, pageContainer);  // TIL will remove from caches before mutating
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

  private static final Logger LOGGER = LoggerFactory.getLogger(VersioningType.class);

  public static VersioningType fromString(String versioningType) {
    for (final var type : values()) {
      if (type.name().equalsIgnoreCase(versioningType)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No constant with name " + versioningType + " found");
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
      final @NonNegative int revsToRestore, final StorageEngineReader pageReadTrx);

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
      final List<T> pages, final @NonNegative int revsToRestore, final StorageEngineReader pageReadTrx,
      final PageReference reference, final TransactionIntentLog log);

  /**
   * Get all revision root page numbers which are needed to restore a {@link KeyValuePage}.
   *
   * @param previousRevision the previous revision
   * @param revsToRestore    number of revisions to restore
   * @return revision root page numbers needed to restore a {@link KeyValuePage}
   */
  public abstract int[] getRevisionRoots(final @NonNegative int previousRevision, final @NonNegative int revsToRestore);

  /**
   * Propagate FSST symbol table from source page to target page.
   * This is needed when combining page fragments to ensure the combined page
   * can decompress string values.
   *
   * @param sourcePage the source page with the FSST symbol table
   * @param targetPage the target page to set the symbol table on
   * @param <V> the data record type
   * @param <T> the key-value page type
   */
  protected static <V extends DataRecord, T extends KeyValuePage<V>> void propagateFsstSymbolTable(
      final T sourcePage, final T targetPage) {
    if (sourcePage instanceof io.sirix.page.KeyValueLeafPage sourceKvp 
        && targetPage instanceof io.sirix.page.KeyValueLeafPage targetKvp) {
      byte[] fsstSymbolTable = sourceKvp.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        targetKvp.setFsstSymbolTable(fsstSymbolTable);
      }
    }
  }

  // ===== HOT Leaf Page Combining Methods =====

  /**
   * Combine multiple HOT leaf page fragments into a single complete page.
   *
   * <p>Unlike slot-based combining for KeyValueLeafPage, HOT pages use key-based
   * merging with NodeReferences OR semantics. Newer fragments take precedence,
   * and tombstones (empty NodeReferences) indicate deletions.</p>
   *
   * @param pages the list of HOT leaf page fragments (newest first)
   * @param revToRestore the revision to restore
   * @param pageReadTrx the storage engine reader
   * @return the combined HOT leaf page
   */
  public io.sirix.page.HOTLeafPage combineHOTLeafPages(
      final List<io.sirix.page.HOTLeafPage> pages,
      final @NonNegative int revToRestore,
      final StorageEngineReader pageReadTrx) {
    
    if (pages.isEmpty()) {
      throw new IllegalArgumentException("No pages to combine");
    }
    
    if (pages.size() == 1) {
      return pages.getFirst();
    }
    
    // Start with a copy of the newest page
    io.sirix.page.HOTLeafPage result = pages.getFirst().copy();
    
    // Merge older fragments (skip first as it's already the base)
    for (int i = 1; i < pages.size(); i++) {
      io.sirix.page.HOTLeafPage olderPage = pages.get(i);
      
      // Merge each entry from older page
      for (int j = 0; j < olderPage.getEntryCount(); j++) {
        byte[] key = olderPage.getKey(j);
        
        // Check if key exists in result
        int existingIdx = result.findEntry(key);
        if (existingIdx < 0) {
          // Key doesn't exist in newer - copy from older
          byte[] value = olderPage.getValue(j);
          if (!io.sirix.index.hot.NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
            // Not a tombstone - add entry
            result.mergeWithNodeRefs(key, key.length, value, value.length);
          }
          // If it's a tombstone in older but not in newer, it stays deleted
        }
        // If key exists in newer fragment, it takes precedence (already in result)
      }
    }
    
    return result;
  }

  /**
   * Combine HOT leaf page fragments for modification (COW).
   *
   * <p>Creates a copy of the combined page for modification while preserving
   * the original for readers (Copy-on-Write isolation).</p>
   *
   * @param pages the list of HOT leaf page fragments (newest first)
   * @param revToRestore the revision to restore
   * @param pageReadTrx the storage engine reader
   * @param reference the page reference
   * @param log the transaction intent log
   * @return the page container with complete and modified pages
   */
  public PageContainer combineHOTLeafPagesForModification(
      final List<io.sirix.page.HOTLeafPage> pages,
      final @NonNegative int revToRestore,
      final StorageEngineReader pageReadTrx,
      final PageReference reference,
      final TransactionIntentLog log) {
    
    // Combine fragments
    io.sirix.page.HOTLeafPage completePage = combineHOTLeafPages(pages, revToRestore, pageReadTrx);
    
    // Create COW copy for modification
    io.sirix.page.HOTLeafPage modifiedPage = completePage.copy();
    
    // Create container with both pages
    final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
    log.put(reference, pageContainer);
    
    return pageContainer;
  }
}


