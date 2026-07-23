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
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.BitmapChunkPage;
import io.sirix.page.FsstAwareSlotCopier;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.PageFragmentKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
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
        final int revToRestore, final StorageEngineReader storageEngineReader) {
      assert pages.size() == 1 : "Only one version of the page!";
      var firstPage = pages.getFirst();
      T completePage =  firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), storageEngineReader);

      final KeyValueLeafPage srcKvl = (KeyValueLeafPage) firstPage;
      final KeyValueLeafPage dstKvl = (KeyValueLeafPage) completePage;
      dstKvl.ensureSlottedPage();

      // Use populatedSlots() for O(k) bitmap-driven iteration
      final int[] populated = srcKvl.populatedSlots();
      for (final int i : populated) {
        var slot = firstPage.getSlot(i);

        if (slot == null) {
          continue;
        }

        dstKvl.setSlotWithNodeKind(slot, i, srcKvl.getSlotNodeKindId(i));
        completePage.setDeweyId(firstPage.getDeweyId(i), i);
      }

      // Overflow records (> MAX_RECORD_SIZE) live as page REFERENCES, not slots — they must
      // be carried over too. FULL pages are self-contained (readers never consult older
      // fragments), so omitting this dropped every overflow record from the combined page.
      for (final Entry<Long, PageReference> entry : firstPage.referenceEntrySet()) {
        completePage.setPageReference(entry.getKey(), entry.getValue());
      }

      // Propagate FSST symbol table for string compression
      propagateFsstSymbolTable(firstPage, completePage);

      // Propagate PAX number region from the donor (first) fragment — for
      // read-only resources this is an O(1) copy. Multi-fragment merges fall
      // back to a slotted-page walk inside ensureNumberRegion.
      ((KeyValueLeafPage) completePage).ensureNumberRegion((KeyValueLeafPage) firstPage);

      return completePage;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() == 1;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();

      // OPTIMIZATION: Create only ONE page for modifications (not two)
      // FULL versioning stores complete pages, so both complete and modified can be the same
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);

      final KeyValueLeafPage srcKvl = (KeyValueLeafPage) firstPage;
      final KeyValueLeafPage dstKvl = (KeyValueLeafPage) modifiedPage;
      dstKvl.ensureSlottedPage();

      // Copy data once (not twice) - use populatedSlots() for O(k) iteration
      final int[] populated = srcKvl.populatedSlots();
      for (final int i : populated) {
        var slot = firstPage.getSlot(i);
        if (slot == null) {
          continue;
        }
        dstKvl.setSlotWithNodeKind(slot, i, srcKvl.getSlotNodeKindId(i));
        modifiedPage.setDeweyId(firstPage.getDeweyId(i), i);
      }

      // Overflow records (> MAX_RECORD_SIZE) live as page REFERENCES, not slots. Unlike the
      // DIFFERENTIAL/INCREMENTAL/SLIDING_SNAPSHOT combines, this branch had NO reference copy:
      // modifying ANY record on a FULL page that also held an untouched >150KB overflow record
      // produced a new self-contained page WITHOUT that record's reference — silently and
      // permanently absent from the new revision onward (and unreadable within the writing
      // transaction itself).
      for (final Entry<Long, PageReference> entry : firstPage.referenceEntrySet()) {
        modifiedPage.setPageReference(entry.getKey(), entry.getValue());
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
    public int[] getRevisionRoots(int previousRevision, int revsToRestore) {
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
        final int revToRestore, final StorageEngineReader storageEngineReader) {
      assert pages.size() <= 2;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T pageToReturn = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);

      final T latest = pages.get(0);
      final T fullDump = pages.size() == 1 ? pages.get(0) : pages.get(1);

      assert latest.getPageKey() == recordPageKey;
      assert fullDump.getPageKey() == recordPageKey;

      final KeyValueLeafPage latestKvp = (KeyValueLeafPage) latest;
      final KeyValueLeafPage returnKvp = (KeyValueLeafPage) pageToReturn;
      returnKvp.ensureSlottedPage();

      final boolean singleFragment = pages.size() == 1;

      if (singleFragment) {
        // Fast path — target is a bit-identical copy of the sole fragment.
        // Raw-copy slots, propagate the FSST table (same compressed bytes),
        // and short-circuit the PAX region as an O(1) reference copy.
        final int[] latestSlots = latestKvp.populatedSlots();
        for (int i = 0; i < latestSlots.length; i++) {
          final int offset = latestSlots[i];
          returnKvp.setSlotWithNodeKind(firstPage.getSlot(offset), offset, latestKvp.getSlotNodeKindId(offset));
          final var deweyId = firstPage.getDeweyId(offset);
          if (deweyId != null) {
            pageToReturn.setDeweyId(deweyId, offset);
          }
        }

        for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
          pageToReturn.setPageReference(entry.getKey(), entry.getValue());
        }

        propagateFsstSymbolTable(firstPage, pageToReturn);
        returnKvp.ensureNumberRegion(latestKvp);
        return pageToReturn;
      }

      // Multi-fragment combine — decompress-on-merge so each compressed string
      // on a source fragment is decoded through that fragment's own FSST table
      // before landing on the target. After the loop, the target holds no
      // compressed bytes and carries no FSST table of its own.
      final FsstAwareSlotCopier latestCopier = new FsstAwareSlotCopier(latestKvp.getFsstSymbolTable());

      final int[] latestSlots = latestKvp.populatedSlots();
      for (int i = 0; i < latestSlots.length; i++) {
        final int offset = latestSlots[i];
        copySlotDecompressing(latestKvp, returnKvp, offset, latestKvp.getSlotNodeKindId(offset), latestCopier);
        final var deweyId = firstPage.getDeweyId(offset);
        if (deweyId != null) {
          pageToReturn.setDeweyId(deweyId, offset);
        }
      }

      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        pageToReturn.setPageReference(entry.getKey(), entry.getValue());
      }

      if (returnKvp.populatedSlotCount() < Constants.NDP_NODE_COUNT) {
        final KeyValueLeafPage fullDumpKvp = (KeyValueLeafPage) fullDump;
        final FsstAwareSlotCopier fullDumpCopier = new FsstAwareSlotCopier(fullDumpKvp.getFsstSymbolTable());
        final long[] filledBitmap = returnKvp.getSlotBitmap();
        final boolean latestHasReferences = !latest.referenceEntrySet().isEmpty();

        final int[] fullDumpSlots = fullDumpKvp.populatedSlots();
        for (int i = 0; i < fullDumpSlots.length; i++) {
          final int offset = fullDumpSlots[i];
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (latestHasReferences && slotShadowedByNewerOverflowReference(pageToReturn, recordPageKey, offset)) {
            continue;
          }

          copySlotDecompressing(fullDumpKvp, returnKvp, offset, fullDumpKvp.getSlotNodeKindId(offset), fullDumpCopier);

          final var deweyId = fullDump.getDeweyId(offset);
          if (deweyId != null) {
            pageToReturn.setDeweyId(deweyId, offset);
          }
        }

        for (final Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (referenceShadowedByNewerSlot(filledBitmap, entry.getKey())) {
            continue;
          }
          if (pageToReturn.getPageReference(entry.getKey()) == null) {
            pageToReturn.setPageReference(entry.getKey(), entry.getValue());
            if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
              break;
            }
          }
        }
      }

      // Target holds merged content from multiple fragments. Rebuild the PAX
      // number region from the combined slotted heap — a donor shortcut from
      // any single fragment would miss values contributed by the others.
      returnKvp.ensureNumberRegion();
      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() <= 2;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final int revision = storageEngineReader.getUberPage().getRevisionNumber();

      // Update pageFragments on original reference
      final List<PageFragmentKey> pageFragmentKeys = List.of(new PageFragmentKeyImpl(
          firstPage.getRevision(), 
          reference.getKey(),
          (int) storageEngineReader.getDatabaseId(),
          (int) storageEngineReader.getResourceId()));
      reference.setPageFragments(pageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);

      final T latest = firstPage;
      final T fullDump = pages.size() == 1 ? firstPage : pages.get(1);
      final boolean isFullDumpRevision = revision % revToRestore == 0;

      final KeyValueLeafPage latestKvp = (KeyValueLeafPage) latest;
      final KeyValueLeafPage completeKvp = (KeyValueLeafPage) completePage;
      final KeyValueLeafPage modifiedKvp = (KeyValueLeafPage) modifiedPage;
      completeKvp.ensureSlottedPage();
      modifiedKvp.ensureSlottedPage();

      final boolean singleFragment = pages.size() == 1;
      final FsstAwareSlotCopier latestCopier =
          singleFragment ? null : new FsstAwareSlotCopier(latestKvp.getFsstSymbolTable());

      // Copy all populated slots from latest to completePage using bitmap iteration.
      // For modifiedPage: use lazy copy — mark for preservation, actual copy deferred to commit time.
      final int[] latestSlots = latestKvp.populatedSlots();
      for (int i = 0; i < latestSlots.length; i++) {
        final int offset = latestSlots[i];
        if (singleFragment) {
          completeKvp.setSlotWithNodeKind(firstPage.getSlot(offset), offset, latestKvp.getSlotNodeKindId(offset));
        } else {
          copySlotDecompressing(latestKvp, completeKvp, offset, latestKvp.getSlotNodeKindId(offset), latestCopier);
        }
        final var deweyId = firstPage.getDeweyId(offset);
        if (deweyId != null) {
          completePage.setDeweyId(deweyId, offset);
        }
        modifiedKvp.markSlotForPreservation(offset);
      }

      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        completePage.setPageReference(entry.getKey(), entry.getValue());
        modifiedPage.setPageReference(entry.getKey(), entry.getValue());
      }

      if (completeKvp.populatedSlotCount() < Constants.NDP_NODE_COUNT && pages.size() == 2) {
        final KeyValueLeafPage fullDumpKvp = (KeyValueLeafPage) fullDump;
        final FsstAwareSlotCopier fullDumpCopier = new FsstAwareSlotCopier(fullDumpKvp.getFsstSymbolTable());
        final long[] filledBitmap = completeKvp.getSlotBitmap();
        final boolean latestHasReferences = !latest.referenceEntrySet().isEmpty();

        final int[] fullDumpSlots = fullDumpKvp.populatedSlots();
        for (int j = 0; j < fullDumpSlots.length; j++) {
          final int offset = fullDumpSlots[j];
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (latestHasReferences && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset)) {
            continue;
          }

          copySlotDecompressing(fullDumpKvp, completeKvp, offset, fullDumpKvp.getSlotNodeKindId(offset), fullDumpCopier);
          final var deweyId = fullDump.getDeweyId(offset);
          if (deweyId != null) {
            completePage.setDeweyId(deweyId, offset);
          }

          if (isFullDumpRevision) {
            modifiedKvp.markSlotForPreservation(offset);
          }
        }

        for (final Map.Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (referenceShadowedByNewerSlot(filledBitmap, entry.getKey())) {
            continue;
          }
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

      // Single-fragment: completePage is a byte-copy of latest and can safely
      // inherit the FSST table. Multi-fragment: completePage has uncompressed
      // strings from decompress-on-merge — no FSST table to propagate.
      if (singleFragment) {
        propagateFsstSymbolTable(firstPage, completePage);
      }

      modifiedKvp.setCompletePageRef(completeKvp);

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
      log.put(reference, pageContainer);  // TIL will remove from caches before mutating
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(int previousRevision, int revsToRestore) {
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
   *
   */
  INCREMENTAL {
    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
        final int revToRestore, final StorageEngineReader storageEngineReader) {
      assert pages.size() <= revToRestore;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T pageToReturn = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), storageEngineReader);

      // Track which slots are already filled using bitmap from pageToReturn
      // This enables O(k) iteration instead of O(1024)
      final KeyValueLeafPage returnPage = (KeyValueLeafPage) pageToReturn;
      returnPage.ensureSlottedPage();
      final long[] filledBitmap = returnPage.getSlotBitmap();
      
      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      // Overflow references claimed so far — fast guard for the large-value shadow check (#1076).
      int claimedReferences = 0;

      final boolean singleFragment = pages.size() == 1;

      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        final KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        // Per-fragment copier — amortizes the FSST symbol-table parse across
        // every slot on this fragment. In the single-fragment case we bypass
        // the copier entirely (raw-copy is byte-identical to the source).
        final FsstAwareSlotCopier copier =
            singleFragment ? null : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(pageToReturn, recordPageKey, offset)) {
            continue;
          }

          if (singleFragment) {
            returnPage.setSlotWithNodeKind(page.getSlot(offset), offset, kvPage.getSlotNodeKindId(offset));
          } else {
            copySlotDecompressing(kvPage, returnPage, offset, kvPage.getSlotNodeKindId(offset), copier);
          }
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
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
            if (referenceShadowedByNewerSlot(filledBitmap, key)) {
              continue;
            }
            if (pageToReturn.getPageReference(key) == null) {
              pageToReturn.setPageReference(key, entry.getValue());
              claimedReferences++;
              if (pageToReturn.size() == Constants.NDP_NODE_COUNT) {
                break;
              }
            }
          }
        }
      }

      if (singleFragment) {
        // Bit-identical copy: FSST table propagation + donor PAX region shortcut.
        propagateFsstSymbolTable(firstPage, pageToReturn);
        returnPage.ensureNumberRegion((KeyValueLeafPage) firstPage);
      } else {
        // Merged content from multiple fragments — decompress-on-merge already
        // made every string slot uncompressed; the target intentionally carries
        // no FSST table. Rebuild the PAX number region from the combined heap.
        returnPage.ensureNumberRegion();
      }

      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader, PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(
          firstPage.getRevision(), 
          reference.getKey(),
          (int) storageEngineReader.getDatabaseId(),
          (int) storageEngineReader.getResourceId()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      // Update pageFragments on original reference.
      // NOTE (F7, re-deferred 2026-06-10): resetting this chain to empty on a full dump
      // (pages.size()==revToRestore) is the intuitive optimization, but `pages.size()==
      // revToRestore` in this combine is NOT the same predicate as "the serialized newest
      // fragment is a self-contained full dump" — emptying the chain here made reads
      // reconstruct from the newest fragment alone and MISS slots still only present in older
      // fragments (187 sirix-core failures: structural over/under-reads in ConcurrentAxis/
      // Versioning/diff). The chain reset must be gated on the SAME predicate the serializer
      // uses to emit a full bitmap snapshot (shouldStoreBitmapFullSnapshot); aligning the two
      // is a storage-format-adjacent change to make deliberately, not a patch.
      reference.setPageFragments(previousPageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);
      final boolean isFullDump = pages.size() == revToRestore;

      final long[] inWindowBitmap = new long[PageLayout.BITMAP_WORDS];

      final KeyValueLeafPage completeKvp = (KeyValueLeafPage) completePage;
      final KeyValueLeafPage modifiedKvp = (KeyValueLeafPage) modifiedPage;
      completeKvp.ensureSlottedPage();
      modifiedKvp.ensureSlottedPage();
      final long[] filledBitmap = completeKvp.getSlotBitmap();

      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      // Overflow references claimed so far — fast guard for the large-value shadow check (#1076).
      int claimedReferences = 0;

      final boolean singleFragment = pages.size() == 1;

      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        final KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        final FsstAwareSlotCopier copier =
            singleFragment ? null : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset)) {
            continue;
          }

          if (singleFragment) {
            completeKvp.setSlotWithNodeKind(page.getSlot(offset), offset, kvPage.getSlotNodeKindId(offset));
          } else {
            copySlotDecompressing(kvPage, completeKvp, offset, kvPage.getSlotNodeKindId(offset), copier);
          }
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          filledSlotCount++;

          if (isFullDump) {
            modifiedKvp.markSlotForPreservation(offset);
          }

          final var deweyId = page.getDeweyId(offset);
          if (deweyId != null) {
            completePage.setDeweyId(deweyId, offset);
          }

          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        if (filledSlotCount < Constants.NDP_NODE_COUNT) {
          for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
            final Long key = entry.getKey();
            assert key != null;
            if (referenceShadowedByNewerSlot(filledBitmap, key)) {
              continue;
            }
            if (completePage.getPageReference(key) == null) {
              completePage.setPageReference(key, entry.getValue());
              claimedReferences++;

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

      if (singleFragment) {
        propagateFsstSymbolTable(firstPage, completePage);
      }

      if (isFullDump) {
        modifiedKvp.setCompletePageRef(completeKvp);
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
      log.put(reference, pageContainer);  // TIL will remove from caches before mutating
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(final int previousRevision, final int revsToRestore) {
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
        final int revToRestore, final StorageEngineReader storageEngineReader) {
      assert pages.size() <= revToRestore;
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T returnVal = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), storageEngineReader);

      final KeyValueLeafPage returnKvp = (KeyValueLeafPage) returnVal;

      // Single-fragment fast path: the donor fragment is fully materialized;
      // bulk-copy its slotted page + propagate FSST/NumberRegion + copy
      // overflow refs. One MemorySegment.copy replaces the 1024-slot loop.
      if (pages.size() == 1) {
        final KeyValueLeafPage srcKvl = (KeyValueLeafPage) firstPage;
        returnKvp.copySlottedPageFrom(srcKvl);
        propagateFsstSymbolTable(firstPage, returnVal);
        returnKvp.ensureNumberRegion(srcKvl);
        for (final Entry<Long, PageReference> e : firstPage.referenceEntrySet()) {
          returnVal.setPageReference(e.getKey(), e.getValue());
        }
        return returnVal;
      }

      // Track which slots are already filled using bitmap from returnVal
      // This enables O(k) iteration instead of O(1024)
      returnKvp.ensureSlottedPage();
      final long[] filledBitmap = returnKvp.getSlotBitmap();

      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      // Overflow references claimed so far — fast guard for the large-value shadow check (#1076).
      int claimedReferences = 0;

      final boolean singleFragment = false;

      for (final T page : pages) {
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        final KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        final FsstAwareSlotCopier copier =
            singleFragment ? null : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(returnVal, recordPageKey, offset)) {
            continue;
          }

          if (singleFragment) {
            returnKvp.setSlotWithNodeKind(page.getSlot(offset), offset, kvPage.getSlotNodeKindId(offset));
          } else {
            copySlotDecompressing(kvPage, returnKvp, offset, kvPage.getSlotNodeKindId(offset), copier);
          }
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
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
            if (referenceShadowedByNewerSlot(filledBitmap, key)) {
              continue;
            }
            if (returnVal.getPageReference(key) == null) {
              returnVal.setPageReference(key, entry.getValue());
              claimedReferences++;
              if (returnVal.size() == Constants.NDP_NODE_COUNT) {
                break;
              }
            }
          }
        }
      }

      if (singleFragment) {
        propagateFsstSymbolTable(firstPage, returnVal);
        returnKvp.ensureNumberRegion((KeyValueLeafPage) firstPage);
      } else {
        // Target intentionally holds no FSST table after decompress-on-merge.
        // Rebuild the PAX number region from the combined slotted heap.
        returnKvp.ensureNumberRegion();
      }

      return returnVal;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader, final PageReference reference,
        final TransactionIntentLog log) {
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(
          firstPage.getRevision(), 
          reference.getKey(),
          (int) storageEngineReader.getDatabaseId(),
          (int) storageEngineReader.getResourceId()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size();
           i < previousRefKeysSize && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
        previousPageFragmentKeys.add(reference.getPageFragments().get(i));
      }

      // Update pageFragments on original reference
      reference.setPageFragments(previousPageFragmentKeys);

      // Only create TWO pages instead of THREE - use bitmap instead of temp page
      // This saves 64KB allocation per combine operation
      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);
      final T modifyingPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);
      
      // OPTIMIZATION: Use bitmap (128 bytes) instead of temp page (64KB)
      // inWindowBitmap tracks which slots exist in the sliding window
      final long[] inWindowBitmap = new long[16];  // 16 * 64 = 1024 bits
      
      final KeyValueLeafPage completeKvp = (KeyValueLeafPage) completePage;
      final KeyValueLeafPage modifyingKvp = (KeyValueLeafPage) modifyingPage;
      completeKvp.ensureSlottedPage();
      modifyingKvp.ensureSlottedPage();
      final long[] filledBitmap = completeKvp.getSlotBitmap();
      
      final boolean hasOutOfWindowPage = (pages.size() == revToRestore);
      final int lastInWindowIndex = hasOutOfWindowPage ? pages.size() - 2 : pages.size() - 1;

      final boolean singleFragment = pages.size() == 1;

      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = 0;
      // Overflow references claimed so far — fast guard for the large-value shadow check (#1076).
      int claimedReferences = 0;

      // Phase 1: Process in-window fragments, track populated slots in bitmap
      for (int i = 0; i <= lastInWindowIndex; i++) {
        final T page = pages.get(i);
        assert page.getPageKey() == recordPageKey;

        final KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        final FsstAwareSlotCopier copier =
            singleFragment ? null : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          inWindowBitmap[offset >>> 6] |= (1L << (offset & 63));

          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset)) {
            continue;
          }

          if (singleFragment) {
            completeKvp.setSlotWithNodeKind(page.getSlot(offset), offset, kvPage.getSlotNodeKindId(offset));
          } else {
            copySlotDecompressing(kvPage, completeKvp, offset, kvPage.getSlotNodeKindId(offset), copier);
          }
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          filledSlotCount++;

          final var deweyId = page.getDeweyId(offset);
          if (deweyId != null) {
            completePage.setDeweyId(deweyId, offset);
          }

          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
          final Long key = entry.getKey();
          // The record IS represented in the window (as an overflow reference) — the
          // out-of-window fragment's stale slot must be neither copied nor preserved.
          final int refOffset = StorageEngineReader.recordPageOffset(key);
          inWindowBitmap[refOffset >>> 6] |= (1L << (refOffset & 63));
          if (referenceShadowedByNewerSlot(filledBitmap, key)) {
            continue;
          }
          if (completePage.getPageReference(key) == null) {
            completePage.setPageReference(key, entry.getValue());
            claimedReferences++;
          }
        }

        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }
      }

      // Phase 2: Process out-of-window fragment if present.
      if (hasOutOfWindowPage) {
        final T outOfWindowPage = pages.get(pages.size() - 1);
        assert outOfWindowPage.getPageKey() == recordPageKey;

        final KeyValueLeafPage outOfWindowKvp = (KeyValueLeafPage) outOfWindowPage;
        final FsstAwareSlotCopier outCopier = new FsstAwareSlotCopier(outOfWindowKvp.getFsstSymbolTable());
        final int[] populatedSlots = outOfWindowKvp.populatedSlots();

        for (final int offset : populatedSlots) {
          final var deweyId = outOfWindowPage.getDeweyId(offset);

          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) == 0
              && !(claimedReferences > 0
                  && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset))) {
            copySlotDecompressing(outOfWindowKvp, completeKvp, offset, outOfWindowKvp.getSlotNodeKindId(offset),
                outCopier);
            filledBitmap[offset >>> 6] |= (1L << (offset & 63));
            if (deweyId != null) {
              completePage.setDeweyId(deweyId, offset);
            }
          }

          if ((inWindowBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
            modifyingKvp.markSlotForPreservation(offset);
          }
        }

        for (final Entry<Long, PageReference> entry : outOfWindowPage.referenceEntrySet()) {
          final Long key = entry.getKey();
          if (referenceShadowedByNewerSlot(filledBitmap, key)) {
            continue;
          }
          if (completePage.getPageReference(key) == null) {
            completePage.setPageReference(key, entry.getValue());
            // Only an out-of-window reference NO in-window fragment shadows may enter the
            // NEW fragment (phase 1 merged the in-window references into completePage only,
            // so the old unconditional copy here resurrected stale overflow references past
            // an in-window update: the new fragment is newest at read time and overflow
            // records have no slot to shadow them — the OLD value won durably). Mirrors the
            // completePage-null gating the INCREMENTAL combine already does.
            if (modifyingPage.getPageReference(key) == null) {
              modifyingPage.setPageReference(key, entry.getValue());
            }
          }
        }

        modifyingKvp.setCompletePageRef(completeKvp);
      }

      // Single-fragment only: completePage / modifyingPage are byte-identical
      // to firstPage and can share its FSST table. Multi-fragment combines
      // apply decompress-on-merge so completePage carries no compressed strings
      // and no FSST table; modifyingPage rebuilds its own table at commit.
      if (singleFragment && !hasOutOfWindowPage) {
        propagateFsstSymbolTable(firstPage, completePage);
        propagateFsstSymbolTable(firstPage, modifyingPage);
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifyingPage);
      log.put(reference, pageContainer);  // TIL will remove from caches before mutating
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(final int previousRevision, final int revsToRestore) {
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

  /**
   * Large-value shadowing between page fragments (#1076): within one fragment a record lives
   * EITHER in a slot OR in an overflow reference, and fragments are merged newest-first. A slot
   * in an OLDER fragment is stale when a NEWER fragment already moved the record to overflow
   * storage; without this check the stale slot wins on read (slots have lookup priority) and the
   * record's old value resurrects.
   *
   * @param target        the combine target holding references claimed by newer fragments
   * @param recordPageKey the record page key
   * @param offset        the slot offset of the record within the page
   * @return {@code true} if a newer fragment claimed this record as an overflow reference
   */
  private static boolean slotShadowedByNewerOverflowReference(final KeyValuePage<?> target,
      final long recordPageKey, final int offset) {
    return target.getPageReference((recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + offset) != null;
  }

  /**
   * Counterpart of {@link #slotShadowedByNewerOverflowReference(KeyValuePage, long, int)}: an
   * overflow reference in an OLDER fragment is stale when a NEWER fragment stored the record in
   * a slot again (the value shrank below the overflow threshold).
   *
   * @param filledBitmap the bitmap of slot offsets claimed by newer fragments
   * @param recordKey    the record key of the overflow reference
   * @return {@code true} if a newer fragment claimed this record as a slot
   */
  private static boolean referenceShadowedByNewerSlot(final long[] filledBitmap, final long recordKey) {
    final int offset = StorageEngineReader.recordPageOffset(recordKey);
    return (filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0;
  }

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
      final int revsToRestore, final StorageEngineReader storageEngineReader);

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
      final List<T> pages, final int revsToRestore, final StorageEngineReader storageEngineReader,
      final PageReference reference, final TransactionIntentLog log);

  /**
   * Get all revision root page numbers which are needed to restore a {@link KeyValuePage}.
   *
   * @param previousRevision the previous revision
   * @param revsToRestore    number of revisions to restore
   * @return revision root page numbers needed to restore a {@link KeyValuePage}
   */
  public abstract int[] getRevisionRoots(final int previousRevision, final int revsToRestore);

  /**
   * Propagate an FSST symbol table from a single-fragment source page to the
   * target. <b>Callers must only invoke this in the single-fragment combine
   * path</b> (i.e. when the target is a byte-identical copy of the source), so
   * every compressed slot on the target was encoded with the propagated table.
   *
   * <p>For multi-fragment combines, do not call this — use the decompress-on-
   * merge path (see {@link #copySlotDecompressing}) instead, which rewrites
   * each compressed slot to its uncompressed form so the target correctly
   * carries {@code fsstSymbolTable = null}.
   *
   * @param sourcePage the single-fragment source page
   * @param targetPage the target page to set the symbol table on
   */
  protected static <V extends DataRecord, T extends KeyValuePage<V>> void propagateFsstSymbolTable(
      final T sourcePage, final T targetPage) {
    if (sourcePage instanceof KeyValueLeafPage sourceKvp
        && targetPage instanceof KeyValueLeafPage targetKvp) {
      final byte[] fsstSymbolTable = sourceKvp.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        targetKvp.setFsstSymbolTable(fsstSymbolTable);
      }
    }
  }

  /**
   * Copy a single slot from {@code src} to {@code dst} during a multi-fragment
   * combine. If {@code src} holds an FSST symbol table and the slot is a
   * string-kind slot whose compressed-flag byte is {@code 1}, the payload is
   * decoded through the source's table and the rewritten uncompressed slot is
   * stored on {@code dst}. All other slots (including uncompressed string
   * slots) are raw-copied.
   *
   * <p>Using this helper across every fragment of a multi-fragment combine is
   * the invariant that lets the target page safely carry
   * {@code fsstSymbolTable = null}. The next commit re-runs
   * {@code buildFsstSymbolTable} + {@code compressStringValues} so the page
   * lands on disk with a single coherent table — zero growth in disk footprint.
   *
   * @param src    source fragment
   * @param dst    target page being assembled
   * @param offset slot index (0-1023)
   * @param nodeKindId directory {@code nodeKindId} for the slot on the source
   * @param copier per-fragment copier carrying {@code src}'s parsed FSST table;
   *               may be {@code null} or inactive when the source has no table
   *               — callers commonly pass the same copier across the whole
   *               fragment loop to amortize the symbol-table parse
   */
  protected static void copySlotDecompressing(final KeyValueLeafPage src, final KeyValueLeafPage dst,
      final int offset, final int nodeKindId, final FsstAwareSlotCopier copier) {
    final MemorySegment slot = src.getSlot(offset);
    if (slot == null) {
      return;
    }
    if (copier != null && copier.active()) {
      final byte[] rewritten = copier.decompressSlot(slot, nodeKindId);
      if (rewritten != null) {
        dst.setSlotWithNodeKind(MemorySegment.ofArray(rewritten), offset, nodeKindId);
        return;
      }
    }
    dst.setSlotWithNodeKind(slot, offset, nodeKindId);
  }

  // ===== HOT Leaf Page Combining Methods =====

  /**
   * Combine multiple HOT leaf page fragments into a single complete page.
   *
   * <p>Cross-fragment merge happens by full key (not by entry index). Newer fragments take
   * precedence; tombstones (single-byte 0xFE value) shadow older entries; missing keys are
   * filled in from older fragments. Strategy dispatch mirrors
   * {@link #combineRecordPages(List, int, StorageEngineReader)} for {@link KeyValueLeafPage}.</p>
   *
   * @param pages the list of HOT leaf page fragments (newest first)
   * @param revToRestore the maximum number of fragments to merge per the active strategy
   * @param storageEngineReader the storage engine reader
   * @return the combined HOT leaf page
   */
  public HOTLeafPage combineHOTLeafPages(
      final List<HOTLeafPage> pages,
      final int revToRestore,
      final StorageEngineReader storageEngineReader) {

    if (pages == null || pages.isEmpty()) {
      throw new IllegalArgumentException("No pages to combine");
    }

    return switch (this) {
      // FULL: only the newest fragment is read at all (older fragments aren't loaded). Single
      // fragment is already complete; nothing to merge.
      case FULL -> pages.getFirst();

      // DIFFERENTIAL: at most {newest, fullDump} pair. Merge by key with newest winning.
      // INCREMENTAL: chain up to revsToRestore fragments. Same merge semantics — strategy
      // already enforced fragment count at load time.
      // SLIDING_SNAPSHOT: window-bounded chain. Same merge.
      // The merge contract is identical across non-FULL strategies because HOTLeafPage uses
      // tombstone shadowing rather than per-slot in-window bitmaps.
      case DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT ->
          mergeHOTFragmentsByKey(pages);
    };
  }

  /**
   * Merge HOT fragments by full key. Single newest-fragment fast path returns the page directly.
   * Multi-fragment path copies the newest, then walks older fragments inserting any keys absent
   * from the result. Tombstones in newer fragments shadow older entries; tombstones in older
   * fragments without a newer entry remain dropped.
   */
  private static HOTLeafPage mergeHOTFragmentsByKey(final List<HOTLeafPage> pages) {
    if (pages.size() == 1) {
      return pages.getFirst();
    }

    final HOTLeafPage newest = pages.getFirst();
    if (newest.isCompleteDump()) {
      return newest;
    }

    // Newest fragment is the base; copy() bulk-copies its entries and resets the dirty bitmap on
    // the result, so cross-fragment fills below are safely tracked as fresh writes if needed.
    final HOTLeafPage result = newest.copy();
    // The result is a freshly-merged read-only page — clear the slot-CoW link so it's treated as
    // a fully-materialized (no-completePageRef) leaf by any subsequent CoW.
    result.setCompletePageRef(null);
    result.clearDirtyBitmap();

    for (int i = 1; i < pages.size(); i++) {
      final HOTLeafPage olderPage = pages.get(i);
      final int olderCount = olderPage.getEntryCount();
      for (int j = 0; j < olderCount; j++) {
        final byte[] key = olderPage.getKey(j);
        final int existingIdx = result.findEntry(key);
        if (existingIdx >= 0) {
          // Newer fragment owns this key (possibly as a tombstone); skip older.
          continue;
        }
        // Add the entry — including tombstones — to the result. A tombstone in the older
        // fragment is the SHADOW that hides any non-tombstone value in still-older fragments;
        // skipping it would let the older value resurrect on the next iteration.
        // mergeWithNodeRefs takes the insert-new-entry branch when the key is absent and
        // preserves the tombstone byte verbatim.
        final byte[] value = olderPage.getValue(j);
        result.mergeWithNodeRefs(key, key.length, value, value.length);
      }
    }

    // Re-tighten the prefix after cross-fragment fills — the original combine path lacked this
    // step, leaving the merged leaf with a stale (potentially shorter) prefix from
    // handlePrefixForInsert. recomputePrefix is idempotent and a no-op when the prefix is already
    // tight.
    result.recomputePrefixForCombine();
    return result;
  }

  /**
   * Combine HOT leaf page fragments for modification (COW) and update the fragment chain on
   * {@code reference}. Mirrors {@link #combineRecordPagesForModification} for KVLP including the
   * chain bump done at lines 254-259 / 458-470 / 683-695.
   *
   * <p>Under non-FULL strategies, the writer commits a sparse fragment at a NEW disk offset; the
   * reader needs the prior fragment chain on {@code reference} to walk older fragments at the
   * subsequent revision read. Without this hook the chain stays empty and entries from prior
   * revisions are lost on read.</p>
   *
   * @param pages the list of HOT leaf page fragments (newest first); for the HOT writer the read-
   *              side combine has already collapsed the chain into a single complete page so this
   *              is typically a singleton list
   * @param revToRestore the maximum number of fragments per strategy
   * @param storageEngineReader the storage engine reader
   * @param reference the page reference (mutated: pageFragments updated in place)
   * @param log the transaction intent log
   * @return the page container with complete and modified pages
   */
  public PageContainer combineHOTLeafPagesForModification(
      final List<HOTLeafPage> pages,
      final int revToRestore,
      final StorageEngineReader storageEngineReader,
      final PageReference reference,
      final TransactionIntentLog log) {

    final HOTLeafPage completePage = combineHOTLeafPages(pages, revToRestore, storageEngineReader);
    final boolean forceFullEmit = bumpHOTPageFragmentChain(reference, completePage.getRevision(),
        revToRestore, storageEngineReader.getDatabaseId(), storageEngineReader.getResourceId());

    final HOTLeafPage modifiedPage = completePage.copy();

    if (this == FULL || forceFullEmit) {
      modifiedPage.markAllEntriesDirty();
    }

    final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
    log.put(reference, pageContainer);
    return pageContainer;
  }

  /**
   * Update the fragment chain on {@code reference} prior to the next CoW write of a HOT leaf.
   * Mirrors KVLP's chain bump at {@link #combineRecordPagesForModification} lines 254-259 /
   * 458-470 / 683-695, and additionally returns whether this commit must emit a full leaf
   * (snapshot rotation).
   *
   * <p>The chain is grown by prepending the prior on-disk offset; the result is bounded by the
   * strategy. FULL keeps no chain at all (every revision is a full dump). DIFFERENTIAL keeps
   * exactly one entry. INCREMENTAL and SLIDING_SNAPSHOT prepend up to {@code revToRestore - 1}
   * entries. When the chain would otherwise overflow, the chain is reset and the caller is told
   * to force a full emit so future readers can reconstruct from a fresh snapshot — without this
   * the OLDEST keys would fall off the chain and become unreadable.</p>
   *
   * <p>If {@code reference.getKey() < 0} the leaf was never persisted (no prior on-disk
   * fragment). Returns {@code false} and leaves the list untouched.</p>
   *
   * @param reference   the leaf reference (mutated)
   * @param revision    the revision number of the prior on-disk fragment (the page being CoW'd)
   * @param revToRestore strategy-bounded chain length
   * @param databaseId  the database id propagated into the new {@link PageFragmentKeyImpl}
   * @param resourceId  the resource id propagated into the new {@link PageFragmentKeyImpl}
   * @return {@code true} if the caller must force a full emit at commit (chain rotated) — only
   *         possible under non-FULL strategies; {@code false} otherwise
   */
  public boolean bumpHOTPageFragmentChain(final PageReference reference, final int revision,
      final int revToRestore, final long databaseId, final long resourceId) {
    if (this == FULL) {
      return false;
    }
    final long priorKey = reference.getKey();
    if (priorKey < 0) {
      return false;
    }
    final List<PageFragmentKey> existing = reference.getPageFragments();

    if (this == DIFFERENTIAL) {
      reference.setPageFragments(List.of(
          new PageFragmentKeyImpl(revision, priorKey, databaseId, resourceId)));
      return false;
    }

    final int chainCap = Math.max(0, revToRestore - 1);

    if (this == SLIDING_SNAPSHOT) {
      // True sliding snapshot: keep the newest `chainCap` fragments and let the OLDEST fall off the
      // window every commit once the window is full — NO forced full re-emit. The writer carries
      // the aging fragment's still-live entries forward into the new fragment
      // (carryForwardAgingHOTEntries), so nothing becomes unreachable when the oldest drops. This
      // is what distinguishes SLIDING_SNAPSHOT from INCREMENTAL, whose rotation below re-dumps the
      // whole leaf.
      final int slidingExistingSize = existing.size();
      final ArrayList<PageFragmentKey> slidingNext =
          new ArrayList<>(Math.min(slidingExistingSize + 1, Math.max(chainCap, 0)));
      if (chainCap > 0) {
        slidingNext.add(new PageFragmentKeyImpl(revision, priorKey, databaseId, resourceId));
        for (int i = 0; i < slidingExistingSize && slidingNext.size() < chainCap; i++) {
          slidingNext.add(existing.get(i));
        }
      }
      reference.setPageFragments(slidingNext);
      assert slidingNext.size() <= chainCap : "sliding chain overflow: size=" + slidingNext.size()
          + " > chainCap=" + chainCap;
      return false;
    }

    // INCREMENTAL: bounded delta chain with a periodic full re-emit at rotation.
    if (existing.size() + 1 > chainCap) {
      reference.setPageFragments(List.of());
      return true;
    }

    final int existingSize = existing.size();
    final ArrayList<PageFragmentKey> next = new ArrayList<>(existingSize + 1);
    next.add(new PageFragmentKeyImpl(revision, priorKey, databaseId, resourceId));
    for (int i = 0; i < existingSize && next.size() < chainCap; i++) {
      next.add(existing.get(i));
    }
    reference.setPageFragments(next);
    // Invariant: the post-bump chain length never exceeds chainCap. If it does, future readers
    // would walk fragments past the window and the rotation logic that depends
    // on overflow detection breaks. Enabled only with `-ea`.
    assert next.size() <= chainCap : "chain overflow: size=" + next.size() + " > chainCap="
        + chainCap;
    return false;
  }

  /**
   * Whether the next SLIDING_SNAPSHOT commit on {@code reference} will evict the oldest fragment
   * from the window — i.e. the fragment chain is already at its cap, so prepending the current
   * on-disk fragment pushes the oldest out. When {@code true} the writer must carry that oldest
   * fragment's still-live entries forward ({@link #carryForwardAgingHOTEntries}) so they stay
   * reachable after it drops. Must be read BEFORE {@link #bumpHOTPageFragmentChain} mutates the
   * chain.
   *
   * @param reference    the HOT leaf reference
   * @param revToRestore the versioning window (fragments kept readable)
   * @return {@code true} if the oldest fragment is about to age out of the window
   */
  public static boolean hotSlidingSnapshotEvicts(final PageReference reference, final int revToRestore) {
    if (reference.getKey() < 0) {
      return false; // never persisted — no on-disk fragment to prepend, so nothing is evicted
    }
    final int chainCap = Math.max(0, revToRestore - 1);
    return reference.getPageFragments().size() + 1 > chainCap;
  }

  /**
   * SLIDING_SNAPSHOT carry-forward: mark on {@code modifiedLeaf} every entry whose newest copy lives
   * in the fragment about to age out of the window, so this commit's new (sparse) fragment re-emits
   * it and it stays reachable after the oldest fragment drops. Replaces the coarse
   * {@code forceFullEmit} full re-emit — only genuinely-aging entries are rewritten, not the whole
   * leaf.
   *
   * <p>An entry of the oldest fragment is carried forward iff it is (a) <b>not a tombstone</b> —
   * once a tombstone becomes the oldest in-window fragment every value it shadowed is already out of
   * the window, so it has nothing left to shadow — and (b) <b>absent from every newer in-window
   * fragment</b>, because a newer fragment that still carries the key already keeps it reachable.</p>
   *
   * @param fragmentsNewestFirst the window's raw fragments, newest first (as returned by
   *                             {@link io.sirix.api.StorageEngineReader#loadHOTLeafFragments})
   * @param modifiedLeaf         the writer's copy of the combined leaf; carried entries are marked
   *                             dirty here so the sparse emit includes them
   */
  public static void carryForwardAgingHOTEntries(final List<HOTLeafPage> fragmentsNewestFirst,
      final HOTLeafPage modifiedLeaf) {
    final int fragmentCount = fragmentsNewestFirst.size();
    if (fragmentCount == 0) {
      return;
    }
    final HOTLeafPage oldest = fragmentsNewestFirst.get(fragmentCount - 1);
    final int oldestEntryCount = oldest.getEntryCount();
    for (int j = 0; j < oldestEntryCount; j++) {
      final byte[] value = oldest.getValue(j);
      // Tombstones aging out need no preservation: anything they shadowed is already gone from the
      // window (older than the oldest fragment), so re-emitting them would only leak dead markers.
      if (value == null || NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
        continue;
      }
      final byte[] key = oldest.getKey(j);
      boolean shadowedByNewerFragment = false;
      for (int f = 0; f < fragmentCount - 1; f++) {
        if (fragmentsNewestFirst.get(f).findEntry(key) >= 0) {
          shadowedByNewerFragment = true; // a newer in-window fragment already carries this key
          break;
        }
      }
      if (shadowedByNewerFragment) {
        continue;
      }
      final int idx = modifiedLeaf.findEntry(key);
      if (idx >= 0) {
        modifiedLeaf.markEntryDirty(idx);
      }
    }
  }

  // ===== Bitmap Chunk Page Versioning =====

  /**
   * Combine BitmapChunkPage fragments according to versioning strategy.
   *
   * <p>Takes a list of bitmap chunk page fragments (newest first) and combines them
   * into a complete bitmap representing the current state.</p>
   *
   * @param fragments the list of bitmap chunk page fragments (newest first)
   * @param revToRestore the revision to restore
   * @param storageEngineReader the storage engine reader
   * @return the combined bitmap chunk page with complete data
   */
  public BitmapChunkPage combineBitmapChunks(
      final List<BitmapChunkPage> fragments,
      final int revToRestore,
      final StorageEngineReader storageEngineReader) {
    
    if (fragments.isEmpty()) {
      throw new IllegalArgumentException("No fragments to combine");
    }
    
    if (fragments.size() == 1) {
      BitmapChunkPage singlePage = fragments.getFirst();
      if (singlePage.isDeleted()) {
        return singlePage; // Return tombstone as-is
      }
      if (singlePage.isFullSnapshot()) {
        return singlePage; // Full snapshot - already complete
      }
      // Delta page without base - shouldn't happen in valid state
      LOGGER.warn("Single delta page without base for chunk range [{}, {})",
          singlePage.getRangeStart(), singlePage.getRangeEnd());
      return singlePage.copyAsFull(singlePage.getRevision());
    }
    
    // Find the base snapshot (should be the last/oldest page)
    BitmapChunkPage basePage = fragments.getLast();
    if (!basePage.isFullSnapshot() && !basePage.isDeleted()) {
      LOGGER.warn("Base page is not a full snapshot for chunk range [{}, {})",
          basePage.getRangeStart(), basePage.getRangeEnd());
    }
    
    // Start with base bitmap
    org.roaringbitmap.longlong.Roaring64Bitmap combined = 
        basePage.getBitmap() != null ? basePage.getBitmap().clone() : new org.roaringbitmap.longlong.Roaring64Bitmap();
    
    // Apply deltas from oldest to newest (skip base)
    for (int i = fragments.size() - 2; i >= 0; i--) {
      BitmapChunkPage deltaPage = fragments.get(i);
      
      if (deltaPage.isDeleted()) {
        // Tombstone - clear everything
        combined = new org.roaringbitmap.longlong.Roaring64Bitmap();
        continue;
      }
      
      if (deltaPage.isFullSnapshot()) {
        // Full snapshot replaces everything
        combined = deltaPage.getBitmap() != null ? deltaPage.getBitmap().clone() : new org.roaringbitmap.longlong.Roaring64Bitmap();
        continue;
      }
      
      if (deltaPage.isDelta()) {
        // Apply additions
        if (deltaPage.getAdditions() != null && !deltaPage.getAdditions().isEmpty()) {
          combined.or(deltaPage.getAdditions());
        }
        // Apply removals
        if (deltaPage.getRemovals() != null && !deltaPage.getRemovals().isEmpty()) {
          combined.andNot(deltaPage.getRemovals());
        }
      }
    }
    
    // Create result as full snapshot
    BitmapChunkPage newestPage = fragments.getFirst();
    return BitmapChunkPage.createFull(
        newestPage.getPageKey(),
        newestPage.getRevision(),
        newestPage.getIndexType(),
        newestPage.getRangeStart(),
        newestPage.getRangeEnd(),
        combined
    );
  }

  /**
   * Prepare a BitmapChunkPage for modification.
   *
   * <p>Loads existing fragments, combines them, and creates a new page for
   * the current transaction. The versioning strategy determines whether
   * to create a full snapshot or a delta page.</p>
   *
   * @param fragments existing page fragments (newest first), may be empty for new chunks
   * @param currentRevision the current transaction revision
   * @param revsToRestore the threshold for full snapshots
   * @param rangeStart the chunk range start
   * @param rangeEnd the chunk range end
   * @param indexType the index type
   * @param reference the page reference
   * @param log the transaction intent log
   * @param storageEngineReader the storage engine reader
   * @return the page container with complete and modified pages
   */
  public PageContainer prepareBitmapChunkForModification(
      final List<BitmapChunkPage> fragments,
      final int currentRevision,
      final int revsToRestore,
      final long rangeStart,
      final long rangeEnd,
      final io.sirix.index.IndexType indexType,
      final PageReference reference,
      final TransactionIntentLog log,
      final StorageEngineReader storageEngineReader) {
    
    // Determine if we should create a full snapshot
    final boolean isFullDump = shouldStoreBitmapFullSnapshot(fragments, currentRevision, revsToRestore);
    
    final long pageKey = reference.getKey() >= 0 ? reference.getKey() : allocateNewPageKey(storageEngineReader);
    
    if (fragments.isEmpty()) {
      // New chunk - create empty full snapshot
      BitmapChunkPage newPage = BitmapChunkPage.createEmptyFull(
          pageKey, currentRevision, indexType, rangeStart, rangeEnd);
      PageContainer container = PageContainer.getInstance(newPage, newPage);
      log.put(reference, container);
      return container;
    }
    
    // Combine existing fragments
    BitmapChunkPage completePage = combineBitmapChunks(fragments, revsToRestore, storageEngineReader);
    
    // Create modified page based on versioning strategy
    BitmapChunkPage modifiedPage;
    if (isFullDump) {
      // Full snapshot - copy the complete page
      modifiedPage = completePage.copyAsFull(currentRevision);
    } else {
      // Delta mode - create empty delta for tracking changes
      modifiedPage = BitmapChunkPage.createEmptyDelta(
          pageKey, currentRevision, indexType, rangeStart, rangeEnd);
    }
    
    PageContainer container = PageContainer.getInstance(completePage, modifiedPage);
    log.put(reference, container);
    return container;
  }

  /**
   * Determine if a full snapshot should be stored for bitmap chunks.
   *
   * <p>Strategy-specific logic:</p>
   * <ul>
   *   <li>FULL: Always returns true</li>
   *   <li>INCREMENTAL: Returns true when chain length >= revsToRestore - 1</li>
   *   <li>DIFFERENTIAL: Returns true when currentRevision % revsToRestore == 0</li>
   *   <li>SLIDING_SNAPSHOT: Same as INCREMENTAL with window-based GC</li>
   * </ul>
   *
   * @param fragments existing fragments (for chain length check)
   * @param currentRevision the current revision
   * @param revsToRestore the threshold
   * @return true if full snapshot should be stored
   */
  public boolean shouldStoreBitmapFullSnapshot(
      final List<BitmapChunkPage> fragments,
      final int currentRevision,
      final int revsToRestore) {
    
    // First revision is always full
    if (currentRevision == 1 || fragments.isEmpty()) {
      return true;
    }
    
    return switch (this) {
      case FULL -> true;
      case DIFFERENTIAL -> currentRevision % revsToRestore == 0;
      case INCREMENTAL, SLIDING_SNAPSHOT -> fragments.size() >= revsToRestore - 1;
    };
  }

  /**
   * Allocate a new page key.
   * This is a placeholder - actual implementation uses RevisionRootPage.
   */
  private long allocateNewPageKey(StorageEngineReader storageEngineReader) {
    // TODO: Integrate with RevisionRootPage page key allocation
    return System.nanoTime(); // Temporary unique key
  }
}


