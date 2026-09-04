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
import io.sirix.index.IndexType;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.node.interfaces.DataRecord;
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
import java.util.concurrent.atomic.LongAdder;

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
      assert noFragmentIsLazy(pages);
      var firstPage = pages.getFirst();
      T completePage = firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), storageEngineReader);

      final KeyValueLeafPage srcKvl = (KeyValueLeafPage) firstPage;
      final KeyValueLeafPage dstKvl = (KeyValueLeafPage) completePage;
      dstKvl.ensureSlottedPage();

      // Use populatedSlots() for O(k) bitmap-driven iteration
      final int[] populated = srcKvl.populatedSlots();
      for (final int i : populated) {
        copySlotPreservingMetadata(srcKvl, dstKvl, i, srcKvl.getSlotNodeKindId(i), null);
      }

      // Overflow records (> MAX_RECORD_SIZE) live as page REFERENCES, not slots — they must
      // be carried over too. FULL pages are self-contained (readers never consult older
      // fragments), so omitting this dropped every overflow record from the combined page.
      for (final Entry<Long, PageReference> entry : firstPage.referenceEntrySet()) {
        copyOverflowCarrier(firstPage, completePage, entry.getKey(), entry.getValue());
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
      assert noFragmentIsLazy(pages);
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
        copySlotPreservingMetadata(srcKvl, dstKvl, i, srcKvl.getSlotNodeKindId(i), null);
      }

      // Overflow records (> MAX_RECORD_SIZE) live as page REFERENCES, not slots. Unlike the
      // DIFFERENTIAL/INCREMENTAL/SLIDING_SNAPSHOT combines, this branch had NO reference copy:
      // modifying ANY record on a FULL page that also held an untouched >512-byte encoded record
      // produced a new self-contained page WITHOUT that record's reference — silently and
      // permanently absent from the new revision onward (and unreadable within the writing
      // transaction itself).
      for (final Entry<Long, PageReference> entry : firstPage.referenceEntrySet()) {
        copyOverflowCarrier(firstPage, modifiedPage, entry.getKey(), entry.getValue());
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
      return new int[] {previousRevision};
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
      assert noFragmentIsLazy(pages);
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T pageToReturn = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);

      final T latest = pages.get(0);
      final T fullDump = pages.size() == 1
          ? pages.get(0)
          : pages.get(1);

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
          copySlotPreservingMetadata(latestKvp, returnKvp, offset, latestKvp.getSlotNodeKindId(offset), null);
        }

        for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
          copyOverflowCarrier(latest, pageToReturn, entry.getKey(), entry.getValue());
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
        copySlotPreservingMetadata(latestKvp, returnKvp, offset, latestKvp.getSlotNodeKindId(offset), latestCopier);
      }

      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        copyOverflowCarrier(latest, pageToReturn, entry.getKey(), entry.getValue());
      }

      if (returnKvp.populatedSlotCount() < Constants.NDP_NODE_COUNT) {
        final KeyValueLeafPage fullDumpKvp = (KeyValueLeafPage) fullDump;
        final FsstAwareSlotCopier fullDumpCopier = new FsstAwareSlotCopier(fullDumpKvp.getFsstSymbolTable());
        final long[] filledBitmap = returnKvp.getSlotBitmap();
        // Copying/decompressing a latest inline slot can itself spill on the bounded target frame.
        // Consult the post-copy TARGET, not merely the source fragment, or an older full-dump slot
        // can overwrite that newly installed overflow carrier and resurrect a stale value.
        final boolean targetHasReferences = !returnKvp.getReferencesMap().isEmpty();

        final int[] fullDumpSlots = fullDumpKvp.populatedSlots();
        for (int i = 0; i < fullDumpSlots.length; i++) {
          final int offset = fullDumpSlots[i];
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (targetHasReferences && slotShadowedByNewerOverflowReference(pageToReturn, recordPageKey, offset)) {
            continue;
          }

          copySlotPreservingMetadata(fullDumpKvp, returnKvp, offset, fullDumpKvp.getSlotNodeKindId(offset),
              fullDumpCopier);
        }

        for (final Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (referenceShadowedByNewerInlineSlot(returnKvp, filledBitmap, entry.getKey())) {
            continue;
          }
          if (pageToReturn.getPageReference(entry.getKey()) == null) {
            copyOverflowCarrier(fullDump, pageToReturn, entry.getKey(), entry.getValue());
          }
        }
      }

      // Target holds merged content from multiple fragments. Rebuild the PAX
      // number region from the combined slotted heap — a donor shortcut from
      // any single fragment would miss values contributed by the others.
      returnKvp.ensureColumnRegions();
      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader,
        final PageReference reference, final TransactionIntentLog log) {
      assert pages.size() <= 2;
      assert noFragmentIsLazy(pages);
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final int revision = storageEngineReader.getUberPage().getRevisionNumber();

      // Update pageFragments on original reference
      final List<PageFragmentKey> pageFragmentKeys = List.of(new PageFragmentKeyImpl(firstPage.getRevision(),
          reference.getKey(), (int) storageEngineReader.getDatabaseId(), (int) storageEngineReader.getResourceId()));
      reference.setPageFragments(pageFragmentKeys);

      final T completePage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);
      final T modifiedPage = firstPage.newInstance(recordPageKey, firstPage.getIndexType(), storageEngineReader);

      final T latest = firstPage;
      final T fullDump = pages.size() == 1
          ? firstPage
          : pages.get(1);
      final boolean isFullDumpRevision = revision % revToRestore == 0;

      final KeyValueLeafPage latestKvp = (KeyValueLeafPage) latest;
      final KeyValueLeafPage completeKvp = (KeyValueLeafPage) completePage;
      final KeyValueLeafPage modifiedKvp = (KeyValueLeafPage) modifiedPage;
      completeKvp.ensureSlottedPage();
      modifiedKvp.ensureSlottedPage();

      final boolean singleFragment = pages.size() == 1;
      final FsstAwareSlotCopier latestCopier = singleFragment
          ? null
          : new FsstAwareSlotCopier(latestKvp.getFsstSymbolTable());

      // Copy all populated slots from latest to completePage using bitmap iteration.
      // For modifiedPage: use lazy copy — mark for preservation, actual copy deferred to commit time.
      final int[] latestSlots = latestKvp.populatedSlots();
      for (int i = 0; i < latestSlots.length; i++) {
        final int offset = latestSlots[i];
        copySlotPreservingMetadata(latestKvp, completeKvp, offset, latestKvp.getSlotNodeKindId(offset), latestCopier);
        modifiedKvp.markSlotForPreservation(offset);
      }

      for (final Map.Entry<Long, PageReference> entry : latest.referenceEntrySet()) {
        copyOverflowCarrier(latest, completePage, entry.getKey(), entry.getValue());
        copyOverflowCarrier(latest, modifiedPage, entry.getKey(), entry.getValue());
      }

      if (completeKvp.populatedSlotCount() < Constants.NDP_NODE_COUNT && pages.size() == 2) {
        final KeyValueLeafPage fullDumpKvp = (KeyValueLeafPage) fullDump;
        final FsstAwareSlotCopier fullDumpCopier = new FsstAwareSlotCopier(fullDumpKvp.getFsstSymbolTable());
        final long[] filledBitmap = completeKvp.getSlotBitmap();
        // copySlotPreservingMetadata can create a target-only overflow carrier while decoding or
        // appending the latest fragment. The shadow guard must observe that resulting target state.
        final boolean targetHasReferences = !completeKvp.getReferencesMap().isEmpty();

        final int[] fullDumpSlots = fullDumpKvp.populatedSlots();
        for (int j = 0; j < fullDumpSlots.length; j++) {
          final int offset = fullDumpSlots[j];
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (targetHasReferences && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset)) {
            continue;
          }

          copySlotPreservingMetadata(fullDumpKvp, completeKvp, offset, fullDumpKvp.getSlotNodeKindId(offset),
              fullDumpCopier);

          if (isFullDumpRevision) {
            modifiedKvp.markSlotForPreservation(offset);
          }
        }

        for (final Map.Entry<Long, PageReference> entry : fullDump.referenceEntrySet()) {
          if (referenceShadowedByNewerInlineSlot(completeKvp, filledBitmap, entry.getKey())) {
            continue;
          }
          final PageReference winningReference = completePage.getPageReference(entry.getKey());
          if (winningReference == null) {
            copyOverflowCarrier(fullDump, completePage, entry.getKey(), entry.getValue());
          }

          // Mirror only the carrier which actually won in the complete page. In particular, decoding
          // a newer inline FSST slot can create a target-only side/reference carrier; an older full-
          // dump reference must not then be published into the new self-contained fragment merely
          // because modifiedPage is still empty at this key.
          final boolean sourceCarrierWon = winningReference == null || winningReference == entry.getValue();
          if (isFullDumpRevision && sourceCarrierWon && modifiedPage.getPageReference(entry.getKey()) == null) {
            copyOverflowCarrier(fullDump, modifiedPage, entry.getKey(), entry.getValue());
          }
        }
      }

      // Single-fragment: completePage is a byte-copy of latest and can safely
      // inherit the FSST table; the modified page must share the binding (see
      // propagateFsstSymbolTable's javadoc). Multi-fragment: completePage has
      // uncompressed strings from decompress-on-merge — no table to propagate.
      if (singleFragment) {
        propagateFsstSymbolTable(firstPage, completePage);
        propagateFsstSymbolTable(firstPage, modifiedPage);
      }

      modifiedKvp.setCompletePageRef(completeKvp);

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
      log.put(reference, pageContainer); // TIL will remove from caches before mutating
      return pageContainer;
    }

    @Override
    public int[] getRevisionRoots(int previousRevision, int revsToRestore) {
      final int revisionsToRestore = previousRevision % revsToRestore;
      final int lastFullDump = previousRevision - revisionsToRestore;
      if (lastFullDump == previousRevision) {
        return new int[] {lastFullDump};
      } else {
        return new int[] {previousRevision, lastFullDump};
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
      assert noFragmentIsLazy(pages);
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final T pageToReturn =
          firstPage.newInstance(firstPage.getPageKey(), firstPage.getIndexType(), storageEngineReader);

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
        final FsstAwareSlotCopier copier = singleFragment
            ? null
            : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(pageToReturn, recordPageKey, offset)) {
            continue;
          }

          copySlotPreservingMetadata(kvPage, returnPage, offset, kvPage.getSlotNodeKindId(offset), copier);
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          filledSlotCount++;

          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
          final Long key = entry.getKey();
          if (referenceShadowedByNewerInlineSlot(returnPage, filledBitmap, key)) {
            continue;
          }
          if (pageToReturn.getPageReference(key) == null) {
            copyOverflowCarrier(page, pageToReturn, key, entry.getValue());
            claimedReferences++;
          }
        }
      }

      if (singleFragment) {
        // Bit-identical copy: FSST table propagation + donor PAX region shortcut.
        propagateFsstSymbolTable(firstPage, pageToReturn);
        returnPage.ensureNumberRegion((KeyValueLeafPage) firstPage);
      } else {
        // Merged content from multiple fragments — decompress-on-merge already made every string
        // slot uncompressed; the target intentionally carries no FSST table. Rebuild the column
        // regions from the combined heap so the page can still be served columnar afterwards.
        returnPage.ensureColumnRegions();
      }

      return pageToReturn;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader,
        PageReference reference, final TransactionIntentLog log) {
      assert noFragmentIsLazy(pages);
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(firstPage.getRevision(), reference.getKey(),
          (int) storageEngineReader.getDatabaseId(), (int) storageEngineReader.getResourceId()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size(); i < previousRefKeysSize
          && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
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
        final FsstAwareSlotCopier copier = singleFragment
            ? null
            : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset)) {
            continue;
          }

          copySlotPreservingMetadata(kvPage, completeKvp, offset, kvPage.getSlotNodeKindId(offset), copier);
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          filledSlotCount++;

          if (isFullDump) {
            modifiedKvp.markSlotForPreservation(offset);
          }

          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
          final Long key = entry.getKey();
          assert key != null;
          if (referenceShadowedByNewerInlineSlot(completeKvp, filledBitmap, key)) {
            continue;
          }
          final PageReference winningReference = completePage.getPageReference(key);
          if (winningReference == null) {
            copyOverflowCarrier(page, completePage, key, entry.getValue());
          }
          claimedReferences++;

          // copySlotPreservingMetadata may already have installed this source fragment's descriptor
          // companion on completePage. Mirror that exact winner, but never an older carrier shadowed
          // by a target-only reference created while decoding a newer inline slot.
          final boolean sourceCarrierWon = winningReference == null || winningReference == entry.getValue();
          if (isFullDump && sourceCarrierWon && modifiedPage.getPageReference(key) == null) {
            copyOverflowCarrier(page, modifiedPage, key, entry.getValue());
          }
        }
      }

      if (singleFragment) {
        propagateFsstSymbolTable(firstPage, completePage);
        // Both incremental shapes (full dump via preservation marks, plain delta via
        // prepareRecordForModification) raw-copy compressed slots into the modified page —
        // it must share the binding; see propagateFsstSymbolTable's javadoc.
        propagateFsstSymbolTable(firstPage, modifiedPage);
      }

      if (isFullDump) {
        modifiedKvp.setCompletePageRef(completeKvp);
      }

      final var pageContainer = PageContainer.getInstance(completePage, modifiedPage);
      log.put(reference, pageContainer); // TIL will remove from caches before mutating
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
      assert noFragmentIsLazy(pages);
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
          copyOverflowCarrier(firstPage, returnVal, e.getKey(), e.getValue());
        }
        return returnVal;
      }

      final long slotCopyStart = COMBINE_DIAG
          ? System.nanoTime()
          : 0L;

      // Seed from the newest fragment with ONE bulk copy instead of re-appending its slots one at
      // a time. The newest fragment wins every slot it holds — that is exactly what the
      // filled-bitmap loop below would decide for it — so copying its whole slotted page first is
      // the same answer arrived at by a memcpy rather than by ~1,000 per-slot heap appends, each of
      // which cost ~950 ns (measured: 510 ms of a cold scan's 632 ms of merge time).
      //
      // Only when the seed's slots stay readable afterwards: the target inherits the seed's FSST
      // symbol table, so its still-compressed slots resolve. Fragments merged in afterwards are
      // decompressed against THEIR OWN table exactly as before, so a page assembled from fragments
      // bound to different tables stays correct.
      final KeyValueLeafPage seed = (KeyValueLeafPage) firstPage;
      final boolean bulkSeed = seed.getSlottedPage() != null;
      int startIndex = 0;
      if (bulkSeed) {
        returnKvp.copySlottedPageFrom(seed);
        propagateFsstSymbolTable(firstPage, returnVal);
        // copySlottedPageFrom includes each slot's inline DeweyID trailer; no per-slot replay.
        for (final Entry<Long, PageReference> e : firstPage.referenceEntrySet()) {
          copyOverflowCarrier(firstPage, returnVal, e.getKey(), e.getValue());
        }
        startIndex = 1;
      }

      // Track which slots are already filled using bitmap from returnVal
      // This enables O(k) iteration instead of O(1024)
      returnKvp.ensureSlottedPage();
      final long[] filledBitmap = returnKvp.getSlotBitmap();

      // Track slot count incrementally - CRITICAL: don't call populatedSlotCount() in loop
      int filledSlotCount = bulkSeed
          ? returnKvp.getCachedPopulatedCount()
          : 0;
      // Overflow references claimed so far — fast guard for the large-value shadow check (#1076).
      int claimedReferences = bulkSeed
          ? returnKvp.getReferencesMap().size()
          : 0;

      for (int pageIndex = startIndex; pageIndex < pages.size(); pageIndex++) {
        final T page = pages.get(pageIndex);
        assert page.getPageKey() == recordPageKey;
        if (filledSlotCount == Constants.NDP_NODE_COUNT) {
          break;
        }

        final KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(returnVal, recordPageKey, offset)) {
            continue;
          }

          copySlotPreservingMetadata(kvPage, returnKvp, offset, kvPage.getSlotNodeKindId(offset), copier);
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          filledSlotCount++;

          if (filledSlotCount == Constants.NDP_NODE_COUNT) {
            break;
          }
        }

        for (final Entry<Long, PageReference> entry : page.referenceEntrySet()) {
          final Long key = entry.getKey();
          if (referenceShadowedByNewerInlineSlot(returnKvp, filledBitmap, key)) {
            continue;
          }
          if (returnVal.getPageReference(key) == null) {
            copyOverflowCarrier(page, returnVal, key, entry.getValue());
            claimedReferences++;
          }
        }
      }
      if (COMBINE_DIAG) {
        SLOT_COPY_NANOS.add(System.nanoTime() - slotCopyStart);
        SLOTS_COPIED.add(filledSlotCount);
      }

      // Rebuild the page's column regions from the combined slotted heap. Not just the numeric
      // one: a reconstructed page that carries no field-name column cannot be served by a column
      // scan at all, so it would fall back to its records on every future query — the reason it
      // was reconstructed in the first place, made permanent.
      final long regionStart = COMBINE_DIAG
          ? System.nanoTime()
          : 0L;
      returnKvp.ensureColumnRegions();
      if (COMBINE_DIAG) {
        REGION_REBUILD_NANOS.add(System.nanoTime() - regionStart);
      }

      return returnVal;
    }

    @Override
    public <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
        final List<T> pages, final int revToRestore, final StorageEngineReader storageEngineReader,
        final PageReference reference, final TransactionIntentLog log) {
      assert noFragmentIsLazy(pages);
      final T firstPage = pages.getFirst();
      final long recordPageKey = firstPage.getPageKey();
      final var previousPageFragmentKeys = new ArrayList<PageFragmentKey>(reference.getPageFragments().size() + 1);
      previousPageFragmentKeys.add(new PageFragmentKeyImpl(firstPage.getRevision(), reference.getKey(),
          (int) storageEngineReader.getDatabaseId(), (int) storageEngineReader.getResourceId()));
      for (int i = 0, previousRefKeysSize = reference.getPageFragments().size(); i < previousRefKeysSize
          && previousPageFragmentKeys.size() < revToRestore - 1; i++) {
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
      final long[] inWindowBitmap = new long[16]; // 16 * 64 = 1024 bits

      final KeyValueLeafPage completeKvp = (KeyValueLeafPage) completePage;
      final KeyValueLeafPage modifyingKvp = (KeyValueLeafPage) modifyingPage;
      completeKvp.ensureSlottedPage();
      modifyingKvp.ensureSlottedPage();
      final long[] filledBitmap = completeKvp.getSlotBitmap();

      final boolean hasOutOfWindowPage = (pages.size() == revToRestore);
      final int lastInWindowIndex = hasOutOfWindowPage
          ? pages.size() - 2
          : pages.size() - 1;

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
        final FsstAwareSlotCopier copier = singleFragment
            ? null
            : new FsstAwareSlotCopier(kvPage.getFsstSymbolTable());
        final int[] populatedSlots = kvPage.populatedSlots();

        for (final int offset : populatedSlots) {
          inWindowBitmap[offset >>> 6] |= (1L << (offset & 63));

          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) != 0) {
            continue;
          }
          if (claimedReferences > 0 && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset)) {
            continue;
          }

          copySlotPreservingMetadata(kvPage, completeKvp, offset, kvPage.getSlotNodeKindId(offset), copier);
          filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          filledSlotCount++;

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
          if (referenceShadowedByNewerInlineSlot(completeKvp, filledBitmap, key)) {
            continue;
          }
          if (completePage.getPageReference(key) == null) {
            copyOverflowCarrier(page, completePage, key, entry.getValue());
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
          if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) == 0 && !(claimedReferences > 0
              && slotShadowedByNewerOverflowReference(completePage, recordPageKey, offset))) {
            copySlotPreservingMetadata(outOfWindowKvp, completeKvp, offset, outOfWindowKvp.getSlotNodeKindId(offset),
                outCopier);
            filledBitmap[offset >>> 6] |= (1L << (offset & 63));
          }

          if ((inWindowBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
            modifyingKvp.markSlotForPreservation(offset);
          }
        }

        for (final Entry<Long, PageReference> entry : outOfWindowPage.referenceEntrySet()) {
          final Long key = entry.getKey();
          if (referenceShadowedByNewerInlineSlot(completeKvp, filledBitmap, key)) {
            continue;
          }
          if (completePage.getPageReference(key) == null) {
            copyOverflowCarrier(outOfWindowPage, completePage, key, entry.getValue());
          }
          // Only an out-of-window reference NO in-window fragment shadows may enter the NEW
          // fragment. copySlotPreservingMetadata may already have installed the companion on
          // completePage, so the modifying-page publication is intentionally independent of the
          // complete-page null check.
          final int refOffset = StorageEngineReader.recordPageOffset(key);
          if ((inWindowBitmap[refOffset >>> 6] & (1L << (refOffset & 63))) == 0
              && modifyingPage.getPageReference(key) == null) {
            copyOverflowCarrier(outOfWindowPage, modifyingPage, key, entry.getValue());
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
      log.put(reference, pageContainer); // TIL will remove from caches before mutating
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
   * Large-value shadowing between page fragments (#1076): within one fragment a record lives EITHER
   * in a slot OR in an overflow reference, and fragments are merged newest-first. A slot in an OLDER
   * fragment is stale when a NEWER fragment already moved the record to overflow storage; without
   * this check the stale slot wins on read (slots have lookup priority) and the record's old value
   * resurrects.
   *
   * @param target the combine target holding references claimed by newer fragments
   * @param recordPageKey the record page key
   * @param offset the slot offset of the record within the page
   * @return {@code true} if a newer fragment claimed this record as an overflow reference
   */
  private static boolean slotShadowedByNewerOverflowReference(final KeyValuePage<?> target, final long recordPageKey,
      final int offset) {
    return target.getPageReference((recordPageKey << Constants.NDP_NODE_COUNT_EXPONENT) + offset) != null;
  }

  /**
   * Counterpart of {@link #slotShadowedByNewerOverflowReference(KeyValuePage, long, int)}: an
   * overflow reference in an OLDER fragment is stale when a NEWER fragment stored the record inline
   * again (the value shrank below the overflow threshold). A populated DeweyID-only slot does not
   * shadow its colocated overflow reference: its record-only length is zero.
   *
   * @param filledBitmap the bitmap of slot offsets claimed by newer fragments
   * @param recordKey the record key of the overflow reference
   * @return {@code true} if a newer fragment claimed this record as an inline record
   */
  private static boolean referenceShadowedByNewerInlineSlot(final KeyValueLeafPage target, final long[] filledBitmap,
      final long recordKey) {
    final int offset = StorageEngineReader.recordPageOffset(recordKey);
    if ((filledBitmap[offset >>> 6] & (1L << (offset & 63))) == 0) {
      return false;
    }
    final MemorySegment slottedPage = target.getSlottedPage();
    return slottedPage != null && PageLayout.getRecordOnlyLength(slottedPage, offset) > 0
        && !target.isFusedOverflowDescriptor(offset);
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
   * Method to reconstruct a complete {@link KeyValuePage} with the help of partly filled pages plus a
   * revision-delta which determines the necessary steps back.
   *
   * @param pages the base of the complete {@link KeyValuePage}
   * @param revsToRestore the number of revisions needed to build the complete record page
   * @return the complete {@link KeyValuePage}
   */
  /**
   * Split of a versioned page reconstruction into its two halves — the per-slot copy loop and the PAX
   * region rebuild — off unless {@code -Dsirix.versioning.diag=true}. Reconstruction is the one part
   * of a cold analytical scan that still works on the row representation, so knowing which half costs
   * what decides whether the fix is a faster merge or a columnar one.
   */
  private static final boolean COMBINE_DIAG = Boolean.getBoolean("sirix.versioning.diag");

  private static final LongAdder SLOT_COPY_NANOS = new LongAdder();
  private static final LongAdder REGION_REBUILD_NANOS = new LongAdder();
  private static final LongAdder SLOTS_COPIED = new LongAdder();

  /** CPU nanos in the per-slot copy loop of multi-fragment combines. */
  public static long combineSlotCopyNanos() {
    return SLOT_COPY_NANOS.sum();
  }

  /** CPU nanos rebuilding the PAX number region after a multi-fragment combine. */
  public static long combineRegionRebuildNanos() {
    return REGION_REBUILD_NANOS.sum();
  }

  /** Slots copied by multi-fragment combines. */
  public static long combineSlotsCopied() {
    return SLOTS_COPIED.sum();
  }

  public static void resetCombineDiag() {
    SLOT_COPY_NANOS.reset();
    REGION_REBUILD_NANOS.reset();
    SLOTS_COPIED.reset();
  }

  /**
   * Whether every fragment about to be combined already holds its records.
   *
   * <p>
   * Not a gate — combine reaches the heap only through accessors that expand for themselves, so a
   * lazy fragment here would produce the right answer. It would just produce it the expensive way,
   * one chunk at a time, on a path that reads every slot of every fragment and was promised eager
   * pages by the load policy (plan amendment A6). An assertion rather than a check because the thing
   * it guards is a policy decision made elsewhere: if that decision regresses, the tests should say
   * so, and production should not pay a branch per combine to find out.
   */
  private static <V extends DataRecord, T extends KeyValuePage<V>> boolean noFragmentIsLazy(final List<T> pages) {
    for (final T page : pages) {
      if (page instanceof KeyValueLeafPage kvlPage && !kvlPage.isFullyMaterialized()) {
        throw new AssertionError("fragment " + kvlPage.getPageKey() + " reached a combine with " + kvlPage.chunkCount()
            + " chunks still unexpanded — the load policy handed a lazily loaded"
            + " page to a consumer that reads all of it");
      }
    }
    return true;
  }

  public abstract <V extends DataRecord, T extends KeyValuePage<V>> T combineRecordPages(final List<T> pages,
      final int revsToRestore, final StorageEngineReader storageEngineReader);

  /**
   * Method to reconstruct a complete {@link KeyValuePage} for reading as well as a
   * {@link KeyValuePage} for serializing with the nodes to write.
   *
   * @param pages the base of the complete {@link KeyValuePage}
   * @param revsToRestore the revisions needed to build the complete record page
   * @return a {@link PageContainer} holding a complete {@link KeyValuePage} for reading and one for
   *         writing
   */
  public abstract <V extends DataRecord, T extends KeyValuePage<V>> PageContainer combineRecordPagesForModification(
      final List<T> pages, final int revsToRestore, final StorageEngineReader storageEngineReader,
      final PageReference reference, final TransactionIntentLog log);

  /**
   * Get all revision root page numbers which are needed to restore a {@link KeyValuePage}.
   *
   * @param previousRevision the previous revision
   * @param revsToRestore number of revisions to restore
   * @return revision root page numbers needed to restore a {@link KeyValuePage}
   */
  public abstract int[] getRevisionRoots(final int previousRevision, final int revsToRestore);

  /**
   * Copy one logical overflow carrier. A reference-only record has no side slot; a dense-page fused
   * record additionally carries a projection-visible slot image which must version atomically with
   * that reference. Keeping this pairing here prevents one versioning strategy from silently dropping
   * the side half while the others remain correct.
   */
  private static void copyOverflowCarrier(final KeyValuePage<?> sourcePage, final KeyValuePage<?> targetPage,
      final long recordKey, final PageReference reference) {
    targetPage.setPageReference(recordKey, reference);
    if (sourcePage instanceof KeyValueLeafPage sourceKvl && targetPage instanceof KeyValueLeafPage targetKvl) {
      final int slot = StorageEngineReader.recordPageOffset(recordKey);
      if (sourceKvl.hasSideSlot(slot)) {
        targetKvl.copySideSlotFrom(sourceKvl, slot);
      }
    }
  }

  /**
   * Propagate an FSST symbol table from a single-fragment source page to the target. <b>Callers must
   * only invoke this in the single-fragment combine path</b> (i.e. when the target is a
   * byte-identical copy of the source), so every compressed slot on the target was encoded with the
   * propagated table.
   *
   * <p>
   * For multi-fragment combines, do not call this — use the decompress-on- merge path (see
   * {@link #copySlotPreservingMetadata}) instead, which rewrites each compressed slot to its
   * uncompressed form so the target correctly carries {@code fsstSymbolTable = null}.
   *
   * <p>
   * In the modification combines, the MODIFIED page needs this binding just as much as the complete
   * page: still-compressed slots from the bound complete page reach it later by raw copy — via
   * preservation marks ({@code addReferences} at commit) or via {@code prepareRecordForModification}
   * the moment any record on the page is modified — and an unbound modified page would be free to
   * bind to a NEWER table (insert-time or distribution) and serialize those old-table bytes under the
   * wrong claim, or to no table at all, leaving them undecodable.
   *
   * @param sourcePage the single-fragment source page
   * @param targetPage the target page to set the symbol table on
   */
  protected static <V extends DataRecord, T extends KeyValuePage<V>> void propagateFsstSymbolTable(final T sourcePage,
      final T targetPage) {
    if (sourcePage instanceof KeyValueLeafPage sourceKvp && targetPage instanceof KeyValueLeafPage targetKvp) {
      final byte[] fsstSymbolTable = sourceKvp.getFsstSymbolTable();
      if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
        targetKvp.setFsstSymbolTable(fsstSymbolTable);
      }
      // The reference travels too. A fragment fresh off disk may carry only the dictionary id —
      // the table is fetched lazily on the first string read — and a target that lost the id
      // would hold compressed string bytes with nothing left to say which symbols they were
      // encoded against.
      final long fsstSymbolTableId = sourceKvp.getFsstSymbolTableId();
      if (fsstSymbolTableId != KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID) {
        targetKvp.setFsstSymbolTableId(fsstSymbolTableId);
      }
    }
  }

  /**
   * Copy a single slot from {@code src} to {@code dst} during a multi-fragment combine. If
   * {@code src} holds an FSST symbol table and the slot is a string-kind slot whose compressed-flag
   * byte is {@code 1}, the payload is decoded through the source's table and the rewritten
   * uncompressed slot is stored on {@code dst}. All other slots (including uncompressed string slots)
   * are raw-copied.
   *
   * <p>
   * Using this helper across every fragment of a multi-fragment combine is the invariant that lets
   * the target page safely carry {@code fsstSymbolTable = null}. At the next commit the writer hands
   * the page the revision's pooled symbol table and {@code compressStringValues} re-encodes, so the
   * page lands on disk with a single coherent table — zero growth in disk footprint.
   *
   * @param src source fragment
   * @param dst target page being assembled
   * @param offset slot index (0-1023)
   * @param nodeKindId directory {@code nodeKindId} for the slot on the source
   * @param copier per-fragment copier carrying {@code src}'s parsed FSST table; may be {@code null}
   *        or inactive when the source has no table — callers commonly pass the same copier across
   *        the whole fragment loop to amortize the symbol-table parse
   */
  protected static void copySlotPreservingMetadata(final KeyValueLeafPage src, final KeyValueLeafPage dst,
      final int offset, final int nodeKindId, final FsstAwareSlotCopier copier) {
    // The fused overflow descriptor carries no FSST payload even when its source page has a table.
    // Raw-copy it together with its companion reference; sending marker 2 through the ordinary
    // "could not decompress" branch can falsely reject a safe cross-table fragment merge.
    if (src.isFusedOverflowDescriptor(offset)) {
      dst.copySlotFromPage(src, offset);
      return;
    }

    // A raw copy moves the record and its inline DeweyID trailer in one allocation. It also handles
    // the DeweyID-only slot used beside an overflow reference (record length zero, kind zero).
    if (copier == null || !copier.active()) {
      dst.copySlotFromPage(src, offset);
      return;
    }

    final MemorySegment slot = src.getSlot(offset);
    if (slot == null) {
      dst.copySlotFromPage(src, offset);
      return;
    }
    final byte[] rewritten = copier.decompressSlot(slot, nodeKindId);
    if (rewritten != null) {
      dst.copyDecompressedStringSlotFrom(src, offset, nodeKindId, rewritten);
      return;
    }
    // FsstAwareSlotCopier returns null only for a slot proved independent of the source table
    // (non-string or explicitly raw). Malformed table-dependent strings throw from the copier, so
    // this raw copy is safe even when SLIDING_SNAPSHOT's bulk seed bound the target to another table.
    dst.copySlotFromPage(src, offset);
  }

  // ===== HOT Leaf Page Combining Methods =====

  /**
   * Combine multiple HOT leaf page fragments into a single complete page.
   *
   * <p>
   * Cross-fragment merge happens by full key (not by entry index). Newer fragments take precedence;
   * index-type-specific tombstones (zero length for PROJECTION, the serializer's single-byte marker
   * for posting indexes) shadow older entries; missing keys are filled in from older fragments.
   * Strategy dispatch mirrors {@link #combineRecordPages(List, int, StorageEngineReader)} for
   * {@link KeyValueLeafPage}.
   * </p>
   *
   * @param pages the list of HOT leaf page fragments (newest first)
   * @param revToRestore the maximum number of fragments to merge per the active strategy
   * @param storageEngineReader the storage engine reader
   * @return the combined HOT leaf page
   */
  public HOTLeafPage combineHOTLeafPages(final List<HOTLeafPage> pages, final int revToRestore,
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
      case DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT -> mergeHOTFragmentsByKey(pages);
    };
  }

  /**
   * Fragment-merge instrumentation, OFF unless {@code -Dsirix.hot.mergeDiag=true}, following the
   * {@link #COMBINE_DIAG} convention already used in this file.
   *
   * <h2>Why this is gated rather than always-on</h2>
   *
   * {@code mergeHOTFragmentsByKey} is the DEFAULT READ path — its single-fragment branch runs on
   * every versioned page read — and {@code carryForwardAgingHOTEntries} runs inside the COMMIT path
   * once per entry of an aging fragment (measured: 1018 entries in one ordinary commit). A shared
   * atomic on either is a contended cache line on a hot path, which is precisely what the performance
   * rules in CLAUDE.md forbid. The gate is a {@code static final} read of a non-constant, so the
   * branch is folded away entirely once C2 sees it: with diagnostics off these counters cost nothing
   * at all.
   *
   * <h2>And nothing shared is touched per iteration, even when ON</h2>
   *
   * The two loops accumulate into LOCALS and publish ONE add per merge / per rotation. So the
   * per-entry cost is a register increment rather than an atomic, and the counters keep their exact
   * values. Granularity of the STORAGE is per-call; granularity of the NUMBER is unchanged.
   *
   * <h2>The counters themselves</h2>
   *
   * This path had NO instrumentation at all, which is why a versioned-merge test once passed while
   * its merge counters read zero: a warm cache served already-merged pages, every assertion was
   * satisfied by the single-fragment path, and nothing could tell the difference. Any gate that
   * claims to exercise fragment reconstruction has to prove it ENTERED — an absence of failures
   * proves only that the code may never have run.
   *
   * <p>
   * {@link #completeDumpsWalkedPast()} is the sharp one. The merge loop below now TERMINATES at a
   * complete dump — a dump is a replacement snapshot, and walking past it could resurrect keys a
   * split relocated (task #57) — so this counter is structurally zero today. It is kept as a
   * permanent sentinel: if a future change removes or weakens that break, the counter goes nonzero
   * and the tests asserting on it point back at this investigation, instead of at a corrupt database
   * months later. Observability only — nothing here changes what the merge does.
   * </p>
   */
  private static final boolean HOT_MERGE_DIAG = Boolean.getBoolean("sirix.hot.mergeDiag");

  private static final LongAdder SINGLE_FRAGMENT_READS = new LongAdder();

  private static final LongAdder MULTI_FRAGMENT_MERGES = new LongAdder();

  private static final LongAdder FRAGMENTS_WALKED = new LongAdder();

  private static final LongAdder COMPLETE_DUMP_SHORT_CIRCUITS = new LongAdder();

  private static final LongAdder COMPLETE_DUMPS_WALKED_PAST = new LongAdder();

  private static final LongAdder CARRY_FORWARD_ROTATIONS = new LongAdder();

  private static final LongAdder CARRY_FORWARD_ENTRIES_REEMITTED = new LongAdder();

  /**
   * Whether merge diagnostics are collecting. A test that asserts on these counters MUST check this
   * first: with the gate off every counter reads zero, and "zero walked past a complete dump" from a
   * disabled instrument is indistinguishable from the same reading from a healthy one. Assert the
   * instrument is live, then assert on it.
   */
  public static boolean hotMergeDiagEnabled() {
    return HOT_MERGE_DIAG;
  }

  /** SLIDING_SNAPSHOT window rotations that ran the aging-entry carry-forward. */
  public static long carryForwardRotations() {
    return CARRY_FORWARD_ROTATIONS.sum();
  }

  /**
   * Entries the carry-forward re-emitted. A VOLUME COUNTER, NOT A DEFECT WITNESS — re-emitting live
   * entries that would otherwise age out is the carry-forward's job, and a healthy commit measures
   * over a thousand. Use it as a denominator, never as evidence that something went wrong.
   */
  public static long carryForwardEntriesReemitted() {
    return CARRY_FORWARD_ENTRIES_REEMITTED.sum();
  }

  /** Reads served by a single fragment — no reconstruction happened. */
  public static long singleFragmentReads() {
    return SINGLE_FRAGMENT_READS.sum();
  }

  /** Reads that actually reconstructed a page from a chain of fragments. */
  public static long multiFragmentMerges() {
    return MULTI_FRAGMENT_MERGES.sum();
  }

  /** Older fragments visited across all merges. */
  public static long fragmentsWalked() {
    return FRAGMENTS_WALKED.sum();
  }

  /** Merges answered wholly by a complete newest fragment. */
  public static long completeDumpShortCircuits() {
    return COMPLETE_DUMP_SHORT_CIRCUITS.sum();
  }

  /**
   * Older fragments walked THROUGH despite a fragment having declared the chain complete — the task
   * #57 precondition. Structurally zero while the merge loop's complete-dump break stands; it is kept
   * as a permanent sentinel so that if the break is ever removed or weakened, a test fails instead of
   * a database quietly corrupting.
   */
  public static long completeDumpsWalkedPast() {
    return COMPLETE_DUMPS_WALKED_PAST.sum();
  }

  /** Zero every merge counter; a counter that cannot be reset cannot witness a specific operation. */
  public static void resetFragmentMergeCounters() {
    SINGLE_FRAGMENT_READS.reset();
    MULTI_FRAGMENT_MERGES.reset();
    FRAGMENTS_WALKED.reset();
    COMPLETE_DUMP_SHORT_CIRCUITS.reset();
    COMPLETE_DUMPS_WALKED_PAST.reset();
    CARRY_FORWARD_ROTATIONS.reset();
    CARRY_FORWARD_ENTRIES_REEMITTED.reset();
  }

  /**
   * Merge HOT fragments by full key. Single newest-fragment fast path returns the page directly.
   * Multi-fragment path copies the newest, then walks older fragments inserting any keys absent from
   * the result until it reaches a complete dump. The first encountered value for each key wins,
   * including a tombstone: carrying that tombstone into the result is what prevents a still-older
   * live value from being resurrected on the next fragment.
   */
  private static HOTLeafPage mergeHOTFragmentsByKey(final List<HOTLeafPage> pages) {
    if (pages.size() == 1) {
      // THE DEFAULT READ PATH. Gated so it folds to nothing when diagnostics are off.
      if (HOT_MERGE_DIAG) {
        SINGLE_FRAGMENT_READS.increment();
      }
      return pages.getFirst();
    }

    final HOTLeafPage newest = pages.getFirst();
    if (newest.isCompleteDump()) {
      if (HOT_MERGE_DIAG) {
        COMPLETE_DUMP_SHORT_CIRCUITS.increment();
      }
      return newest;
    }
    if (HOT_MERGE_DIAG) {
      MULTI_FRAGMENT_MERGES.increment();
    }

    // Newest fragment is the base; copy() bulk-copies its entries and resets the dirty bitmap on
    // the result, so cross-fragment fills below are safely tracked as fresh writes if needed.
    final HOTLeafPage result = newest.copy();
    // The result is a freshly-merged read-only page — clear the slot-CoW link so it's treated as
    // a fully-materialized (no-completePageRef) leaf by any subsequent CoW.
    result.setCompletePageRef(null);
    result.clearDirtyBitmap();

    try {
      // TASK #57 SENTINEL — OBSERVATION ONLY. The break at the bottom of this loop is the guard: a
      // complete dump is a replacement snapshot, and walking past it could resurrect keys a split
      // relocated. The locals below count what the walk does, and walkedPastDump can only become
      // nonzero if a future change removes or weakens that break — at which point the counter goes
      // nonzero and the tests asserting on it point back here instead of at a corrupt database
      // months later. Accumulated in LOCALS and published once below, so the loop never touches
      // shared state.
      boolean pastACompleteDump = false;
      int walked = 0;
      int walkedPastDump = 0;
      for (int i = 1; i < pages.size(); i++) {
        final HOTLeafPage olderPage = pages.get(i);
        walked++;
        if (pastACompleteDump) {
          walkedPastDump++;
        }
        final int olderCount = olderPage.getEntryCount();
        for (int j = 0; j < olderCount; j++) {
          final byte[] key = olderPage.getKey(j);
          if (key == null) {
            throw new IllegalStateException(
                "HOT fragment merge cannot read key " + j + " of leaf " + olderPage.getPageKey());
          }
          final int existingIdx = result.findEntry(key);
          if (existingIdx >= 0) {
            // Newer fragment owns this key (possibly as a tombstone); skip older.
            continue;
          }
          // Add the entry — including tombstones — to the result. A tombstone in the older
          // fragment is the SHADOW that hides any non-tombstone value in still-older fragments;
          // skipping it would let the older value resurrect on the next iteration. Exact-copying
          // from the packed reference preserves projection's zero-length tombstone and fails closed
          // on an unreadable extent instead of silently converting corruption into a deletion.
          final byte[] value = olderPage.copyStoredValue(j);
          final boolean inserted = result.getIndexType() == IndexType.PROJECTION
              ? result.putOrReplace(key, value)
              : result.mergeWithNodeRefs(key, key.length, value, value.length);
          if (!inserted) {
            throw new IllegalStateException("HOT fragment merge cannot fit key from leaf " + olderPage.getPageKey()
                + " into leaf " + result.getPageKey());
          }
        }

        // A complete dump is a replacement snapshot, not another delta to layer on top of its
        // predecessors. It can occur in the middle of a chain after a leaf split: entries moved to
        // the right-hand leaf are absent from this page but still exist in older fragments for its
        // former range. Continuing past this boundary would merge those stale entries back into the
        // left-hand leaf and make them visible again.
        if (olderPage.isCompleteDump()) {
          // Arm the sentinel before breaking: if this break is ever removed, the next iteration is
          // exactly what walkedPastDump records.
          pastACompleteDump = true;
          break;
        }
      }

      // ONE publish per merge rather than one per fragment.
      if (HOT_MERGE_DIAG) {
        FRAGMENTS_WALKED.add(walked);
        COMPLETE_DUMPS_WALKED_PAST.add(walkedPastDump);
      }

      // Re-tighten the prefix after cross-fragment fills — the original combine path lacked this
      // step, leaving the merged leaf with a stale (potentially shorter) prefix from
      // handlePrefixForInsert. recomputePrefix is idempotent and a no-op when the prefix is already
      // tight.
      result.recomputePrefixForCombine();
      return result;
    } catch (final RuntimeException | Error failure) {
      try {
        result.close();
      } catch (final RuntimeException | Error cleanupFailure) {
        addSuppressedSafely(failure, cleanupFailure);
      }
      throw failure;
    }
  }

  /**
   * Copy-on-write a HOT leaf for modification: produce the sparse fragment to serialize this commit
   * and update {@code reference}'s fragment chain per this strategy. The HOT analogue of
   * {@link #combineRecordPagesForModification} for {@link KeyValueLeafPage} — the writer makes a
   * single polymorphic call and the enum owns the whole per-strategy CoW policy (chain bump + which
   * entries the sparse emit must re-materialize).
   *
   * <p>
   * Two intrinsic differences from the KVLP method, both inherent to HOT: it operates on the
   * already-combined, already-mutated live {@code hotLeaf} (HOT writes mutate the leaf in place
   * before CoW) rather than rebuilding from a fragment list; and it returns the modified leaf
   * directly, leaving the caller to register it in the transaction log.
   * </p>
   *
   * <p>
   * Policy by strategy:
   * </p>
   * <ul>
   * <li>FULL, or any strategy forced to rotate ({@code forceFullEmit}) —
   * {@link HOTLeafPage#markAllEntriesDirty()}: emit a complete dump so a reader reconstructs from a
   * fresh baseline.</li>
   * <li>SLIDING_SNAPSHOT at a window eviction — {@link #carryForwardAgingHOTEntries}: re-emit only
   * the entries whose newest copy sits in the fragment about to age out.</li>
   * <li>DIFFERENTIAL between full dumps — {@link #carryForwardDifferentialDelta}: re-emit the prior
   * cumulative delta's entries so this delta stays cumulative since the last full dump. (This
   * carry-forward is mandatory: a former, removed variant that skipped it lost every entry not
   * touched inside the 2-fragment read window.)</li>
   * <li>otherwise (window not yet full) — a plain sparse delta of just this commit's own
   * changes.</li>
   * </ul>
   *
   * <p>
   * Window fragments are loaded BEFORE {@link #bumpHOTPageFragmentChain} mutates the chain (the
   * carry-forward strategies read the pre-bump window, which the bump would otherwise drop), and
   * every loaded fragment except {@code hotLeaf} itself is closed before returning.
   * </p>
   *
   * @param hotLeaf the combined, already-mutated leaf to CoW; its revision identifies the current
   *        durable head fragment (see {@link HOTLeafPage#getRevision()})
   * @param revsToRestore the versioning window
   * @param storageEngineReader supplies the window fragments, the database / resource ids, and the
   *        advancing commit revision
   * @param reference the leaf reference whose fragment chain is updated in place
   * @return the modified (sparse) leaf to serialize this commit
   */
  public HOTLeafPage combineHOTLeafPagesForModification(final HOTLeafPage hotLeaf, final int revsToRestore,
      final StorageEngineReader storageEngineReader, final PageReference reference) {
    // Every strategy replaces (rather than mutates) the fragment list below. Keep the exact prior
    // object so any failed CoW attempt can put the writer-private reference back into its pre-call
    // state without allocating on the failure path. This matters even though the reference itself is
    // already private: callers may catch the failure, and cleanup/release can fail after the bump but
    // before the new leaf has transferred to the transaction-intent log.
    final List<PageFragmentKey> originalPageFragments = reference.getPageFragments();
    // Snapshot the window's fragments BEFORE the chain bump (which mutates the chain), for the two
    // carry-forward strategies that re-read prior fragments:
    // - SLIDING_SNAPSHOT rotation: carry the aging (about-to-drop) fragment's still-live entries.
    // - DIFFERENTIAL non-full-dump delta: re-emit the prior cumulative delta's entries so the new
    // delta stays cumulative (a length-1 chain already anchors the last full dump).
    final boolean slidingRotation = this == SLIDING_SNAPSHOT && hotSlidingSnapshotEvicts(reference, revsToRestore);
    final boolean differentialCumulative =
        this == DIFFERENTIAL && reference.getKey() >= 0 && !reference.getPageFragments().isEmpty();
    final List<HOTLeafPage> windowFragments = (slidingRotation || differentialCumulative)
        ? storageEngineReader.loadHOTLeafFragments(reference)
        : null;

    // Everything after the window is loaded must sit inside the try: the guards are already held
    // here, and both bumpHOTPageFragmentChain and hotLeaf.copyForRevision() can fail — copying
    // allocates a full page and throws OutOfMemoryError under allocator pressure. A throw before the
    // finally would leave the fragments guarded forever, and a permanently guarded entry is skipped
    // by every eviction path while still counting against the cache budget.
    HOTLeafPage modifiedLeaf = null;
    Throwable primaryFailure = null;
    try {
      // Capture the commit revision once. It stamps the new physical page and drives DIFFERENTIAL's
      // full-dump cadence; hotLeaf.getRevision() remains the actual revision of the prior physical
      // head recorded in every newly prepended PageFragmentKey.
      final int currentRevision = storageEngineReader.getRevisionNumber();
      final boolean forceFullEmit = bumpHOTPageFragmentChain(reference, hotLeaf.getRevision(), currentRevision,
          revsToRestore, storageEngineReader.getDatabaseId(), storageEngineReader.getResourceId());

      modifiedLeaf = hotLeaf.copyForRevision(currentRevision);
      if (this == FULL || forceFullEmit) {
        modifiedLeaf.markAllEntriesDirty();
      } else if (slidingRotation && windowFragments != null) {
        carryForwardAgingHOTEntries(windowFragments, modifiedLeaf);
      } else if (differentialCumulative && windowFragments != null && !windowFragments.isEmpty()) {
        carryForwardDifferentialDelta(windowFragments.getFirst(), modifiedLeaf);
      }
    } catch (final RuntimeException | Error failure) {
      primaryFailure = failure;
      reference.setPageFragments(originalPageFragments);
      retireModifiedHOTLeafAfterFailure(hotLeaf, modifiedLeaf, failure);
      throw failure;
    } finally {
      if (windowFragments != null) {
        // Chain fragments are guarded cache entries: release them, never close. hotLeaf belongs to
        // the caller and is never part of this window, but is passed as keepOpen defensively.
        try {
          storageEngineReader.releaseHOTLeafFragments(windowFragments, hotLeaf);
        } catch (final RuntimeException | Error releaseFailure) {
          if (primaryFailure != null) {
            addSuppressedSafely(primaryFailure, releaseFailure);
          } else {
            // A successful copy has not escaped to the caller yet. If releasing the borrowed
            // fragment window fails, this method still owns that copy and must retire it.
            reference.setPageFragments(originalPageFragments);
            retireModifiedHOTLeafAfterFailure(hotLeaf, modifiedLeaf, releaseFailure);
            throw releaseFailure;
          }
        }
      }
    }
    return modifiedLeaf;
  }

  /** Retire a locally owned CoW result without replacing the failure that prevented its return. */
  private static void retireModifiedHOTLeafAfterFailure(final HOTLeafPage sourceLeaf, final HOTLeafPage modifiedLeaf,
      final Throwable primaryFailure) {
    if (modifiedLeaf == null || modifiedLeaf == sourceLeaf) {
      return;
    }
    try {
      modifiedLeaf.retire();
    } catch (final RuntimeException | Error retirementFailure) {
      addSuppressedSafely(primaryFailure, retirementFailure);
    }
  }

  /** Keep the operation failure authoritative even for self/suppression-disabled throwables. */
  private static void addSuppressedSafely(final Throwable primary, final Throwable secondary) {
    if (primary == secondary) {
      return;
    }
    try {
      primary.addSuppressed(secondary);
    } catch (final RuntimeException | Error ignored) {
      // The operation failure remains authoritative.
    }
  }

  /**
   * Update the fragment chain on {@code reference} prior to the next CoW write of a HOT leaf. Mirrors
   * KVLP's chain bump at {@link #combineRecordPagesForModification} lines 254-259 / 458-470 /
   * 683-695, and additionally returns whether this commit must emit a full leaf (snapshot rotation).
   *
   * <p>
   * The chain is grown by prepending the prior on-disk offset; the result is bounded by the strategy.
   * FULL keeps no chain at all (every revision is a full dump). DIFFERENTIAL keeps exactly one entry.
   * INCREMENTAL and SLIDING_SNAPSHOT prepend up to {@code revToRestore - 1} entries. When the chain
   * would otherwise overflow, the chain is reset and the caller is told to force a full emit so
   * future readers can reconstruct from a fresh snapshot — without this the OLDEST keys would fall
   * off the chain and become unreadable.
   * </p>
   *
   * <p>
   * If {@code reference.getKey() < 0} the leaf was never persisted (no prior on-disk fragment).
   * Returns {@code false} and leaves the list untouched.
   * </p>
   *
   * @param reference the leaf reference (mutated)
   * @param priorFragmentRevision actual revision written in the prior on-disk fragment's header;
   *        every new {@link PageFragmentKeyImpl} records this value
   * @param currentRevision revision currently being committed; DIFFERENTIAL compares it with the
   *        actual last-full-dump revision stored on its anchor
   * @param revToRestore strategy-bounded chain length
   * @param databaseId the database id propagated into the new {@link PageFragmentKeyImpl}
   * @param resourceId the resource id propagated into the new {@link PageFragmentKeyImpl}
   * @return {@code true} if the caller must force a full emit at commit (chain rotated) — only
   *         possible under non-FULL strategies; {@code false} otherwise
   */
  public boolean bumpHOTPageFragmentChain(final PageReference reference, final int priorFragmentRevision,
      final int currentRevision, final int revToRestore, final long databaseId, final long resourceId) {
    if (this == FULL) {
      return false;
    }
    final long priorKey = reference.getKey();
    if (priorKey < 0) {
      return false;
    }
    final List<PageFragmentKey> existing = reference.getPageFragments();

    if (this == DIFFERENTIAL) {
      // Mirror KVLP DIFFERENTIAL (combineRecordPagesForModification lines 269-367): a periodic FULL
      // dump, and BETWEEN full dumps a CUMULATIVE delta anchored — via a length-1 chain — to the
      // last full dump. A read then combines exactly {newest cumulative delta, last full dump}. The
      // cumulative property is produced writer-side by carryForwardDifferentialDelta (the HOT
      // analogue of KVLP marking every latest slot for preservation). WITHOUT both, a 2-fragment
      // read loses any entry not re-emitted in the window.
      //
      // Cadence is by revision distance from the actual last full dump. The anchor's revision is
      // metadata for the physical page named by its key, never an unrelated cadence sentinel. A
      // period of revToRestore therefore admits revToRestore-1 cumulative deltas after a full dump
      // and emits the next full image at the same cadence as KVLP. Distance (not modulo) is used
      // because a HOT leaf is only committed when modified and may skip revisions. Once the anchor
      // exists, a long global-revision gap can therefore make the NEXT mutation rotate earlier in
      // leaf-write count than a continuously hot leaf. This is only a leaf-local serialization
      // decision (clear the chain and dirty the copied leaf); it never rebuilds the HOT subtree or
      // index.
      final boolean fullDumpRevision = revToRestore <= 1
          || (!existing.isEmpty() && (long) currentRevision - existing.getFirst().revision() >= revToRestore);
      if (fullDumpRevision) {
        reference.setPageFragments(List.of()); // this commit re-dumps the whole leaf; no chain
        return true;
      }
      if (existing.isEmpty()) {
        // The prior on-disk fragment IS the last full dump. Its key and revision must describe that
        // same physical image; currentRevision belongs exclusively to the new head being written.
        reference.setPageFragments(
            List.of(new PageFragmentKeyImpl(priorFragmentRevision, priorKey, databaseId, resourceId)));
      }
      // else: keep the existing chain (already the last-full-dump anchor); the prior on-disk fragment
      // is a cumulative delta that the writer re-emits, so do NOT advance the anchor to it.
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
        slidingNext.add(new PageFragmentKeyImpl(priorFragmentRevision, priorKey, databaseId, resourceId));
        for (int i = 0; i < slidingExistingSize && slidingNext.size() < chainCap; i++) {
          slidingNext.add(existing.get(i));
        }
      }
      reference.setPageFragments(slidingNext);
      assert slidingNext.size() <= chainCap
          : "sliding chain overflow: size=" + slidingNext.size() + " > chainCap=" + chainCap;
      return false;
    }

    // INCREMENTAL: bounded delta chain with a periodic full re-emit at rotation.
    if (existing.size() + 1 > chainCap) {
      reference.setPageFragments(List.of());
      return true;
    }

    final int existingSize = existing.size();
    final ArrayList<PageFragmentKey> next = new ArrayList<>(existingSize + 1);
    next.add(new PageFragmentKeyImpl(priorFragmentRevision, priorKey, databaseId, resourceId));
    for (int i = 0; i < existingSize && next.size() < chainCap; i++) {
      next.add(existing.get(i));
    }
    reference.setPageFragments(next);
    // Invariant: the post-bump chain length never exceeds chainCap. If it does, future readers
    // would walk fragments past the window and the rotation logic that depends
    // on overflow detection breaks. Enabled only with `-ea`.
    assert next.size() <= chainCap : "chain overflow: size=" + next.size() + " > chainCap=" + chainCap;
    return false;
  }

  /**
   * Whether the next SLIDING_SNAPSHOT commit on {@code reference} will evict the oldest fragment from
   * the window — i.e. the fragment chain is already at its cap, so prepending the current on-disk
   * fragment pushes the oldest out. When {@code true} the writer must carry that oldest fragment's
   * still-live entries forward ({@link #carryForwardAgingHOTEntries}) so they stay reachable after it
   * drops. Must be read BEFORE {@link #bumpHOTPageFragmentChain} mutates the chain.
   *
   * @param reference the HOT leaf reference
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
   * <p>
   * An entry of the oldest fragment is carried forward iff it is (a) <b>not a tombstone</b> — once a
   * tombstone becomes the oldest in-window fragment every value it shadowed is already out of the
   * window, so it has nothing left to shadow — and (b) <b>absent from every newer in-window
   * fragment</b>, because a newer fragment that still carries the key already keeps it reachable.
   * </p>
   *
   * @param fragmentsNewestFirst the window's raw fragments, newest first (as returned by
   *        {@link io.sirix.api.StorageEngineReader#loadHOTLeafFragments})
   * @param modifiedLeaf the writer's copy of the combined leaf; carried entries are marked dirty here
   *        so the sparse emit includes them
   */
  public static void carryForwardAgingHOTEntries(final List<HOTLeafPage> fragmentsNewestFirst,
      final HOTLeafPage modifiedLeaf) {
    final int fragmentCount = fragmentsNewestFirst.size();
    if (fragmentCount == 0) {
      return;
    }
    if (HOT_MERGE_DIAG) {
      CARRY_FORWARD_ROTATIONS.increment();
    }
    // Re-emissions accumulate in a LOCAL and publish once at the end: this loop runs once per entry
    // of the aging fragment on the DEFAULT COMMIT PATH — over a thousand iterations in an ordinary
    // commit — so it must not touch a shared counter per iteration.
    long reemitted = 0;
    final HOTLeafPage oldest = fragmentsNewestFirst.get(fragmentCount - 1);
    final boolean projectionValues = oldest.getIndexType() == IndexType.PROJECTION;
    final int oldestEntryCount = oldest.getEntryCount();
    for (int j = 0; j < oldestEntryCount; j++) {
      // Classify off the off-heap slice: getValue would copy the ENTIRE payload (a serialized
      // bitmap, or a projection descriptor up to the slot-value limit) out of off-heap memory just
      // to read one byte, once per entry of the aging fragment, on the default commit path.
      final MemorySegment value = oldest.getValueSlice(j);
      // Tombstones aging out need no preservation: anything they shadowed is already gone from the
      // window (older than the oldest fragment), so re-emitting them would only leak dead markers.
      // Projection values are opaque bytes and use the zero-length payload as their tombstone.
      // In particular, a live one-byte projection value may legitimately equal the posting
      // index's 0xFE wire marker and must still be carried forward when this fragment ages out.
      // Keep the classification allocation-free: both predicates inspect the resident slice.
      final boolean tombstone = projectionValues
          ? value.byteSize() == 0
          : NodeReferencesSerializer.isTombstone(value);
      if (tombstone) {
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
        // NOTE WHAT IS RE-EMITTED: the entry of modifiedLeaf — the CURRENT combined state — and
        // never the aging fragment's bytes. That is why carry-forward cannot resurrect a deleted
        // key: for a tombstoned key the current entry IS the tombstone, so marking it dirty
        // re-emits the DELETE. The shadowing test above is therefore an optimisation (skip work a
        // newer fragment already covers), not a correctness guard — verified by mutation, which
        // made it treat a newer tombstone as non-shadowing and changed only the re-emission count
        // (6->7, 18->19), never an answer.
        reemitted++;
        modifiedLeaf.markEntryDirty(idx);
      }
    }
    // ONE publish per rotation rather than one per entry.
    if (HOT_MERGE_DIAG) {
      CARRY_FORWARD_ENTRIES_REEMITTED.add(reemitted);
    }
  }

  /**
   * DIFFERENTIAL cumulative delta: mark on {@code modifiedLeaf} every entry the prior cumulative
   * delta carried, so this commit's new delta re-emits the FULL set of entries changed since the last
   * full dump — the HOT analogue of KVLP's "preserve every latest slot" (combineRecordPages-
   * ForModification line 316). {@code priorDelta} is the prior on-disk fragment (itself cumulative);
   * every one of its keys is re-emitted with the modified leaf's CURRENT value, so a 2-fragment read
   * of {this delta, last full dump} recovers the complete state.
   *
   * <p>
   * Tombstones ARE carried (unlike the sliding-snapshot carry): a delete since the last full dump
   * must keep shadowing that full dump's live value, or the merge would resurrect it.
   *
   * @param priorDelta the prior cumulative delta fragment (the newest of the loaded window)
   * @param modifiedLeaf the writer's copy of the combined leaf; carried entries are marked dirty here
   */
  public static void carryForwardDifferentialDelta(final HOTLeafPage priorDelta, final HOTLeafPage modifiedLeaf) {
    final int priorEntryCount = priorDelta.getEntryCount();
    for (int j = 0; j < priorEntryCount; j++) {
      final byte[] key = priorDelta.getKey(j);
      final int idx = modifiedLeaf.findEntry(key);
      if (idx >= 0) {
        modifiedLeaf.markEntryDirty(idx);
      }
    }
  }

}
