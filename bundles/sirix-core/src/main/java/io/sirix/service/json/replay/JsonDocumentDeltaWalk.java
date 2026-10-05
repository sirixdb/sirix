package io.sirix.service.json.replay;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/** Paired immutable document tries; no presentation hashes or numeric-frontier scans. */
final class JsonDocumentDeltaWalk {
  private static final int GUARD_ATTEMPTS = 3;
  private final JsonNodeReadOnlyTrx before;
  private final JsonNodeReadOnlyTrx after;
  private final StorageEngineReader oldReader;
  private final StorageEngineReader newReader;
  private final IndexLogKey oldPageKey;
  private final IndexLogKey newPageKey;
  private final int[] exponents;
  private final long[] slots = new long[Constants.NDP_NODE_COUNT / Long.SIZE];
  private final Long2ObjectOpenHashMap<JsonReplayRecord> puts = new Long2ObjectOpenHashMap<>();
  private final LongOpenHashSet deletes = new LongOpenHashSet();

  JsonDocumentDeltaWalk(final JsonNodeReadOnlyTrx before, final JsonNodeReadOnlyTrx after) {
    this.before = before;
    this.after = after;
    oldReader = before.getStorageEngineReader();
    newReader = after.getStorageEngineReader();
    oldPageKey = new IndexLogKey(IndexType.DOCUMENT, 0, -1, before.getRevisionNumber());
    newPageKey = new IndexLogKey(IndexType.DOCUMENT, 0, -1, after.getRevisionNumber());
    exponents = newReader.getUberPage().getPageCountExp(IndexType.DOCUMENT);
  }

  JsonIdentityDelta read(final JsonReplayManifest manifest) {
    final var oldRoot = oldReader.getActualRevisionRootPage();
    final var newRoot = newReader.getActualRevisionRootPage();
    final int oldHeight = oldRoot.getCurrentMaxLevelOfDocumentIndexIndirectPages();
    final int newHeight = newRoot.getCurrentMaxLevelOfDocumentIndexIndirectPages();
    if (oldHeight < 0 || newHeight < 0 || oldHeight > exponents.length || newHeight > exponents.length) {
      throw new IllegalStateException("Invalid document trie height");
    }
    walk(oldRoot.getIndirectDocumentIndexPageReference(), oldHeight, newRoot.getIndirectDocumentIndexPageReference(),
        newHeight, 0);
    return new JsonIdentityDelta(manifest, puts, deletes);
  }

  private void walk(final PageReference oldRef, final int oldHeight, final PageReference newRef, final int newHeight,
      final long prefix) {
    if (oldRef == null && newRef == null || oldHeight == newHeight && sameDurableRegion(oldRef, newRef)) {
      return;
    }
    final int height = Math.max(oldHeight, newHeight);
    if (height == 0) {
      compareLeaf(oldRef != null, newRef != null, prefix);
      return;
    }
    final IndirectPage oldPage = oldRef != null && oldHeight == height
        ? oldReader.dereferenceIndirectPageReference(oldRef)
        : null;
    final IndirectPage newPage = newRef != null && newHeight == height
        ? newReader.dereferenceIndirectPageReference(newRef)
        : null;
    if (oldRef != null && oldHeight == height && oldPage == null
        || newRef != null && newHeight == height && newPage == null) {
      throw new IllegalStateException("Missing indirect page for a populated document reference");
    }
    final int shift = exponents[exponents.length - height];
    for (int offset = 0; offset < Constants.INP_REFERENCE_COUNT; offset++) {
      final PageReference oldChild = oldHeight == height
          ? oldPage == null
              ? null
              : oldPage.referenceAt(offset)
          : offset == 0
              ? oldRef
              : null;
      final PageReference newChild = newHeight == height
          ? newPage == null
              ? null
              : newPage.referenceAt(offset)
          : offset == 0
              ? newRef
              : null;
      if (oldChild == null && newChild == null) {
        continue;
      }
      if (shift >= Long.SIZE - 1 && offset != 0) {
        throw new IllegalStateException("Document trie contains an overflowing page key");
      }
      final long childPrefix = offset == 0
          ? prefix
          : prefix | (long) offset << shift;
      walk(oldChild, Math.min(oldHeight, height - 1), newChild, Math.min(newHeight, height - 1), childPrefix);
    }
  }

  private static boolean sameDurableRegion(final PageReference left, final PageReference right) {
    return left != null && right != null && left.getKey() >= 0 && left.getKey() == right.getKey()
        && left.getDatabaseId() == right.getDatabaseId() && left.getResourceId() == right.getResourceId()
        && left.getPageFragments().equals(right.getPageFragments());
  }

  private void compareLeaf(final boolean oldExists, final boolean newExists, final long pageKey) {
    readBitmap(oldReader, oldPageKey.setRecordPageKey(pageKey), oldExists, false);
    readBitmap(newReader, newPageKey.setRecordPageKey(pageKey), newExists, true);
    if (pageKey > Long.MAX_VALUE >>> Constants.NDP_NODE_COUNT_EXPONENT) {
      throw new IllegalStateException("Document page key exceeds the node identity range");
    }
    final long firstKey = pageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    for (int word = 0; word < slots.length; word++) {
      long remaining = slots[word];
      while (remaining != 0) {
        final long key = firstKey + (long) word * Long.SIZE + Long.numberOfTrailingZeros(remaining);
        final JsonReplayRecord oldRecord = oldExists && before.moveTo(key)
            ? JsonReplayRecord.capture(before)
            : null;
        final JsonReplayRecord newRecord = newExists && after.moveTo(key)
            ? JsonReplayRecord.capture(after)
            : null;
        if (newRecord == null) {
          if (oldRecord != null) {
            deletes.add(key);
          }
        } else if (!newRecord.equals(oldRecord)) {
          puts.put(key, newRecord);
        }
        remaining &= remaining - 1;
      }
    }
  }

  private void readBitmap(final StorageEngineReader reader, final IndexLogKey pageKey, final boolean exists,
      final boolean merge) {
    if (!exists) {
      if (!merge) {
        for (int word = 0; word < slots.length; word++) {
          slots[word] = 0;
        }
      }
      return;
    }
    for (int attempt = 0; attempt < GUARD_ATTEMPTS; attempt++) {
      final var resolved = reader.getRecordPage(pageKey);
      if (resolved == null || !(resolved.page() instanceof KeyValueLeafPage page)) {
        throw new IllegalStateException("Missing document page for a populated reference: " + pageKey);
      }
      if (!page.acquireGuard()) {
        continue;
      }
      try {
        page.ensureAllChunks();
        for (int word = 0; word < slots.length; word++) {
          final long bits = page.logicalSlotBitmapWord(word);
          slots[word] = merge
              ? slots[word] | bits
              : bits;
        }
        return;
      } finally {
        page.releaseGuard();
      }
    }
    throw new IllegalStateException("Could not guard document page " + pageKey);
  }
}
