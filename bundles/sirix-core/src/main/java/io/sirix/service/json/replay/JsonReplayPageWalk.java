package io.sirix.service.json.replay;

import io.sirix.utils.ReplayWorkDiagnostics;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

/** Paired immutable record tries; no presentation hashes or numeric-frontier scans. */
final class JsonReplayPageWalk {
  @FunctionalInterface
  interface RecordVisitor {
    void compare(long key, boolean oldExists, boolean newExists);
  }

  private static final int GUARD_ATTEMPTS = 3;
  private final RecordVisitor visitor;
  private final StorageEngineReader oldReader;
  private final StorageEngineReader newReader;
  private final IndexLogKey oldPageKey;
  private final IndexLogKey newPageKey;
  private final int[] exponents;
  private final long[] slots = new long[Constants.NDP_NODE_COUNT / Long.SIZE];

  JsonReplayPageWalk(final StorageEngineReader oldReader, final StorageEngineReader newReader, final IndexType type,
      final int index, final RecordVisitor visitor) {
    this.oldReader = oldReader;
    this.newReader = newReader;
    this.visitor = visitor;
    oldPageKey = new IndexLogKey(type, 0, index, oldReader.getRevisionNumber());
    newPageKey = new IndexLogKey(type, 0, index, newReader.getRevisionNumber());
    exponents = newReader.getUberPage().getPageCountExp(type);
  }

  void read(final @Nullable PageReference oldRoot, final int oldHeight, final @Nullable PageReference newRoot,
      final int newHeight) {
    if (oldHeight < 0 || newHeight < 0 || oldHeight > exponents.length || newHeight > exponents.length) {
      throw new IllegalStateException("Invalid record trie height");
    }
    walk(populated(oldRoot), oldHeight, populated(newRoot), newHeight, 0);
  }

  private static @Nullable PageReference populated(final @Nullable PageReference reference) {
    return reference == null
        || (reference.getKey() < 0 && reference.getPage() == null && reference.getPageFragments().isEmpty())
            ? null
            : reference;
  }

  private void walk(final @Nullable PageReference oldRef, final int oldHeight, final @Nullable PageReference newRef,
      final int newHeight, final long prefix) {
    if ((oldRef == null && newRef == null) || (oldHeight == newHeight && sameDurableRegion(oldRef, newRef))) {
      return;
    }
    final int height = Math.max(oldHeight, newHeight);
    if (height == 0) {
      compareLeaf(oldRef != null, newRef != null, prefix);
      return;
    }
    final IndirectPage oldPage = dereferenceAtHeight(oldReader, oldRef, oldHeight, height);
    final IndirectPage newPage = dereferenceAtHeight(newReader, newRef, newHeight, height);
    if (missingIndirectPage(oldRef, oldHeight, height, oldPage)
        || missingIndirectPage(newRef, newHeight, height, newPage)) {
      throw new IllegalStateException("Missing indirect page for a populated record reference");
    }
    final int shift = exponents[exponents.length - height];
    for (int offset = 0; offset < Constants.INP_REFERENCE_COUNT; offset++) {
      final PageReference oldChild = childReference(oldRef, oldHeight, height, oldPage, offset);
      final PageReference newChild = childReference(newRef, newHeight, height, newPage, offset);
      if (oldChild == null && newChild == null) {
        continue;
      }
      if (shift >= Long.SIZE - 1 && offset != 0) {
        throw new IllegalStateException("Record trie contains an overflowing page key");
      }
      final long childPrefix = offset == 0
          ? prefix
          : prefix | (long) offset << shift;
      walk(populated(oldChild), Math.min(oldHeight, height - 1), populated(newChild), Math.min(newHeight, height - 1),
          childPrefix);
    }
  }

  private static @Nullable IndirectPage dereferenceAtHeight(final StorageEngineReader reader,
      final @Nullable PageReference reference, final int referenceHeight, final int height) {
    return reference != null && referenceHeight == height
        ? dereference(reader, reference)
        : null;
  }

  private static boolean missingIndirectPage(final @Nullable PageReference reference, final int referenceHeight,
      final int height, final @Nullable IndirectPage page) {
    return reference != null && referenceHeight == height && page == null;
  }

  private static @Nullable PageReference childReference(final @Nullable PageReference reference,
      final int referenceHeight, final int height, final @Nullable IndirectPage page, final int offset) {
    return referenceHeight == height
        ? page == null
            ? null
            : page.referenceAt(offset)
        : offset == 0
            ? reference
            : null;
  }

  private static IndirectPage dereference(final StorageEngineReader reader, final PageReference reference) {
    ReplayWorkDiagnostics.fallbackPage();
    return reader.dereferenceIndirectPageReference(reference);
  }

  private static boolean sameDurableRegion(final @Nullable PageReference left, final @Nullable PageReference right) {
    return left != null && right != null && left.getKey() >= 0 && left.getKey() == right.getKey()
        && left.getDatabaseId() == right.getDatabaseId() && left.getResourceId() == right.getResourceId()
        && left.getPageFragments().equals(right.getPageFragments());
  }

  private void compareLeaf(final boolean oldExists, final boolean newExists, final long pageKey) {
    readBitmap(oldReader, oldPageKey.setRecordPageKey(pageKey), oldExists, false);
    readBitmap(newReader, newPageKey.setRecordPageKey(pageKey), newExists, true);
    if (pageKey > Long.MAX_VALUE >>> Constants.NDP_NODE_COUNT_EXPONENT) {
      throw new IllegalStateException("Record page key exceeds the node identity range");
    }
    final long firstKey = pageKey << Constants.NDP_NODE_COUNT_EXPONENT;
    for (int word = 0; word < slots.length; word++) {
      long remaining = slots[word];
      while (remaining != 0) {
        final long key = firstKey + (long) word * Long.SIZE + Long.numberOfTrailingZeros(remaining);
        visitor.compare(key, oldExists, newExists);
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
      ReplayWorkDiagnostics.fallbackPage();
      final var resolved = reader.getRecordPage(pageKey);
      if (resolved == null || !(resolved.page() instanceof KeyValueLeafPage page)) {
        throw new IllegalStateException("Missing record page for a populated reference: " + pageKey);
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
    throw new IllegalStateException("Could not guard record page " + pageKey);
  }
}
