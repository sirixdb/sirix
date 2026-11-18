package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;

import java.util.Arrays;
import java.util.Objects;

/**
 * Reference-only normalization of transaction-owned HOT blocks immediately before recursive commit.
 */
public final class HOTCommitNormalizer {
  private HOTCommitNormalizer() {}

  /**
   * Bounded, session-owned workspace, confined to its exclusive writer. No references survive a
   * normalization boundary.
   */
  public static final class Scratch {
    final HOTIndirectPage[] ancestors = new HOTIndirectPage[64];
    final int[] parentBits = new int[Integer.SIZE];
    final int[] childBits = new int[Integer.SIZE];
    final int[] unionBits = new int[Integer.SIZE];
    final int[] partials = new int[HOTIndirectPage.MAX_NODE_ENTRIES];
    final PageReference[] children = new PageReference[HOTIndirectPage.MAX_NODE_ENTRIES];
    final int[] shuffleIndices = new int[32];
    int unionBitCount;
    private boolean inUse;

    private void reset() {
      Arrays.fill(ancestors, null);
      Arrays.fill(children, null);
      // Primitive lanes are overwritten before use; the count makes stale lanes inaccessible.
      unionBitCount = 0;
      inUse = false;
    }
  }

  /**
   * Normalize one dirty HOT subtree. Clean blocks are opaque: only their root metadata is read. The
   * caller must finish pending background writes and exclude concurrent mutations before entry.
   * Leaves, side references and persistent page identities are preserved. Any failure poisons the
   * transaction because a descendant may already have been replaced in its private log.
   */
  public static HOTIndirectPage normalize(final PageReference reference, final HOTIndirectPage page,
      final StorageEngineWriter writer, final Scratch scratch) {
    Objects.requireNonNull(reference, "reference");
    Objects.requireNonNull(page, "page");
    Objects.requireNonNull(writer, "writer");
    Objects.requireNonNull(scratch, "scratch");
    if (!page.needsNormalization() || writer.getLog().get(reference) == null) {
      return page;
    }
    if (scratch.inUse) {
      throw new IllegalStateException("HOT normalization scratch is already in use");
    }
    scratch.inUse = true;
    try {
      if (page.getHeight() > scratch.ancestors.length) {
        throw new IllegalStateException("Dirty HOT commit spine exceeds bounded writer depth");
      }
      return normalize(reference, page, writer, scratch, 0);
    } catch (final RuntimeException | Error failure) {
      writer.markTransactionRollbackOnly(failure);
      throw failure;
    } finally {
      scratch.reset();
    }
  }

  private static HOTIndirectPage normalize(final PageReference reference, final HOTIndirectPage page,
      final StorageEngineWriter writer, final Scratch scratch, final int depth) {
    final TransactionIntentLog log = writer.getLog();
    final PageContainer owner = log.get(reference);
    if (owner == null) {
      return page;
    }
    if (owner.getModified() != page) {
      throw new IllegalStateException("HOT normalization requires the current transaction-owned page");
    }
    if (depth >= scratch.ancestors.length) {
      throw new IllegalStateException("Dirty HOT commit spine exceeds bounded writer depth");
    }
    for (int ancestor = 0; ancestor < depth; ancestor++) {
      if (scratch.ancestors[ancestor] == page) {
        throw new IllegalStateException("Cycle in dirty HOT commit spine");
      }
    }
    scratch.ancestors[depth] = page;

    boolean repairHeight = false;
    for (int slot = 0; slot < page.getNumChildren(); slot++) {
      final PageReference childRef = page.getChildReference(slot);
      final PageContainer childOwner = log.get(childRef);
      Page child = childOwner == null
          ? childRef.getPage()
          : childOwner.getModified();
      if (childOwner != null && child instanceof HOTIndirectPage indirect && indirect.needsNormalization()) {
        final int oldHeight = indirect.getHeight();
        final HOTIndirectPage normalizedChild = normalize(childRef, indirect, writer, scratch, depth + 1);
        final int newHeight = normalizedChild.getHeight();
        repairHeight |=
            newHeight != oldHeight && (oldHeight + 1 == page.getHeight() || newHeight + 1 > page.getHeight());
        child = normalizedChild;
      }
      if (child == null) {
        if (childRef.getKey() < 0) {
          throw new IllegalStateException("Unresolved non-durable HOT child during commit normalization");
        }
        // Valid clean input is opaque. Its height cannot change unless a dirty descendant folds.
      } else {
        if (!(child instanceof HOTIndirectPage) && !(child instanceof HOTLeafPage)) {
          throw new IllegalStateException("Non-HOT child during commit normalization");
        }
        childRef.setPage(child);
      }
    }

    HOTIndirectPage result = page;
    int slot = 0;
    while (result.getNumChildren() < HOTIndirectPage.MAX_NODE_ENTRIES && slot < result.getNumChildren()) {
      final PageReference childRef = result.getChildReference(slot);
      final PageContainer childOwner = log.get(childRef);
      if (childOwner == null || !(childOwner.getModified() instanceof HOTIndirectPage child)) {
        slot++;
        continue;
      }
      final int count = HOTIncrementalInsert.prepareCompatibleChild(result, slot, child, scratch);
      if (count == 0) {
        slot++;
      } else {
        // Only an accepted fold needs these clean boundary roots. Never unpack leaf entries.
        for (int retained = 0; retained < count; retained++) {
          resolveChild(scratch.children[retained], writer);
        }
        result = HOTIncrementalInsert.materializePreparedFold(page.getRevision(), page.getPageKey(), count, scratch);
        // The child was already normalized; its inserted references cannot gain capacity or
        // mask room in this larger parent. Earlier refusals cannot improve as both grow.
        slot += child.getNumChildren();
      }
    }
    if (result == page && repairHeight) {
      int height = 1;
      for (int child = 0; child < page.getNumChildren(); child++) {
        final Page resolved = resolveChild(page.getChildReference(child), writer);
        if (resolved instanceof HOTIndirectPage indirect) {
          height = Math.max(height, indirect.getHeight() + 1);
        }
      }
      page.setHeightForWrite(height);
    }
    if (result != page) {
      log.put(reference, PageContainer.getInstance(result, result));
    }
    result.markNormalized();
    scratch.ancestors[depth] = null;
    return result;
  }

  private static Page resolveChild(final PageReference reference, final StorageEngineWriter writer) {
    final Page child = writer.loadHOTPage(reference);
    if (!(child instanceof HOTIndirectPage) && !(child instanceof HOTLeafPage)) {
      throw new IllegalStateException("Unresolved or non-HOT child during commit normalization");
    }
    reference.setPage(child);
    return child;
  }

}
