package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import java.io.IOException;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageKind;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The pinned trie spill writes a page's serialized image mid-transaction and later reloads that
 * image STANDALONE — a single uncommitted offset with no fragment chain behind it. A leaf that
 * descends from a committed complete fragment and carries uncommitted modifications serializes as a
 * sparse VERSIONED FRAGMENT (only the dirty entries), which is correct for the committed read
 * path's chain combine and catastrophic for a standalone reload: the dirty subset comes back
 * presented as the whole leaf, and every clean entry silently vanishes from the live trie. The
 * vanished keys surface far away as routing contradictions — an I8-unsafe branch demanding a
 * subtree rebuild, or a descent landing on a since-released sibling — which is exactly the
 * intermittent windows-latest/query failure this test pins the mechanism of.
 */
@DisplayName("Pinned trie spill must not write sparse-fragment-shaped HOT leaves")
final class PinnedTrieSpillSparseFragmentTest {

  private static final int COMMITTED_ENTRIES = 6;

  @Test
  @DisplayName("a committed-then-dirtied leaf is not spill-eligible (the shape serializes as a fragment)")
  void sparseFragmentShapedLeafIsNotSpillEligible() {
    final HOTLeafPage committed = populatedLeaf(COMMITTED_ENTRIES);
    final HOTLeafPage writerCopy = committed.copy();
    try {
      assertTrue(writerCopy.put(keyOf(COMMITTED_ENTRIES), valueOf(COMMITTED_ENTRIES)),
          "the writer copy must accept the dirtying put");

      assertTrue(writerCopy.wouldEmitSparseFragment(),
          "a copy of a complete leaf with uncommitted puts is the sparse-fragment shape");
      assertFalse(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(writerCopy),
          "the spill must refuse the sparse-fragment shape: its standalone reload would drop every clean entry");
    } finally {
      writerCopy.close();
      committed.close();
    }
  }

  @Test
  @DisplayName("a fresh leaf with no committed ancestry stays spill-eligible")
  void freshLeafStaysSpillEligible() {
    final HOTLeafPage fresh = populatedLeaf(COMMITTED_ENTRIES);
    try {
      assertFalse(fresh.wouldEmitSparseFragment(), "a fresh leaf has no complete ref and serializes fully");
      assertTrue(NodeStorageEngineWriter.isPinnedTrieSpillPageEligible(fresh),
          "excluding the sparse shape must not cost the fresh-leaf population its spill eligibility");
    } finally {
      fresh.close();
    }
  }

  /**
   * The hazard itself, pinned so the eligibility exclusion above can never look arbitrary: a sparse
   * fragment reloaded without its chain IS a partial leaf. This is the serializer working as designed
   * for the committed path — the defect was feeding its output to a standalone reload.
   */
  @Test
  @DisplayName("a standalone reload of a sparse fragment loses every clean entry")
  void standaloneReloadOfASparseFragmentLosesCleanEntries() throws IOException {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("spill-sparse-fragment").versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                                  .build();
    final HOTLeafPage committed = populatedLeaf(COMMITTED_ENTRIES);
    final HOTLeafPage writerCopy = committed.copy();
    HOTLeafPage reloaded = null;
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1 << 16)) {
      assertTrue(writerCopy.put(keyOf(COMMITTED_ENTRIES), valueOf(COMMITTED_ENTRIES)));
      assertEquals(COMMITTED_ENTRIES + 1, writerCopy.getEntryCount(), "the live leaf holds every entry");

      PageKind.HOT_LEAF_PAGE.serializePage(config, sink, writerCopy, SerializationType.DATA);
      reloaded = (HOTLeafPage) new PagePersister().deserializePage(config, Bytes.wrapForRead(sink.toByteArray()),
          SerializationType.DATA);

      assertEquals(1, reloaded.getEntryCount(),
          "the fragment carries exactly the dirty entry — a standalone reload has lost the " + COMMITTED_ENTRIES
              + " clean entries, which is why the spill must never write this shape");
    } finally {
      if (reloaded != null) {
        reloaded.close();
      }
      writerCopy.close();
      committed.close();
    }
  }

  private static HOTLeafPage populatedLeaf(final int entryCount) {
    final HOTLeafPage page = new HOTLeafPage(17L, 23, IndexType.PROJECTION);
    for (int i = 0; i < entryCount; i++) {
      assertTrue(page.put(keyOf(i), valueOf(i)), "failed to populate key " + i);
    }
    return page;
  }

  private static byte[] keyOf(final int value) {
    return new byte[] {'p', 'r', 'o', 'j', ':', (byte) (value >>> 8), (byte) value};
  }

  private static byte[] valueOf(final int value) {
    final byte[] bytes = new byte[192];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (value * 31 + i * 17);
    }
    return bytes;
  }
}
