package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.DeletedNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.json.ArrayNode;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Inline publication must supersede a cached tombstone on every flyweight creation path. */
final class KeyValueLeafPageRecreationTest {
  private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3();

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(256L * 1024 * 1024);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void inlineWriteReplacesCachedTombstone(final boolean direct) {
    final var config = ResourceConfiguration.newBuilder("slot-recreation").useDeweyIDs(false).build();
    try (final var page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false)) {
      page.setRecord(new DeletedNode(new NodeDelegate(1, -1, null, -1, 1, (SirixDeweyID) null)));
      final var array = new ArrayNode(1, 0, 0, -1, 1, -1, -1, 2, 2, 1, 1, 37, HASH_FUNCTION,
          (SirixDeweyID) null);
      if (direct) {
        final long offset = page.prepareHeapForDirectWrite(array.estimateSerializedSize(), 0);
        final int bytes = array.serializeToHeap(page.getSlottedPage(), offset);
        page.completeDirectWrite(NodeKind.ARRAY.getId(), 1, 1, bytes, null);
      } else {
        page.serializeNewRecord(array, 1, 1);
      }
      assertNull(page.getRecord(1), "the replacement slot must have no cached tombstone");
      final var read = new ArrayNode(1, HASH_FUNCTION);
      read.bind(page.getSlottedPage(), PageLayout.heapAbsoluteOffset(PageLayout.getDirHeapOffset(page.getSlottedPage(), 1)), 1, 1);
      assertEquals(0, read.getParentKey());
      assertEquals(2, read.getFirstChildKey());
      assertEquals(2, read.getLastChildKey());
      assertEquals(1, read.getChildCount());
      assertEquals(37, read.getHash());
      read.clearBinding();
      array.clearBinding();
    }
  }
}
