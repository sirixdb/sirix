package io.sirix.node;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;

final class DocumentRootHeapEncodingTest {
  private static volatile int encodedSize;
  private static volatile int[] allocationWitness;

  @Test
  void repeatedEncodingReusesScratchWithoutAllocatingPerRecord() {
    final ThreadMXBean accounting = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
    assumeTrue(accounting != null && accounting.isThreadAllocatedMemorySupported());
    if (!accounting.isThreadAllocatedMemoryEnabled()) {
      accounting.setThreadAllocatedMemoryEnabled(true);
    }
    verifyAllocation(accounting, new JsonDocumentRootNode(0, 1, 129, 2, 3, null));
    verifyAllocation(accounting, new XmlDocumentRootNode(0, 1, 129, 2, 3, null));
  }

  private static void verifyAllocation(final ThreadMXBean accounting, final AbstractDocumentRootNode root) {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment heap = arena.allocate(64);
      for (int iteration = 0; iteration < 20_000; iteration++) {
        encodedSize = root.serializeToHeap(heap, 0);
      }
      final long threadId = Thread.currentThread().threadId();
      final long start = accounting.getThreadAllocatedBytes(threadId);
      for (int iteration = 0; iteration < 20_000; iteration++) {
        encodedSize = root.serializeToHeap(heap, 0);
      }
      final long allocated = accounting.getThreadAllocatedBytes(threadId) - start;
      assertEquals(23, encodedSize);
      assertTrue(allocated <= 1024, "repeated encoding allocated " + allocated + " bytes");

      final long witnessStart = accounting.getThreadAllocatedBytes(threadId);
      for (int iteration = 0; iteration < 20_000; iteration++) {
        allocationWitness = new int[7];
      }
      final long witnessBytes = accounting.getThreadAllocatedBytes(threadId) - witnessStart;
      assertTrue(witnessBytes > 1024, "allocation accounting must detect per-record scratch arrays");
    }
  }

  @Test
  void bothKindsKeepDeweyIdAndByteCachesConsistent() {
    final AbstractDocumentRootNode[] roots = {new JsonDocumentRootNode(0, null), new XmlDocumentRootNode(0, null)};
    final SirixDeweyID id = SirixDeweyID.newRootID();
    final byte[] bytes = id.toBytes();
    for (final AbstractDocumentRootNode root : roots) {
      root.setDeweyID(id);
      assertSame(id, root.getDeweyID());
      assertArrayEquals(bytes, root.getDeweyIDAsBytes());
      root.setDeweyIDBytes(bytes);
      assertEquals(id, root.getDeweyID());
      assertSame(bytes, root.getDeweyIDAsBytes());
      root.setDeweyIDAfterCreation(id, bytes);
      assertSame(id, root.getDeweyID());
      assertSame(bytes, root.getDeweyIDAsBytes());
      root.setDeweyID(null);
      assertEquals(id, root.getDeweyID());
      root.setDeweyIDBytes(null);
      assertArrayEquals(bytes, root.getDeweyIDAsBytes());
    }
  }

  @Test
  void bothKindsPreserveThePersistedSevenFieldLayout() {
    verifyRoot(new JsonDocumentRootNode(0, 1, 129, 2, 3, null));
    verifyRoot(new XmlDocumentRootNode(0, 1, 129, 2, 3, null));
  }

  private static void verifyRoot(final AbstractDocumentRootNode root) {
    final StructNode fields = (StructNode) root;
    fields.setHash(0x0807060504030201L);
    // Persisted heap record contract: kind, seven byte offsets, varints, little-endian hash.
    final byte[] expected =
        {root.getKind().getId(), 0, 1, 3, 4, 5, 6, 7, 3, (byte) 0x83, 2, 4, 6, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8};
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment heap = arena.allocate(64);
      final int size = root.serializeToHeap(heap, 3);
      assertEquals(expected.length, size);
      assertArrayEquals(expected, heap.asSlice(3, size).toArray(ValueLayout.JAVA_BYTE));
      final int[] scratch = root.getHeapOffsets();
      root.serializeToHeap(heap, 3);
      assertSame(scratch, root.getHeapOffsets());
      assertFalse(root.isWriteSingleton());
      root.setWriteSingleton(true);
      assertTrue(root.isWriteSingleton());
      root.bind(heap, 3, 0, 4);
      assertTrue(root.isBoundTo(heap));
      assertEquals(4, root.getSlotIndex());
      assertEquals(1, fields.getFirstChildKey());
      assertEquals(129, fields.getLastChildKey());
      assertEquals(2, fields.getChildCount());
      assertEquals(3, fields.getDescendantCount());
      assertEquals(0x0807060504030201L, fields.getHash());
      root.unbind();
      assertFalse(root.isBound());
      assertNull(root.getOwnerPage());
      assertEquals(1, fields.getFirstChildKey());
      assertEquals(129, fields.getLastChildKey());
      assertEquals(2, fields.getChildCount());
      assertEquals(3, fields.getDescendantCount());
      assertEquals(0x0807060504030201L, fields.getHash());
      root.setWriteSingleton(false);
      assertFalse(root.isWriteSingleton());
    }
  }
}
