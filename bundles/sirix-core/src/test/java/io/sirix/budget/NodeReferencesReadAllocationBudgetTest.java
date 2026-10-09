package io.sirix.budget;

import com.sun.management.ThreadMXBean;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Isolated
final class NodeReferencesReadAllocationBudgetTest {
  @Test
  void packedReadsAllocateOnlyTheirOwnedRunAndReferences() {
    final var threads = ManagementFactory.getThreadMXBean();
    assumeTrue(threads instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) threads;
    assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
    final long[] keys = {0, 1, 255, 1023, 4096, 32767, 65534, 65535};
    final byte[] payload = NodeReferencesSerializer.serialize(NodeReferences.ofSortedArray(keys));
    final NodeReferences[] retained = new NodeReferences[64];
    measure(bean, payload, true, 20_000, retained);
    measure(bean, payload, false, 20_000, retained);
    final int operations = 10_000;
    final long compact = measure(bean, payload, true, operations, retained);
    final long bitmap = measure(bean, payload, false, operations, retained);
    assertTrue(compact <= operations * 256L, "packed read allocation bytes=" + compact);
    assertTrue(bitmap > compact * 2, "the existing bitmap decoder must remain a positive allocation witness");
  }

  @Test
  void readChunksPreserveKeysOwnershipMutationAndDomainValidation() {
    for (final int count : new int[] {1, 8, 64, 65}) {
      final long[] keys = new long[count];
      for (int index = 0; index < count; index++) {
        keys[index] = index;
      }
      final byte[] payload = NodeReferencesSerializer.serialize(NodeReferences.ofSortedArray(keys));
      final byte[] original = payload.clone();
      final NodeReferences refs = NodeReferencesSerializer.deserializeReadChunk(payload);
      assertArrayEquals(keys, refs.toSortedArray());
      assertEquals(count, refs.cardinality());
      refs.getNodeKeys().addLong(60000);
      assertTrue(refs.contains(60000));
      assertFalse(NodeReferencesSerializer.deserializeReadChunk(original).contains(60000));
      Arrays.fill(payload, (byte) 0);
      assertTrue(refs.contains(count - 1));
    }
    final byte[] tooWide = NodeReferencesSerializer.serialize(NodeReferences.ofSortedArray(new long[] {65536}));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserializeReadChunk(tooWide));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserializeReadChunk(new byte[0]));
    final byte[] duplicate = NodeReferencesSerializer.serialize(NodeReferences.ofSortedArray(new long[] {1, 2}));
    System.arraycopy(duplicate, 2, duplicate, 10, Long.BYTES);
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserializeReadChunk(duplicate));
    assertFalse(NodeReferencesSerializer.deserializeReadChunk(new byte[] {(byte) 0xFE}).hasNodeKeys());
  }

  private static long measure(final ThreadMXBean bean, final byte[] payload, final boolean compact,
      final int operations, final NodeReferences[] retained) {
    final long start = bean.getCurrentThreadAllocatedBytes();
    long count = 0;
    for (int index = 0; index < operations; index++) {
      final NodeReferences refs = compact
          ? NodeReferencesSerializer.deserializeReadChunk(payload)
          : NodeReferencesSerializer.deserializeChunk(payload);
      retained[index & 63] = refs;
      count += refs.cardinality();
    }
    final long bytes = bean.getCurrentThreadAllocatedBytes() - start;
    assertEquals(operations * 8L, count);
    return bytes;
  }
}
