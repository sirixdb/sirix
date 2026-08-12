/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.utils.OS;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HOTLeafPage} including merge, updateValue, and copy operations.
 */
class HOTLeafPageTest {

  private static final long SIXTYFOUR_KB = 64 * 1024;

  @BeforeEach
  void setUp() {
    // Initialize allocator for off-heap memory
    if (!OS.isWindows()) {
      Allocators.getInstance().init(SIXTYFOUR_KB * 1024);
    }
  }

  @Test
  void testBasicPutAndFindEntry() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    byte[] key = "testKey".getBytes(StandardCharsets.UTF_8);
    byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);

    assertTrue(page.put(key, value));

    int index = page.findEntry(key);
    assertTrue(index >= 0, "Entry should be found");
    assertEquals(1, page.getEntryCount());

    assertArrayEquals(key, page.getKey(index));
    assertArrayEquals(value, page.getValue(index));

    page.close();
  }

  @Test
  void testSortedInsertion() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    // Insert out of order
    page.put("charlie".getBytes(StandardCharsets.UTF_8), "3".getBytes(StandardCharsets.UTF_8));
    page.put("alpha".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8));
    page.put("bravo".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8));

    assertEquals(3, page.getEntryCount());

    // Keys should be in sorted order
    assertEquals("alpha", new String(page.getKey(0), StandardCharsets.UTF_8));
    assertEquals("bravo", new String(page.getKey(1), StandardCharsets.UTF_8));
    assertEquals("charlie", new String(page.getKey(2), StandardCharsets.UTF_8));

    page.close();
  }

  @Test
  void testFindEntryNotFound() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    page.put("key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8));

    int index = page.findEntry("key2".getBytes(StandardCharsets.UTF_8));
    assertTrue(index < 0, "Entry should not be found");

    // Insertion point should be correct
    int insertPos = -(index + 1);
    assertEquals(1, insertPos);

    page.close();
  }

  @Test
  void testUpdateValue() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value1 = "short".getBytes(StandardCharsets.UTF_8);
    byte[] value2 = "longerValue".getBytes(StandardCharsets.UTF_8);

    page.put(key, value1);
    int index = page.findEntry(key);

    assertTrue(page.updateValue(index, value2));

    byte[] retrieved = page.getValue(index);
    assertArrayEquals(value2, retrieved);

    page.close();
  }

  @Test
  void testMergeWithNodeRefs() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);

    // First insert
    NodeReferences refs1 = new NodeReferences();
    refs1.addNodeKey(100L);
    byte[] value1 = NodeReferencesSerializer.serialize(refs1);

    page.mergeWithNodeRefs(key, key.length, value1, value1.length);

    // Second merge - should add to existing
    NodeReferences refs2 = new NodeReferences();
    refs2.addNodeKey(200L);
    byte[] value2 = NodeReferencesSerializer.serialize(refs2);

    page.mergeWithNodeRefs(key, key.length, value2, value2.length);

    // Verify merged result
    int index = page.findEntry(key);
    assertTrue(index >= 0);

    byte[] mergedBytes = page.getValue(index);
    NodeReferences merged = NodeReferencesSerializer.deserialize(mergedBytes);

    assertTrue(merged.contains(100L), "Should contain 100");
    assertTrue(merged.contains(200L), "Should contain 200");
    assertEquals(2, merged.getNodeKeys().getLongCardinality());

    page.close();
  }

  @Test
  void testCopy() {
    HOTLeafPage original = new HOTLeafPage(1L, 1, IndexType.CAS);

    original.put("key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8));
    original.put("key2".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8));

    HOTLeafPage copy = original.copy();

    // Verify copy has same content
    assertEquals(2, copy.getEntryCount());
    assertArrayEquals(original.getKey(0), copy.getKey(0));
    assertArrayEquals(original.getValue(0), copy.getValue(0));

    // Verify COW isolation - modify original
    byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);
    original.put(key3, "value3".getBytes(StandardCharsets.UTF_8));

    assertEquals(3, original.getEntryCount());
    assertEquals(2, copy.getEntryCount()); // Copy should be unaffected

    original.close();
    copy.close();
  }

  @Test
  void testMergeFrom() {
    HOTLeafPage page1 = new HOTLeafPage(1L, 1, IndexType.CAS);
    HOTLeafPage page2 = new HOTLeafPage(2L, 2, IndexType.CAS);

    page1.put("a".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8));
    page1.put("c".getBytes(StandardCharsets.UTF_8), "3".getBytes(StandardCharsets.UTF_8));

    page2.put("b".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8));
    page2.put("d".getBytes(StandardCharsets.UTF_8), "4".getBytes(StandardCharsets.UTF_8));

    assertTrue(page1.mergeFrom(page2));

    assertEquals(4, page1.getEntryCount());

    page1.close();
    page2.close();
  }

  @Test
  void testGuardManagement() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    assertEquals(0, page.getGuardCount());

    page.acquireGuard();
    assertEquals(1, page.getGuardCount());
    assertTrue(page.isHot());

    page.acquireGuard();
    assertEquals(2, page.getGuardCount());

    page.releaseGuard();
    assertEquals(1, page.getGuardCount());

    page.releaseGuard();
    assertEquals(0, page.getGuardCount());

    page.close();
  }

  @Test
  void testNeedsSplit() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    assertFalse(page.needsSplit());

    // Fill page to near capacity
    for (int i = 0; i < HOTLeafPage.MAX_ENTRIES - 1; i++) {
      String key = String.format("key%05d", i);
      String value = "v";
      page.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    assertFalse(page.needsSplit());

    // Add one more
    page.put("lastkey".getBytes(StandardCharsets.UTF_8), "x".getBytes(StandardCharsets.UTF_8));

    assertTrue(page.needsSplit());

    page.close();
  }

  @Test
  void testZeroCopySlices() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    byte[] key = "testKey".getBytes(StandardCharsets.UTF_8);
    byte[] value = "testValue".getBytes(StandardCharsets.UTF_8);

    page.put(key, value);

    // Get zero-copy slices
    var keySlice = page.getKeySlice(0);
    var valueSlice = page.getValueSlice(0);

    assertEquals(key.length, keySlice.byteSize());
    assertEquals(value.length, valueSlice.byteSize());

    page.close();
  }

  /** Composite key {@code logicalPrefix ‖ chunkIdx_be4}, as the chunked posting lists store them. */
  private static byte[] composite(String logicalPrefix, int chunkIdx) {
    final byte[] prefix = logicalPrefix.getBytes(StandardCharsets.UTF_8);
    final byte[] key = new byte[prefix.length + 4];
    System.arraycopy(prefix, 0, key, 0, prefix.length);
    key[prefix.length] = (byte) (chunkIdx >>> 24);
    key[prefix.length + 1] = (byte) (chunkIdx >>> 16);
    key[prefix.length + 2] = (byte) (chunkIdx >>> 8);
    key[prefix.length + 3] = (byte) chunkIdx;
    return key;
  }

  /**
   * A leaf whose entries share the common prefix {@code "shared/"} (maintained by {@code put}'s LCP
   * tracking), so the zero-copy key accessors exercise BOTH regions: on-heap commonPrefix and
   * off-heap suffix.
   */
  private static HOTLeafPage prefixedLeaf() {
    final HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);
    final byte[] value = new byte[] {1};
    assertTrue(page.put(composite("shared/alpha", 5), value));
    assertTrue(page.put(composite("shared/alpha", 0x00010002), value));
    assertTrue(page.put(composite("shared/alphaX", 1), value));
    assertTrue(page.put(composite("shared/beta", 0), value));
    return page;
  }

  @Test
  void testGetKeyLengthMatchesMaterializedKey() {
    final HOTLeafPage page = prefixedLeaf();
    for (int i = 0; i < page.getEntryCount(); i++) {
      assertEquals(page.getKey(i).length, page.getKeyLength(i), "entry " + i);
    }
    page.close();
  }

  @Test
  void testCompareKeyPrefixSpansPrefixAndSuffix() {
    final HOTLeafPage page = prefixedLeaf();
    final byte[] alpha = "shared/alpha".getBytes(StandardCharsets.UTF_8);
    final int alphaChunkIdx = page.findEntry(composite("shared/alpha", 5));
    final int extensionIdx = page.findEntry(composite("shared/alphaX", 1));
    final int betaIdx = page.findEntry(composite("shared/beta", 0));
    assertTrue(alphaChunkIdx >= 0 && extensionIdx >= 0 && betaIdx >= 0);

    // Exact chunk slot and extension slot both START WITH the logical prefix.
    assertEquals(0, page.compareKeyPrefix(alphaChunkIdx, alpha, alpha.length));
    assertEquals(0, page.compareKeyPrefix(extensionIdx, alpha, alpha.length));
    // A key on a different branch compares by its first differing byte ('b' > 'a').
    assertTrue(page.compareKeyPrefix(betaIdx, alpha, alpha.length) > 0);
    // Divergence INSIDE the shared commonPrefix region ("shared/" vs "sharez/").
    final byte[] sharez = "sharez/alpha".getBytes(StandardCharsets.UTF_8);
    assertTrue(page.compareKeyPrefix(alphaChunkIdx, sharez, sharez.length) < 0);
    // A probe longer than the key: equal through the key's bytes -> the key is less.
    final byte[] longProbe = composite("shared/alpha", 5); // 16 bytes, == full key of alphaChunkIdx
    final byte[] longer = new byte[longProbe.length + 2];
    System.arraycopy(longProbe, 0, longer, 0, longProbe.length);
    assertTrue(page.compareKeyPrefix(alphaChunkIdx, longer, longer.length) < 0);
    page.close();
  }

  @Test
  void testCompareKeyPrefixPartExcludesChunkTrailer() {
    final HOTLeafPage page = prefixedLeaf();
    final byte[] alpha = "shared/alpha".getBytes(StandardCharsets.UTF_8);
    final int alphaChunkIdx = page.findEntry(composite("shared/alpha", 5));
    final int extensionIdx = page.findEntry(composite("shared/alphaX", 1));

    // Trimming the 4-byte trailer leaves exactly the logical prefix.
    assertEquals(0, page.compareKeyPrefixPart(alphaChunkIdx, 4, alpha, alpha.length));
    // The extension key's trimmed part ("shared/alphaX") is longer -> greater.
    assertTrue(page.compareKeyPrefixPart(extensionIdx, 4, alpha, alpha.length) > 0);
    // Against a longer probe the trimmed part is shorter -> less.
    final byte[] alphaY = "shared/alphaY".getBytes(StandardCharsets.UTF_8);
    assertTrue(page.compareKeyPrefixPart(alphaChunkIdx, 4, alphaY, alphaY.length) < 0);
    page.close();
  }

  @Test
  void testReadKeyIntBEAcrossRegions() {
    final HOTLeafPage page = prefixedLeaf();
    for (int i = 0; i < page.getEntryCount(); i++) {
      final byte[] key = page.getKey(i);
      // Every alignment: fully inside the commonPrefix, spanning the boundary, and the trailer.
      for (final int pos : new int[] {0, 3, 5, key.length - 4}) {
        final int expected = ((key[pos] & 0xFF) << 24) | ((key[pos + 1] & 0xFF) << 16) | ((key[pos + 2] & 0xFF) << 8)
            | (key[pos + 3] & 0xFF);
        assertEquals(expected, page.readKeyIntBE(i, pos), "entry " + i + " pos " + pos);
      }
    }
    page.close();
  }

  @Test
  void testMergeChunkIntoPackedTombstoneAndRoaring() {
    final HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);

    // Packed chunk (2 bit16 values), a tombstone, and a Roaring chunk (>64 values).
    final NodeReferences packed = new NodeReferences();
    packed.addNodeKey(0x0001);
    packed.addNodeKey(0xFFFF);
    final NodeReferences tombstone = new NodeReferences();
    final NodeReferences roaring = new NodeReferences();
    for (int v = 0; v < 100; v++) {
      roaring.addNodeKey(v * 3);
    }
    assertTrue(page.put(composite("k", 3), NodeReferencesSerializer.serialize(packed)));
    assertTrue(page.put(composite("k", 4), NodeReferencesSerializer.serialize(tombstone)));
    assertTrue(page.put(composite("k", 5), NodeReferencesSerializer.serialize(roaring)));

    Roaring64Bitmap merged = null;
    for (int i = 0; i < page.getEntryCount(); i++) {
      final long chunkIdx = page.readKeyIntBE(i, page.getKeyLength(i) - 4) & 0xFFFFFFFFL;
      merged = NodeReferencesSerializer.mergeChunkInto(page, page.valueRef(i), chunkIdx << 16, merged);
    }

    final Roaring64Bitmap expected = new Roaring64Bitmap();
    expected.add((3L << 16) | 0x0001);
    expected.add((3L << 16) | 0xFFFF);
    for (int v = 0; v < 100; v++) {
      expected.add((5L << 16) | (v * 3));
    }
    assertEquals(expected, merged);
    page.close();
  }
}

