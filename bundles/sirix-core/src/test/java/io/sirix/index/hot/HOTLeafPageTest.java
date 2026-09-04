/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameReusedException;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.utils.OS;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  private static byte[] singleBit(final long nodeKey) {
    final NodeReferences references = new NodeReferences();
    references.addNodeKey(nodeKey);
    return NodeReferencesSerializer.serialize(references);
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
  void packedMergeGrowthSurvivesRequiredCompactionWithoutLosingScratchOrOtherSlots() {
    final HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);
    final byte[] targetKey = "a000".getBytes(StandardCharsets.UTF_8);
    final byte[] fragmentedKey = "z000".getBytes(StandardCharsets.UTF_8);
    final byte[] fillerValue = new byte[600];
    Arrays.fill(fillerValue, (byte) 0x44);

    try {
      assertTrue(page.put(targetKey, singleBit(10L)));
      assertTrue(page.put(fragmentedKey, fillerValue));

      int fillerNumber = 0;
      while (page.getRemainingSpace() > 1_200) {
        final byte[] key = String.format("m%04d", fillerNumber++).getBytes(StandardCharsets.UTF_8);
        assertTrue(page.put(key, fillerValue));
      }

      // Consume the physical tail exactly. The next growing replacement can succeed only by
      // compacting the hole created below; this exercises the internal merge scratch across compact.
      final byte[] tailKey = "y9999".getBytes(StandardCharsets.UTF_8);
      final int tailValueLength = Math.toIntExact(page.getRemainingSpace()) - 4 - tailKey.length;
      assertTrue(tailValueLength > 0);
      final byte[] tailValue = new byte[tailValueLength];
      Arrays.fill(tailValue, (byte) 0x66);
      assertTrue(page.put(tailKey, tailValue));
      assertEquals(0L, page.getRemainingSpace());

      final int fragmentedIndex = page.findEntry(fragmentedKey);
      assertTrue(fragmentedIndex >= 0);
      assertTrue(page.updateValueRange(fragmentedIndex, new byte[] {0x55}, 0, 1));
      assertEquals(0L, page.getRemainingSpace(), "a non-tail shrink must leave a physical hole");

      final int usedBeforeMerge = page.getUsedSlotsSize();
      final byte[] incoming = singleBit(20L);
      assertTrue(page.mergeWithNodeRefs(targetKey, targetKey.length, incoming, incoming.length));
      assertTrue(page.getUsedSlotsSize() < usedBeforeMerge, "the growing merge must compact the fragmented page");

      final NodeReferences merged = NodeReferencesSerializer.deserialize(page.getValue(page.findEntry(targetKey)));
      assertEquals(2, merged.getNodeKeys().getLongCardinality());
      assertTrue(merged.contains(10L));
      assertTrue(merged.contains(20L));
      assertArrayEquals(new byte[] {0x55}, page.getValue(page.findEntry(fragmentedKey)));
      assertArrayEquals(tailValue, page.getValue(page.findEntry(tailKey)));
    } finally {
      page.close();
    }
  }

  @Test
  void growingRangeReplacementThatCannotFitPreservesOldValue() {
    final HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);
    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    final byte[] oldValue = singleBit(7L);
    try {
      assertTrue(page.put(key, oldValue));
      final int index = page.findEntry(key);
      final byte[] tooLarge = new byte[0xFFFF];

      assertFalse(page.updateValueRange(index, tooLarge, 0, tooLarge.length));
      assertArrayEquals(oldValue, page.getValue(index));
    } finally {
      page.close();
    }
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
  void testCloseImmediatelyReleasesRetainedReferencesWhenUnguarded() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);
    HOTLeafPage completePage = new HOTLeafPage(2L, 1, IndexType.CAS);
    page.setCompletePageRef(completePage);
    for (int key = 0; key < 128; key++) {
      page.setPageReference(key, new PageReference().setKey(key + 1L));
    }

    assertEquals(128, page.segmentRefCount());
    assertSame(completePage, page.getCompletePageRef());

    page.close();

    assertTrue(page.isClosed());
    assertEquals(0, page.getGuardCount());
    assertThrows(FrameReusedException.class, page::segmentRefCount);
    assertNull(page.getCompletePageRef());
    assertThrows(FrameReusedException.class, () -> page.getPageReference(0L));
    completePage.close();
  }

  @Test
  void testCloseDefersRetainedReferenceReleaseUntilLastGuard() {
    HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);
    HOTLeafPage completePage = new HOTLeafPage(2L, 1, IndexType.CAS);
    page.setCompletePageRef(completePage);
    for (int key = 0; key < 128; key++) {
      page.setPageReference(key, new PageReference().setKey(key + 1L));
    }
    assertTrue(page.acquireGuard());

    page.close();

    assertFalse(page.isClosed());
    assertEquals(1, page.getGuardCount());
    assertEquals(128, page.segmentRefCount());
    assertSame(completePage, page.getCompletePageRef());

    page.releaseGuard();

    assertTrue(page.isClosed());
    assertEquals(0, page.getGuardCount());
    assertThrows(FrameReusedException.class, page::segmentRefCount);
    assertNull(page.getCompletePageRef());
    assertThrows(FrameReusedException.class, () -> page.getPageReference(0L));
    completePage.close();
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
  void testChunkAccumulatorPackedTombstoneAndRoaring() {
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

    final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
    for (int i = 0; i < page.getEntryCount(); i++) {
      final long chunkIdx = page.readKeyIntBE(i, page.getKeyLength(i) - 4) & 0xFFFFFFFFL;
      accumulator.addChunk(page, page.valueRef(i), chunkIdx << 16);
    }
    final NodeReferences merged = accumulator.toNodeReferencesAndReset();

    final Roaring64Bitmap expected = new Roaring64Bitmap();
    expected.add((3L << 16) | 0x0001);
    expected.add((3L << 16) | 0xFFFF);
    for (int v = 0; v < 100; v++) {
      expected.add((5L << 16) | (v * 3));
    }
    assertEquals(NodeReferences.owning(expected), merged);
    page.close();
  }

  // ===== Differential coverage for the zero-alloc key comparators =====
  //
  // These read the suffix EIGHT BYTES AT A TIME and finish with a byte tail, so their failure modes
  // are alignment-specific: a key whose suffix is 7, 8 or 9 bytes long exercises three different
  // code paths, and an off-by-one in the tail is invisible to any fixed set of hand-picked keys.
  // Everything below therefore cross-checks against Arrays.compareUnsigned over the materialized
  // key, across a key set that lands the compare at every offset in the eight-byte stride and a
  // probe set that perturbs every byte position.

  /** Shared head of every {@link #alignmentLeaf} key — becomes the leaf's commonPrefix. */
  private static final byte[] ALIGNMENT_HEAD = {'P', 'R', 'E'};

  /** Shortest and longest key in {@link #alignmentLeaf}; the span covers 0..37-byte suffixes. */
  private static final int ALIGNMENT_MIN_LEN = ALIGNMENT_HEAD.length;
  private static final int ALIGNMENT_MAX_LEN = 40;

  /**
   * A key of exactly {@code len} bytes: the shared head, then bytes derived from {@code len} so that
   * no two keys collide and every key carries bytes above {@code 0x7F} — the comparators treat bytes
   * as unsigned, which a purely ASCII corpus would never catch.
   */
  private static byte[] alignmentKey(final int len) {
    final byte[] key = new byte[len];
    System.arraycopy(ALIGNMENT_HEAD, 0, key, 0, Math.min(len, ALIGNMENT_HEAD.length));
    for (int i = ALIGNMENT_HEAD.length; i < len; i++) {
      key[i] = (byte) ((len * 31 + i * 17) & 0xFF);
    }
    return key;
  }

  /** Keys of every length in {@code [ALIGNMENT_MIN_LEN, ALIGNMENT_MAX_LEN]}, in insertion order. */
  private static List<byte[]> alignmentKeys() {
    final List<byte[]> keys = new ArrayList<>(ALIGNMENT_MAX_LEN - ALIGNMENT_MIN_LEN + 1);
    for (int len = ALIGNMENT_MIN_LEN; len <= ALIGNMENT_MAX_LEN; len++) {
      keys.add(alignmentKey(len));
    }
    return keys;
  }

  private static HOTLeafPage alignmentLeaf() {
    final HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.CAS);
    final byte[] value = {1};
    for (final byte[] key : alignmentKeys()) {
      assertTrue(page.put(key, value), "put failed for key of length " + key.length);
    }
    return page;
  }

  /**
   * Probes derived from the stored keys: each key itself, each key with one byte bumped up and down
   * at every position, and each key truncated and extended by up to three bytes. The truncations and
   * extensions are what move a divergence across the eight-byte stride boundary.
   */
  private static List<byte[]> alignmentProbes() {
    final List<byte[]> probes = new ArrayList<>();
    probes.add(new byte[0]);
    probes.add(new byte[] {'P'});
    probes.add(new byte[] {'P', 'R'});
    probes.add(new byte[] {'P', 'S'});
    for (final byte[] key : alignmentKeys()) {
      probes.add(key);
      for (int p = 0; p < key.length; p++) {
        final byte[] lower = key.clone();
        lower[p] = (byte) ((lower[p] & 0xFF) - 1);
        probes.add(lower);
        final byte[] higher = key.clone();
        higher[p] = (byte) ((higher[p] & 0xFF) + 1);
        probes.add(higher);
      }
      for (int drop = 1; drop <= 3 && key.length - drop >= 0; drop++) {
        probes.add(Arrays.copyOf(key, key.length - drop));
      }
      for (int add = 1; add <= 3; add++) {
        probes.add(Arrays.copyOf(key, key.length + add));
      }
    }
    return probes;
  }

  /** Unsigned-lex comparison of the first {@code n} bytes of each side. */
  private static int compareFirstBytes(final byte[] a, final byte[] b, final int n) {
    for (int i = 0; i < n; i++) {
      final int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
      if (diff != 0) {
        return diff;
      }
    }
    return 0;
  }

  @Test
  void compareKeyWithBoundMatchesArraysCompareAtEveryAlignment() {
    final HOTLeafPage page = alignmentLeaf();
    try {
      for (int i = 0; i < page.getEntryCount(); i++) {
        final byte[] stored = page.getKey(i);
        for (final byte[] probe : alignmentProbes()) {
          assertEquals(Integer.signum(Arrays.compareUnsigned(stored, probe)),
              Integer.signum(page.compareKeyWithBound(i, probe)),
              "entry " + i + " (len " + stored.length + ") vs probe of length " + probe.length);
        }
      }
    } finally {
      page.close();
    }
  }

  @Test
  void compareKeyPrefixMatchesReferenceAtEveryAlignment() {
    final HOTLeafPage page = alignmentLeaf();
    try {
      for (int i = 0; i < page.getEntryCount(); i++) {
        final byte[] stored = page.getKey(i);
        for (final byte[] probe : alignmentProbes()) {
          final int compared = Math.min(stored.length, probe.length);
          final int head = compareFirstBytes(stored, probe, compared);
          final int expected = head != 0
              ? Integer.signum(head)
              : (stored.length < probe.length
                  ? -1
                  : 0);
          assertEquals(expected, Integer.signum(page.compareKeyPrefix(i, probe, probe.length)),
              "entry " + i + " (len " + stored.length + ") vs prefix of length " + probe.length);
        }
      }
    } finally {
      page.close();
    }
  }

  @Test
  void compareKeyPrefixPartMatchesReferenceAtEveryAlignment() {
    final HOTLeafPage page = alignmentLeaf();
    try {
      for (int trailer = 0; trailer <= 5; trailer++) {
        for (int i = 0; i < page.getEntryCount(); i++) {
          final byte[] stored = page.getKey(i);
          final int partLen = Math.max(0, stored.length - trailer);
          for (final byte[] probe : alignmentProbes()) {
            final int compared = Math.min(partLen, probe.length);
            final int head = compareFirstBytes(stored, probe, compared);
            final int expected = head != 0
                ? Integer.signum(head)
                : Integer.signum(partLen - probe.length);
            assertEquals(expected, Integer.signum(page.compareKeyPrefixPart(i, trailer, probe, probe.length)), "entry "
                + i + " (len " + stored.length + ") trailer " + trailer + " vs probe of length " + probe.length);
          }
        }
      }
    } finally {
      page.close();
    }
  }

  @Test
  void findEntryMatchesLinearSearchAtEveryAlignment() {
    final HOTLeafPage page = alignmentLeaf();
    try {
      final int entryCount = page.getEntryCount();
      final byte[][] sorted = new byte[entryCount][];
      for (int i = 0; i < entryCount; i++) {
        sorted[i] = page.getKey(i);
      }
      for (final byte[] probe : alignmentProbes()) {
        int expected = -(entryCount + 1);
        for (int i = 0; i < entryCount; i++) {
          final int cmp = Arrays.compareUnsigned(sorted[i], probe);
          if (cmp == 0) {
            expected = i;
            break;
          }
          if (cmp > 0) {
            expected = -(i + 1);
            break;
          }
        }
        assertEquals(expected, page.findEntry(probe), "probe of length " + probe.length);
      }
    } finally {
      page.close();
    }
  }
}
