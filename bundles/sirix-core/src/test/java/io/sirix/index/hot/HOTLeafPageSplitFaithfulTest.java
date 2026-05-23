/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIncrementalInsert.BiNode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification of {@link HOTIncrementalInsert#splitLeafPage} — step 1 of the faithful incremental
 * port. A leaf-page split must cut the bucket at its key-set MSDB into two complete
 * {@code R(S)}-subtree halves: every key in the left half has the split bit clear, every key in
 * the right half has it set, the union of the halves is exactly the input plus the new entry,
 * and tombstones / merged values are carried through.
 */
@DisplayName("HOTIncrementalInsert.splitLeafPage — faithful leaf-page split")
final class HOTLeafPageSplitFaithfulTest {

  private static final byte[] VALUE = {1, 2, 3};
  private static final byte[] TOMBSTONE = {(byte) 0xFE};

  @Test
  @DisplayName("splits adversarial leaf pages into two clean R(S)-subtree halves")
  void splitsAdversarialLeafPages() {
    final int[] sizes = {2, 7, 64, 300, 512};
    int checked = 0;
    for (final int size : sizes) {
      for (int seed = 0; seed < 6; seed++) {
        final Random random = new Random((size * 131L) ^ seed);
        final TreeSet<Long> keys = new TreeSet<>(Long::compareUnsigned);
        while (keys.size() < size + 1) {
          keys.add(random.nextLong());
        }
        final List<Long> sorted = new ArrayList<>(keys);
        final long newKey = sorted.remove(random.nextInt(sorted.size()));
        checkSplit(sorted, newKey, VALUE, "size=" + size + " seed=" + seed);
        checked++;
      }
    }
    System.out.println("[leaf-split] " + checked + " adversarial splits — clean");
  }

  @Test
  @DisplayName("a new key duplicating an existing one OR-merges values with no entry growth")
  void duplicateKeyOrMerges() {
    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage source = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
    final TreeSet<Long> keys = new TreeSet<>(Long::compareUnsigned);
    final Random random = new Random(0xDEAD_BEEFL);
    while (keys.size() < 200) {
      keys.add(random.nextLong());
    }
    final List<Long> sorted = new ArrayList<>(keys);
    final long dupKey = sorted.get(97);
    for (final long k : sorted) {
      source.put(beKey(k), k == dupKey ? nodeRefs(1L, 2L) : VALUE);
    }
    final BiNode result = HOTIncrementalInsert.splitLeafPage(source, beKey(dupKey),
        nodeRefs(3L), 1, IndexType.CAS, allocator::getAndIncrement);

    final Map<String, byte[]> entries = new HashMap<>();
    collectEntries(result.left().getPage(), entries);
    collectEntries(result.right().getPage(), entries);
    assertEquals(sorted.size(), entries.size(), "duplicate key must not grow the entry count");
    final byte[] mergedValue = entries.get(hex(beKey(dupKey)));
    final NodeReferences merged = NodeReferencesSerializer.deserialize(mergedValue);
    assertTrue(merged.getNodeKeys().contains(1L) && merged.getNodeKeys().contains(2L)
        && merged.getNodeKeys().contains(3L), "values OR-merged: expected {1,2,3}");

    source.close();
    closeLeaves(result.left().getPage());
    closeLeaves(result.right().getPage());
  }

  @Test
  @DisplayName("variable-length keys and tombstone entries survive the split")
  void variableLengthKeysAndTombstones() {
    final Random random = new Random(0x5151_2026L);
    final TreeSet<byte[]> keys = new TreeSet<>(Arrays::compareUnsigned);
    while (keys.size() < 260) {
      final byte[] key = new byte[1 + random.nextInt(20)];
      random.nextBytes(key);
      keys.add(key);
    }
    final List<byte[]> sorted = new ArrayList<>(keys);
    final byte[] newKey = sorted.remove(130);

    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage source = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
    int tombstones = 0;
    for (int i = 0; i < sorted.size(); i++) {
      final boolean tomb = i % 9 == 4;
      if (tomb) {
        tombstones++;
      }
      source.put(sorted.get(i), tomb ? TOMBSTONE : VALUE);
    }
    final BiNode result = HOTIncrementalInsert.splitLeafPage(source, newKey, VALUE, 1,
        IndexType.CAS, allocator::getAndIncrement);

    final Map<String, byte[]> entries = new HashMap<>();
    collectEntries(result.left().getPage(), entries);
    collectEntries(result.right().getPage(), entries);
    assertEquals(sorted.size() + 1, entries.size(), "every variable-length key preserved");
    int tombstonesAfter = 0;
    for (final byte[] value : entries.values()) {
      if (NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
        tombstonesAfter++;
      }
    }
    assertEquals(tombstones, tombstonesAfter, "every tombstone carried through the split");

    source.close();
    closeLeaves(result.left().getPage());
    closeLeaves(result.right().getPage());
  }

  // ===== helpers =====

  private static void checkSplit(final List<Long> sourceKeys, final long newKey,
      final byte[] newValue, final String label) {
    final AtomicLong allocator = new AtomicLong(1);
    final HOTLeafPage source = new HOTLeafPage(allocator.getAndIncrement(), 1, IndexType.CAS);
    for (final long k : sourceKeys) {
      source.put(beKey(k), VALUE);
    }
    final BiNode result = HOTIncrementalInsert.splitLeafPage(source, beKey(newKey), newValue, 1,
        IndexType.CAS, allocator::getAndIncrement);

    final TreeSet<Long> union = new TreeSet<>(Long::compareUnsigned);
    union.addAll(sourceKeys);
    union.add(newKey);
    final int expectedBit = HOTBulkBuilder.msdb(beKey(union.first()), beKey(union.last()));
    assertEquals(expectedBit, result.discriminativeBitIndex(), label + ": split bit is key-set MSDB");

    final Map<String, byte[]> left = new HashMap<>();
    final Map<String, byte[]> right = new HashMap<>();
    collectEntries(result.left().getPage(), left);
    collectEntries(result.right().getPage(), right);
    assertFalse(left.isEmpty(), label + ": left half non-empty");
    assertFalse(right.isEmpty(), label + ": right half non-empty");

    for (final String k : left.keySet()) {
      assertFalse(HOTBulkBuilder.bitAt(HexFormat.of().parseHex(k), expectedBit),
          label + ": left key has split bit clear");
    }
    for (final String k : right.keySet()) {
      assertTrue(HOTBulkBuilder.bitAt(HexFormat.of().parseHex(k), expectedBit),
          label + ": right key has split bit set");
    }

    final TreeSet<String> got = new TreeSet<>();
    got.addAll(left.keySet());
    got.addAll(right.keySet());
    assertEquals(left.size() + right.size(), got.size(), label + ": halves are key-disjoint");
    final TreeSet<String> expected = new TreeSet<>();
    for (final long k : union) {
      expected.add(hex(beKey(k)));
    }
    assertEquals(expected, got, label + ": union of halves == input keys + new key");

    assertEquals(1 + Math.max(heightOf(result.left().getPage()), heightOf(result.right().getPage())),
        result.height(), label + ": height = 1 + max child height");

    source.close();
    closeLeaves(result.left().getPage());
    closeLeaves(result.right().getPage());
  }

  private static void collectEntries(final Page page, final Map<String, byte[]> out) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        out.put(hex(leaf.getKey(i)), leaf.getValue(i));
      }
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          collectEntries(reference.getPage(), out);
        }
      }
    }
  }

  private static void closeLeaves(final Page page) {
    if (page instanceof HOTLeafPage leaf) {
      leaf.close();
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        final PageReference reference = indirect.getChildReference(i);
        if (reference != null && reference.getPage() != null) {
          closeLeaves(reference.getPage());
        }
      }
    }
  }

  private static int heightOf(final Page page) {
    return page instanceof HOTIndirectPage indirect ? indirect.getHeight() : 0;
  }

  private static byte[] nodeRefs(final long... bits) {
    final NodeReferences references = new NodeReferences();
    for (final long bit : bits) {
      references.getNodeKeys().add(bit);
    }
    final byte[] out = new byte[NodeReferencesSerializer.computeSerializedSize(references)];
    NodeReferencesSerializer.serialize(references, out, 0);
    return out;
  }

  private static byte[] beKey(final long value) {
    final byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) (value >>> (56 - 8 * i));
    }
    return bytes;
  }

  private static String hex(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
