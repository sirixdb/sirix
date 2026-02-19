/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HOTLeafPage}.
 */
class HOTLeafPageTest {

  private static final int PAGE_SIZE = 64 * 1024;

  private HOTLeafPage hotLeafPage;
  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    MemorySegment slotMemory = arena.allocate(PAGE_SIZE);

    // Use the constructor that accepts pre-allocated memory
    hotLeafPage = new HOTLeafPage(1L, 1, IndexType.PATH, slotMemory, null, // null releaser - Arena will handle cleanup
        new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
  }

  @AfterEach
  void tearDown() {
    if (hotLeafPage != null && !hotLeafPage.isClosed()) {
      hotLeafPage.close();
      hotLeafPage = null;
    }
    if (arena != null) {
      arena.close();
      arena = null;
    }
  }

  @Test
  void testBasicCreation() {
    assertEquals(1L, hotLeafPage.getPageKey());
    assertEquals(1, hotLeafPage.getRevision());
    assertEquals(IndexType.PATH, hotLeafPage.getIndexType());
    assertEquals(0, hotLeafPage.getEntryCount());
    assertFalse(hotLeafPage.isClosed());
  }

  @Test
  void testInsertAndFind() {
    byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] value = "world".getBytes(StandardCharsets.UTF_8);

    assertTrue(hotLeafPage.put(key, value));
    assertEquals(1, hotLeafPage.getEntryCount());

    int index = hotLeafPage.findEntry(key);
    assertTrue(index >= 0, "Key should be found");

    byte[] retrievedValue = hotLeafPage.getValue(index);
    assertArrayEquals(value, retrievedValue);
  }

  @Test
  void testSortedOrder() {
    // Insert keys in random order
    byte[] key1 = "apple".getBytes(StandardCharsets.UTF_8);
    byte[] key2 = "banana".getBytes(StandardCharsets.UTF_8);
    byte[] key3 = "cherry".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);

    // Insert out of order
    hotLeafPage.put(key2, value);
    hotLeafPage.put(key1, value);
    hotLeafPage.put(key3, value);

    assertEquals(3, hotLeafPage.getEntryCount());

    // Keys should be found in sorted positions
    assertTrue(hotLeafPage.findEntry(key1) >= 0);
    assertTrue(hotLeafPage.findEntry(key2) >= 0);
    assertTrue(hotLeafPage.findEntry(key3) >= 0);

    // Verify sorted order by checking positions
    int pos1 = hotLeafPage.findEntry(key1);
    int pos2 = hotLeafPage.findEntry(key2);
    int pos3 = hotLeafPage.findEntry(key3);

    assertTrue(pos1 < pos2, "apple should come before banana");
    assertTrue(pos2 < pos3, "banana should come before cherry");
  }

  @Test
  void testNotFound() {
    byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] value = "world".getBytes(StandardCharsets.UTF_8);

    hotLeafPage.put(key, value);

    byte[] notFoundKey = "notfound".getBytes(StandardCharsets.UTF_8);
    int index = hotLeafPage.findEntry(notFoundKey);
    assertTrue(index < 0, "Key should not be found");

    // Verify insertion point is returned as -(insertionPoint + 1)
    int insertionPoint = -(index + 1);
    assertTrue(insertionPoint >= 0);
  }

  @Test
  void testZeroCopyKeySlice() {
    byte[] key = "testkey".getBytes(StandardCharsets.UTF_8);
    byte[] value = "testvalue".getBytes(StandardCharsets.UTF_8);

    hotLeafPage.put(key, value);

    int index = hotLeafPage.findEntry(key);
    MemorySegment keySlice = hotLeafPage.getKeySlice(index);

    assertNotNull(keySlice);
    assertEquals(key.length, keySlice.byteSize());

    // Verify contents match
    byte[] sliceBytes = new byte[(int) keySlice.byteSize()];
    MemorySegment.copy(keySlice, ValueLayout.JAVA_BYTE, 0, sliceBytes, 0, sliceBytes.length);
    assertArrayEquals(key, sliceBytes);
  }

  @Test
  void testZeroCopyValueSlice() {
    byte[] key = "testkey".getBytes(StandardCharsets.UTF_8);
    byte[] value = "testvalue".getBytes(StandardCharsets.UTF_8);

    hotLeafPage.put(key, value);

    int index = hotLeafPage.findEntry(key);
    MemorySegment valueSlice = hotLeafPage.getValueSlice(index);

    assertNotNull(valueSlice);
    assertEquals(value.length, valueSlice.byteSize());

    // Verify contents match
    byte[] sliceBytes = new byte[(int) valueSlice.byteSize()];
    MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, sliceBytes, 0, sliceBytes.length);
    assertArrayEquals(value, sliceBytes);
  }

  @Test
  void testGuardManagement() {
    assertEquals(0, hotLeafPage.getGuardCount());

    hotLeafPage.acquireGuard();
    assertEquals(1, hotLeafPage.getGuardCount());
    assertTrue(hotLeafPage.isHot());

    hotLeafPage.acquireGuard();
    assertEquals(2, hotLeafPage.getGuardCount());

    hotLeafPage.releaseGuard();
    assertEquals(1, hotLeafPage.getGuardCount());

    hotLeafPage.releaseGuard();
    assertEquals(0, hotLeafPage.getGuardCount());
  }

  @Test
  void testGuardUnderflowThrows() {
    assertThrows(IllegalStateException.class, () -> {
      hotLeafPage.releaseGuard();
    });
  }

  @Test
  void testOrphanedCleanup() {
    hotLeafPage.acquireGuard();
    hotLeafPage.markOrphaned();

    assertTrue(hotLeafPage.isOrphaned());
    assertFalse(hotLeafPage.isClosed(), "Should not close while guarded");

    hotLeafPage.releaseGuard();
    assertTrue(hotLeafPage.isClosed(), "Should close when guard released and orphaned");
  }

  @Test
  void testClose() {
    assertFalse(hotLeafPage.isClosed());
    hotLeafPage.close();
    assertTrue(hotLeafPage.isClosed());

    // Double close should be safe
    hotLeafPage.close();
    assertTrue(hotLeafPage.isClosed());
  }

  @Test
  void testNeedsSplit() {
    assertFalse(hotLeafPage.needsSplit());

    // Insert entries until the page is full
    // Note: We may hit space limit before MAX_ENTRIES due to key/value size
    int inserted = 0;
    for (int i = 0; i < HOTLeafPage.MAX_ENTRIES && !hotLeafPage.needsSplit(); i++) {
      byte[] key = String.format("key%05d", i).getBytes(StandardCharsets.UTF_8);
      byte[] value = "v".getBytes(StandardCharsets.UTF_8);
      if (hotLeafPage.put(key, value)) {
        inserted++;
      } else {
        break; // Space limit hit
      }
    }

    // Either we hit MAX_ENTRIES or ran out of space
    assertTrue(inserted > 0, "Should have inserted at least one entry");
    assertTrue(hotLeafPage.needsSplit() || inserted == HOTLeafPage.MAX_ENTRIES);
  }

  @Test
  void testVersionIncrement() {
    assertEquals(0, hotLeafPage.getVersion());

    int newVersion = hotLeafPage.incrementVersion();
    assertEquals(1, newVersion);
    assertEquals(1, hotLeafPage.getVersion());
  }

  @Test
  void testHotFlag() {
    assertFalse(hotLeafPage.isHot());

    hotLeafPage.acquireGuard();
    assertTrue(hotLeafPage.isHot());

    hotLeafPage.clearHot();
    assertFalse(hotLeafPage.isHot());

    hotLeafPage.releaseGuard();
  }

  @Test
  void testEmptyPage() {
    assertEquals(0, hotLeafPage.getEntryCount());
    assertEquals(0, hotLeafPage.size());

    byte[] key = "notfound".getBytes(StandardCharsets.UTF_8);
    int index = hotLeafPage.findEntry(key);
    assertTrue(index < 0);
  }

  @Test
  void testBinaryKeys() {
    // Test with binary data (not just ASCII)
    byte[] key1 = new byte[] {0x00, 0x01, 0x02};
    byte[] key2 = new byte[] {0x00, 0x01, 0x03};
    byte[] key3 = new byte[] {(byte) 0xFF, (byte) 0xFE};
    byte[] value = new byte[] {0x10, 0x20};

    hotLeafPage.put(key1, value);
    hotLeafPage.put(key2, value);
    hotLeafPage.put(key3, value);

    assertEquals(3, hotLeafPage.getEntryCount());

    assertTrue(hotLeafPage.findEntry(key1) >= 0);
    assertTrue(hotLeafPage.findEntry(key2) >= 0);
    assertTrue(hotLeafPage.findEntry(key3) >= 0);

    // High bytes should sort after low bytes (unsigned comparison)
    int posLow = hotLeafPage.findEntry(key1);
    int posHigh = hotLeafPage.findEntry(key3);
    assertTrue(posLow < posHigh, "Low bytes should sort before high bytes");
  }

  @Test
  void testToString() {
    String str = hotLeafPage.toString();
    assertNotNull(str);
    assertTrue(str.contains("HOTLeafPage"));
    assertTrue(str.contains("pageKey=1"));
    assertTrue(str.contains("indexType=PATH"));
  }

  /**
   * REGRESSION: recalculateUsedMemory() previously used raw short (sign-extended) for key/value
   * lengths, giving wrong results for lengths >= 32768. Test that compact() + getRemainingSpace()
   * are consistent when a value of length >= 32768 is stored.
   */
  @Test
  void testRecalculateUsedMemoryLargeValueLength() {
    // A page that can hold one very large entry (we need > 32767 bytes for value)
    // Default page size is 64KB = 65536 bytes.
    // One entry: 2 (keyLen) + 1 (key) + 2 (valueLen) + 32768 (value) = 32773 bytes
    byte[] smallKey = new byte[] {0x42};
    byte[] largeValue = new byte[32768]; // exactly 0x8000 → sign-extends to -32768 when read as short
    java.util.Arrays.fill(largeValue, (byte) 0xAB);

    boolean inserted = hotLeafPage.put(smallKey, largeValue);
    assertTrue(inserted, "Large value should fit in a 64KB page");
    assertEquals(1, hotLeafPage.getEntryCount());

    // Verify round-trip: the stored value equals what we inserted
    int idx = hotLeafPage.findEntry(smallKey);
    assertTrue(idx >= 0, "Key must be findable");
    byte[] retrieved = hotLeafPage.getValue(idx);
    assertArrayEquals(largeValue, retrieved, "Retrieved value must match inserted value");

    // compact() calls recalculateUsedMemory() internally; must not corrupt usedMemory
    int reclaimed = hotLeafPage.compact();
    assertTrue(reclaimed >= 0, "Compact must not return negative (sign-overflow symptom)");

    // After compact, the entry must still be findable and correct
    idx = hotLeafPage.findEntry(smallKey);
    assertTrue(idx >= 0, "Key must be findable after compact");
    retrieved = hotLeafPage.getValue(idx);
    assertArrayEquals(largeValue, retrieved, "Value must be intact after compact");

    // Remaining space must be consistent: DEFAULT_SIZE - overhead - entrySize
    // If recalculateUsedMemory signed-extended 32768 as -32768, usedMemory would be negative,
    // making getRemainingSpace() return a huge positive value — detectable as > DEFAULT_SIZE.
    long remaining = hotLeafPage.getRemainingSpace();
    assertTrue(remaining >= 0, "Remaining space must not be negative");
    assertTrue(remaining < HOTLeafPage.DEFAULT_SIZE,
        "Remaining space must be less than page size (was " + remaining + "), sign-overflow if >= DEFAULT_SIZE");
  }

  @Test
  void testCompactWithLargeKeys() {
    // Insert keys longer than 8 bytes to exercise compareKeysSimd's > 8 byte path
    byte[] key9  = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8};      // 9 bytes
    byte[] key16 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 1}; // 16 bytes, differs at byte 15
    byte[] key17 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 2}; // 16 bytes, differs at byte 15
    byte[] value = new byte[] {1};

    hotLeafPage.put(key9, value);
    hotLeafPage.put(key16, value);
    hotLeafPage.put(key17, value);

    assertEquals(3, hotLeafPage.getEntryCount());

    // All three must be found (exercises compareKeysSimd chunk loop)
    assertTrue(hotLeafPage.findEntry(key9) >= 0);
    assertTrue(hotLeafPage.findEntry(key16) >= 0);
    assertTrue(hotLeafPage.findEntry(key17) >= 0);

    // compact should leave all entries intact
    hotLeafPage.compact();
    assertEquals(3, hotLeafPage.getEntryCount());
    assertTrue(hotLeafPage.findEntry(key9) >= 0);
    assertTrue(hotLeafPage.findEntry(key16) >= 0);
    assertTrue(hotLeafPage.findEntry(key17) >= 0);
  }
}

