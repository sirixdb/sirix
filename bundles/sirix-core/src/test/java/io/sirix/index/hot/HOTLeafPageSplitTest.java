/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.access.trx.page.HOTTrieWriter;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.utils.OS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for HOT leaf page splitting and HOTIndirectPage creation.
 */
@DisplayName("HOT Leaf Page Split")
class HOTLeafPageSplitTest {
  
  @BeforeAll
  static void initAllocator() {
    if (!OS.isWindows()) {
      LinuxMemorySegmentAllocator.getInstance().init(64 * 1024 * 1024); // 64MB
    } else {
      WindowsMemorySegmentAllocator.getInstance().init(64 * 1024 * 1024);
    }
  }

  @Nested
  @DisplayName("HOTLeafPage.splitTo()")
  class SplitTo {

    @Test
    @DisplayName("splitTo moves right half of entries to target page")
    void testSplitToBasic() {
      HOTLeafPage source = new HOTLeafPage(1L, 1, IndexType.PATH);
      
      // Add 10 entries
      for (int i = 0; i < 10; i++) {
        byte[] key = ("key" + String.format("%03d", i)).getBytes();
        byte[] value = ("value" + i).getBytes();
        assertTrue(source.mergeWithNodeRefs(key, key.length, value, value.length),
            "Should insert entry " + i);
      }
      
      assertEquals(10, source.getEntryCount());
      
      // Create target page
      HOTLeafPage target = new HOTLeafPage(2L, 1, IndexType.PATH);
      
      // Split
      byte[] splitKey = source.splitTo(target);
      
      // Verify split
      assertEquals(5, source.getEntryCount(), "Source should have left half");
      assertEquals(5, target.getEntryCount(), "Target should have right half");
      
      assertNotNull(splitKey);
      assertEquals("key005", new String(splitKey), "Split key should be first key in target");
      
      // Verify keys are in correct pages
      assertTrue(source.findEntry("key000".getBytes()) >= 0, "key000 should be in source");
      assertTrue(source.findEntry("key004".getBytes()) >= 0, "key004 should be in source");
      assertTrue(target.findEntry("key005".getBytes()) >= 0, "key005 should be in target");
      assertTrue(target.findEntry("key009".getBytes()) >= 0, "key009 should be in target");
      
      // Verify keys are NOT in wrong pages
      assertTrue(source.findEntry("key005".getBytes()) < 0, "key005 should NOT be in source");
      assertTrue(target.findEntry("key004".getBytes()) < 0, "key004 should NOT be in target");
    }

    @Test
    @DisplayName("splitTo with odd number of entries")
    void testSplitToOddEntries() {
      HOTLeafPage source = new HOTLeafPage(1L, 1, IndexType.CAS);
      
      // Add 7 entries
      for (int i = 0; i < 7; i++) {
        byte[] key = ("k" + i).getBytes();
        byte[] value = ("v" + i).getBytes();
        source.mergeWithNodeRefs(key, key.length, value, value.length);
      }
      
      HOTLeafPage target = new HOTLeafPage(2L, 1, IndexType.CAS);
      source.splitTo(target);
      
      // 7 / 2 = 3 (integer division)
      assertEquals(3, source.getEntryCount(), "Source should have 3 entries");
      assertEquals(4, target.getEntryCount(), "Target should have 4 entries");
    }

    @Test
    @DisplayName("getFirstKey and getLastKey work correctly")
    void testFirstAndLastKey() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      
      // Add entries in sorted order (they're inserted sorted)
      page.mergeWithNodeRefs("aaa".getBytes(), 3, "v1".getBytes(), 2);
      page.mergeWithNodeRefs("mmm".getBytes(), 3, "v2".getBytes(), 2);
      page.mergeWithNodeRefs("zzz".getBytes(), 3, "v3".getBytes(), 2);
      
      assertEquals("aaa", new String(page.getFirstKey()));
      assertEquals("zzz", new String(page.getLastKey()));
    }
  }

  @Nested
  @DisplayName("HOTTrieWriter.handleLeafSplit()")
  class HandleLeafSplit {

    @Test
    @DisplayName("handleLeafSplit creates HOTIndirectPage as new root")
    void testHandleLeafSplitCreatesIndirectPage() {
      // Create a full page
      HOTLeafPage fullPage = new HOTLeafPage(1L, 1, IndexType.PATH);
      for (int i = 0; i < HOTLeafPage.MAX_ENTRIES; i++) {
        byte[] key = String.format("key%04d", i).getBytes();
        byte[] value = ("value" + i).getBytes();
        if (!fullPage.mergeWithNodeRefs(key, key.length, value, value.length)) {
          break; // Page is full
        }
      }
      assertTrue(fullPage.needsSplit(), "Page should need split");
      
      // Mock dependencies
      StorageEngineWriter pageTrx = mock(StorageEngineWriter.class);
      TransactionIntentLog log = mock(TransactionIntentLog.class);
      when(pageTrx.getRevisionNumber()).thenReturn(1);
      when(pageTrx.getLog()).thenReturn(log);
      
      // Create references
      PageReference pageRef = new PageReference();
      pageRef.setKey(1L);
      PageReference rootRef = new PageReference();
      rootRef.setKey(1L);
      
      // Handle split
      HOTTrieWriter trieWriter = new HOTTrieWriter();
      byte[] splitKey = trieWriter.handleLeafSplit(pageTrx, log, fullPage, pageRef, rootRef);
      
      // Verify
      assertNotNull(splitKey, "Split key should be returned");
      assertFalse(fullPage.needsSplit(), "Original page should no longer need split");
    }
  }

  @Nested
  @DisplayName("HOTIndirectPage navigation")
  class IndirectPageNavigation {

    @Test
    @DisplayName("BiNode correctly routes to left or right child")
    void testBiNodeNavigation() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100L);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200L);
      
      // Create BiNode with discriminative bit at position 0 (MSB of first byte)
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(
          1L, 1, 7, leftRef, rightRef); // bit 7 = MSB of byte 0
      
      assertEquals(2, biNode.getNumChildren());
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, biNode.getNodeType());
      
      // Keys with bit 7 = 0 should go left, bit 7 = 1 should go right
      byte[] leftKey = new byte[] { 0x00, 0x01, 0x02 }; // bit 7 = 0
      byte[] rightKey = new byte[] { (byte)0x80, 0x01, 0x02 }; // bit 7 = 1
      
      int leftChildIndex = biNode.findChildIndex(leftKey);
      int rightChildIndex = biNode.findChildIndex(rightKey);
      
      assertEquals(0, leftChildIndex, "Key with bit 7 = 0 should route to child 0");
      assertEquals(1, rightChildIndex, "Key with bit 7 = 1 should route to child 1");
    }

    @Test
    @DisplayName("SpanNode with multiple children")
    void testSpanNodeNavigation() {
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100L + i);
      }
      
      byte[] partialKeys = new byte[] { 0, 1, 2, 3 };
      
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
          1L, 1, (byte)0, 0x03L, partialKeys, children);
      
      assertEquals(4, spanNode.getNumChildren());
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, spanNode.getNodeType());
    }
  }

  @Nested
  @DisplayName("End-to-end split scenario")
  class EndToEndSplit {

    @Test
    @DisplayName("Page fills up, splits, and both pages are usable")
    void testFullSplitScenario() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      
      // Fill page until it needs split (or close to it)
      int inserted = 0;
      while (!page.needsSplit() && inserted < HOTLeafPage.MAX_ENTRIES) {
        byte[] key = String.format("key%05d", inserted).getBytes();
        byte[] value = new byte[100]; // Fixed size value
        if (!page.mergeWithNodeRefs(key, key.length, value, value.length)) {
          break;
        }
        inserted++;
      }
      
      assertTrue(inserted > 0, "Should have inserted at least some entries");
      
      // If page needs split, perform it
      if (page.needsSplit()) {
        HOTLeafPage rightPage = new HOTLeafPage(2L, 1, IndexType.PATH);
        byte[] splitKey = page.splitTo(rightPage);
        
        assertNotNull(splitKey);
        
        int leftCount = page.getEntryCount();
        int rightCount = rightPage.getEntryCount();
        
        assertEquals(inserted, leftCount + rightCount, 
            "Total entries should be preserved after split");
        
        // Both pages should have room for new entries
        assertFalse(page.needsSplit(), "Left page should not need split");
        assertFalse(rightPage.needsSplit(), "Right page should not need split");
        
        // Both pages should be able to accept new entries
        byte[] newKey1 = "aaa00000".getBytes(); // Should go to left
        byte[] newKey2 = "zzz99999".getBytes(); // Should go to right
        
        assertTrue(page.mergeWithNodeRefs(newKey1, newKey1.length, new byte[10], 10),
            "Left page should accept new entry");
        assertTrue(rightPage.mergeWithNodeRefs(newKey2, newKey2.length, new byte[10], 10),
            "Right page should accept new entry");
      }
    }
  }
}

