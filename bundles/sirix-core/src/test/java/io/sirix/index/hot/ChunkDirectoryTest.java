/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ChunkDirectory} and {@link ChunkDirectorySerializer}.
 */
@DisplayName("ChunkDirectory")
class ChunkDirectoryTest {

  @Nested
  @DisplayName("Empty directory")
  class EmptyDirectory {

    @Test
    @DisplayName("new directory is empty")
    void testNewDirectoryIsEmpty() {
      ChunkDirectory dir = new ChunkDirectory();
      
      assertEquals(0, dir.chunkCount());
      assertTrue(dir.isEmpty());
      assertFalse(dir.isModified());
    }

    @Test
    @DisplayName("getChunkRef returns null for non-existent chunk")
    void testGetChunkRefNull() {
      ChunkDirectory dir = new ChunkDirectory();
      
      assertNull(dir.getChunkRef(0));
      assertNull(dir.getChunkRef(10));
    }
  }

  @Nested
  @DisplayName("Adding chunks")
  class AddingChunks {

    @Test
    @DisplayName("getOrCreateChunkRef creates new chunk")
    void testGetOrCreateCreatesNew() {
      ChunkDirectory dir = new ChunkDirectory();
      
      PageReference ref = dir.getOrCreateChunkRef(0);
      
      assertNotNull(ref);
      assertEquals(1, dir.chunkCount());
      assertTrue(dir.isModified());
    }

    @Test
    @DisplayName("getOrCreateChunkRef returns existing chunk")
    void testGetOrCreateReturnsExisting() {
      ChunkDirectory dir = new ChunkDirectory();
      
      PageReference ref1 = dir.getOrCreateChunkRef(0);
      ref1.setKey(1234);
      
      PageReference ref2 = dir.getOrCreateChunkRef(0);
      
      assertSame(ref1, ref2);
      assertEquals(1234, ref2.getKey());
    }

    @Test
    @DisplayName("chunks are stored in sorted order")
    void testChunksSortedOrder() {
      ChunkDirectory dir = new ChunkDirectory();
      
      dir.getOrCreateChunkRef(5);
      dir.getOrCreateChunkRef(2);
      dir.getOrCreateChunkRef(8);
      dir.getOrCreateChunkRef(1);
      
      assertEquals(4, dir.chunkCount());
      assertEquals(1, dir.getChunkIndex(0));
      assertEquals(2, dir.getChunkIndex(1));
      assertEquals(5, dir.getChunkIndex(2));
      assertEquals(8, dir.getChunkIndex(3));
    }

    @Test
    @DisplayName("setChunkRef sets reference")
    void testSetChunkRef() {
      ChunkDirectory dir = new ChunkDirectory();
      
      PageReference ref = new PageReference();
      ref.setKey(5678);
      dir.setChunkRef(3, ref);
      
      assertEquals(1, dir.chunkCount());
      assertEquals(5678, dir.getChunkRef(3).getKey());
    }

    @Test
    @DisplayName("negative chunk index throws")
    void testNegativeChunkIndexThrows() {
      ChunkDirectory dir = new ChunkDirectory();
      
      assertThrows(IllegalArgumentException.class, () -> dir.getOrCreateChunkRef(-1));
    }
  }

  @Nested
  @DisplayName("Modification tracking")
  class ModificationTracking {

    @Test
    @DisplayName("clearModified clears the flag")
    void testClearModified() {
      ChunkDirectory dir = new ChunkDirectory();
      dir.getOrCreateChunkRef(0);
      assertTrue(dir.isModified());
      
      dir.clearModified();
      
      assertFalse(dir.isModified());
    }
  }

  @Nested
  @DisplayName("Copy operations")
  class CopyOperations {

    @Test
    @DisplayName("copy creates deep copy")
    void testCopyCreatesDeepCopy() {
      ChunkDirectory original = new ChunkDirectory();
      PageReference ref1 = original.getOrCreateChunkRef(0);
      ref1.setKey(100);
      PageReference ref2 = original.getOrCreateChunkRef(5);
      ref2.setKey(500);
      
      ChunkDirectory copy = original.copy();
      
      assertEquals(original.chunkCount(), copy.chunkCount());
      assertNotSame(original.getChunkRef(0), copy.getChunkRef(0));
      assertEquals(100, copy.getChunkRef(0).getKey());
      assertEquals(500, copy.getChunkRef(5).getKey());
      
      // Modify original
      original.getChunkRef(0).setKey(999);
      
      // Copy should be independent
      assertEquals(100, copy.getChunkRef(0).getKey());
    }
  }

  @Nested
  @DisplayName("Static utility methods")
  class StaticMethods {

    @Test
    @DisplayName("chunkIndexFor delegates to BitmapChunkPage")
    void testChunkIndexFor() {
      assertEquals(0, ChunkDirectory.chunkIndexFor(0));
      assertEquals(0, ChunkDirectory.chunkIndexFor(65535));
      assertEquals(1, ChunkDirectory.chunkIndexFor(65536));
    }
  }

  @Nested
  @DisplayName("Serialization")
  class Serialization {

    @Test
    @DisplayName("serialize empty directory")
    void testSerializeEmpty() {
      ChunkDirectory dir = new ChunkDirectory();
      
      byte[] bytes = new byte[100];
      int written = ChunkDirectorySerializer.serialize(dir, bytes, 0);
      
      assertEquals(4, written); // Just chunkCount
      
      ChunkDirectory deserialized = ChunkDirectorySerializer.deserialize(bytes, 0, written);
      
      assertEquals(0, deserialized.chunkCount());
      assertTrue(deserialized.isEmpty());
    }

    @Test
    @DisplayName("serialize directory with chunks")
    void testSerializeWithChunks() {
      ChunkDirectory dir = new ChunkDirectory();
      
      PageReference ref1 = dir.getOrCreateChunkRef(0);
      ref1.setKey(100);
      
      PageReference ref2 = dir.getOrCreateChunkRef(5);
      ref2.setKey(500);
      
      PageReference ref3 = dir.getOrCreateChunkRef(10);
      ref3.setKey(1000);
      
      int size = ChunkDirectorySerializer.serializedSize(dir);
      byte[] bytes = new byte[size];
      int written = ChunkDirectorySerializer.serialize(dir, bytes, 0);
      
      assertEquals(size, written);
      
      ChunkDirectory deserialized = ChunkDirectorySerializer.deserialize(bytes, 0, written);
      
      assertEquals(3, deserialized.chunkCount());
      assertEquals(0, deserialized.getChunkIndex(0));
      assertEquals(5, deserialized.getChunkIndex(1));
      assertEquals(10, deserialized.getChunkIndex(2));
      assertEquals(100, deserialized.getChunkRef(0).getKey());
      assertEquals(500, deserialized.getChunkRef(5).getKey());
      assertEquals(1000, deserialized.getChunkRef(10).getKey());
    }

    @Test
    @DisplayName("serialize to ByteBuffer")
    void testSerializeToByteBuffer() {
      ChunkDirectory dir = new ChunkDirectory();
      PageReference ref = dir.getOrCreateChunkRef(3);
      ref.setKey(333);
      
      ByteBuffer buffer = ByteBuffer.allocate(100);
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      
      int written = ChunkDirectorySerializer.serializeToBuffer(dir, buffer);
      
      buffer.flip();
      ChunkDirectory deserialized = ChunkDirectorySerializer.deserializeFromBuffer(buffer);
      
      assertEquals(1, deserialized.chunkCount());
      assertEquals(333, deserialized.getChunkRef(3).getKey());
    }

    @Test
    @DisplayName("isTombstone detects empty directory")
    void testIsTombstone() {
      ChunkDirectory emptyDir = new ChunkDirectory();
      byte[] emptyBytes = new byte[10];
      ChunkDirectorySerializer.serialize(emptyDir, emptyBytes, 0);
      
      assertTrue(ChunkDirectorySerializer.isTombstone(emptyBytes, 0, 4));
      
      ChunkDirectory nonEmptyDir = new ChunkDirectory();
      nonEmptyDir.getOrCreateChunkRef(0);
      int size = ChunkDirectorySerializer.serializedSize(nonEmptyDir);
      byte[] nonEmptyBytes = new byte[size];
      ChunkDirectorySerializer.serialize(nonEmptyDir, nonEmptyBytes, 0);
      
      assertFalse(ChunkDirectorySerializer.isTombstone(nonEmptyBytes, 0, size));
    }
  }

  private static void assertSame(PageReference expected, PageReference actual) {
    assertTrue(expected == actual, "Expected same reference");
  }
}

