/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BitmapChunkPage}.
 */
@DisplayName("BitmapChunkPage")
class BitmapChunkPageTest {

  @Nested
  @DisplayName("Static utility methods")
  class StaticMethods {

    @Test
    @DisplayName("chunkIndexFor calculates correct chunk index")
    void testChunkIndexFor() {
      assertEquals(0, BitmapChunkPage.chunkIndexFor(0));
      assertEquals(0, BitmapChunkPage.chunkIndexFor(65535));
      assertEquals(1, BitmapChunkPage.chunkIndexFor(65536));
      assertEquals(1, BitmapChunkPage.chunkIndexFor(131071));
      assertEquals(2, BitmapChunkPage.chunkIndexFor(131072));
      assertEquals(10, BitmapChunkPage.chunkIndexFor(655360));
    }

    @Test
    @DisplayName("chunkRangeStart calculates correct start")
    void testChunkRangeStart() {
      assertEquals(0, BitmapChunkPage.chunkRangeStart(0));
      assertEquals(65536, BitmapChunkPage.chunkRangeStart(1));
      assertEquals(131072, BitmapChunkPage.chunkRangeStart(2));
      assertEquals(655360, BitmapChunkPage.chunkRangeStart(10));
    }

    @Test
    @DisplayName("chunkRangeEnd calculates correct end")
    void testChunkRangeEnd() {
      assertEquals(65536, BitmapChunkPage.chunkRangeEnd(0));
      assertEquals(131072, BitmapChunkPage.chunkRangeEnd(1));
      assertEquals(196608, BitmapChunkPage.chunkRangeEnd(2));
    }

    @Test
    @DisplayName("chunkIndexFor rejects negative keys")
    void testChunkIndexForNegative() {
      assertThrows(IllegalArgumentException.class, () -> BitmapChunkPage.chunkIndexFor(-1));
    }
  }

  @Nested
  @DisplayName("Full mode pages")
  class FullMode {

    @Test
    @DisplayName("createFull creates a full snapshot page")
    void testCreateFull() {
      Roaring64Bitmap bitmap = new Roaring64Bitmap();
      bitmap.add(100);
      bitmap.add(200);
      bitmap.add(300);

      BitmapChunkPage page = BitmapChunkPage.createFull(1L, 5, IndexType.PATH, 0, 65536, bitmap);

      assertFalse(page.isDelta());
      assertFalse(page.isDeleted());
      assertTrue(page.isFullSnapshot());
      assertEquals(1L, page.getPageKey());
      assertEquals(5, page.getRevision());
      assertEquals(IndexType.PATH, page.getIndexType());
      assertEquals(0, page.getRangeStart());
      assertEquals(65536, page.getRangeEnd());
      assertNotNull(page.getBitmap());
      assertEquals(3, page.getBitmap().getLongCardinality());
    }

    @Test
    @DisplayName("createEmptyFull creates an empty full page")
    void testCreateEmptyFull() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyFull(1L, 1, IndexType.CAS, 0, 65536);

      assertTrue(page.isFullSnapshot());
      assertNotNull(page.getBitmap());
      assertEquals(0, page.getBitmap().getLongCardinality());
    }

    @Test
    @DisplayName("addKey adds to bitmap in full mode")
    void testAddKeyFull() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyFull(1L, 1, IndexType.PATH, 0, 65536);

      page.addKey(100);
      page.addKey(200);
      page.addKey(300);

      assertTrue(page.containsKey(100));
      assertTrue(page.containsKey(200));
      assertTrue(page.containsKey(300));
      assertFalse(page.containsKey(400));
    }

    @Test
    @DisplayName("removeKey removes from bitmap in full mode")
    void testRemoveKeyFull() {
      Roaring64Bitmap bitmap = new Roaring64Bitmap();
      bitmap.add(100);
      bitmap.add(200);
      bitmap.add(300);
      BitmapChunkPage page = BitmapChunkPage.createFull(1L, 1, IndexType.PATH, 0, 65536, bitmap);

      page.removeKey(200);

      assertTrue(page.containsKey(100));
      assertFalse(page.containsKey(200));
      assertTrue(page.containsKey(300));
    }

    @Test
    @DisplayName("addKey rejects keys outside range")
    void testAddKeyOutOfRange() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyFull(1L, 1, IndexType.PATH, 65536, 131072);

      assertThrows(IllegalArgumentException.class, () -> page.addKey(100));
      assertThrows(IllegalArgumentException.class, () -> page.addKey(131072));
    }
  }

  @Nested
  @DisplayName("Delta mode pages")
  class DeltaMode {

    @Test
    @DisplayName("createDelta creates a delta page")
    void testCreateDelta() {
      Roaring64Bitmap additions = new Roaring64Bitmap();
      additions.add(100);
      additions.add(200);
      Roaring64Bitmap removals = new Roaring64Bitmap();
      removals.add(50);

      BitmapChunkPage page = BitmapChunkPage.createDelta(1L, 5, IndexType.PATH, 0, 65536, additions, removals);

      assertTrue(page.isDelta());
      assertFalse(page.isDeleted());
      assertFalse(page.isFullSnapshot());
      assertNull(page.getBitmap());
      assertNotNull(page.getAdditions());
      assertNotNull(page.getRemovals());
      assertEquals(2, page.getAdditions().getLongCardinality());
      assertEquals(1, page.getRemovals().getLongCardinality());
    }

    @Test
    @DisplayName("createEmptyDelta creates an empty delta page")
    void testCreateEmptyDelta() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyDelta(1L, 1, IndexType.NAME, 0, 65536);

      assertTrue(page.isDelta());
      assertNotNull(page.getAdditions());
      assertNotNull(page.getRemovals());
      assertEquals(0, page.getAdditions().getLongCardinality());
      assertEquals(0, page.getRemovals().getLongCardinality());
    }

    @Test
    @DisplayName("addKey adds to additions and removes from removals")
    void testAddKeyDelta() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyDelta(1L, 1, IndexType.PATH, 0, 65536);

      // Add a key that was previously "removed"
      page.getRemovals().add(100);
      page.addKey(100);

      assertTrue(page.getAdditions().contains(100));
      assertFalse(page.getRemovals().contains(100));
    }

    @Test
    @DisplayName("removeKey adds to removals and removes from additions")
    void testRemoveKeyDelta() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyDelta(1L, 1, IndexType.PATH, 0, 65536);

      // Add a key then remove it
      page.addKey(100);
      page.removeKey(100);

      assertFalse(page.getAdditions().contains(100));
      assertTrue(page.getRemovals().contains(100));
    }

    @Test
    @DisplayName("containsKey throws for delta mode")
    void testContainsKeyDelta() {
      BitmapChunkPage page = BitmapChunkPage.createEmptyDelta(1L, 1, IndexType.PATH, 0, 65536);

      assertThrows(IllegalStateException.class, () -> page.containsKey(100));
    }
  }

  @Nested
  @DisplayName("Tombstone pages")
  class TombstoneMode {

    @Test
    @DisplayName("createTombstone creates a deleted page")
    void testCreateTombstone() {
      BitmapChunkPage page = BitmapChunkPage.createTombstone(1L, 1, IndexType.PATH, 0, 65536);

      assertTrue(page.isDeleted());
      assertFalse(page.isDelta());
      assertFalse(page.isFullSnapshot());
      assertNull(page.getBitmap());
      assertNull(page.getAdditions());
      assertNull(page.getRemovals());
    }

    @Test
    @DisplayName("containsKey returns false for tombstone")
    void testContainsKeyTombstone() {
      BitmapChunkPage page = BitmapChunkPage.createTombstone(1L, 1, IndexType.PATH, 0, 65536);

      assertFalse(page.containsKey(100));
    }

    @Test
    @DisplayName("addKey throws for tombstone")
    void testAddKeyTombstone() {
      BitmapChunkPage page = BitmapChunkPage.createTombstone(1L, 1, IndexType.PATH, 0, 65536);

      assertThrows(IllegalStateException.class, () -> page.addKey(100));
    }
  }

  @Nested
  @DisplayName("Copy operations")
  class CopyOperations {

    @Test
    @DisplayName("copy creates independent copy")
    void testCopy() {
      Roaring64Bitmap bitmap = new Roaring64Bitmap();
      bitmap.add(100);
      bitmap.add(200);
      bitmap.add(300);
      BitmapChunkPage original = BitmapChunkPage.createFull(1L, 1, IndexType.PATH, 0, 65536, bitmap);

      BitmapChunkPage copy = original.copy(2);

      assertEquals(2, copy.getRevision());
      assertEquals(original.getPageKey(), copy.getPageKey());

      // Modify original
      original.addKey(400);

      // Copy should be independent
      assertFalse(copy.containsKey(400));
    }

    @Test
    @DisplayName("copyAsFull converts to full mode")
    void testCopyAsFull() {
      BitmapChunkPage delta = BitmapChunkPage.createEmptyDelta(1L, 1, IndexType.PATH, 0, 65536);
      delta.addKey(100);

      BitmapChunkPage full = delta.copyAsFull(2);

      assertEquals(2, full.getRevision());
      assertTrue(full.isFullSnapshot());
      assertNotNull(full.getBitmap());
    }
  }

  @Nested
  @DisplayName("Serialization")
  class Serialization {

    @Test
    @DisplayName("serialize and deserialize full page")
    void testSerializeDeserializeFull() throws IOException {
      Roaring64Bitmap bitmap = new Roaring64Bitmap();
      bitmap.add(100);
      bitmap.add(200);
      bitmap.add(300);
      bitmap.add(50000);
      BitmapChunkPage original = BitmapChunkPage.createFull(42L, 5, IndexType.CAS, 0, 65536, bitmap);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      original.serialize(new DataOutputStream(baos));

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      BitmapChunkPage deserialized = BitmapChunkPage.deserialize(new DataInputStream(bais), 42L);

      assertEquals(original.getPageKey(), deserialized.getPageKey());
      assertEquals(original.getRevision(), deserialized.getRevision());
      assertEquals(original.getIndexType(), deserialized.getIndexType());
      assertEquals(original.getRangeStart(), deserialized.getRangeStart());
      assertEquals(original.getRangeEnd(), deserialized.getRangeEnd());
      assertEquals(original.isDelta(), deserialized.isDelta());
      assertEquals(original.isDeleted(), deserialized.isDeleted());
      assertEquals(original.getBitmap().getLongCardinality(), deserialized.getBitmap().getLongCardinality());
      assertTrue(deserialized.containsKey(100));
      assertTrue(deserialized.containsKey(50000));
    }

    @Test
    @DisplayName("serialize and deserialize delta page")
    void testSerializeDeserializeDelta() throws IOException {
      Roaring64Bitmap additions = new Roaring64Bitmap();
      additions.add(100);
      additions.add(200);
      Roaring64Bitmap removals = new Roaring64Bitmap();
      removals.add(50);
      removals.add(75);
      BitmapChunkPage original = BitmapChunkPage.createDelta(42L, 5, IndexType.PATH, 0, 65536, additions, removals);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      original.serialize(new DataOutputStream(baos));

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      BitmapChunkPage deserialized = BitmapChunkPage.deserialize(new DataInputStream(bais), 42L);

      assertTrue(deserialized.isDelta());
      assertEquals(2, deserialized.getAdditions().getLongCardinality());
      assertEquals(2, deserialized.getRemovals().getLongCardinality());
      assertTrue(deserialized.getAdditions().contains(100));
      assertTrue(deserialized.getRemovals().contains(50));
    }

    @Test
    @DisplayName("serialize and deserialize tombstone")
    void testSerializeDeserializeTombstone() throws IOException {
      BitmapChunkPage original = BitmapChunkPage.createTombstone(42L, 5, IndexType.NAME, 65536, 131072);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      original.serialize(new DataOutputStream(baos));

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      BitmapChunkPage deserialized = BitmapChunkPage.deserialize(new DataInputStream(bais), 42L);

      assertTrue(deserialized.isDeleted());
      assertEquals(65536, deserialized.getRangeStart());
      assertEquals(131072, deserialized.getRangeEnd());
    }
  }
}

