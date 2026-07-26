/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.pageskip;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PageSkipRegistryTest {

  @AfterEach
  void tearDown() {
    PageSkipRegistry.clear();
  }

  @Test
  void lookupMissesBeforeAnyScan() {
    assertNull(PageSkipRegistry.lookup("res/A"));
  }

  @Test
  void recordedBitmapReturnedInOrder() {
    final RoaringBitmap bm = new RoaringBitmap();
    bm.add(3);
    bm.add(7);
    bm.add(42);
    PageSkipRegistry.recordCompleteScan("res/A", 100 /* nameKey */, bm, 100L /* totalPages */);

    final PageSkipRegistry.Handle h = PageSkipRegistry.lookup("res/A");
    final int[] pages = h.pagesForIfValid(100, 100L);
    assertArrayEquals(new int[] {3, 7, 42}, pages);
  }

  @Test
  void emptyBitmapStillCached() {
    PageSkipRegistry.recordCompleteScan("res/A", 42, new RoaringBitmap(), 10L);
    final PageSkipRegistry.Handle h = PageSkipRegistry.lookup("res/A");
    final int[] pages = h.pagesForIfValid(42, 10L);
    assertEquals(0, pages.length,
        "empty cached bitmap must still be returned — tells caller no page has this nameKey");
  }

  @Test
  void totalPagesMismatchInvalidatesLookup() {
    final RoaringBitmap bm = new RoaringBitmap();
    bm.add(1); bm.add(2); bm.add(3);
    PageSkipRegistry.recordCompleteScan("res/A", 100, bm, 50L);
    final PageSkipRegistry.Handle h = PageSkipRegistry.lookup("res/A");
    // Valid: same totalPages.
    assertArrayEquals(new int[] {1, 2, 3}, h.pagesForIfValid(100, 50L));
    // Invalidated: a write grew the resource to 60 pages.
    assertNull(h.pagesForIfValid(100, 60L));
    // Different nameKey never cached.
    assertNull(h.pagesForIfValid(999, 50L));
  }

  @Test
  void firstWriterWinsIsolation() {
    final RoaringBitmap first = new RoaringBitmap();
    first.add(10); first.add(20);
    PageSkipRegistry.recordCompleteScan("res/A", 7, first, 30L);

    // A later scan may observe different pages (e.g. race where the
    // cache was temporarily gone); we keep the first published entry.
    final RoaringBitmap later = new RoaringBitmap();
    later.add(99);
    PageSkipRegistry.recordCompleteScan("res/A", 7, later, 30L);

    final PageSkipRegistry.Handle h = PageSkipRegistry.lookup("res/A");
    assertArrayEquals(new int[] {10, 20}, h.pagesForIfValid(7, 30L));
  }

  @Test
  void perResourceIsolation() {
    final RoaringBitmap a = new RoaringBitmap();
    a.add(1); a.add(2);
    final RoaringBitmap b = new RoaringBitmap();
    b.add(5); b.add(6);
    PageSkipRegistry.recordCompleteScan("res/A", 7, a, 10L);
    PageSkipRegistry.recordCompleteScan("res/B", 7, b, 10L);
    assertArrayEquals(new int[] {1, 2},
        PageSkipRegistry.lookup("res/A").pagesForIfValid(7, 10L));
    assertArrayEquals(new int[] {5, 6},
        PageSkipRegistry.lookup("res/B").pagesForIfValid(7, 10L));
  }

  @Test
  void uninstallRemovesResource() {
    PageSkipRegistry.recordCompleteScan("res/X", 1, new RoaringBitmap(), 1L);
    assertTrue(PageSkipRegistry.lookup("res/X") != null);
    PageSkipRegistry.uninstall("res/X");
    assertNull(PageSkipRegistry.lookup("res/X"));
  }
}
