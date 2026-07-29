/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.redblacktree.keyvalue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the two ways a {@link NodeReferences} can be built around an existing bitmap.
 *
 * <p>{@link NodeReferences#getNodeKeys()} hands out the live set and {@link
 * NodeReferences#addNodeKey} mutates in place, so whether a constructor copies is not a detail —
 * it decides whether two reference sets can silently overwrite each other. The index writer
 * merges a reference set per indexed node, which is why the non-copying form exists at all; these
 * tests keep the distinction honest in both directions.
 */
@DisplayName("NodeReferences bitmap ownership")
final class NodeReferencesOwnershipTest {

  @Test
  @DisplayName("the constructor copies, so later writes to the source do not leak in")
  void constructorTakesADefensiveCopy() {
    final Roaring64Bitmap source = new Roaring64Bitmap();
    source.add(1L);
    source.add(2L);

    final NodeReferences refs = new NodeReferences(source);
    assertNotSame(source, refs.getNodeKeys());

    // Mutating either side must not be visible on the other.
    source.add(3L);
    assertFalse(refs.isPresent(3L), "a write to the source bitmap leaked into the copy");

    refs.addNodeKey(4L);
    assertFalse(source.contains(4L), "a write to the copy leaked into the source bitmap");
  }

  @Test
  @DisplayName("owning() adopts the bitmap instead of copying it")
  void owningAdoptsTheBitmap() {
    final Roaring64Bitmap source = new Roaring64Bitmap();
    source.add(1L);
    source.add(2L);

    final NodeReferences refs = NodeReferences.owning(source);
    assertSame(source, refs.getNodeKeys(),
        "owning() must hand the caller's bitmap straight through — copying it would put the "
            + "per-merge allocation it exists to avoid straight back");
    assertTrue(refs.isPresent(1L));

    refs.addNodeKey(3L);
    assertTrue(source.contains(3L), "the adopted bitmap is the live set");
  }

  @Test
  @DisplayName("both forms reject a null bitmap rather than failing later")
  void nullBitmapIsRejected() {
    assertThrows(NullPointerException.class, () -> new NodeReferences(null));
    assertThrows(NullPointerException.class, () -> NodeReferences.owning(null));
  }

  @Test
  @DisplayName("a copy equals its source and keeps every key")
  void copyPreservesContents() {
    final Roaring64Bitmap source = new Roaring64Bitmap();
    for (long key = 0; key < 5000; key += 3) {
      source.add(key);
    }

    final NodeReferences copied = new NodeReferences(source);
    final NodeReferences adopted = NodeReferences.owning(source);
    assertTrue(copied.equals(adopted), "copy and adoption must observe the same key set");
    for (long key = 0; key < 5000; key += 3) {
      assertTrue(copied.isPresent(key), "missing key " + key);
    }
    assertFalse(copied.isPresent(1L));
  }
}
