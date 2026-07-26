/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the ownership contract of {@code NodeReferences.adopt} against the copying constructor.
 *
 * <p>The two differ in exactly one observable way — whether later mutation of the caller's bitmap
 * is visible through the returned references — and that difference is the whole point of adopt.
 * If adopt ever started copying, the allocation it exists to remove would come back silently; if
 * the constructor ever stopped copying, every caller that keeps mutating its bitmap would start
 * corrupting previously-returned references.
 */
final class NodeReferencesAdoptTest {

  @Test
  void adoptDoesNotCopy() {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.add(1L);

    final NodeReferences refs = NodeReferences.adopt(bitmap);

    assertSame(bitmap, refs.getNodeKeys(), "adopt must hand back the very same bitmap");
    // Ownership transferred: a later mutation IS visible, which is why callers must not mutate.
    bitmap.add(2L);
    assertTrue(refs.isPresent(2L));
  }

  @Test
  void constructorStillCopies() {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    bitmap.add(1L);

    final NodeReferences refs = new NodeReferences(bitmap);

    // Defensive copy: later mutation of the caller's bitmap must NOT leak in.
    bitmap.add(2L);
    assertTrue(refs.isPresent(1L));
    assertFalse(refs.isPresent(2L), "the copying constructor must stay defensive");
  }

  @Test
  void adoptRejectsNull() {
    assertThrows(NullPointerException.class, () -> NodeReferences.adopt(null));
  }
}
