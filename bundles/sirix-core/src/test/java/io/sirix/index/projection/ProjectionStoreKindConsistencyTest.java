/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A projection store whose leaves disagree about a column's encoding must be REFUSED at
 * construction, and refused as inconsistent rather than corrupt (tasks #45 and #50).
 *
 * <p>
 * The store reads its column kinds from leaf 0 and dispatches every route on them, so a leaf that
 * declares a different kind is not a bad column — it means the leaves no longer describe the same
 * projection. Task #45's wrong answer was exactly this state, produced by commit-time maintenance,
 * and it surfaced four rounds downstream as "known-corrupt BODY segment": the wrong component and
 * the wrong problem, because nothing had ever compared the leaves.
 * </p>
 *
 * <p>
 * NON-VACUITY: {@link #anAgreeingStoreIsAccepted()} is the control — it builds leaves the same way
 * and must construct cleanly, so a check that simply threw always would fail it. And the
 * disagreement test asserts the message NAMES the offending leaf and column, which a blind throw
 * could not produce. Both matter: this gate is one edit away from passing for the wrong reason.
 * </p>
 */
final class ProjectionStoreKindConsistencyTest {

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN};

  @Test
  void anAgreeingStoreIsAccepted() {
    final ProjectionColumnStore store = new ProjectionColumnStore(List.of(directory(1, KINDS), directory(2, KINDS)));
    assertEquals(KINDS.length, store.columnCount(), "the control must build a usable store");
  }

  @Test
  void aLeafThatDisagreesAboutAColumnKindIsRefused() {
    // Leaf 2 carries the resource-wide encoding for column 1 where leaf 1 carries the per-leaf one —
    // the shape maintenance produced when it rebuilt a leaf from the DECLARED kinds.
    final byte[] divergent = KINDS.clone();
    divergent[1] = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;

    final List<RowGroupDirectory> directories = List.of(directory(1, KINDS), directory(2, divergent));
    final ProjectionStoreInconsistentException refused =
        assertThrows(ProjectionStoreInconsistentException.class, () -> new ProjectionColumnStore(directories));

    assertEquals(1, refused.leaf(), "the refusal must name WHICH leaf disagreed");
    final String message = refused.getMessage();
    assertTrue(message.contains("column 1"), "the refusal must name the column: " + message);
    assertTrue(message.contains("INCONSISTENT"), "the refusal must not call this corruption: " + message);
    assertFalse(message.toLowerCase(java.util.Locale.ROOT).contains("corrupt bytes"),
        "the bytes decode fine — blaming them is what task #45 spent four rounds chasing: " + message);
  }

  @Test
  void aSingleLeafStoreHasNothingToDisagreeWith() {
    final ProjectionColumnStore store = new ProjectionColumnStore(List.of(directory(1, KINDS)));
    assertEquals(KINDS.length, store.columnCount());
  }

  /** The typed refusal must remain catchable by handlers that only know the general failure. */
  @Test
  void theRefusalStaysCompatibleWithExistingHandlers() {
    assertTrue(IllegalStateException.class.isAssignableFrom(ProjectionStoreInconsistentException.class),
        "existing catch sites must keep degrading safely rather than seeing an unknown unchecked type");
  }

  private static RowGroupDirectory directory(final long rowGroupId, final byte[] kinds) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    // A STRING_GLOBAL column interns through a dictionary writer, so one has to exist for the leaf
    // to be BUILDABLE at all. Using the real writer keeps the divergent leaf a genuine artifact
    // rather than a hand-patched descriptor byte.
    GlobalValueDictionaryWriter[] dictionaries = null;
    for (int c = 0; c < kinds.length; c++) {
      if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        if (dictionaries == null) {
          dictionaries = new GlobalValueDictionaryWriter[kinds.length];
        }
        dictionaries[c] = new GlobalValueDictionaryWriter();
      }
    }
    if (dictionaries != null) {
      page.setGlobalDictionaries(dictionaries);
    }
    final long[] longs = new long[kinds.length];
    final boolean[] bools = new boolean[kinds.length];
    final String[] strings = new String[kinds.length];
    final boolean[] present = new boolean[kinds.length];
    final boolean[] unrep = new boolean[kinds.length];
    final boolean[] nonIntegral = new boolean[kinds.length];
    final boolean[] nonDoubleSource = new boolean[kinds.length];
    for (int row = 0; row < 4; row++) {
      longs[0] = row;
      strings[1] = "v" + row;
      bools[2] = (row & 1) == 0;
      for (int c = 0; c < kinds.length; c++) {
        present[c] = true;
      }
      page.appendRow(rowGroupId * 1000 + row, longs, bools, strings, present, unrep, nonIntegral, nonDoubleSource);
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    final int segments = encoded.columnSegmentIds().length;
    final int[] ids = new int[segments];
    final long[] offsets = new long[segments];
    for (int i = 0; i < segments; i++) {
      ids[i] = encoded.columnSegmentIds()[i];
      offsets[i] = 1_000L + i;
    }
    return new RowGroupDirectory(rowGroupId, encoded.descriptor(), ids, offsets, new byte[ids.length][]);
  }
}
