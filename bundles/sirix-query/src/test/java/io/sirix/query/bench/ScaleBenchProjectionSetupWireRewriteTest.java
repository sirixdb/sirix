/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench;

import io.sirix.index.projection.ProjectionDoubleEncoding;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ScaleBenchProjectionSetupWireRewriteTest {

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};

  @Test
  void reencodeRoundTripPreservesEveryLocalLogicalLane() {
    final byte[] persisted =
        page(7L, true, "middle", new String[] {"set-a", "set-b"}, true, false, false, false).serialize();

    final List<byte[]> rewritten = ScaleBenchProjectionSetup.reencodeLeaves(List.of(persisted), KINDS);

    assertArrayEquals(persisted, rewritten.getFirst());
  }

  @Test
  void semanticGuardRejectsValueChangesThatKeepShapeIdentityAndZoneBounds() {
    final ProjectionIndexRowGroupPage before =
        page(7L, false, "middle", new String[] {"set-a", "set-b"}, true, false, false, false);

    assertSemanticMismatch(before,
        page(8L, false, "middle", new String[] {"set-a", "set-b"}, true, false, false, false), "column 0 value");
    assertSemanticMismatch(before, page(7L, true, "middle", new String[] {"set-a", "set-b"}, true, false, false, false),
        "column 2 boolean");
    assertSemanticMismatch(before,
        page(7L, false, "changed", new String[] {"set-a", "set-b"}, true, false, false, false),
        "column 3 string dictionary");
    assertSemanticMismatch(before,
        page(7L, false, "middle", new String[] {"set-a", "changed"}, true, false, false, false),
        "column 4 set dictionary");
  }

  @Test
  void semanticGuardRejectsPresenceAndAggregateProvenanceChanges() {
    final ProjectionIndexRowGroupPage before =
        page(7L, false, "middle", new String[] {"set-a", "set-b"}, true, false, false, false);

    assertSemanticMismatch(before,
        page(7L, false, "middle", new String[] {"set-a", "set-b"}, false, false, false, false), "column 0 presence");
    assertSemanticMismatch(before, page(7L, false, "middle", new String[] {"set-a", "set-b"}, true, true, false, false),
        "column 0 provenance");
    assertSemanticMismatch(before, page(7L, false, "middle", new String[] {"set-a", "set-b"}, true, false, true, false),
        "column 0 provenance");
    assertSemanticMismatch(before, page(7L, false, "middle", new String[] {"set-a", "set-b"}, true, false, false, true),
        "column 1 provenance");
  }

  @Test
  void writerMustStillBeBasedOnTheProbedRevision() {
    assertDoesNotThrow(() -> ScaleBenchProjectionSetup.validateWriterBaseRevision(7, 8));

    final IllegalStateException stale =
        assertThrows(IllegalStateException.class, () -> ScaleBenchProjectionSetup.validateWriterBaseRevision(7, 9));
    assertTrue(stale.getMessage().contains("based on revision 8"), stale.getMessage());
  }

  private static void assertSemanticMismatch(final ProjectionIndexRowGroupPage before,
      final ProjectionIndexRowGroupPage after, final String expectedDetail) {
    final IllegalStateException mismatch = assertThrows(IllegalStateException.class,
        () -> ScaleBenchProjectionSetup.validateWireRewrite(before, after, KINDS, 17));
    assertTrue(mismatch.getMessage().contains(expectedDetail), mismatch.getMessage());
    assertTrue(mismatch.getMessage().contains("document position 17"), mismatch.getMessage());
  }

  private static ProjectionIndexRowGroupPage page(final long middleNumber, final boolean middleBoolean,
      final String middleString, final String[] middleSet, final boolean middleNumberPresent,
      final boolean middleNonIntegral, final boolean middleUnrepresentable, final boolean middleNonDoubleSource) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    append(page, 11L, 1L, false, "first", new String[] {"fixed-a"}, true, false, false, false);
    append(page, 19L, middleNumber, middleBoolean, middleString, middleSet, middleNumberPresent, middleNonIntegral,
        middleUnrepresentable, middleNonDoubleSource);
    append(page, 27L, 10L, true, "last", new String[] {"fixed-z"}, true, false, false, false);
    return page;
  }

  private static void append(final ProjectionIndexRowGroupPage page, final long recordKey, final long number,
      final boolean bool, final String string, final String[] set, final boolean numberPresent,
      final boolean nonIntegral, final boolean unrepresentable, final boolean nonDoubleSource) {
    final long[] longs = new long[KINDS.length];
    longs[0] = number;
    longs[1] = ProjectionDoubleEncoding.encode(2.5d);
    final boolean[] bools = new boolean[KINDS.length];
    bools[2] = bool;
    final String[] strings = new String[KINDS.length];
    strings[3] = string;
    final String[][] sets = new String[KINDS.length][];
    sets[4] = set;
    final boolean[] present = new boolean[KINDS.length];
    Arrays.fill(present, true);
    present[0] = numberPresent;
    final boolean[] unrepresentableColumns = new boolean[KINDS.length];
    unrepresentableColumns[0] = unrepresentable;
    final boolean[] nonIntegralColumns = new boolean[KINDS.length];
    nonIntegralColumns[0] = nonIntegral;
    final boolean[] nonDoubleSourceColumns = new boolean[KINDS.length];
    nonDoubleSourceColumns[1] = nonDoubleSource;
    assertTrue(page.appendRow(recordKey, longs, bools, strings, sets, present, unrepresentableColumns,
        nonIntegralColumns, nonDoubleSourceColumns));
  }
}
