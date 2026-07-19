/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip and robustness coverage for {@link ProjectionIndexMetadata} —
 * the self-describing shape payload persisted at HOT slot 0 of a projection
 * sub-tree.
 */
public final class ProjectionIndexMetadataTest {

  private static final String ROOT = "/wrapper/records/[]";
  private static final String[] PATHS = { "/wrapper/records/[]/age", "/wrapper/records/[]/active",
      "/wrapper/records/[]/dept" };
  private static final String[] NAMES = { "age", "active", "dept" };
  private static final byte[] KINDS = { ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN, ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT };

  private static long[] fences(final int leafCount, final long base) {
    final long[] fences = new long[leafCount];
    for (int i = 0; i < leafCount; i++) {
      fences[i] = base + i * 1000L;
    }
    return fences;
  }

  @Test
  public void roundTripsThroughSerializeAndParse() {
    final long[] firsts = fences(42, 1);
    final long[] lasts = fences(42, 900);
    final ProjectionIndexMetadata metadata =
        new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 42, 7, firsts, lasts);
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(metadata.serialize());
    assertEquals(ROOT, parsed.rootPath());
    assertArrayEquals(PATHS, parsed.fieldPaths());
    assertArrayEquals(NAMES, parsed.fieldNames());
    assertArrayEquals(KINDS, parsed.columnKinds());
    assertEquals(42, parsed.leafCount());
    assertEquals(7, parsed.buildRevision());
    for (int i = 0; i < 42; i++) {
      assertEquals(firsts[i], parsed.leafFirstRecordKey(i), "first fence " + i);
      assertEquals(lasts[i], parsed.leafLastRecordKey(i), "last fence " + i);
    }
    assertFalse(parsed.isStale());
  }

  @Test
  public void staleTombstoneRoundTrips() {
    final ProjectionIndexMetadata parsed =
        ProjectionIndexMetadata.parse(ProjectionIndexMetadata.staleTombstone().serialize());
    assertTrue(parsed.isStale());
    assertEquals(0, parsed.leafCount());
    assertEquals(0, parsed.fieldNames().length);
  }

  @Test
  public void matchesComparesRootFieldPathsAndKinds() {
    final ProjectionIndexMetadata metadata =
        new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 1, 1, fences(1, 1), fences(1, 9));
    assertTrue(metadata.matches(ROOT, PATHS, KINDS));
    assertFalse(metadata.matches("/[]", PATHS, KINDS));
    assertFalse(metadata.matches(ROOT, new String[] { PATHS[0], PATHS[1] },
        new byte[] { KINDS[0], KINDS[1] }));
    final byte[] otherKinds = KINDS.clone();
    otherKinds[0] = ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT;
    assertFalse(metadata.matches(ROOT, PATHS, otherKinds));
  }

  @Test
  public void parseReturnsNullWithoutTheMagic() {
    assertNull(ProjectionIndexMetadata.parse(null));
    assertNull(ProjectionIndexMetadata.parse(new byte[0]));
    assertNull(ProjectionIndexMetadata.parse(new byte[] { 1, 2, 3, 4, 5, 6 }));
    // A compact leaf payload starts with the PIXC magic — must not parse.
    assertNull(ProjectionIndexMetadata.parse(new byte[] { 0x49, 0x50, 0x58, 0x43, 0, 0, 0, 0 }));
  }

  @Test
  public void truncatedPayloadFailsLoudly() {
    final byte[] serialized =
        new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 1, 1, fences(1, 1), fences(1, 9))
            .serialize();
    // A cut below 6 bytes fails the magic-length precheck and parses to null
    // instead — the loud-failure contract starts at the header fields.
    assertNull(ProjectionIndexMetadata.parse(Arrays.copyOf(serialized, 5)));
    // Cuts inside the header, the leaf fences, and the string section all
    // fail loudly (fences span bytes 14..30 for one leaf).
    for (final int cut : new int[] { 6, 9, 15, 29, serialized.length / 2, serialized.length - 1 }) {
      final byte[] truncated = Arrays.copyOf(serialized, cut);
      assertThrows(IllegalStateException.class, () -> ProjectionIndexMetadata.parse(truncated),
          "cut at " + cut);
    }
  }

  @Test
  public void misalignedArraysAreRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata(ROOT, PATHS, new String[] { "age" }, KINDS, 1, 1,
            fences(1, 1), fences(1, 9)));
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, -1, 1, fences(0, 0),
            fences(0, 0)));
    // Fence arrays must carry exactly one entry per leaf.
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 2, 1, fences(1, 1),
            fences(2, 9)));
  }
}
