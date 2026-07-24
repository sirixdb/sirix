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
 * sub-tree. The per-leaf fences moved out to {@link ProjectionIndexFences} in
 * wire VERSION 2, so this payload now carries shape only.
 */
public final class ProjectionIndexMetadataTest {

  private static final String ROOT = "/wrapper/records/[]";
  private static final String[] PATHS = { "/wrapper/records/[]/age", "/wrapper/records/[]/active",
      "/wrapper/records/[]/dept" };
  private static final String[] NAMES = { "age", "active", "dept" };
  private static final byte[] KINDS = { ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT };

  @Test
  public void roundTripsThroughSerializeAndParse() {
    final ProjectionIndexMetadata metadata =
        new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 42, 7);
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(metadata.serialize());
    assertEquals(ROOT, parsed.rootPath());
    assertArrayEquals(PATHS, parsed.fieldPaths());
    assertArrayEquals(NAMES, parsed.fieldNames());
    assertArrayEquals(KINDS, parsed.columnKinds());
    assertEquals(42, parsed.rowGroupCount());
    assertEquals(7, parsed.buildRevision());
    assertFalse(parsed.isStale());
  }

  @Test
  public void staleTombstoneRoundTrips() {
    final ProjectionIndexMetadata parsed =
        ProjectionIndexMetadata.parse(ProjectionIndexMetadata.staleTombstone().serialize());
    assertTrue(parsed.isStale());
    assertEquals(0, parsed.rowGroupCount());
    assertEquals(0, parsed.fieldNames().length);
  }

  @Test
  public void matchesComparesRootFieldPathsAndKinds() {
    final ProjectionIndexMetadata metadata =
        new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 1, 1);
    assertTrue(metadata.matches(ROOT, PATHS, KINDS));
    assertFalse(metadata.matches("/[]", PATHS, KINDS));
    assertFalse(metadata.matches(ROOT, new String[] { PATHS[0], PATHS[1] },
        new byte[] { KINDS[0], KINDS[1] }));
    final byte[] otherKinds = KINDS.clone();
    otherKinds[0] = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
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
  public void oldFencedVersionOneBlobParsesToNull() {
    // A VERSION-1 blob (magic + version byte 1) is rejected cleanly — the
    // version bump is the ONLY signal that the bytes after the header are a
    // fence array rather than the root path, so an old blob must NOT be
    // misread as a v2 shape payload.
    final byte[] serialized = new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 1, 1).serialize();
    serialized[4] = 1; // downgrade the version byte
    assertNull(ProjectionIndexMetadata.parse(serialized));
  }

  @Test
  public void truncatedPayloadFailsLoudly() {
    final byte[] serialized = new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, 1, 1).serialize();
    // A cut below 6 bytes fails the magic-length precheck and parses to null
    // instead — the loud-failure contract starts at the header fields.
    assertNull(ProjectionIndexMetadata.parse(Arrays.copyOf(serialized, 5)));
    // Cuts inside the header (rowGroupCount/buildRevision) and the string sections
    // all fail loudly. Header is 14 bytes; the root path follows.
    for (final int cut : new int[] { 6, 9, 15, 20, serialized.length / 2, serialized.length - 1 }) {
      final byte[] truncated = Arrays.copyOf(serialized, cut);
      assertThrows(IllegalStateException.class, () -> ProjectionIndexMetadata.parse(truncated),
          "cut at " + cut);
    }
  }

  @Test
  public void misalignedArraysAreRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata(ROOT, PATHS, new String[] { "age" }, KINDS, 1, 1));
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata(ROOT, PATHS, NAMES, KINDS, -1, 1));
  }
}
