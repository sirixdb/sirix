/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

final class RevisionRecordDurabilityFrontierTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void timelineInvalidationDropsTheCompleteDurableFrameIdentity() {
    final Path revisions = temporaryDirectory.resolve(IOStorage.REVISIONS_FILENAME);
    final RevisionRecordDurability durability = RevisionRecordDurability.forFile(revisions, 17L, 23L);
    durability.storeFrontiers(20_000L, 40_000L, 8_192L, 4, 19_000L, 91L);
    assertArrayEquals(new long[] {20_000L, 40_000L, 8_192L, 4L, 19_000L, 91L}, durability.cachedFrontiers());

    RevisionRecordDurability.invalidateFor(revisions);

    assertArrayEquals(new long[] {-1L, -1L, -1L, -1L, -1L, 0L},
        RevisionRecordDurability.forFile(revisions, 17L, 23L).cachedFrontiers());
  }
}
