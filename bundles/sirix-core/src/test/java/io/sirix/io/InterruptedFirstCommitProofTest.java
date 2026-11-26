package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FMT-401: an empty-bootstrap proof still requires the settings identity on both files. */
final class InterruptedFirstCommitProofTest {
  @TempDir
  Path directory;

  @Test
  void noCommitEvidenceCannotAuthorizeAnotherResourcesFiles() throws IOException {
    final Path database = directory.resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final var opened = Databases.openJsonDatabase(database)) {
      assertTrue(opened.createResource(ResourceConfiguration.newBuilder("resource").build()));
    }
    final ResourceConfiguration config = ResourceConfiguration.deserialize(database.resolve("resources/resource"));
    final long msb = config.resourceUuid.getMostSignificantBits();
    final long lsb = config.resourceUuid.getLeastSignificantBits();
    final Path data = directory.resolve(IOStorage.FILENAME);
    final Path revisions = directory.resolve(IOStorage.REVISIONS_FILENAME);
    for (final String foreign : new String[] {"neither", "data", "revisions"}) {
      Files.write(data, Arrays.copyOf(Superblock.build(Superblock.ROLE_DATA, foreign.equals("data")
          ? msb ^ 1L
          : msb, lsb).array(), (int) IOStorage.DATA_REGION_START + 1));
      Files.write(revisions, Superblock.build(Superblock.ROLE_REVISIONS, foreign.equals("revisions")
          ? msb ^ 1L
          : msb, lsb).array());
      final byte[] beforeData = Files.readAllBytes(data);
      final byte[] beforeRevisions = Files.readAllBytes(revisions);
      final boolean proven =
          InterruptedFirstCommitRecovery.provesAtMostEmptyBootstrapCommitted(config, data, revisions);
      if (foreign.equals("neither")) {
        assertTrue(proven, "valid identities and no committed data permit the empty bootstrap proof");
      } else {
        assertFalse(proven, "no committed record does not excuse a mismatched " + foreign + " identity");
      }
      assertArrayEquals(beforeData, Files.readAllBytes(data));
      assertArrayEquals(beforeRevisions, Files.readAllBytes(revisions));
    }
  }
}
