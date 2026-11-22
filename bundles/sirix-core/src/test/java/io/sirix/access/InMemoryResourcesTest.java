package io.sirix.access;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TXN-615 clause 6: only provable ownership permits retiring filesystem leftovers. */
final class InMemoryResourcesTest {
  @Test
  void unnamedOrUnknownDirectoriesAreNotOwned(@TempDir final Path directory) {
    assertFalse(InMemoryResources.isInMemoryOwned(directory.getRoot()));
    assertFalse(InMemoryResources.isInMemoryOwned(directory.resolve("unknown")));
    assertFalse(InMemoryResources.isInMemoryOwned(directory.resolve("unknown$derived")));
  }

  @Test
  void tombstonesNeedNoSettings(@TempDir final Path directory) {
    assertTrue(InMemoryResources.isInMemoryOwned(directory.resolve("$retired-incomplete")));
  }
}
