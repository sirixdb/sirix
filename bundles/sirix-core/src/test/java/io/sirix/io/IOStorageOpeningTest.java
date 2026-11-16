package io.sirix.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.io.ram.RAMStorage;
import io.sirix.node.Bytes;
import io.sirix.page.PageReference;
import io.sirix.page.StoreFeatures;
import io.sirix.page.UberPage;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IOStorageOpeningTest {
  @TempDir
  Path directory;

  @Test
  void defaultOpeningPageDistinguishesEmptyAndCommittedStorage() {
    final var config = ResourceConfiguration.newBuilder("ram").storageType(StorageType.IN_MEMORY).build();
    config.resourcePath = directory;
    final var storage = new RAMStorage(config);
    assertTrue(storage.getOpeningUberPage().isBootstrap());
    final var committed = new UberPage(7, new StoreFeatures(0, 0, 4));
    final var reference = new PageReference();
    reference.setPage(committed);
    try (final var bytes = Bytes.elasticHeapByteBuffer(); final var writer = storage.createWriter()) {
      writer.writeUberPageReference(config, reference, committed, bytes);
    }
    final UberPage opened = storage.getOpeningUberPage();
    assertFalse(opened.isBootstrap());
    assertEquals(7, opened.getRevisionCount());
    assertEquals(new StoreFeatures(0, 0, 4), opened.getFeatures());
  }
}
