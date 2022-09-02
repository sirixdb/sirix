package org.sirix.access.node.json;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.ResourceSession;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.io.StorageType;
import org.sirix.settings.VersioningType;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the {@link ResourceSession}.
 *
 * @author Johannes Lichtenberger
 */
public final class ResourceSessionTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @DisplayName("throw exception when multiple read-write transactions are started")
  @Test
  public void test_whenMultipleReadWriteTrxStarted_throwException() {
    final Exception exception = assertThrows(RuntimeException.class, () -> createTransactions(resourceManager -> {
      resourceManager.beginNodeTrx();
      resourceManager.beginNodeTrx();
    }));

    assertion(exception);
  }

  private void createTransactions(Consumer<JsonResourceSession> startTransactions) {
    final var resource = "resource";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.MEMORY_MAPPED)
                                                   .build());
      try (final var manager = database.beginResourceSession(resource)) {
        startTransactions.accept(manager);
      }
    }
  }

  private void assertion(Exception exception) {
    final var expectedMessage =
        "No read-write transaction available, please close the running read-write transaction first.";
    final var actualMessage = exception.getMessage();

    assertTrue(actualMessage.contains(expectedMessage));
  }
}
