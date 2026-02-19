package io.sirix.access.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;

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
    final Exception exception = assertThrows(RuntimeException.class, () -> createTransactions(resourceSession -> {
      resourceSession.beginNodeTrx();
      resourceSession.beginNodeTrx();
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
      try (final var session = database.beginResourceSession(resource)) {
        startTransactions.accept(session);
      }
    }
  }

  private void assertion(Exception exception) {
    final var expectedMessage =
        "No read-write transaction available, please close the running read-write transaction first.";
    final var actualMessage = exception.getMessage();
    System.out.println("Actual message: " + actualMessage);
    assertTrue(actualMessage.contains(expectedMessage));
  }
}
