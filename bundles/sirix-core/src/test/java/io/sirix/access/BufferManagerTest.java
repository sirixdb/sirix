package io.sirix.access;

import io.sirix.JsonTestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class BufferManagerTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    JsonTestHelper.createTestDocument();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var firstRtx = manager.beginNodeReadOnlyTrx();
        final var secondRtx = manager.beginNodeReadOnlyTrx()) {

    }
  }
}
