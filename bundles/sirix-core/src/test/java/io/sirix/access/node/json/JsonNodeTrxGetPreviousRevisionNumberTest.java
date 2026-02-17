package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.settings.Constants;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class JsonNodeTrxGetPreviousRevisionNumberTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @DisplayName("Get previous revision number of changed nodes")
  @Test
  public void testPreviousRevisionOfNodes() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();

      // Set object key name.
      wtx.moveTo(2);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setObjectKeyName("newKey");
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(2);
      assertEquals(1, wtx.getPreviousRevisionNumber());

      // Set string value.
      wtx.moveTo(4);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setStringValue("changeStringValue");
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(4);
      assertEquals(1, wtx.getPreviousRevisionNumber());

      // Set number value.
      wtx.moveTo(6);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setNumberValue(123);
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(6);
      assertEquals(1, wtx.getPreviousRevisionNumber());

      // Set boolean value.
      wtx.moveTo(12);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setBooleanValue(false);
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(12);
      assertEquals(1, wtx.getPreviousRevisionNumber());
    }
  }
}
