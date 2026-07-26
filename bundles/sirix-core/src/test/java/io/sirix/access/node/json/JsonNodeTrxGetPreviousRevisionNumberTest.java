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
    JsonTestHelper.deleteEverything();
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

      // Set string value. iter#32 fusion: STRING_VALUE("bar") (first array element of "foo")
      // shifted from key 4 -> 3 because OBJECT_KEY+ARRAY collapsed into one OBJECT_NAMED_ARRAY.
      wtx.moveTo(3);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setStringValue("changeStringValue");
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(3);
      assertEquals(1, wtx.getPreviousRevisionNumber());

      // Set number value. iter#32 fusion: NUMBER_VALUE(2.33) (third array element of "foo")
      // shifted from key 6 -> 5.
      wtx.moveTo(5);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setNumberValue(123);
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(5);
      assertEquals(1, wtx.getPreviousRevisionNumber());

      // Set boolean value. iter#32 fusion: "helloo":true is now an OBJECT_NAMED_BOOLEAN
      // record at key 8 (was a separate BOOLEAN_VALUE child at key 12 in legacy). The fused
      // record also serves as the structural key, so setBooleanValue mutates the inline
      // primitive payload in-place via setBooleanValueFused.
      wtx.moveTo(8);
      assertEquals(Constants.NULL_REVISION_NUMBER, wtx.getPreviousRevisionNumber());
      wtx.setBooleanValue(false);
      assertEquals(1, wtx.getPreviousRevisionNumber());
      wtx.commit();
      wtx.moveTo(8);
      assertEquals(1, wtx.getPreviousRevisionNumber());
    }
  }
}
