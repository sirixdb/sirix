package io.sirix.service.json;

import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.JsonTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Settles whether editing a PREVIOUSLY-COMMITTED fused number field via the direct wtx API persists.
 * Reads the value at the EXACT latest revision (via getMostRecentRevisionNumber) to avoid the
 * off-by-one revision-number mistake that made an earlier throwaway test read the pre-edit value.
 */
public final class FusedNumberEditPersistTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void directWtxNumberEditPersists() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"n\":10}"));
        wtx.commit();
      }

      // setNumberValue — the exact path JSONiq's setNumericValue uses.
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(2);
        wtx.setNumberValue(99);
        wtx.commit();
      }
      try (final var rtx = manager.beginNodeReadOnlyTrx(manager.getMostRecentRevisionNumber())) {
        rtx.moveTo(2);
        assertEquals(99, rtx.getNumberValue().intValue());
      }

      // replaceObjectRecordValue — the path the explorer/other callers use.
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(2);
        wtx.replaceObjectRecordValue(new NumberValue(123));
        wtx.commit();
      }
      try (final var rtx = manager.beginNodeReadOnlyTrx(manager.getMostRecentRevisionNumber())) {
        rtx.moveTo(2);
        assertEquals(123, rtx.getNumberValue().intValue());
      }
    }
  }
}
