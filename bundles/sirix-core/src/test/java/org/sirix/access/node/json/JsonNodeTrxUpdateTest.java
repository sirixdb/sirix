package org.sirix.access.node.json;

import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.json.JsonNodeReadOnlyTrx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonNodeTrxUpdateTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testUpdateObjectRecordName() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(2);

      assertEquals(new QNm("foo"), wtx.getName());
      wtx.setObjectKeyName("foobar");

      assertsForUpdateObjectRecordName(wtx);

      wtx.commit();

      assertsForUpdateObjectRecordName(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateObjectRecordName(rtx);
      }
    }
  }

  private void assertsForUpdateObjectRecordName(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(2);

    assertEquals(new QNm("foobar"), rtx.getName());
  }

  @Test
  public void testUpdateStringValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(4);

      assertEquals("bar", wtx.getValue());
      wtx.setStringValue("baz");

      assertsForUpdateStringValue(wtx);

      wtx.commit();

      assertsForUpdateStringValue(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateStringValue(rtx);
      }
    }
  }

  private void assertsForUpdateStringValue(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(4);

    assertEquals("baz", rtx.getValue());
  }

  @Test
  public void testUpdateNumberValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(6);

      assertEquals(2.33, wtx.getNumberValue());
      wtx.setNumberValue(5.77);

      assertsForUpdateNumberValue(wtx);

      wtx.commit();

      assertsForUpdateNumberValue(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateNumberValue(rtx);
      }
    }
  }

  private void assertsForUpdateNumberValue(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(6);

    assertEquals(5.77, rtx.getNumberValue());
  }

  @Test
  public void testUpdateBooleanValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(12);

      assertTrue(wtx.getBooleanValue());
      wtx.setBooleanValue(false);

      assertsForUpdateBooleanValue(wtx);

      wtx.commit();

      assertsForUpdateBooleanValue(wtx);

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertsForUpdateBooleanValue(rtx);
      }
    }
  }

  private void assertsForUpdateBooleanValue(JsonNodeReadOnlyTrx rtx) {
    rtx.moveTo(12);

    assertFalse(rtx.getBooleanValue());
  }
}
