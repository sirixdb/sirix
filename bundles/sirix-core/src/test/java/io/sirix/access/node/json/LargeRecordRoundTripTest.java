package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for issue #1076: values whose serialized size exceeds
 * {@code PageConstants.MAX_RECORD_SIZE} (150,000 bytes) must round-trip through commit —
 * originally they were silently lost (the in-memory OverflowPage was never written); after the
 * overflow write path landed, values beyond the ~512 KB slot cap still failed in
 * {@code growSlottedPage} instead of diverting to an OverflowPage.
 */
final class LargeRecordRoundTripTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    // Multi-megabyte values leave outsized pages in the global buffer caches; drop them so
    // later test classes in the same JVM start from a clean slate (same pattern as the HOT
    // stress suites).
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private static String repeat(final char c, final int count) {
    return String.valueOf(c).repeat(count);
  }

  private void roundTrip(final int valueLength) {
    final String bigValue = repeat('x', valueLength);
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"big\":\"" + bigValue + "\",\"marker\":42}"));
        wtx.commit();
      }
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "document must have a root object");
        assertTrue(rtx.moveToFirstChild(), "object must have the 'big' field");
        assertEquals("big", rtx.getName().getLocalName());
        final String stored = rtx.getValue();
        assertEquals(valueLength, stored.length(), "stored value truncated or lost");
        assertEquals(bigValue, stored, "stored value corrupted");
      }
    }
  }

  @Test
  void roundTrip200KbValue_overMaxRecordSize() {
    roundTrip(200_000);
  }

  @Test
  void roundTrip600KbValue_overSlotCap() {
    roundTrip(600_000);
  }

  @Test
  void roundTrip2MbValue() {
    roundTrip(2_000_000);
  }
}
