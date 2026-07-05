package io.sirix.page;

import io.brackit.query.atomic.QNm;
import io.sirix.JsonTestHelper;
import io.sirix.XmlTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for #1076: values whose serialized size exceeds the slotted-page capacity
 * ceiling ({@link KeyValueLeafPage#MAX_SLOTTED_PAGE_CAPACITY}, the largest allocator size class)
 * or the per-record threshold ({@code PageConstants.MAX_RECORD_SIZE}) must be stored in
 * {@link OverflowPage}s and round-trip through commit, fresh read-only transactions, and a full
 * database reopen (cold read from disk). Previously such inserts/updates crashed with
 * {@code IllegalArgumentException: requested size ... exceeds largest class} from the frame-slot
 * allocator, and the (unreachable) overflow branch never persisted its pages.
 *
 * <p>Note: the test-helper databases are process-cached — they must not be closed directly;
 * {@code closeEverything()} closes and evicts them, which is what the cold-reopen phases rely on.
 */
public final class LargeValueOverflowTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
    XmlTestHelper.closeEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /** Deterministic content so full-equality assertions are meaningful. */
  private static String bigValue(final int length, final char marker) {
    final StringBuilder sb = new StringBuilder(length);
    while (sb.length() < length) {
      sb.append(marker).append(sb.length()).append('-');
    }
    sb.setLength(length);
    return sb.toString();
  }

  private static void assertJsonValue(final JsonResourceSession session, final long nodeKey, final String expected) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(nodeKey));
      assertEquals(expected, rtx.getValue());
    }
  }

  @Test
  public void jsonInsertLargeStringValuesRoundTrip() {
    // Just over the 150k per-record overflow threshold, ~1 MiB, and ~5 MiB.
    final String big200k = bigValue(200_000, 'a');
    final String big1M = bigValue(1_048_576, 'b');
    final String big5M = bigValue(5 * 1_048_576, 'c');
    final String small = "small-value";

    final long smallKey;
    final long big200kKey;
    final long big1MKey;
    final long big5MKey;

    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        wtx.insertStringValueAsFirstChild(small);
        smallKey = wtx.getNodeKey();
        wtx.insertStringValueAsRightSibling(big200k);
        big200kKey = wtx.getNodeKey();
        // In-transaction visibility before commit (record lives as a heap object).
        assertEquals(big200k, wtx.getValue());
        wtx.insertStringValueAsRightSibling(big1M);
        big1MKey = wtx.getNodeKey();
        wtx.insertStringValueAsRightSibling(big5M);
        big5MKey = wtx.getNodeKey();
        wtx.commit();
      }

      // Fresh read-only trx against the committed revision.
      assertJsonValue(session, smallKey, small);
      assertJsonValue(session, big200kKey, big200k);
      assertJsonValue(session, big1MKey, big1M);
      assertJsonValue(session, big5MKey, big5M);
    }

    // Full reopen with cold caches — forces the OverflowPage reads from persisted pages.
    JsonTestHelper.closeEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
    final Database<JsonResourceSession> reopened = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      assertJsonValue(session, smallKey, small);
      assertJsonValue(session, big200kKey, big200k);
      assertJsonValue(session, big1MKey, big1M);
      assertJsonValue(session, big5MKey, big5M);
    }
  }

  @Test
  public void jsonUpdateToLargeValueAndBackRoundTrip() {
    final String big300k = bigValue(300_000, 'u');
    final String tiny = "tiny";
    final long stringKey;

    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        wtx.insertStringValueAsFirstChild("initial");
        stringKey = wtx.getNodeKey();
        wtx.commit();
      }

      // Update the existing slotted record to an oversized value (unbound-flyweight path).
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(stringKey));
        wtx.setStringValue(big300k);
        assertEquals(big300k, wtx.getValue());
        wtx.commit();
      }

      assertJsonValue(session, stringKey, big300k);

      // Shrink back to a slotted value — the stale overflow reference must not resurrect.
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(stringKey));
        wtx.setStringValue(tiny);
        wtx.commit();
      }

      assertJsonValue(session, stringKey, tiny);

      // The large value must still be readable in its historic revision.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertTrue(rtx.moveTo(stringKey));
        assertEquals(big300k, rtx.getValue());
      }
    }

    JsonTestHelper.closeEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
    final Database<JsonResourceSession> reopened = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      assertJsonValue(session, stringKey, tiny);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertTrue(rtx.moveTo(stringKey));
        assertEquals(big300k, rtx.getValue());
      }
    }
  }

  @Test
  public void jsonLargeValueCarriedForwardAcrossRevisions() {
    final String big400k = bigValue(400_000, 'r');
    final long bigKey;
    final long laterKey;

    final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        wtx.insertStringValueAsFirstChild(big400k);
        bigKey = wtx.getNodeKey();
        wtx.commit();
      }
      // Unrelated modification in the next revision — likely on the same record page, so the
      // page is reconstructed and the overflow reference must be carried forward untouched.
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(bigKey));
        wtx.insertStringValueAsRightSibling("sibling");
        laterKey = wtx.getNodeKey();
        wtx.commit();
      }

      assertJsonValue(session, bigKey, big400k);
      assertJsonValue(session, laterKey, "sibling");
    }

    JsonTestHelper.closeEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
    final Database<JsonResourceSession> reopened = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Most recent revision.
      assertJsonValue(session, bigKey, big400k);
      // First revision.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertTrue(rtx.moveTo(bigKey));
        assertEquals(big400k, rtx.getValue());
      }
    }
  }

  @Test
  public void xmlLargeTextAndAttributeRoundTrip() {
    final String bigText = bigValue(400_000, 'x');
    final String bigAttribute = bigValue(200_000, 'y');
    final long textKey;
    final long attributeKey;

    final Database<XmlResourceSession> database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      try (final XmlNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertElementAsFirstChild(new QNm("root"));
        attributeKey = wtx.insertAttribute(new QNm("big"), bigAttribute).getNodeKey();
        wtx.moveToParent();
        textKey = wtx.insertTextAsFirstChild(bigText).getNodeKey();
        wtx.commit();
      }

      try (final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(textKey));
        assertEquals(bigText, rtx.getValue());
        assertTrue(rtx.moveTo(attributeKey));
        assertEquals(bigAttribute, rtx.getValue());
      }
    }

    XmlTestHelper.closeEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
    final Database<XmlResourceSession> reopened = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
    try (final XmlResourceSession session = reopened.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(textKey));
      assertEquals(bigText, rtx.getValue());
      assertTrue(rtx.moveTo(attributeKey));
      assertEquals(bigAttribute, rtx.getValue());
    }
  }
}
