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

    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
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

      // Fresh read-only trx against the committed revision (overflow pages on disk).
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(smallKey));
        assertEquals(small, rtx.getValue());
        assertTrue(rtx.moveTo(big200kKey));
        assertEquals(big200k, rtx.getValue());
        assertTrue(rtx.moveTo(big1MKey));
        assertEquals(big1M, rtx.getValue());
        assertTrue(rtx.moveTo(big5MKey));
        assertEquals(big5M, rtx.getValue());
      }
    }

    // Full reopen with cold caches — forces the OverflowPage reads from persisted pages.
    Databases.getGlobalBufferManager().clearAllCaches();
    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(smallKey));
      assertEquals(small, rtx.getValue());
      assertTrue(rtx.moveTo(big200kKey));
      assertEquals(big200k, rtx.getValue());
      assertTrue(rtx.moveTo(big1MKey));
      assertEquals(big1M, rtx.getValue());
      assertTrue(rtx.moveTo(big5MKey));
      assertEquals(big5M, rtx.getValue());
    }
  }

  @Test
  public void jsonUpdateToLargeValueAndBackRoundTrip() {
    final String big300k = bigValue(300_000, 'u');
    final String tiny = "tiny";
    final long stringKey;

    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
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

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(stringKey));
        assertEquals(big300k, rtx.getValue());
      }

      // Shrink back to a slotted value — the stale overflow reference must not resurrect.
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(stringKey));
        wtx.setStringValue(tiny);
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(stringKey));
        assertEquals(tiny, rtx.getValue());
      }

      // The large value must still be readable in its historic revision.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertTrue(rtx.moveTo(stringKey));
        assertEquals(big300k, rtx.getValue());
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();
    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(stringKey));
        assertEquals(tiny, rtx.getValue());
      }
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

    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
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

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(bigKey));
        assertEquals(big400k, rtx.getValue());
        assertTrue(rtx.moveTo(laterKey));
        assertEquals("sibling", rtx.getValue());
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();
    try (final Database<JsonResourceSession> database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Most recent revision.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(bigKey));
        assertEquals(big400k, rtx.getValue());
      }
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

    try (final Database<XmlResourceSession> database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
        final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertElementAsFirstChild(new QNm("root"));
      attributeKey = wtx.insertAttribute(new QNm("big"), bigAttribute).getNodeKey();
      wtx.moveToParent();
      textKey = wtx.insertTextAsFirstChild(bigText).getNodeKey();
      wtx.commit();

      try (final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(textKey));
        assertEquals(bigText, rtx.getValue());
        assertTrue(rtx.moveTo(attributeKey));
        assertEquals(bigAttribute, rtx.getValue());
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();
    try (final Database<XmlResourceSession> database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
        final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(textKey));
      assertEquals(bigText, rtx.getValue());
      assertTrue(rtx.moveTo(attributeKey));
      assertEquals(bigAttribute, rtx.getValue());
    }
  }
}
