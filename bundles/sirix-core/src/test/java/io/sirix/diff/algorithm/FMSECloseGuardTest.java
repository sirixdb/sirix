package io.sirix.diff.algorithm;

import io.sirix.XmlTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.diff.algorithm.fmse.DefaultNodeComparisonFactory;
import io.sirix.diff.algorithm.fmse.FMSE;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.service.xml.shredder.XmlShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Regression tests for {@code FMSE#close()} and {@code JsonFMSE#close()} (issue #1067).
 * <p>
 * Before the fix, {@code close()} unconditionally dereferenced the write transaction: calling
 * {@code close()} without a prior successful {@code diff()} threw a {@link NullPointerException}
 * (e.g. from try-with-resources after a failed construction of the transactions).
 */
public final class FMSECloseGuardTest {

  private static final String NEW_RESOURCE = "imported";

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testCloseWithoutDiffDoesNotThrow() {
    // Pre-fix: NPE from wtx.commit() on the never-assigned write transaction.
    try (final FMSE fmse = FMSE.createInstance(new DefaultNodeComparisonFactory())) {
      assertEquals("Fast Matching / Edit Script", fmse.getName());
    }
  }

  @Test
  public void testJsonFmseCloseWithoutDiffDoesNotThrow() {
    // Pre-fix: NPE from wtx.commit() on the never-assigned write transaction.
    try (final JsonFMSE fmse = JsonFMSE.createInstance()) {
      assertEquals("Fast Matching / Edit Script (JSON)", fmse.getName());
    }
  }

  @Test
  public void testCloseAfterSuccessfulDiffCommits() {
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(
          new ResourceConfiguration.Builder(NEW_RESOURCE).buildPathSummary(true).useDeweyIDs(true).build());

      // Old revision (the resource to update).
      try (final XmlResourceSession oldSession = database.beginResourceSession(XmlTestHelper.RESOURCE);
          final XmlNodeTrx wtx = oldSession.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader("<root><a>foo</a></root>"), XmlNodeTrx.Commit.No);
        wtx.commit();
      }

      // New document (shredded into a second resource).
      try (final XmlResourceSession newSession = database.beginResourceSession(NEW_RESOURCE);
          final XmlNodeTrx wtx = newSession.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader("<root><a>foo</a><b>bar</b></root>"),
            XmlNodeTrx.Commit.No);
        wtx.commit();
      }

      try (final XmlResourceSession oldSession = database.beginResourceSession(XmlTestHelper.RESOURCE);
          final XmlResourceSession newSession = database.beginResourceSession(NEW_RESOURCE)) {
        final int revisionsBeforeDiff = oldSession.getMostRecentRevisionNumber();
        assertEquals(1, revisionsBeforeDiff);

        try (final XmlNodeTrx wtx = oldSession.beginNodeTrx();
            final XmlNodeReadOnlyTrx rtx = newSession.beginNodeReadOnlyTrx();
            final FMSE fmse = FMSE.createInstance(new DefaultNodeComparisonFactory())) {
          fmse.diff(wtx, rtx);
        }

        assertEquals("a successful diff() followed by close() must commit a new revision",
            revisionsBeforeDiff + 1, oldSession.getMostRecentRevisionNumber());
      }
    }
  }
}
