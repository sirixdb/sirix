/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 */
package io.sirix.settings;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.settings.StringCompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for correct FSST decompression under multi-fragment page
 * combines across every {@link VersioningType}.
 *
 * <p>The historical bug: compressed string slots carry no reference to the
 * symbol table they were encoded with; at combine time, slots from
 * fragment A and fragment B landed in the target page together, but the
 * target could only carry <em>one</em> {@code fsstSymbolTable} — decoding
 * B's bytes with A's table silently corrupted the value. The fix
 * (decompress-on-merge) decodes each compressed slot through its source
 * fragment's own table while combining, leaves the target with no
 * compressed content, and lets the commit path rebuild a fresh coherent
 * table before writing to disk.
 *
 * <p>The test writes a large enough population of similar strings across
 * multiple revisions that every versioning strategy reaches a combine
 * with {@code pages.size() &gt; 1}. At the final revision it reads every
 * string back and verifies byte-exact equality — any table mismatch
 * during combine would manifest as a garbled value here.
 */
public final class FsstMultiFragmentCombineTest {

  // FSSTCompressor requires at least MIN_SAMPLES_FOR_TABLE (currently 64) strings
  // on a page to build a table. We need every revision's fragment to cross that
  // threshold so the multi-fragment combine actually carries FSST-compressed
  // content from several sources at once.
  private static final int STRINGS_PER_REVISION = 80;
  private static final int REVISION_COUNT = 6;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @ParameterizedTest(name = "[{0}] multi-fragment combine preserves string values")
  @EnumSource(VersioningType.class)
  void stringsSurviveMultiFragmentCombine(final VersioningType versioning) {
    final String resource = "fsstMultiFragment_" + versioning.name();
    // Snapshot of the expected values at the end of each revision, keyed by
    // revision number (1-indexed). Reading revision k must return exactly
    // expectedByRevision.get(k).
    final List<Set<String>> expectedByRevision = new ArrayList<>(REVISION_COUNT + 1);
    expectedByRevision.add(null); // index 0 unused

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
          .stringCompressionType(StringCompressionType.FSST)
          .versioningApproach(versioning)
          .build());

      try (final JsonResourceSession session = database.beginResourceSession(resource)) {
        final Set<String> cumulative = new HashSet<>(STRINGS_PER_REVISION * REVISION_COUNT * 2);

        // Revision 1 — seed an array with one similar-string cluster.
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertArrayAsFirstChild();
          for (int i = 0; i < STRINGS_PER_REVISION; i++) {
            final String v = makeString(1, i);
            cumulative.add(v);
            wtx.insertStringValueAsFirstChild(v);
            wtx.moveToParent();
          }
          wtx.commit();
        }
        expectedByRevision.add(new HashSet<>(cumulative));

        // Revisions 2..N — each revision inserts a fresh cluster. Every commit
        // triggers a new FSST table build for the leaf pages that changed,
        // which is exactly the condition that historically corrupted reads at
        // the merged revision.
        for (int rev = 2; rev <= REVISION_COUNT; rev++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // array
            wtx.moveToFirstChild();
            for (int i = 0; i < STRINGS_PER_REVISION; i++) {
              final String v = makeString(rev, i);
              cumulative.add(v);
              wtx.insertStringValueAsRightSibling(v);
            }
            wtx.commit();
          }
          expectedByRevision.add(new HashSet<>(cumulative));
        }

        assertEquals(REVISION_COUNT, session.getMostRecentRevisionNumber(),
            "must have produced the expected number of revisions");

        // Verify every revision read — this exercises both single-fragment
        // (full-dump revisions) and multi-fragment (delta revisions) combine
        // paths under each versioning scheme. A mismatch here means FSST
        // decompression during combine picked up the wrong symbol table.
        for (int rev = 1; rev <= REVISION_COUNT; rev++) {
          final Set<String> expected = expectedByRevision.get(rev);
          assertRevisionContentsEqual(session, rev, expected, versioning);
        }
      }
    }
  }

  /**
   * Open a read-only transaction at {@code revision} and compare every array
   * entry against {@code expected} as an unordered set.
   */
  private static void assertRevisionContentsEqual(final JsonResourceSession session, final int revision,
      final Set<String> expected, final VersioningType versioning) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      assertTrue(rtx.moveToFirstChild(),
          "revision " + revision + " array must have at least one child (versioning=" + versioning.name() + ")");

      final Set<String> actual = new HashSet<>(expected.size() * 2);
      int total = 0;
      do {
        assertEquals(NodeKind.STRING_VALUE, rtx.getKind(),
            "revision " + revision + ": every array entry must be STRING_VALUE — got " + rtx.getKind());
        actual.add(rtx.getValue());
        total++;
      } while (rtx.moveToRightSibling());

      assertEquals(expected.size(), total,
          "revision " + revision + ": count mismatch under versioning=" + versioning.name());
      for (final String v : expected) {
        assertTrue(actual.contains(v),
            "revision " + revision + ": missing value '" + v + "' under versioning=" + versioning.name());
      }
    }
  }

  /**
   * Produce a string with a long common prefix (so FSST extracts symbols)
   * followed by a per-revision and per-index differentiator (so different
   * revisions yield slightly different symbol distributions, making the
   * per-revision FSST tables non-identical).
   */
  private static String makeString(final int revision, final int index) {
    return "common-shared-prefix-shared-payload/rev=" + revision + "/idx=" + index
        + "/suffix-lorem-ipsum-dolor-sit-amet-consectetur";
  }
}
