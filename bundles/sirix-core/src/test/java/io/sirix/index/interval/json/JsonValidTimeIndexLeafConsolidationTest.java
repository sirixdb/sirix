/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval.json;

import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.hot.HOTIncrementalInsert;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end load of a JSON resource with a declared valid-time index, in the shape that made a HOT
 * leaf rebuild write past its buffer.
 *
 * <p>
 * Nothing about the data is unusual: objects carrying {@code validFrom}/{@code validTo}, appended
 * publication by publication, a later publication correcting earlier facts. Many facts share a
 * bound, so the interval index's posting values grow large and only a few dozen fill a leaf. The
 * index writer's periodic leaf consolidation then pours two sibling leaves into one; the right
 * sibling's first key shortens the merged leaf's common prefix and every entry already poured in
 * grows at once — past 64 KiB. Before the fix the load died with an
 * {@code IndexOutOfBoundsException} from {@code HOTLeafPage.rebuildForShorterPrefix}; the pair must
 * instead be left unmerged.
 * </p>
 *
 * <p>
 * That the load still reaches that pair is asserted rather than assumed, by two counters read
 * independently, each for only what it counts: a prefix shrink refused because the rebuilt
 * residents plus the pending entry do not fit
 * ({@link HOTIncrementalInsert#PREFIX_SHRINK_REFUSED_FOR_CAPACITY}), and consolidation leaving an
 * adjacent pair unmerged because the merged leaf refused an entry poured into it
 * ({@link HOTIncrementalInsert#CONSOLIDATION_PAIR_DID_NOT_FIT}, which does not distinguish which
 * refusal made the union not fit). The record layout fixes the node keys, hence the posting sizes
 * and the leaf shapes consolidation meets, so an unrelated layout change can move that pair — but
 * it can no longer silently remove it, and any layout that still reaches the refusal keeps the
 * guard.
 * </p>
 *
 * <p>
 * Surviving the load is not enough — every answer must stay exact. Each stabbing query is compared
 * with a brute-force scan of the facts, at the latest revision and at a historical one.
 * </p>
 */
final class JsonValidTimeIndexLeafConsolidationTest {

  private static final String RESOURCE = "valid-time-consolidation";
  private static final int INDEX_ID = 0;

  private static final int PUBLICATIONS = 4;
  private static final int FACTS_PER_PUBLICATION = 8_000;
  private static final int FACTS = PUBLICATIONS * FACTS_PER_PUBLICATION;

  /** Distinct valid-from days and distinct interval lengths: few bounds, many facts per bound. */
  private static final int DISTINCT_FROM_DAYS = 150;
  private static final int DISTINCT_LENGTHS = 8;
  private static final long LENGTH_STEP_DAYS = 30L;

  /** Every ninth fact of the first publication gets its valid-to corrected. */
  private static final int CORRECTION_STRIDE = 9;

  private static final Instant BASE = Instant.parse("2020-01-01T00:00:00Z");

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("publication-by-publication load with corrections keeps every valid-time answer exact")
  void loadWithCorrectionsStaysExact() {
    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final long[] objectKeys = new long[FACTS];
    final long[] from = new long[FACTS];
    final long[] to = new long[FACTS];
    final long[] historicalTo;
    final int historicalRevision;
    final int historicalFacts = 2 * FACTS_PER_PUBLICATION;
    final int latestRevision;
    final long refusedShrinksBefore = HOTIncrementalInsert.PREFIX_SHRINK_REFUSED_FOR_CAPACITY.get();
    final long refusedPairsBefore = HOTIncrementalInsert.CONSOLIDATION_PAIR_DID_NOT_FIT.get();

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(true)
                                                              .validTimePaths("validFrom", "validTo")
                                                              .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                              .maxNumberOfRevisionsToRestore(8)
                                                              .build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"facts\":[]}"), JsonNodeTrx.Commit.NO);
        wtx.commit();
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(validTimeDefinition()), wtx);
        wtx.commit();

        final long factsArray = factsArrayKey(wtx);
        final StringBuilder json = new StringBuilder(160);
        int historical = -1;
        long[] toAtHistoricalRevision = null;
        for (int publication = 0, fact = 0; publication < PUBLICATIONS; publication++) {
          assertTrue(wtx.moveTo(factsArray));
          for (int i = 0; i < FACTS_PER_PUBLICATION; i++, fact++) {
            final Instant validFrom = BASE.plus((fact * 7L) % DISTINCT_FROM_DAYS, ChronoUnit.DAYS);
            final Instant validTo = validFrom.plus(LENGTH_STEP_DAYS * (1 + fact % DISTINCT_LENGTHS), ChronoUnit.DAYS);
            from[fact] = validFrom.toEpochMilli();
            to[fact] = validTo.toEpochMilli();
            json.setLength(0);
            json.append("{\"id\":")
                .append(fact)
                .append(",\"validFrom\":\"")
                .append(validFrom)
                .append("\",\"validTo\":\"")
                .append(validTo)
                .append("\",\"payload\":\"p")
                .append(fact % 97)
                .append("\"}");
            wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
            objectKeys[fact] = wtx.getNodeKey();
            assertTrue(wtx.moveToParent());
          }
          wtx.commit();
          if (fact == historicalFacts) {
            historical = session.getMostRecentRevisionNumber();
            toAtHistoricalRevision = to.clone();
          }
        }
        historicalRevision = historical;
        historicalTo = toAtHistoricalRevision;

        // A later publication corrects earlier facts: each correction removes the fact's interval
        // from the index and inserts the corrected one into the consolidated, split structure.
        for (int fact = 0; fact < FACTS_PER_PUBLICATION; fact += CORRECTION_STRIDE) {
          final Instant corrected = Instant.ofEpochMilli(to[fact]).plus(400L + fact % 50, ChronoUnit.DAYS);
          assertTrue(wtx.moveTo(namedStringChildKey(wtx, objectKeys[fact], "validTo")));
          wtx.setStringValue(corrected.toString());
          to[fact] = corrected.toEpochMilli();
        }
        wtx.commit();
        latestRevision = session.getMostRecentRevisionNumber();
      }

      assertTrue(historicalRevision > 0 && historicalRevision < latestRevision);
      assertTrue(HOTIncrementalInsert.PREFIX_SHRINK_REFUSED_FOR_CAPACITY.get() > refusedShrinksBefore,
          "the load must reach a prefix shrink whose rebuilt entries do not fit; without one it no "
              + "longer covers the rebuild that overflowed and its record layout must be re-tuned");
      assertTrue(HOTIncrementalInsert.CONSOLIDATION_PAIR_DID_NOT_FIT.get() > refusedPairsBefore,
          "consolidation must leave an adjacent pair unmerged because the merged leaf refused an "
              + "entry poured into it; this counter does not say which refusal made the union not fit");
      assertExactStabs(database, latestRevision, FACTS, objectKeys, from, to);
      assertExactStabs(database, historicalRevision, historicalFacts, objectKeys, from, historicalTo);
    }
  }

  /**
   * Stab the index at instants spread over the whole populated range — between bounds, exactly on a
   * shared valid-from, exactly on a shared valid-to, and outside every interval — and compare each
   * answer with a scan of the first {@code facts} facts. Intervals are closed on both ends.
   */
  private static void assertExactStabs(final Database<JsonResourceSession> database, final int revision,
      final int facts, final long[] objectKeys, final long[] from, final long[] to) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final IntervalDomain domain = new IntervalDomain();
      final RelationalIntervalTree tree =
          ValidTimeIntervalIndexFactory.createReaderTree(rtx.getStorageEngineReader(), INDEX_ID, domain);
      final LongArrayList actual = new LongArrayList();
      final LongArrayList expected = new LongArrayList();
      long nonEmptyAnswers = 0;

      final Instant firstProbe = BASE.minus(1, ChronoUnit.DAYS);
      final long probeDays = DISTINCT_FROM_DAYS + LENGTH_STEP_DAYS * DISTINCT_LENGTHS + 460L;
      for (long day = 0; day <= probeDays; day += 11) {
        // Whole days hit shared bounds exactly; the half-day offset lands strictly between them.
        for (final long offsetHours : new long[] {0L, 12L}) {
          final Instant probe = firstProbe.plus(day, ChronoUnit.DAYS).plus(offsetHours, ChronoUnit.HOURS);
          final long point = probe.toEpochMilli();

          expected.clear();
          for (int fact = 0; fact < facts; fact++) {
            if (from[fact] <= point && point <= to[fact]) {
              expected.add(objectKeys[fact]);
            }
          }
          expected.sort(null);

          actual.clear();
          tree.stab(domain.point(probe), actual::add);
          actual.sort(null);

          assertEquals(expected, actual, "valid-time answer at " + probe + " in revision " + revision);
          if (!expected.isEmpty()) {
            nonEmptyAnswers++;
          }
        }
      }
      assertTrue(nonEmptyAnswers > 20, "the probes must actually exercise the index, got " + nonEmptyAnswers);
    }
  }

  private static IndexDef validTimeDefinition() {
    return IndexDefs.createValidTimeIdxDef(new LinkedHashSet<>(
        List.of(parse("/facts/[]/validFrom", PathParser.Type.JSON), parse("/facts/[]/validTo", PathParser.Type.JSON))),
        INDEX_ID, IndexDef.DbType.JSON);
  }

  /**
   * Node key of the {@code facts} array, whether or not the record and its array are one fused node.
   */
  private static long factsArrayKey(final JsonNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    assertTrue(wtx.moveToFirstChild(), "document object");
    assertTrue(wtx.moveToFirstChild(), "facts record");
    if (wtx.getKind() != NodeKind.OBJECT_NAMED_ARRAY && wtx.getKind() != NodeKind.ARRAY) {
      assertTrue(wtx.moveToFirstChild(), "facts array");
    }
    return wtx.getNodeKey();
  }

  private static long namedStringChildKey(final JsonNodeTrx wtx, final long objectKey, final String name) {
    assertTrue(wtx.moveTo(objectKey));
    if (wtx.moveToFirstChild()) {
      do {
        if (wtx.getKind() == NodeKind.OBJECT_NAMED_STRING && name.equals(wtx.getName().getLocalName())) {
          return wtx.getNodeKey();
        }
      } while (wtx.moveToRightSibling());
    }
    throw new AssertionError("missing string field " + name + " below " + objectKey);
  }
}
