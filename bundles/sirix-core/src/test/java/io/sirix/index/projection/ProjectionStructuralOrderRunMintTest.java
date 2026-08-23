/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Labelling a long run of consecutive UNLABELLED siblings is the shape every projection build
 * takes: {@code ProjectionIndexBuilder.buildAndPersist} resolves one order label per record, in
 * document order, over an array whose records all already exist and none of which owns a slot yet.
 *
 * <p>
 * Discovering each label's bounds by walking outwards makes that quadratic — the first record's
 * probe walks the whole array looking for a labelled right neighbour, the second walks all but one,
 * and so on — so the work must be bounded per record instead: the run's bounding pair is resolved
 * once and the whole run assigned in a single pass.
 *
 * <p>
 * This drives {@link ProjectionStructuralOrderDirectory} through its own accessor with an
 * instrumented node lookup, so the assertion is on work actually performed and on the labels
 * actually produced, not on how the code is written.
 */
final class ProjectionStructuralOrderRunMintTest {

  private static final String RESOURCE_NAME = "structural-order-run";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;
  private static final int RECORDS = 2000;

  /**
   * Sibling steps allowed per labelled record. Resolving a run once costs a small constant per member
   * (walk to the run's head, scan it for its upper bound, assign it); re-probing per record costs
   * order-of-{@code RECORDS} steps each, which for this array is about 500 per record.
   */
  private static final int MAX_LOOKUPS_PER_RECORD = 12;

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void labellingALongUnlabelledSiblingRunStaysBoundedPerRecordAndCommits() throws Exception {
    final StringBuilder json = new StringBuilder("[");
    for (int record = 0; record < RECORDS; record++) {
      json.append(record == 0
          ? ""
          : ",").append("{\"score\":").append(record).append('}');
    }
    json.append(']');

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx();
          var parser = JacksonJsonShredder.createStringParser(json.toString())) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      }

      final List<Long> recordKeys = new ArrayList<>(RECORDS);
      final List<SirixDeweyID> labels = new ArrayList<>(RECORDS);
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveToFirstChild(), "the shredded document must expose its array");
        assertTrue(wtx.moveToFirstChild(), "the array must expose its first record");
        do {
          recordKeys.add(wtx.getNodeKey());
        } while (wtx.moveToRightSibling());
        assertEquals(RECORDS, recordKeys.size());

        final AtomicLong lookups = new AtomicLong();
        final LongFunction<ImmutableNode> countingLookup = nodeKey -> {
          lookups.incrementAndGet();
          return (ImmutableNode) wtx.getStorageEngineWriter().getRecord(nodeKey, IndexType.DOCUMENT, -1);
        };
        final ProjectionStructuralOrderDirectory.Accessor directory = ProjectionStructuralOrderDirectory.open(
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER));

        // Document order, exactly as ProjectionIndexBuilder resolves labels while it emits rows.
        for (final long recordKey : recordKeys) {
          labels.add(
              directory.fullLabel(recordKey, countingLookup, ProjectionStructuralOrderDirectory.RelabelSink.SEALED));
        }

        final long budget = (long) RECORDS * MAX_LOOKUPS_PER_RECORD;
        assertTrue(lookups.get() <= budget, "labelling " + RECORDS + " consecutive unlabelled siblings took "
            + lookups.get() + " document lookups; bounded per-record work allows at most " + budget);
        // The transaction must survive it: a long sibling run is not a reason to refuse a document.
        wtx.commit();
      }

      for (int record = 0; record < RECORDS; record++) {
        assertNotNull(labels.get(record));
        if (record > 0) {
          assertTrue(labels.get(record - 1).compareTo(labels.get(record)) < 0,
              "order labels must follow document order at record " + record);
        }
      }

      // Re-resolving after a commit must return the SAME persisted labels, not mint fresh ones.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final LongFunction<ImmutableNode> lookup =
            nodeKey -> (ImmutableNode) wtx.getStorageEngineWriter().getRecord(nodeKey, IndexType.DOCUMENT, -1);
        final ProjectionStructuralOrderDirectory.Accessor directory = ProjectionStructuralOrderDirectory.open(
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER));
        for (int record = 0; record < RECORDS; record++) {
          assertEquals(labels.get(record),
              directory.fullLabel(recordKeys.get(record), lookup,
                  ProjectionStructuralOrderDirectory.RelabelSink.SEALED),
              "a committed order label must be stable at record " + record);
        }
      }
    }
  }
}
