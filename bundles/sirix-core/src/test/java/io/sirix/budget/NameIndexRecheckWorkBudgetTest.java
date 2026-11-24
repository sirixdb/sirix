/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.name.NameFilter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A NAME lookup of a name inside the routable key bound must read no record to re-check it.
 *
 * <p>
 * Only a name past {@link NameKeySerializer#MAX_NAME_KEY_BYTES} is stored under a prefix of itself,
 * so only such a name owes the exact re-check of FMT-305 — one record read per candidate posting.
 * A bound without the long-name control below would also pass if the re-check never ran at all, so
 * the long name establishes the nonzero floor the zero is measured against.
 */
@Isolated
final class NameIndexRecheckWorkBudgetTest {

  private static final String RESOURCE = "data";

  private static final WorkCounter RECHECK_READS = EngineWorkCounters.NAME_RECHECK_RECORD_READS;

  private static final WorkCapture CAPTURE = WorkCapture.of(RECHECK_READS);

  /** Two names sharing a prefix longer than the bound: one stored key, two distinct names. */
  private static final String LONG_HEAD = "l".repeat(NameKeySerializer.MAX_NAME_KEY_BYTES);

  private static final QNm LONG_FIRST = new QNm(LONG_HEAD + "a");

  private static final QNm LONG_SECOND = new QNm(LONG_HEAD + "b");

  private static final QNm SHORT = new QNm("field");

  @TempDir
  Path directory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void onlyNamesPastTheRoutableBoundPayTheExactRecheck() throws Exception {
    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final Path path = directory.resolve("name-recheck-budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        final String json = "[{\"" + SHORT.getLocalName() + "\":0,\"" + LONG_FIRST.getLocalName() + "\":1,\""
            + LONG_SECOND.getLocalName() + "\":2}]";
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        trx.moveToDocumentRoot();
        final var controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createIndexes(Set.of(definition), trx);
        final StorageEngineReader reader = trx.getStorageEngineReader();

        assertTrue(NameKeySerializer.isTruncated(LONG_FIRST), "fixture: the long names exceed the bound");
        assertEquals(NameKeySerializer.MAX_NAME_KEY_BYTES, NameKeySerializer.serializedLength(LONG_FIRST),
            "fixture: a name past the bound is stored at the bound");
        assertEquals(NameKeySerializer.serializedLength(LONG_FIRST), NameKeySerializer.serializedLength(LONG_SECOND),
            "fixture: both long names share one stored key");

        final WorkCapture.Captured<Set<Long>> shortLookup =
            CAPTURE.call(() -> postings(controller, reader, definition, SHORT));
        assertEquals(1, shortLookup.result().size(), "the short name's own posting");
        shortLookup.work()
                   .assertZero(RECHECK_READS,
                       "a name inside the routable key bound answers from its whole stored key and "
                           + "must not read a record to re-check it");

        final WorkCapture.Captured<List<Set<Long>>> longLookups = CAPTURE.call(() -> List.of(
            postings(controller, reader, definition, LONG_FIRST), postings(controller, reader, definition,
                LONG_SECOND)));
        assertEquals(1, longLookups.result().get(0).size(), "the first long name's own posting");
        assertEquals(1, longLookups.result().get(1).size(), "the second long name's own posting");
        assertEquals(Set.of(), intersection(longLookups.result().get(0), longLookups.result().get(1)),
            "two names sharing a stored key must not share a posting");
        // Two candidates under the shared key, re-checked once per candidate per lookup.
        longLookups.work()
                   .assertExactly(RECHECK_READS, 4,
                       "the exact re-check reads one record per candidate posting, and reads the "
                           + "candidates rather than the whole index");
        trx.commit();
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static Set<Long> postings(final io.sirix.access.trx.node.IndexController<?, ?> controller,
      final StorageEngineReader reader, final IndexDef definition, final QNm name) {
    final Set<Long> keys = new TreeSet<>();
    final Iterator<NodeReferences> hits =
        controller.openNameIndex(reader, definition, new NameFilter(Set.of(name), Set.of()));
    while (hits.hasNext()) {
      final LongIterator postings = hits.next().getNodeKeys().getLongIterator();
      while (postings.hasNext()) {
        keys.add(postings.next());
      }
    }
    return keys;
  }

  private static Set<Long> intersection(final Set<Long> first, final Set<Long> second) {
    final Set<Long> shared = new TreeSet<>(first);
    shared.retainAll(second);
    return shared;
  }
}
