/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTInvariantValidator;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A NAME index must stay exact and structurally valid when the discriminative bytes of its keys sit
 * beyond the range a routing node can name (issue 1391).
 *
 * <p>
 * HOT names a discriminative key byte either in a one-byte {@code MULTI_MASK} extraction position or
 * in a two-byte {@code SINGLE_MASK} base position. CAS and VALIDTIME keep their keys inside the
 * first width; NAME keys are serialized names of unbounded length. The family below differs at byte
 * 250 and again at bytes 314 to 316, so a node that routes on both regions spans more than one
 * eight-byte window and has to name byte 314 — which does not fit a byte.
 */
final class NameIndexLongSharedPrefixRoutingTest {

  private static final String RESOURCE = "data";

  /** Common head; the family's first discriminative byte sits right after it. */
  private static final String HEAD = "x".repeat(250);

  /** Filler between the two discriminative regions: forces a span wider than one eight-byte window. */
  private static final String GAP = "y".repeat(63);

  /** Two heads times this many tails exceeds a leaf page, so the build must route through a node. */
  private static final int TAILS = 300;

  /** The member inserted into the already built index. */
  private static final QNm INSERTED = new QNm(HEAD + "0" + GAP + "999");


  @TempDir
  Path directory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void aBulkBuiltAllNamesIndexStaysExactForDiscriminativeBytesBeyondByte255() {
    final List<QNm> names = family();
    final List<QNm> grown = new ArrayList<>(names);
    grown.add(INSERTED);
    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final Path path = directory.resolve("json-far-apart-disc-bytes");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document(names)), JsonNodeTrx.Commit.NO);
        final Map<QNm, Set<Long>> scanned = scan(trx);
        assertEquals(names.size() + 1, scanned.size(), "fixture name count");

        trx.moveToDocumentRoot();
        final var controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createIndexes(Set.of(definition), trx);
        assertIndexEqualsScan(controller, trx.getStorageEngineReader(), definition, names, scanned, "after the build");
        assertStructurallyValid(trx.getStorageEngineReader(), definition, "after the build");
        trx.commit();

        trx.moveToDocumentRoot();
        assertTrue(trx.moveToFirstChild());
        trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"" + INSERTED.getLocalName() + "\":1}"),
            JsonNodeTrx.Commit.NO);
        final Map<QNm, Set<Long>> grownScan = scan(trx);
        assertIndexEqualsScan(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(),
            definition, grown, grownScan, "after the insert");
        assertStructurallyValid(trx.getStorageEngineReader(), definition, "after the insert");
        trx.commit();
      }

      Databases.clearGlobalCaches();
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertIndexEqualsScan(session.getRtxIndexController(rtx.getRevisionNumber()), rtx.getStorageEngineReader(),
            definition, grown, scan(rtx), "after cold reopen");
        assertStructurallyValid(rtx.getStorageEngineReader(), definition, "after cold reopen");
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  /**
   * The same geometry reached one name at a time: the index exists before the document, so every
   * node arrives through the writer's branch-insert route rather than the bulk build. Twice as many
   * tails, because the far-apart bit only reaches a routing node once a leaf has had to split.
   */
  @Test
  void anIncrementallyMaintainedAllNamesIndexStaysExactForDiscriminativeBytesBeyondByte255() {
    final List<QNm> names = new ArrayList<>(family());
    for (int head = 0; head < 2; head++) {
      for (int tail = TAILS; tail < 2 * TAILS; tail++) {
        names.add(name(head, tail));
      }
    }
    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final Path path = directory.resolve("json-incremental-far-apart-disc-bytes");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(definition), trx);
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document(names)), JsonNodeTrx.Commit.NO);
        final Map<QNm, Set<Long>> scanned = scan(trx);
        assertEquals(names.size() + 1, scanned.size(), "fixture name count");
        assertIndexEqualsScan(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(),
            definition, names, scanned, "after incremental maintenance");
        assertStructurallyValid(trx.getStorageEngineReader(), definition, "after incremental maintenance");
        trx.commit();
      }
      Databases.clearGlobalCaches();
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertIndexEqualsScan(session.getRtxIndexController(rtx.getRevisionNumber()), rtx.getStorageEngineReader(),
            definition, names, scan(rtx), "after incremental maintenance and cold reopen");
        assertStructurallyValid(rtx.getStorageEngineReader(), definition, "after incremental maintenance and reopen");
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  /**
   * Names at and past the routable key bound. Past it a stored key is the shared prefix of several
   * names, so an exact read has to re-check its candidates against each node's own name; the index
   * must still equal the scan on both sides of the bound (FMT-305).
   */
  @Test
  void namesAtAndPastTheRoutableBoundStayExact() {
    final int bound = NameKeySerializer.MAX_NAME_KEY_BYTES;
    final QNm atBound = new QNm("a".repeat(bound));
    final QNm pastBoundFirst = new QNm("b".repeat(bound) + "x");
    final QNm pastBoundSecond = new QNm("b".repeat(bound) + "y");
    final QNm pastBoundThird = new QNm("b".repeat(bound) + "y" + "z".repeat(64));
    // document() already carries the short name; these are the ones around the bound.
    final List<QNm> names = List.of(atBound, pastBoundFirst, pastBoundSecond, pastBoundThird);
    assertEquals(bound, NameKeySerializer.serializedLength(atBound), "fixture: this name ends at the bound");
    assertTrue(NameKeySerializer.isTruncated(pastBoundFirst), "fixture: this name is past the bound");
    assertEquals(NameKeySerializer.serializedLength(pastBoundFirst),
        NameKeySerializer.serializedLength(pastBoundSecond), "fixture: the past-bound names share a stored key");

    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final Path path = directory.resolve("json-routable-bound");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document(names)), JsonNodeTrx.Commit.NO);
        final Map<QNm, Set<Long>> scanned = scan(trx);
        assertEquals(names.size() + 1, scanned.size(), "fixture name count");
        trx.moveToDocumentRoot();
        final var controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createIndexes(Set.of(definition), trx);
        assertIndexEqualsScan(controller, trx.getStorageEngineReader(), definition, names, scanned, "at the bound");
        assertStructurallyValid(trx.getStorageEngineReader(), definition, "at the bound");
        trx.commit();
      }
      Databases.clearGlobalCaches();
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertIndexEqualsScan(session.getRtxIndexController(rtx.getRevisionNumber()), rtx.getStorageEngineReader(),
            definition, names, scan(rtx), "at the bound after cold reopen");
        assertStructurallyValid(rtx.getStorageEngineReader(), definition, "at the bound after cold reopen");
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  /** Every name of the family: shared head, a difference at byte 250 and one at bytes 314 to 316. */
  private static List<QNm> family() {
    final List<QNm> names = new ArrayList<>(2 * TAILS);
    for (int head = 0; head < 2; head++) {
      for (int tail = 0; tail < TAILS; tail++) {
        names.add(name(head, tail));
      }
    }
    return names;
  }

  /** One family member: the head, the byte-250 difference, the gap, then the three-digit tail. */
  private static QNm name(final int head, final int tail) {
    return new QNm(HEAD + (char) ('0' + head) + GAP + (char) ('0' + tail / 100) + (char) ('0' + tail / 10 % 10)
        + (char) ('0' + tail % 10));
  }

  /** The fixture document: one ordinary short name plus every name of {@code names}, once each. */
  private static String document(final List<QNm> names) {
    final StringBuilder json = new StringBuilder(32 + names.size() * 330).append("[{\"a\":0");
    for (final QNm name : names) {
      json.append(",\"").append(name.getLocalName()).append("\":0");
    }
    return json.append("}]").toString();
  }

  /** Every named node in document order, grouped by name — the oracle an index read must equal. */
  private static Map<QNm, Set<Long>> scan(final JsonNodeReadOnlyTrx trx) {
    final Map<QNm, Set<Long>> scanned = new HashMap<>();
    final long origin = trx.getNodeKey();
    trx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(trx);
    while (axis.hasNext()) {
      final long key = axis.nextLong();
      final QNm name = trx.getName();
      if (name != null) {
        scanned.computeIfAbsent(name, ignored -> new TreeSet<>()).add(key);
      }
    }
    trx.moveTo(origin);
    return scanned;
  }

  private static void assertIndexEqualsScan(final IndexController<?, ?> controller, final StorageEngineReader reader,
      final IndexDef definition, final List<QNm> names, final Map<QNm, Set<Long>> scanned, final String stage) {
    final Set<Long> all = new TreeSet<>();
    for (final Set<Long> keys : scanned.values()) {
      all.addAll(keys);
    }
    assertEquals(all, collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(), Set.of()))),
        "full NAME index read must equal the scan " + stage);
    for (final QNm name : names) {
      assertEquals(scanned.get(name),
          collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(name), Set.of()))),
          "exact NAME postings " + stage + " for a " + name.getLocalName().length() + "-character name");
    }
  }

  private static void assertStructurallyValid(final StorageEngineReader reader, final IndexDef definition,
      final String stage) {
    final HOTInvariantValidator.Result result =
        HOTInvariantValidator.validateIndex(reader, IndexType.NAME, definition.getID());
    assertEquals(List.of(),
        result.violations().stream().map(HOTInvariantValidator.Violation::invariant).distinct().toList(),
        "HOT invariants " + stage + " (" + result.violations().size() + " violations)");
  }

  private static Set<Long> collect(final Iterator<NodeReferences> hits) {
    final Set<Long> keys = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator postings = hits.next().getNodeKeys().getLongIterator();
      while (postings.hasNext()) {
        keys.add(postings.next());
      }
    }
    return keys;
  }
}
