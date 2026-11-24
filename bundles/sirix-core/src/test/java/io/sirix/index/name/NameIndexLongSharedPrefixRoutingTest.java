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
import io.sirix.index.SearchMode;
import io.sirix.index.hot.AbstractHOTIndexReader.RawKeyBytes;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTInvariantValidator;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    final QNm belowBound = new QNm("a".repeat(bound - 1));
    final QNm pastBoundFirst = new QNm("a".repeat(bound) + "x");
    final QNm pastBoundSecond = new QNm("a".repeat(bound) + "y");
    final QNm pastBoundThird = new QNm("a".repeat(bound) + "y" + "z".repeat(64));
    // document() already carries the short name; these are the ones around the bound.
    final List<QNm> names = List.of(belowBound, atBound, pastBoundFirst, pastBoundSecond, pastBoundThird);
    final byte[] exactKey = serialize(atBound);
    final byte[] firstKey = serialize(pastBoundFirst);
    final byte[] secondKey = serialize(pastBoundSecond);
    assertEquals(bound, exactKey.length, "fixture: this name ends at the bound");
    assertTrue(NameKeySerializer.isTruncated(firstKey, 0, firstKey.length), "fixture: this name is past the bound");
    assertTrue(Arrays.equals(firstKey, secondKey), "fixture: the past-bound names share a stored key");

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
        assertFilteredIndexEqualsScan(controller, trx.getStorageEngineReader(), definition, names, scanned,
            "at the bound");
        assertCandidateCursorsKeepResidualsExact(trx.getStorageEngineReader(), definition, pastBoundFirst,
            pastBoundSecond, scanned);
        assertStructurallyValid(trx.getStorageEngineReader(), definition, "at the bound");
        trx.commit();
      }
      Databases.clearGlobalCaches();
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertIndexEqualsScan(session.getRtxIndexController(rtx.getRevisionNumber()), rtx.getStorageEngineReader(),
            definition, names, scan(rtx), "at the bound after cold reopen");
        assertFilteredIndexEqualsScan(session.getRtxIndexController(rtx.getRevisionNumber()),
            rtx.getStorageEngineReader(), definition, names, scan(rtx), "at the bound after cold reopen");
        assertCandidateCursorsKeepResidualsExact(rtx.getStorageEngineReader(), definition, pastBoundFirst,
            pastBoundSecond, scan(rtx));
        assertStructurallyValid(rtx.getStorageEngineReader(), definition, "at the bound after cold reopen");
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 32})
  void leafOverflowKeepsExactHeightsWhenAFoldNeedsMoreRoutingLevels(final int targetFanout) {
    final List<QNm> names = new ArrayList<>();
    for (int prefix = 0; prefix < 32; prefix++) {
      for (int branch = 0; branch < 2; branch++) {
        for (int tail = 0; tail < 200; tail++) {
          names.add(overflowName(prefix, branch, 2, tail));
        }
      }
    }
    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final Path path = directory.resolve("json-overflow-height-" + targetFanout);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + overflowObject(names) + "]"),
            JsonNodeTrx.Commit.NO);
        trx.moveToDocumentRoot();
        session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(definition), trx);
        final StorageEngineReader reader = trx.getStorageEngineReader();
        final PageReference rootRef = HOTInvariantValidator.resolveRootRef(reader, IndexType.NAME, definition.getID());
        final HOTIndirectPage root = assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(rootRef));
        assertEquals(32, root.getNumChildren(), "fixture: the root cannot absorb another child");
        assertEquals(2, assertExactHeight(reader, rootRef));
        for (int child = 0; child < 32; child++) {
          final HOTIndirectPage parent =
              assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(root.getChildReference(child)));
          assertEquals(2, parent.getNumChildren(), "fixture parent fanout");
        }
        if (targetFanout == 32) {
          final List<QNm> nearbyBranches = new ArrayList<>();
          for (int branch = 0; branch < 30; branch++) {
            for (int tail = 0; tail < 512; tail++) {
              nearbyBranches.add(overflowName(0, branch, 32, tail));
            }
          }
          appendOverflowObject(trx, nearbyBranches);
          names.addAll(nearbyBranches);
        }
        final HOTIndirectPage preparedRoot = assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(rootRef));
        assertEquals(32, preparedRoot.getNumChildren(), "fixture: the root stays full");
        final HOTIndirectPage targetParent = assertInstanceOf(HOTIndirectPage.class,
            reader.loadHOTPage(preparedRoot.getChildReference(0)));
        assertEquals(targetFanout, targetParent.getNumChildren(), "fixture: overflow parent capacity");

        final List<QNm> extension = new ArrayList<>();
        for (int tail = 200; tail < 512; tail++) {
          extension.add(overflowName(0, 0, 2, tail));
        }
        appendOverflowObject(trx, extension);
        assertEquals(2, assertExactHeight(reader, rootRef), "before the leaf overflows");
        assertStructurallyValid(reader, definition, "before leaf overflow");
        final QNm last = overflowName(0, 0, 2, 512);
        appendOverflowObject(trx, List.of(last));
        names.addAll(extension);
        names.add(last);
        assertEquals(3, assertExactHeight(reader, rootRef), "immediately after leaf overflow");
        assertStructurallyValid(reader, definition, "immediately after leaf overflow");
        assertIndexEqualsScan(session.getWtxIndexController(trx.getRevisionNumber()), reader, definition, names,
            scan(trx), "after leaf overflow");
        trx.commit();
        assertExactHeight(trx.getStorageEngineReader(), HOTInvariantValidator.resolveRootRef(
            trx.getStorageEngineReader(), IndexType.NAME, definition.getID()));
      }
      Databases.clearGlobalCaches();
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertEquals(3, assertExactHeight(reader,
            HOTInvariantValidator.resolveRootRef(reader, IndexType.NAME, definition.getID())), "after cold reopen");
        assertStructurallyValid(reader, definition, "after overflow and commit");
        assertIndexEqualsScan(session.getRtxIndexController(rtx.getRevisionNumber()), reader, definition, names,
            scan(rtx), "after overflow and commit");
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static QNm overflowName(final int prefix, final int branch, final int fanout, final int tail) {
    final StringBuilder name = new StringBuilder(303);
    for (int bit = 4; bit >= 0; bit--) {
      name.append((prefix & (1 << bit)) == 0 ? 'a' : 'b');
    }
    name.append("x".repeat(95));
    final int branchBits = fanout == 2 ? 1 : 5;
    for (int bit = branchBits - 1; bit >= 0; bit--) {
      name.append((branch & (1 << bit)) == 0 ? 'a' : 'b');
    }
    name.append("y".repeat(200 - branchBits));
    name.append((char) ('0' + tail / 100));
    name.append((char) ('0' + tail / 10 % 10));
    name.append((char) ('0' + tail % 10));
    return new QNm(name.toString());
  }

  private static String overflowObject(final List<QNm> names) {
    final StringBuilder json = new StringBuilder("{");
    for (final QNm name : names) {
      if (json.length() > 1) {
        json.append(',');
      }
      json.append('"').append(name.getLocalName()).append("\":0");
    }
    return json.append('}').toString();
  }

  private static void appendOverflowObject(final JsonNodeTrx trx, final List<QNm> names) {
    trx.moveToDocumentRoot();
    assertTrue(trx.moveToFirstChild());
    trx.insertSubtreeAsLastChild(JsonShredder.createStringReader(overflowObject(names)), JsonNodeTrx.Commit.NO);
  }

  private static int assertExactHeight(final StorageEngineReader reader, final PageReference reference) {
    final Page page = reader.loadHOTPage(reference);
    if (page instanceof HOTLeafPage) {
      return 0;
    }
    final HOTIndirectPage node = assertInstanceOf(HOTIndirectPage.class, page);
    int height = 0;
    for (int child = 0; child < node.getNumChildren(); child++) {
      height = Math.max(height, assertExactHeight(reader, node.getChildReference(child)) + 1);
    }
    assertEquals(height, node.getHeight(), "stored height must equal the independently walked distance to leaves");
    return height;
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
    final HOTIndexReader<QNm> direct = HOTIndexReader.create(reader, NameKeySerializer.INSTANCE, IndexType.NAME,
        definition.getID());
    for (final QNm name : names) {
      final NodeReferences directPostings = direct.get(name, SearchMode.EQUAL);
      assertTrue(directPostings != null);
      assertEquals(scanned.get(name), collect(List.of(directPostings).iterator()), "direct NAME reader " + stage);
      assertEquals(scanned.get(name),
          collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(name), Set.of()))),
          "exact NAME postings " + stage + " for a " + name.getLocalName().length() + "-character name");
    }
  }

  private static void assertFilteredIndexEqualsScan(final IndexController<?, ?> controller,
      final StorageEngineReader reader, final IndexDef definition, final List<QNm> names,
      final Map<QNm, Set<Long>> scanned, final String stage) {
    final Set<Long> all = new TreeSet<>();
    for (final Set<Long> keys : scanned.values()) {
      all.addAll(keys);
    }
    for (final QNm name : names) {
      final QNm missing = new QNm("missing");
      assertEquals(scanned.get(name),
          collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(name, missing), Set.of()))),
          "multiple includes " + stage);
      final Set<Long> excluded = new TreeSet<>(all);
      excluded.removeAll(scanned.get(name));
      assertEquals(excluded,
          collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(), Set.of(name)))),
          "excluded name " + stage);
      assertEquals(Set.of(), collect(controller.openNameIndex(reader, definition,
          new NameFilter(Set.of(name, missing), Set.of(name)))), "include then exclude " + stage);
    }
  }

  private static void assertCandidateCursorsKeepResidualsExact(final StorageEngineReader storage,
      final IndexDef definition, final QNm first, final QNm second, final Map<QNm, Set<Long>> scanned) {
    final HOTIndexReader<QNm> reader = HOTIndexReader.create(storage, NameKeySerializer.INSTANCE, IndexType.NAME,
        definition.getID());
    final List<Iterator<Map.Entry<QNm, NodeReferences>>> inclusive = List.of(reader.iterator(),
        reader.range(first, second), reader.range(first, second, true, true), reader.iteratorFrom(first, true),
        reader.iteratorTo(second, true));
    for (final Iterator<Map.Entry<QNm, NodeReferences>> cursor : inclusive) {
      int markedBuckets = 0;
      while (cursor.hasNext()) {
        final Map.Entry<QNm, NodeReferences> entry = cursor.next();
        final RawKeyBytes raw = (RawKeyBytes) entry;
        final boolean truncated = NameKeySerializer.isTruncated(raw.rawKeyBytes(), 0, raw.rawKeyLength());
        if (truncated) {
          markedBuckets++;
          for (final QNm name : List.of(first, second)) {
            final NodeReferences exact = NameIndexRecheck.exactPostings(storage, entry.getValue(), name, truncated);
            assertTrue(exact != null);
            assertEquals(scanned.get(name), collect(List.of(exact).iterator()));
          }
        }
      }
      assertEquals(1, markedBuckets, "each candidate cursor includes the shared serialized bucket once");
    }
    final List<Iterator<Map.Entry<QNm, NodeReferences>>> exclusive = List.of(reader.iteratorFrom(first, false),
        reader.iteratorTo(second, false), reader.range(first, second, false, true),
        reader.range(first, second, true, false));
    for (final Iterator<Map.Entry<QNm, NodeReferences>> cursor : exclusive) {
      while (cursor.hasNext()) {
        final RawKeyBytes raw = (RawKeyBytes) cursor.next();
        assertTrue(!NameKeySerializer.isTruncated(raw.rawKeyBytes(), 0, raw.rawKeyLength()),
            "exclusive serialized bounds exclude the entire shared bucket");
      }
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

  private static byte[] serialize(final QNm name) {
    final byte[] buffer = new byte[NameKeySerializer.INSTANCE.maxSerializedLength(name)];
    final int length = NameKeySerializer.INSTANCE.serialize(name, buffer, 0);
    return Arrays.copyOf(buffer, length);
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
