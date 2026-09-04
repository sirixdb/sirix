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
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression gate for transaction-local valid-time interval maintenance. */
final class JsonValidTimeIncrementalMaintenanceTest {

  private static final String RESOURCE = "valid-time-maintenance";
  private static final int INDEX_ID = 0;
  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

  private static final Instant IN_2020 = Instant.parse("2020-06-01T00:00:00Z");
  private static final Instant IN_2021 = Instant.parse("2021-06-01T00:00:00Z");
  private static final Instant IN_2022 = Instant.parse("2022-06-01T00:00:00Z");
  private static final Instant IN_2023 = Instant.parse("2023-06-01T00:00:00Z");
  private static final Instant IN_2024 = Instant.parse("2024-06-01T00:00:00Z");
  private static final Instant IN_2025 = Instant.parse("2025-06-01T00:00:00Z");
  private static final Instant IN_2026 = Instant.parse("2026-06-01T00:00:00Z");
  private static final Instant IN_2030 = Instant.parse("2030-06-01T00:00:00Z");

  private static final String INITIAL_JSON = """
      {
        "left": [
          {"id": 1, "validFrom": "2020-01-01T00:00:00Z", "validTo": "2020-12-31T23:59:59Z"},
          {
            "id": 2,
            "metadata": {"tag": "removed-before-the-bounds"},
            "validFrom": "2021-01-01T00:00:00Z",
            "validTo": "2021-12-31T23:59:59Z"
          },
          {"id": 3, "validFrom": "2022-01-01T00:00:00Z", "validTo": "2022-12-31T23:59:59Z"}
        ],
        "right": [],
        "named": {
          "id": 4,
          "validFrom": "2023-01-01T00:00:00Z",
          "validTo": "2023-12-31T23:59:59Z"
        }
      }
      """;

  /**
   * The builder's contract is first parseable duplicate in document order, independently per bound.
   */
  private static final String DUPLICATE_BOUNDS_JSON = """
      {
        "left": [
          {
            "id": 10,
            "validFrom": "not-an-instant",
            "validFrom": "2020-01-01T00:00:00Z",
            "validFrom": "2022-01-01T00:00:00Z",
            "validTo": "also-not-an-instant",
            "validTo": "2020-12-31T23:59:59Z",
            "validTo": "2022-12-31T23:59:59Z"
          },
          {
            "id": 11,
            "validFrom": "2030-01-01T00:00:00Z",
            "validTo": "2030-12-31T23:59:59Z"
          }
        ]
      }
      """;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0} incrementally maintains valid-time intervals")
  @EnumSource(VersioningType.class)
  void updateDeleteMoveInsertAndColdHistoryRemainCorrect(final VersioningType versioningType) throws Exception {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final IndexDef definition = validTimeDefinition();
    final long id1;
    final long id2;
    final long id3;
    final long id4;
    final long rightArray;
    final long id1From;
    final long id1To;
    final long id4From;
    final long id4To;
    final int indexRevision;
    final int updateRevision;
    final int deleteRevision;
    final int moveRevision;
    final int insertRevision;
    long id5;

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(true)
                                                              .validTimePaths(VALID_FROM, VALID_TO)
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(10)
                                                              .build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(INITIAL_JSON), JsonNodeTrx.Commit.NO);
        wtx.commit();
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
        wtx.commit();
      }
      indexRevision = mostRecentRevision(database);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(indexRevision)) {
        id1 = objectKeyById(rtx, 1);
        id2 = objectKeyById(rtx, 2);
        id3 = objectKeyById(rtx, 3);
        id4 = objectKeyById(rtx, 4);
        rightArray = namedNodeKey(rtx, "right", NodeKind.OBJECT_NAMED_ARRAY);
        id1From = namedStringChildKey(rtx, id1, VALID_FROM);
        id1To = namedStringChildKey(rtx, id1, VALID_TO);
        id4From = namedStringChildKey(rtx, id4, VALID_FROM);
        id4To = namedStringChildKey(rtx, id4, VALID_TO);

        assertTrue(rtx.moveTo(id1));
        assertEquals(NodeKind.OBJECT, rtx.getKind());
        assertTrue(rtx.moveTo(id4));
        assertEquals(NodeKind.OBJECT_NAMED_OBJECT, rtx.getKind(),
            "the named record must exercise fused OBJECT_NAMED_OBJECT containment");
      }
      assertIndex(database, indexRevision, IN_2020, id1);
      assertIndex(database, indexRevision, IN_2021, id2);
      assertIndex(database, indexRevision, IN_2022, id3);
      assertIndex(database, indexRevision, IN_2023, id4);

      // Update both bounds of record 1, then record 4, then revisit record 1. This forces two
      // object-key transitions within one transaction and verifies transaction-current reseeding.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        setString(wtx, id1From, "2024-01-01T00:00:00Z");
        setString(wtx, id1To, "2024-06-30T23:59:59Z");
        setString(wtx, id4From, "2025-01-01T00:00:00Z");
        setString(wtx, id4To, "2025-12-31T23:59:59Z");
        setString(wtx, id1To, "2024-12-31T23:59:59Z");
        wtx.commit();
      }
      updateRevision = mostRecentRevision(database);
      assertIndex(database, updateRevision, IN_2020);
      assertIndex(database, updateRevision, IN_2023);
      assertIndex(database, updateRevision, IN_2024, id1);
      assertIndex(database, updateRevision, IN_2025, id4);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(id2));
        wtx.remove();
        wtx.commit();
      }
      deleteRevision = mostRecentRevision(database);
      assertIndex(database, deleteRevision, IN_2021);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(rightArray));
        wtx.moveSubtreeToFirstChild(id3);
        wtx.commit();
      }
      moveRevision = mostRecentRevision(database);
      assertIndex(database, moveRevision, IN_2022, id3);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(rightArray));
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {"id": 5, "validFrom": "2026-01-01T00:00:00Z", "validTo": "2026-12-31T23:59:59Z"}
            """), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      insertRevision = mostRecentRevision(database);
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(insertRevision)) {
        id5 = objectKeyById(rtx, 5);
      }
      assertIndex(database, insertRevision, IN_2026, id5);
    }

    // A cold database reopen validates every historical index revision under the selected page
    // versioning strategy; no rebuild path is available to these direct RI-tree reads.
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath)) {
      assertEquals(versioningType, resourceVersioning(reopened));
      assertIndex(reopened, indexRevision, IN_2020, id1);
      assertIndex(reopened, indexRevision, IN_2021, id2);
      assertIndex(reopened, indexRevision, IN_2022, id3);
      assertIndex(reopened, indexRevision, IN_2023, id4);

      assertIndex(reopened, updateRevision, IN_2020);
      assertIndex(reopened, updateRevision, IN_2023);
      assertIndex(reopened, updateRevision, IN_2024, id1);
      assertIndex(reopened, updateRevision, IN_2025, id4);

      assertIndex(reopened, deleteRevision, IN_2021);
      assertIndex(reopened, moveRevision, IN_2022, id3);
      assertIndex(reopened, insertRevision, IN_2026, id5);
    }
  }

  @ParameterizedTest(name = "{0} preserves first-parseable duplicate-bound semantics")
  @EnumSource(VersioningType.class)
  void duplicateBoundsRemainBuilderEquivalentAcrossEveryIncrementalMutation(final VersioningType versioningType)
      throws Exception {
    final Path databasePath = temporaryDirectory.resolve("duplicates-" + versioningType.name().toLowerCase());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final IndexDef definition = validTimeDefinition();
    final long objectKey;
    final long destinationObjectKey;
    final long trailingFromKey;
    final long trailingToKey;
    final int initialRevision;
    final int ignoredUpdateRevision;
    final int moveRevision;
    final int deleteRevision;
    final int insertRevision;
    final int selectedUpdateRevision;
    final int crossObjectMoveRevision;
    long insertedFromKey;
    long insertedToKey;

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(true)
                                                              .validTimePaths(VALID_FROM, VALID_TO)
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(10)
                                                              .build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DUPLICATE_BOUNDS_JSON), JsonNodeTrx.Commit.NO);
        wtx.commit();
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
        wtx.commit();
      }
      initialRevision = mostRecentRevision(database);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(initialRevision)) {
        objectKey = objectKeyById(rtx, 10);
        destinationObjectKey = objectKeyById(rtx, 11);
        final List<Long> fromKeys = namedStringChildKeys(rtx, objectKey, VALID_FROM);
        final List<Long> toKeys = namedStringChildKeys(rtx, objectKey, VALID_TO);
        assertEquals(3, fromKeys.size());
        assertEquals(3, toKeys.size());
        trailingFromKey = fromKeys.get(2);
        trailingToKey = toKeys.get(2);
      }
      assertIndex(database, initialRevision, IN_2020, objectKey);
      assertIndex(database, initialRevision, IN_2022);
      assertIndex(database, initialRevision, IN_2030, destinationObjectKey);

      // Updating ignored trailing duplicates must not replace the builder-selected first parseable
      // values. This was the direct event-assignment bug: the listener used to publish 2023 here.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        setString(wtx, trailingFromKey, "2023-01-01T00:00:00Z");
        setString(wtx, trailingToKey, "2023-12-31T23:59:59Z");
        wtx.commit();
      }
      ignoredUpdateRevision = mostRecentRevision(database);
      assertIndex(database, ignoredUpdateRevision, IN_2020, objectKey);
      assertIndex(database, ignoredUpdateRevision, IN_2023);

      // Reordering those parseable duplicates ahead of the old winners arrives as a DELETE/INSERT
      // move pair. Final document order now selects the two 2023 nodes.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(objectKey));
        wtx.moveSubtreeToFirstChild(trailingFromKey);
        assertTrue(wtx.moveTo(objectKey));
        wtx.moveSubtreeToFirstChild(trailingToKey);
        wtx.commit();
      }
      moveRevision = mostRecentRevision(database);
      assertIndex(database, moveRevision, IN_2023, objectKey);
      assertIndex(database, moveRevision, IN_2020);

      // Deleting the selected duplicates reveals the next parseable pair rather than making the
      // bounds unconditionally null.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(trailingFromKey));
        wtx.remove();
        assertTrue(wtx.moveTo(trailingToKey));
        wtx.remove();
        wtx.commit();
      }
      deleteRevision = mostRecentRevision(database);
      assertIndex(database, deleteRevision, IN_2020, objectKey);
      assertIndex(database, deleteRevision, IN_2023);

      // A newly inserted first duplicate wins immediately, exactly as a fresh builder scan would.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(objectKey));
        insertedFromKey =
            wtx.insertObjectRecordAsFirstChild(VALID_FROM, new StringValue("2024-01-01T00:00:00Z")).getNodeKey();
        assertTrue(wtx.moveTo(objectKey));
        insertedToKey =
            wtx.insertObjectRecordAsFirstChild(VALID_TO, new StringValue("2024-12-31T23:59:59Z")).getNodeKey();
        wtx.commit();
      }
      insertRevision = mostRecentRevision(database);
      assertIndex(database, insertRevision, IN_2020);
      assertIndex(database, insertRevision, IN_2024, objectKey);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        setString(wtx, insertedFromKey, "2025-01-01T00:00:00Z");
        setString(wtx, insertedToKey, "2025-12-31T23:59:59Z");
        wtx.commit();
      }
      selectedUpdateRevision = mostRecentRevision(database);
      assertIndex(database, selectedUpdateRevision, IN_2024);
      assertIndex(database, selectedUpdateRevision, IN_2025, objectKey);

      // Moving the selected start bound to another record must reconcile both records: the source
      // falls back to its next parseable duplicate, while the destination selects the moved field.
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(destinationObjectKey));
        wtx.moveSubtreeToFirstChild(insertedFromKey);
        wtx.commit();
      }
      crossObjectMoveRevision = mostRecentRevision(database);
      assertIndex(database, crossObjectMoveRevision, IN_2021, objectKey);
      assertIndex(database, crossObjectMoveRevision, IN_2026, destinationObjectKey);
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath)) {
      assertEquals(versioningType, resourceVersioning(reopened));
      assertIndex(reopened, initialRevision, IN_2020, objectKey);
      assertIndex(reopened, ignoredUpdateRevision, IN_2020, objectKey);
      assertIndex(reopened, moveRevision, IN_2023, objectKey);
      assertIndex(reopened, deleteRevision, IN_2020, objectKey);
      assertIndex(reopened, insertRevision, IN_2024, objectKey);
      assertIndex(reopened, selectedUpdateRevision, IN_2025, objectKey);
      assertIndex(reopened, crossObjectMoveRevision, IN_2021, objectKey);
      assertIndex(reopened, crossObjectMoveRevision, IN_2026, destinationObjectKey);
    }
  }

  private static IndexDef validTimeDefinition() {
    final Set<io.brackit.query.util.path.Path<io.brackit.query.atomic.QNm>> paths = new LinkedHashSet<>();
    paths.add(parse("/left/[]/" + VALID_FROM, PathParser.Type.JSON));
    paths.add(parse("/left/[]/" + VALID_TO, PathParser.Type.JSON));
    return IndexDefs.createValidTimeIdxDef(paths, INDEX_ID, IndexDef.DbType.JSON);
  }

  private static void setString(final JsonNodeTrx wtx, final long nodeKey, final String value) {
    assertTrue(wtx.moveTo(nodeKey));
    assertTrue(wtx.isStringValue());
    wtx.setStringValue(value);
  }

  private static int mostRecentRevision(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      return session.getMostRecentRevisionNumber();
    }
  }

  private static VersioningType resourceVersioning(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      return session.getResourceConfig().versioningType;
    }
  }

  private static void assertIndex(final Database<JsonResourceSession> database, final int revision, final Instant point,
      final long... expectedNodeKeys) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final IntervalDomain domain = new IntervalDomain();
      final RelationalIntervalTree tree =
          ValidTimeIntervalIndexFactory.createReaderTree(rtx.getStorageEngineReader(), INDEX_ID, domain);
      final List<Long> actual = new ArrayList<>();
      tree.stab(domain.point(point), actual::add);
      Collections.sort(actual);

      final List<Long> expected = new ArrayList<>(expectedNodeKeys.length);
      for (final long nodeKey : expectedNodeKeys) {
        expected.add(nodeKey);
      }
      Collections.sort(expected);
      assertEquals(expected, actual, "valid-time entries at " + point + " in revision " + revision);
    }
  }

  private static long objectKeyById(final JsonNodeReadOnlyTrx rtx, final long id) {
    final long restore = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (rtx.getKind() == NodeKind.OBJECT_NAMED_NUMBER && "id".equals(rtx.getName().getLocalName())
            && rtx.getNumberValue().longValue() == id) {
          assertTrue(rtx.moveToParent());
          return rtx.getNodeKey();
        }
      }
      throw new AssertionError("missing record id " + id);
    } finally {
      rtx.moveTo(restore);
    }
  }

  private static long namedNodeKey(final JsonNodeReadOnlyTrx rtx, final String name, final NodeKind kind) {
    final long restore = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (rtx.getKind() == kind && rtx.getName() != null && name.equals(rtx.getName().getLocalName())) {
          return rtx.getNodeKey();
        }
      }
      throw new AssertionError("missing " + kind + " named " + name);
    } finally {
      rtx.moveTo(restore);
    }
  }

  private static long namedStringChildKey(final JsonNodeReadOnlyTrx rtx, final long objectKey, final String name) {
    final long restore = rtx.getNodeKey();
    try {
      assertTrue(rtx.moveTo(objectKey));
      if (rtx.moveToFirstChild()) {
        do {
          if (rtx.getKind() == NodeKind.OBJECT_NAMED_STRING && name.equals(rtx.getName().getLocalName())) {
            return rtx.getNodeKey();
          }
        } while (rtx.moveToRightSibling());
      }
      throw new AssertionError("missing string field " + name + " below " + objectKey);
    } finally {
      rtx.moveTo(restore);
    }
  }

  private static List<Long> namedStringChildKeys(final JsonNodeReadOnlyTrx rtx, final long objectKey,
      final String name) {
    final long restore = rtx.getNodeKey();
    final List<Long> keys = new ArrayList<>();
    try {
      assertTrue(rtx.moveTo(objectKey));
      if (rtx.moveToFirstChild()) {
        do {
          if (rtx.getKind() == NodeKind.OBJECT_NAMED_STRING && name.equals(rtx.getName().getLocalName())) {
            keys.add(rtx.getNodeKey());
          }
        } while (rtx.moveToRightSibling());
      }
      return keys;
    } finally {
      rtx.moveTo(restore);
    }
  }
}
