/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.page.RevisionRootPage;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.roaringbitmap.longlong.LongIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage for secondary-index generation changes.
 *
 * <p>
 * Dropping a definition removes only the revisioned catalogue entry. Its physical HOT root must
 * remain reserved because older revisions still address it; recreating the same logical index must
 * allocate another physical root. This test crosses a real database close/cache clear/reopen and
 * checks that contract for PATH, CAS, and NAME under every page-versioning algorithm.
 * </p>
 */
final class JsonIndexDropRecreateVersioningTest {

  private static final String RESOURCE = "drop-recreate-index-generations";
  private static final String OLD_FIELD = "legacy";
  private static final String FRESH_FIELD = "fresh";
  private static final String OLD_PATH = "/records/[]/" + OLD_FIELD;
  private static final String FRESH_PATH = "/records/[]/" + FRESH_FIELD;
  private static final String OLD_DOCUMENT = """
      {"records":[{"legacy":"old-alpha"},{"legacy":"old-update"},{"legacy":"old-delete"}]}
      """;
  private static final String FRESH_DOCUMENT = """
      {"records":[{"fresh":"new-alpha"},{"fresh":"new-update"},{"fresh":"new-delete"}]}
      """;
  private static final List<String> OLD_VALUES = List.of("old-alpha", "old-inserted", "old-updated");
  private static final List<String> FRESH_VALUES = List.of("latest-updated", "new-alpha", "new-inserted");

  @TempDir
  java.nio.file.Path temporaryDirectory;

  @ParameterizedTest(name = "drop/recreate secondary indexes with {0}")
  @EnumSource(VersioningType.class)
  void coldReopenKeepsHistoricalGenerationAndUsesFreshPhysicalIds(final VersioningType versioningType) {
    final java.nio.file.Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try {
      final Generation oldGeneration;
      final Generation freshGeneration;
      final int historicalRevision;
      final int latestRevision;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .versioningApproach(versioningType)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .buildPathSummary(true)
                                                     .build());

        oldGeneration = buildGeneration(database, OLD_DOCUMENT, OLD_PATH);

        final StructuralCounters beforeOldMutations = StructuralCounters.capture();
        historicalRevision =
            mutateGeneration(database, OLD_FIELD, "old-update", "old-updated", "old-delete", "old-inserted");
        beforeOldMutations.assertUnchanged("old index generation");

        freshGeneration = dropReplaceAndBuildFreshGeneration(database, oldGeneration);
        assertFreshPhysicalIds(oldGeneration, freshGeneration);

        final StructuralCounters beforeFreshMutations = StructuralCounters.capture();
        latestRevision =
            mutateGeneration(database, FRESH_FIELD, "new-update", "latest-updated", "new-delete", "new-inserted");
        beforeFreshMutations.assertUnchanged("fresh index generation");
      }

      // A new Database, ResourceSession, reader, index controller, and page cache must reconstruct
      // both index generations solely from durable state.
      Databases.getGlobalBufferManager().clearAllCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        assertHistoricalGeneration(session, historicalRevision, oldGeneration, freshGeneration);
        assertLatestGeneration(session, latestRevision, oldGeneration, freshGeneration);
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static Generation buildGeneration(final Database<JsonResourceSession> database, final String document,
      final String indexedPath) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      shred(wtx, document);
      final Generation generation = allocateGeneration(wtx, indexedPath);
      final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
      controller.createIndexes(generation.definitions(), wtx);
      wtx.commit();
      return generation;
    }
  }

  private static Generation dropReplaceAndBuildFreshGeneration(final Database<JsonResourceSession> database,
      final Generation oldGeneration) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
      controller.dropIndexes(oldGeneration.definitions(), wtx);

      wtx.moveToDocumentRoot();
      assertTrue(wtx.moveToFirstChild(), "the old document root must exist");
      wtx.remove();
      wtx.moveToDocumentRoot();
      shred(wtx, FRESH_DOCUMENT);

      final Generation freshGeneration = allocateGeneration(wtx, FRESH_PATH);
      controller.createIndexes(freshGeneration.definitions(), wtx);
      wtx.commit();
      return freshGeneration;
    }
  }

  private static int mutateGeneration(final Database<JsonResourceSession> database, final String field,
      final String valueToUpdate, final String updatedValue, final String valueToDelete, final String insertedValue) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(namedStringNodeKey(wtx, field, valueToUpdate)));
      wtx.setStringValue(updatedValue);

      assertTrue(wtx.moveTo(namedStringNodeKey(wtx, field, valueToDelete)));
      wtx.remove();

      assertTrue(wtx.moveTo(namedArrayNodeKey(wtx, "records")));
      wtx.insertObjectAsLastChild().insertObjectRecordAsFirstChild(field, new StringValue(insertedValue));
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static Generation allocateGeneration(final JsonNodeTrx wtx, final String indexedPath) {
    final StorageEngineWriter writer = wtx.getStorageEngineWriter();
    final RevisionRootPage revisionRoot = writer.getActualRevisionRootPage();
    final int pathId = writer.getPathPage(revisionRoot).nextUnallocatedIndex();
    final int casId = writer.getCASPage(revisionRoot).nextUnallocatedIndex();
    final int namePhysicalId = writer.getNamePage(revisionRoot).nextUnallocatedSecondaryNameIndex(DatabaseType.JSON);
    final int nameLogicalId = IndexDefs.logicalNameIndexDefNoForPhysicalSlot(namePhysicalId, IndexDef.DbType.JSON);
    final Set<Path<QNm>> paths = Set.of(Path.parse(indexedPath, PathParser.Type.JSON));
    final IndexDef name = IndexDefs.createNameIdxDef(nameLogicalId, IndexDef.DbType.JSON);
    assertEquals(namePhysicalId, name.getID(), "the NAME factory must preserve the allocated physical slot");
    return new Generation(IndexDefs.createPathIdxDef(paths, pathId, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, paths, casId, IndexDef.DbType.JSON), name);
  }

  private static void assertFreshPhysicalIds(final Generation oldGeneration, final Generation freshGeneration) {
    assertEquals(oldGeneration.path().getID() + 1, freshGeneration.path().getID(),
        "PATH recreation must use the next untouched physical root");
    assertEquals(oldGeneration.cas().getID() + 1, freshGeneration.cas().getID(),
        "CAS recreation must use the next untouched physical root");
    assertEquals(oldGeneration.name().getID() + 1, freshGeneration.name().getID(),
        "NAME recreation must use the next untouched physical root");
  }

  private static void assertHistoricalGeneration(final JsonResourceSession session, final int revision,
      final Generation oldGeneration, final Generation freshGeneration) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final JsonIndexController controller = session.getRtxIndexController(revision);
      final Generation durableOld = requireCatalogGeneration(controller, oldGeneration);
      assertCatalogAbsent(controller, freshGeneration);
      assertPhysicalReservations(rtx, oldGeneration, freshGeneration, false);
      assertGenerationScans(controller, rtx, durableOld, OLD_FIELD, FRESH_FIELD, OLD_VALUES);
    }
  }

  private static void assertLatestGeneration(final JsonResourceSession session, final int revision,
      final Generation oldGeneration, final Generation freshGeneration) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final JsonIndexController controller = session.getRtxIndexController(revision);
      assertCatalogAbsent(controller, oldGeneration);
      final Generation durableFresh = requireCatalogGeneration(controller, freshGeneration);
      assertPhysicalReservations(rtx, oldGeneration, freshGeneration, true);
      assertGenerationScans(controller, rtx, durableFresh, FRESH_FIELD, OLD_FIELD, FRESH_VALUES);
    }
  }

  private static Generation requireCatalogGeneration(final JsonIndexController controller, final Generation expected) {
    final IndexDef path = controller.getIndexes().getIndexDef(expected.path().getID(), IndexType.PATH);
    final IndexDef cas = controller.getIndexes().getIndexDef(expected.cas().getID(), IndexType.CAS);
    final IndexDef name = controller.getIndexes().getIndexDef(expected.name().getID(), IndexType.NAME);
    assertNotNull(path, "PATH definition must survive the cold reopen");
    assertNotNull(cas, "CAS definition must survive the cold reopen");
    assertNotNull(name, "NAME definition must survive the cold reopen");
    assertEquals(expected.path().getPaths(), path.getPaths(), "the revision must restore the right PATH definition");
    assertEquals(expected.cas().getPaths(), cas.getPaths(), "the revision must restore the right CAS paths");
    assertEquals(expected.cas().getContentType(), cas.getContentType(),
        "the revision must restore the right CAS content type");
    return new Generation(path, cas, name);
  }

  private static void assertCatalogAbsent(final JsonIndexController controller, final Generation generation) {
    assertNull(controller.getIndexes().getIndexDef(generation.path().getID(), IndexType.PATH));
    assertNull(controller.getIndexes().getIndexDef(generation.cas().getID(), IndexType.CAS));
    assertNull(controller.getIndexes().getIndexDef(generation.name().getID(), IndexType.NAME));
  }

  private static void assertPhysicalReservations(final JsonNodeReadOnlyTrx rtx, final Generation oldGeneration,
      final Generation freshGeneration, final boolean freshMustExist) {
    final StorageEngineReader reader = rtx.getStorageEngineReader();
    final RevisionRootPage revisionRoot = reader.getActualRevisionRootPage();
    assertTrue(reader.getPathPage(revisionRoot).isIndexInitialized(oldGeneration.path().getID()));
    assertTrue(reader.getCASPage(revisionRoot).isIndexInitialized(oldGeneration.cas().getID()));
    assertTrue(reader.getNamePage(revisionRoot)
                     .isSecondaryNameIndexInitialized(DatabaseType.JSON, oldGeneration.name().getID()));
    assertEquals(freshMustExist, reader.getPathPage(revisionRoot).isIndexInitialized(freshGeneration.path().getID()));
    assertEquals(freshMustExist, reader.getCASPage(revisionRoot).isIndexInitialized(freshGeneration.cas().getID()));
    assertEquals(freshMustExist,
        reader.getNamePage(revisionRoot)
              .isSecondaryNameIndexInitialized(DatabaseType.JSON, freshGeneration.name().getID()));
  }

  private static void assertGenerationScans(final JsonIndexController controller, final JsonNodeReadOnlyTrx rtx,
      final Generation generation, final String indexedName, final String absentName,
      final List<String> expectedValues) {
    assertEquals(expectedValues,
        postingValues(rtx, controller.openPathIndex(rtx.getStorageEngineReader(), generation.path(), null)),
        "PATH scan must expose only this revision's generation");
    assertEquals(expectedValues,
        postingValues(rtx, controller.openCASIndex(rtx.getStorageEngineReader(), generation.cas(), (CASFilter) null)),
        "CAS scan must expose only this revision's generation");
    assertEquals(expectedValues.size(), postingCount(controller.openNameIndex(rtx.getStorageEngineReader(),
        generation.name(), controller.createNameFilter(Set.of(indexedName)))),
        "NAME scan must expose every current field");
    assertEquals(0L, postingCount(controller.openNameIndex(rtx.getStorageEngineReader(), generation.name(),
        controller.createNameFilter(Set.of(absentName)))), "NAME scan must not leak the other generation");
  }

  private static List<String> postingValues(final JsonNodeReadOnlyTrx rtx, final Iterator<NodeReferences> postings) {
    final long restoreNodeKey = rtx.getNodeKey();
    final List<String> values = new ArrayList<>();
    try {
      while (postings.hasNext()) {
        final LongIterator nodeKeys = postings.next().nodeKeyIterator();
        while (nodeKeys.hasNext()) {
          final long nodeKey = nodeKeys.next();
          assertTrue(rtx.moveTo(nodeKey),
              "posting " + nodeKey + " must resolve in revision " + rtx.getRevisionNumber());
          values.add(rtx.getValue());
        }
      }
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
    Collections.sort(values);
    return values;
  }

  private static long postingCount(final Iterator<NodeReferences> postings) {
    long count = 0;
    while (postings.hasNext()) {
      count += postings.next().cardinality();
    }
    return count;
  }

  private static long namedStringNodeKey(final JsonNodeReadOnlyTrx rtx, final String fieldName, final String value) {
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if ((rtx.getKind() == NodeKind.OBJECT_NAMED_STRING || rtx.getKind() == NodeKind.STRING_VALUE)
            && fieldName.equals(currentFieldName(rtx)) && value.equals(rtx.getValue())) {
          return rtx.getNodeKey();
        }
      }
      throw new AssertionError("missing " + fieldName + "=\"" + value + "\"");
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  private static long namedArrayNodeKey(final JsonNodeReadOnlyTrx rtx, final String fieldName) {
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if ((rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY || rtx.getKind() == NodeKind.ARRAY)
            && fieldName.equals(currentFieldName(rtx))) {
          return rtx.getNodeKey();
        }
      }
      throw new AssertionError("missing array named " + fieldName);
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  private static String currentFieldName(final JsonNodeReadOnlyTrx rtx) {
    final QNm ownName = rtx.getName();
    if (ownName != null) {
      return ownName.getLocalName();
    }
    final long nodeKey = rtx.getNodeKey();
    try {
      return rtx.moveToParent() && rtx.getName() != null
          ? rtx.getName().getLocalName()
          : null;
    } finally {
      rtx.moveTo(nodeKey);
    }
  }

  private static void shred(final JsonNodeTrx wtx, final String json) {
    new JsonShredder.Builder(wtx, JsonShredder.createStringReader(json), InsertPosition.AS_FIRST_CHILD).build().call();
  }

  private record Generation(IndexDef path, IndexDef cas, IndexDef name) {
    Set<IndexDef> definitions() {
      return Set.of(path, cas, name);
    }
  }

  private record StructuralCounters(long validationFailures, long propagationFailures) {
    static StructuralCounters capture() {
      return new StructuralCounters(AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
          AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get());
    }

    void assertUnchanged(final String generation) {
      assertEquals(validationFailures, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
          generation + " ordinary mutations must publish invariant-clean structural candidates");
      assertEquals(propagationFailures, AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get(),
          generation + " ordinary mutations must pass pre-publication propagation checks");
    }
  }
}
