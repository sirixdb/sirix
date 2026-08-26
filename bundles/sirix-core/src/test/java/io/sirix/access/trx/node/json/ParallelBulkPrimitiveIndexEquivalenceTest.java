/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.AtomicUtil;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongIterator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Seam-1 gate: a PARALLEL bulk import with PATH/CAS/NAME indexes catalogued must answer index
 * queries identically to a SEQUENTIAL import of the same corpus — the sequential path maintains
 * those families through per-node change notifications, which is the semantics oracle the parallel
 * importer's worker-collected tuple feed has to reproduce.
 *
 * <p>
 * Every probe is checked THREE ways: the two arms must agree with each other AND with the exact
 * count the corpus generator's arithmetic predicts — a defect that empties or inflates both arms
 * identically cannot pass. The corpus is adversarial by construction: tiny chunks force boundaries
 * mid-page, a field name first occurs only in the LAST quarter of the records (so its path class
 * and dictionary key are minted many chunks in), a named array exercises the OBJECT_NAMED_ARRAY
 * dual-role mirror entry, array elements exercise CAS over plain value nodes, and a mistyped CAS
 * definition exercises the skip-on-conversion-failure lane.
 */
final class ParallelBulkPrimitiveIndexEquivalenceTest {

  private static final java.nio.file.Path SEQUENTIAL_DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final java.nio.file.Path PARALLEL_DB_PATH = JsonTestHelper.PATHS.PATH2.getFile();

  private static final int RECORDS = 4000;
  private static final int CHUNK_BUDGET_BYTES = 6 * 1024;
  private static final int BUILDERS = 4;

  // Definition numbers are DENSE PER TYPE — the NamePage-backed dictionaries key sub-structures by
  // definition number within a family, and sparse numbering leaves gapped offsets no commit can
  // serialize.
  private static final int PATH_ALL_DEF_NO = 0;
  private static final int PATH_SELECTIVE_DEF_NO = 1;
  // The NAME factory shifts ids into its own offset space — derive the effective ids from it
  // rather than assuming the raw numbers.
  private static final int NAME_ALL_DEF_NO = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID();
  private static final int NAME_SELECTIVE_DEF_NO =
      IndexDefs.createSelectiveNameIdxDef(Set.of(new QNm("dept")), 1, IndexDef.DbType.JSON).getID();
  private static final int CAS_DEPT_DEF_NO = 0;
  private static final int CAS_SCORE_DEF_NO = 1;
  private static final int CAS_TAG_ELEMENT_DEF_NO = 2;
  private static final int CAS_MISTYPED_DEF_NO = 3;
  /** A definition whose path FIRST EXISTS many chunks in — pins the stale-path-class-cache defect. */
  private static final int CAS_LATECOMER_DEF_NO = 4;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void aParallelImportAnswersIndexQueriesLikeASequentialImport() throws IOException {
    final String corpus = corpus();

    try (Database<JsonResourceSession> sequentialDb = createDatabase(SEQUENTIAL_DB_PATH);
        Database<JsonResourceSession> parallelDb = createDatabase(PARALLEL_DB_PATH)) {
      try (JsonResourceSession sequentialSession = sequentialDb.beginResourceSession(JsonTestHelper.RESOURCE);
          JsonResourceSession parallelSession = parallelDb.beginResourceSession(JsonTestHelper.RESOURCE)) {

        // Sequential oracle leg: catalogue the definitions on the EMPTY resource, then shred with
        // the notifying bulk sink — the listeners maintain every family per node.
        try (JsonNodeTrx wtx = sequentialSession.beginNodeTrx()) {
          sequentialSession.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(indexDefs(), wtx);
          BulkJsonTreeAssembler.assemble(wtx, new StringReader(corpus));
          wtx.commit();
        }

        // Parallel leg: same definitions, worker-collected tuples, adversarial chunking.
        try (JsonNodeTrx wtx = parallelSession.beginNodeTrx()) {
          parallelSession.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(indexDefs(), wtx);
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus.getBytes(StandardCharsets.UTF_8)),
              CHUNK_BUDGET_BYTES, BUILDERS);
          wtx.commit();
        }

        // ==== PATH ====
        // The index-everything definition: the top-level array is the coordinator-fed entry the
        // workers never see; "/[]/dept" covers one fused field per record.
        comparePathProbe(sequentialSession, parallelSession, PATH_ALL_DEF_NO, "/[]", 1);
        comparePathProbe(sequentialSession, parallelSession, PATH_ALL_DEF_NO, "/[]/dept", RECORDS);
        comparePathProbe(sequentialSession, parallelSession, PATH_ALL_DEF_NO, "/[]/nested", countNested());
        comparePathProbe(sequentialSession, parallelSession, PATH_ALL_DEF_NO, "/[]/latecomer", countLatecomer());
        // The selective definition resolves "/[]/tags" at the OBJECT_KEY layer, which only the
        // OBJECT_NAMED_ARRAY dual-role MIRROR entry serves.
        comparePathProbe(sequentialSession, parallelSession, PATH_SELECTIVE_DEF_NO, "/[]/tags", countTagRecords());
        comparePathProbe(sequentialSession, parallelSession, PATH_SELECTIVE_DEF_NO, "/[]/nested", countNested());

        // ==== NAME ====
        compareNameProbe(sequentialSession, parallelSession, NAME_ALL_DEF_NO, "dept", RECORDS);
        compareNameProbe(sequentialSession, parallelSession, NAME_ALL_DEF_NO, "tags", countTagRecords());
        compareNameProbe(sequentialSession, parallelSession, NAME_ALL_DEF_NO, "inner", countNested());
        compareNameProbe(sequentialSession, parallelSession, NAME_ALL_DEF_NO, "latecomer", countLatecomer());
        compareNameProbe(sequentialSession, parallelSession, NAME_SELECTIVE_DEF_NO, "dept", RECORDS);
        compareNameProbe(sequentialSession, parallelSession, NAME_SELECTIVE_DEF_NO, "latecomer", countLatecomer());
        // Not included in the selective definition — present in the corpus, absent from the index.
        compareNameProbe(sequentialSession, parallelSession, NAME_SELECTIVE_DEF_NO, "name", 0);

        // ==== CAS ====
        compareCasProbe(sequentialSession, parallelSession, CAS_DEPT_DEF_NO, "/[]/dept", Type.STR, "d3", countDept(3));
        compareCasProbe(sequentialSession, parallelSession, CAS_DEPT_DEF_NO, "/[]/dept", Type.STR, "d6", countDept(6));
        compareCasProbe(sequentialSession, parallelSession, CAS_SCORE_DEF_NO, "/[]/score", Type.LON, "300", 1);
        compareCasProbe(sequentialSession, parallelSession, CAS_TAG_ELEMENT_DEF_NO, "/[]/tags/[]", Type.STR, "t7",
            countTagElements("t7"));
        // Strings that cannot convert to xs:long are SKIPPED by both legs, not indexed as garbage.
        compareCasProbe(sequentialSession, parallelSession, CAS_MISTYPED_DEF_NO, "/[]/name", Type.LON, "5", 0);
        // The definition's path acquires a path CLASS only in the last quarter of the load — a
        // stale resolved-path cache in any drained builder would leave this probe empty.
        compareCasProbe(sequentialSession, parallelSession, CAS_LATECOMER_DEF_NO, "/[]/latecomer", Type.STR, "L5",
            countLatecomerValue("L5"));
      }
    }
  }

  @Test
  void aValidTimeIndexStillRefusesWithTheExactFamilyMessage() throws IOException {
    try (Database<JsonResourceSession> db = createDatabase(SEQUENTIAL_DB_PATH)) {
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        session.getWtxIndexController(wtx.getRevisionNumber())
               .getIndexes()
               .add(IndexDefs.createValidTimeIdxDef(Set.of(Path.parse("/[]/from", PathParser.Type.JSON)), 0,
                   IndexDef.DbType.JSON));
        final IllegalStateException refusal =
            assertThrows(IllegalStateException.class, () -> ParallelBulkJsonImporter.assembleBytes(wtx,
                new ByteArrayInputStream(corpus().getBytes(StandardCharsets.UTF_8)), CHUNK_BUDGET_BYTES, 1));
        assertTrue(refusal.getMessage().contains("valid-time"),
            "expected the exact valid-time refusal, got: " + refusal.getMessage());
      }
    }
  }

  // ==== probes =================================================================================

  private void comparePathProbe(final JsonResourceSession sequentialSession, final JsonResourceSession parallelSession,
      final int defNumber, final String path, final int expectedCount) {
    final TreeSet<Long> sequential = pathProbe(sequentialSession, defNumber, path);
    final TreeSet<Long> parallel = pathProbe(parallelSession, defNumber, path);
    assertEquals(expectedCount, sequential.size(),
        "sequential PATH probe " + path + " (def " + defNumber + ") disagrees with the corpus arithmetic");
    assertEquals(sequential, parallel, "PATH probe " + path + " (def " + defNumber + ") differs across arms");
  }

  private void compareNameProbe(final JsonResourceSession sequentialSession, final JsonResourceSession parallelSession,
      final int defNumber, final String name, final int expectedCount) {
    final TreeSet<Long> sequential = nameProbe(sequentialSession, defNumber, name);
    final TreeSet<Long> parallel = nameProbe(parallelSession, defNumber, name);
    assertEquals(expectedCount, sequential.size(),
        "sequential NAME probe " + name + " (def " + defNumber + ") disagrees with the corpus arithmetic");
    assertEquals(sequential, parallel, "NAME probe " + name + " (def " + defNumber + ") differs across arms");
  }

  private void compareCasProbe(final JsonResourceSession sequentialSession, final JsonResourceSession parallelSession,
      final int defNumber, final String path, final Type type, final String lexicalValue, final int expectedCount) {
    final TreeSet<Long> sequential = casProbe(sequentialSession, defNumber, path, type, lexicalValue);
    final TreeSet<Long> parallel = casProbe(parallelSession, defNumber, path, type, lexicalValue);
    assertEquals(expectedCount, sequential.size(), "sequential CAS probe " + path + " = " + lexicalValue + " (def "
        + defNumber + ") disagrees with the corpus arithmetic");
    assertEquals(sequential, parallel,
        "CAS probe " + path + " = " + lexicalValue + " (def " + defNumber + ") differs across arms");
  }

  private TreeSet<Long> pathProbe(final JsonResourceSession session, final int defNumber, final String path) {
    try (JsonNodeTrx trx = session.beginNodeTrx()) {
      final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef indexDef = controller.getIndexes().getIndexDef(defNumber, IndexType.PATH);
      return collect(controller.openPathIndex(trx.getStorageEngineReader(), indexDef,
          controller.createPathFilter(Set.of(path), trx)));
    }
  }

  private TreeSet<Long> nameProbe(final JsonResourceSession session, final int defNumber, final String name) {
    try (JsonNodeTrx trx = session.beginNodeTrx()) {
      final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef indexDef = controller.getIndexes().getIndexDef(defNumber, IndexType.NAME);
      if (indexDef == null) {
        throw new IllegalStateException("NAME def " + defNumber + " missing after reopen; registry holds: "
            + controller.getIndexes()
                        .getIndexDefs()
                        .stream()
                        .map(def -> def.getType() + "#" + def.getID())
                        .sorted()
                        .toList());
      }
      return collect(
          controller.openNameIndex(trx.getStorageEngineReader(), indexDef, controller.createNameFilter(Set.of(name))));
    }
  }

  private TreeSet<Long> casProbe(final JsonResourceSession session, final int defNumber, final String path,
      final Type type, final String lexicalValue) {
    try (JsonNodeTrx trx = session.beginNodeTrx()) {
      final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef indexDef = controller.getIndexes().getIndexDef(defNumber, IndexType.CAS);
      final Atomic probeKey = type == Type.STR
          ? new Str(lexicalValue)
          : AtomicUtil.toType(new Str(lexicalValue), type);
      return collect(controller.openCASIndex(trx.getStorageEngineReader(), indexDef,
          controller.createCASFilter(Set.of(path), probeKey, SearchMode.EQUAL, new JsonPCRCollector(trx))));
    }
  }

  private static TreeSet<Long> collect(final Iterator<NodeReferences> hits) {
    final TreeSet<Long> nodeKeys = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator nodeKeyIterator = hits.next().getNodeKeys().getLongIterator();
      while (nodeKeyIterator.hasNext()) {
        nodeKeys.add(nodeKeyIterator.next());
      }
    }
    return nodeKeys;
  }

  // ==== fixtures ===============================================================================

  private static Database<JsonResourceSession> createDatabase(final java.nio.file.Path databasePath) {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
    database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                 .useDeweyIDs(false)
                                                 .hashKind(HashType.NONE)
                                                 .storeNodeHistory(false)
                                                 .buildPathSummary(true)
                                                 .build());
    return database;
  }

  private static Set<IndexDef> indexDefs() {
    return Set.of(IndexDefs.createPathIdxDef(Set.of(), PATH_ALL_DEF_NO, IndexDef.DbType.JSON),
        IndexDefs.createPathIdxDef(
            Set.of(Path.parse("/[]/tags", PathParser.Type.JSON), Path.parse("/[]/nested", PathParser.Type.JSON)),
            PATH_SELECTIVE_DEF_NO, IndexDef.DbType.JSON),
        IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON),
        IndexDefs.createSelectiveNameIdxDef(Set.of(new QNm("dept"), new QNm("latecomer")), 1, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(Path.parse("/[]/dept", PathParser.Type.JSON)),
            CAS_DEPT_DEF_NO, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.LON, Set.of(Path.parse("/[]/score", PathParser.Type.JSON)),
            CAS_SCORE_DEF_NO, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(Path.parse("/[]/tags/[]", PathParser.Type.JSON)),
            CAS_TAG_ELEMENT_DEF_NO, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.LON, Set.of(Path.parse("/[]/name", PathParser.Type.JSON)),
            CAS_MISTYPED_DEF_NO, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(Path.parse("/[]/latecomer", PathParser.Type.JSON)),
            CAS_LATECOMER_DEF_NO, IndexDef.DbType.JSON));
  }

  /**
   * Same adversarial shape as the projection equivalence corpus: varying record widths, absent
   * fields, a nested object, a named string array, and a field whose FIRST occurrence is in the last
   * quarter of the records.
   */
  private static String corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 160);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"n").append(record).append('-').append("x".repeat(record % 17)).append('"');
      json.append(",\"dept\":\"d").append(record % 7).append('"');
      json.append(",\"score\":").append(record * 3L);
      json.append(",\"active\":").append((record & 1) == 0);
      if (record % 3 == 0) {
        json.append(",\"tags\":[\"t").append(record % 11).append("\",\"t").append(record % 13).append("\"]");
      }
      if (record % 4 == 0) {
        json.append(",\"nested\":{\"inner\":").append(record * 2L).append('}');
      }
      if (record >= RECORDS - RECORDS / 4) {
        json.append(",\"latecomer\":\"L").append(record % 23).append('"');
      }
      json.append('}');
    }
    return json.append(']').toString();
  }

  private static int countDept(final int deptModulo) {
    int count = 0;
    for (int record = 0; record < RECORDS; record++) {
      if (record % 7 == deptModulo) {
        count++;
      }
    }
    return count;
  }

  private static int countTagRecords() {
    int count = 0;
    for (int record = 0; record < RECORDS; record++) {
      if (record % 3 == 0) {
        count++;
      }
    }
    return count;
  }

  private static int countTagElements(final String tag) {
    int count = 0;
    for (int record = 0; record < RECORDS; record++) {
      if (record % 3 != 0) {
        continue;
      }
      if (("t" + record % 11).equals(tag)) {
        count++;
      }
      if (("t" + record % 13).equals(tag)) {
        count++;
      }
    }
    return count;
  }

  private static int countNested() {
    int count = 0;
    for (int record = 0; record < RECORDS; record++) {
      if (record % 4 == 0) {
        count++;
      }
    }
    return count;
  }

  private static int countLatecomer() {
    return RECORDS / 4;
  }

  private static int countLatecomerValue(final String value) {
    int count = 0;
    for (int record = RECORDS - RECORDS / 4; record < RECORDS; record++) {
      if (("L" + record % 23).equals(value)) {
        count++;
      }
    }
    return count;
  }
}
