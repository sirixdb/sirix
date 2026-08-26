/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The v2 gate: a PARALLEL bulk import with a projection definition armed must produce, in the same
 * single pass, a projection identical to the one the post-pass build derives from the finished
 * resource.
 *
 * <p>
 * "Identical" is taken literally here rather than at the row-group level the sequential gate
 * settled for: {@link ProjectionStorageSnapshot} sweeps every slot family the projection sub-tree
 * owns — metadata, row-group descriptors, column-segment slots, the assembled leaves, fence chunks
 * including the physical-order header, per-column Bloom manifests and chunks, set-summary chunks,
 * the sparse record locator, the structural-order directory and the value-dictionary blobs — and
 * compares the raw bytes of each. The one field deliberately excluded is the metadata's
 * {@code buildRevision}: it records WHICH revision built the index, and a post-pass build cannot
 * run in the load's own revision by construction (it walks a resource that must already be
 * committed). Every byte that encodes DATA is compared.
 */
final class ParallelBulkProjectionEquivalenceTest {

  private static final java.nio.file.Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final java.nio.file.Path POST_PASS_DATABASE_PATH = JsonTestHelper.PATHS.PATH2.getFile();
  private static final int INDEX_NUMBER = 0;

  private static final String ROOT_PATH = "/[]";
  private static final List<String> FIELD_PATHS = List.of("/[]/name", "/[]/dept", "/[]/score", "/[]/ratio",
      "/[]/active", "/[]/tags/[]", "/[]/latecomer", "/[]/nested/inner");
  private static final List<Type> FIELD_TYPES =
      List.of(Type.STR, Type.STR, Type.LON, Type.DBL, Type.BOOL, Type.STR, Type.STR, Type.LON);

  /** Small enough to run everywhere, large enough to span many pages, chunks and leaves. */
  private static final int RECORDS = 4000;
  /** Forces chunk boundaries to land mid-page and page-sharing stitches on nearly every chunk. */
  private static final int CHUNK_BUDGET_BYTES = 6 * 1024;
  private static final int BUILDERS = 4;

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
  }

  @AfterEach
  void tearDown() throws IOException {
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void aParallelOnePassLoadProducesTheSameProjectionAsThePostPassBuild() throws Exception {
    final byte[] corpus = corpus(RECORDS);

    final ProjectionStorageSnapshot onePass = loadOnePassParallel(DATABASE_PATH, corpus);
    final ProjectionStorageSnapshot postPass = loadParallelThenPostPass(POST_PASS_DATABASE_PATH, corpus);

    assertFalse(onePass.stale(), "the one-pass build must have replaced its own tombstone");
    assertFalse(postPass.stale(), "the post-pass build must have published live metadata");
    assertTrue(onePass.rowGroupCount() > 1,
        "the corpus must span several leaves or the differential proves nothing; got " + onePass.rowGroupCount());
    assertEquivalent(onePass, postPass);
  }

  @Test
  void theDifferentialCanFail() throws Exception {
    // A guard that cannot say "no" is not evidence. Two corpora differing in ONE cell of ONE record
    // must produce snapshots the comparator rejects.
    final ProjectionStorageSnapshot a = loadOnePassParallel(DATABASE_PATH, corpus(RECORDS));
    final ProjectionStorageSnapshot b = loadOnePassParallel(POST_PASS_DATABASE_PATH, corpusWithMutatedRecord(RECORDS));

    assertThrows(AssertionError.class, () -> assertEquivalent(a, b),
        "the comparator must reject two projections that differ in a single cell");
  }

  @Test
  void anUnarmedParallelImportStillRefusesACataloguedProjection() throws Exception {
    // Arming is optional, but a catalogued projection with no armed load must not be silently left
    // unmaintained: the importer's workers never notify, so nothing would ever feed it.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        session.getWtxIndexController(wtx.getRevisionNumber()).getIndexes().add(projectionDef());
        final IllegalStateException refusal = assertThrows(IllegalStateException.class,
            () -> ParallelBulkJsonImporter.assemble(wtx, new ByteArrayInputStream(corpus(8))));
        assertTrue(refusal.getMessage().contains("no load-time build armed"),
            "expected the arm refusal, got: " + refusal.getMessage());
      }
    }
  }


  @Test
  void aRecordStraddlingTheHeldTailPageSurvivesTheFlushEpoch() throws Exception {
    // The bug this pins: a chunk's last record can BEGIN in a page already adopted into the intent
    // log and END in the page held back for the boundary stitch. It cannot be handed to the build
    // until its tail arrives, and if the epoch flushes in between, its head is written out and the
    // record can no longer be read whole — the extraction then reads a recycled page frame and dies
    // (or, worse, would have silently dropped the row). Tiny chunks plus a 64-node flush bound make
    // that straddle happen constantly.
    final byte[] corpus = corpus(400);
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(64, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, 400);
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus), 200, 1);
          wtx.commit();
        }
        // The build's own end-of-load check compares rows emitted against the record-set array's
        // child count and refuses loudly when it comes up short, so reaching a live (non-stale)
        // metadata at all is the witness that every record was read whole.
        final ProjectionStorageSnapshot onePass = snapshot(session);
        assertFalse(onePass.stale(), "every record must have been extracted before its epoch flushed");
      }
    }
  }

  // ==== arms ===================================================================================

  /** Arm (i): one transaction, projection armed before the data, records attributed by the load. */
  private static ProjectionStorageSnapshot loadOnePassParallel(final java.nio.file.Path databasePath,
      final byte[] corpus) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(databasePath)) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, RECORDS);
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus), CHUNK_BUDGET_BYTES, BUILDERS);
          wtx.commit();
        }
        return snapshot(session);
      }
    }
  }

  /** Arm (ii): the same parallel import with nothing armed, then the ordinary post-pass build. */
  private static ProjectionStorageSnapshot loadParallelThenPostPass(final java.nio.file.Path databasePath,
      final byte[] corpus) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(databasePath)) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus), CHUNK_BUDGET_BYTES, BUILDERS);
          wtx.commit();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        return snapshot(session);
      }
    }
  }

  private static ResourceConfiguration resourceConfig() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                .useDeweyIDs(false)
                                .hashKind(HashType.NONE)
                                .storeNodeHistory(false)
                                .buildPathSummary(true)
                                .build();
  }

  private static IndexDef projectionDef() {
    final List<Path<QNm>> fieldPaths = new ArrayList<>(FIELD_PATHS.size());
    for (final String fieldPath : FIELD_PATHS) {
      fieldPaths.add(Path.parse(fieldPath, PathParser.Type.JSON));
    }
    return IndexDefs.createProjectionIdxDef(Path.parse(ROOT_PATH, PathParser.Type.JSON), fieldPaths, FIELD_TYPES,
        INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  // ==== corpus =================================================================================

  /**
   * Adversarial by construction: records of varying width so chunk boundaries fall mid-page, a field
   * that first occurs only in a LATE chunk, absent fields, nulls, a set column with a non-string
   * element in some records, integral and non-integral numbers, and a nested object.
   */
  private static byte[] corpus(final int records) {
    return buildCorpus(records, -1);
  }

  private static byte[] corpusWithMutatedRecord(final int records) {
    return buildCorpus(records, records / 2);
  }

  private static byte[] buildCorpus(final int records, final int mutatedRecord) {
    final StringBuilder json = new StringBuilder(records * 160);
    json.append('[');
    for (int record = 0; record < records; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"n").append(record).append('-').append("x".repeat(record % 17)).append('"');
      json.append(",\"dept\":\"d").append(record % 7).append('"');
      json.append(",\"score\":")
          .append(record == mutatedRecord
              ? 999999
              : record * 3L);
      if (record % 5 != 0) {
        json.append(",\"ratio\":").append(record).append('.').append(record % 100);
      }
      json.append(",\"active\":").append((record & 1) == 0);
      if (record % 3 == 0) {
        json.append(",\"tags\":[\"t").append(record % 11).append("\",\"t").append(record % 13).append("\"]");
      } else if (record % 3 == 1) {
        // A non-string element makes the whole set cell unrepresentable — a poisoning path the
        // extractor owns and any worker-side reimplementation would have to reproduce.
        json.append(",\"tags\":[\"t").append(record % 11).append("\",").append(record).append(']');
      }
      if (record % 4 == 0) {
        json.append(",\"nested\":{\"inner\":").append(record * 2L).append('}');
      } else if (record % 4 == 1) {
        json.append(",\"nested\":{\"inner\":null}");
      }
      // First occurrence deliberately late: the field acquires a path class only after many chunks
      // have already been resolved, reserved, built and adopted.
      if (record >= records - records / 4) {
        json.append(",\"latecomer\":\"L").append(record % 23).append('"');
      }
      json.append('}');
    }
    json.append(']');
    return json.toString().getBytes(StandardCharsets.UTF_8);
  }

  // ==== the slot-for-slot comparator ===========================================================

  /**
   * Every persisted byte of one projection sub-tree, keyed by a human-readable slot name so a failure
   * names the family that diverged.
   */
  private record ProjectionStorageSnapshot(String rootPath, String[] fieldPaths, String[] fieldNames,
      byte[] columnKinds, int rowGroupCount, boolean stale, long[] valueDictionaryHeaderKeys,
      Map<Integer, Map<String, Long>> setValueRowCounts, Map<String, byte[]> slots) {
  }

  /**
   * Every persisted slot of the projection sub-tree, read through a writer-backed storage handle —
   * the reader-side raw-slot API only serves the negative sparse-locator namespace, and the
   * structural-order directory lives in a positive one. The transaction is opened on the committed
   * revision and closed without committing, so it observes rather than changes anything.
   */
  private static ProjectionStorageSnapshot snapshot(final JsonResourceSession session) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final StorageEngineWriter writer = wtx.getStorageEngineWriter();
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer, INDEX_NUMBER);
      final byte[] rawMetadata = storage.getBlob(0L);
      assertNotNull(rawMetadata, "the projection must have metadata at slot 0");
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(rawMetadata);
      final int rowGroups = metadata.rowGroupCount();
      final int columns = metadata.columnKinds().length;
      final Map<String, byte[]> slots = new LinkedHashMap<>();

      // Row groups: the canonical assembled leaf. Its bytes ARE the concatenation of the row
      // group's descriptor and column-segment slots, so comparing it compares those slots.
      for (int rowGroup = 1; rowGroup <= rowGroups; rowGroup++) {
        put(slots, "leaf[" + rowGroup + "]",
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(writer, INDEX_NUMBER, rowGroup));
        put(slots, "rowGroupDescriptor[" + rowGroup + "]",
            blob(storage, ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(rowGroup)));
      }

      // Fences, including the physical-order header the order-exception lane reads.
      put(slots, "fenceOrderHeader", blob(storage, ProjectionIndexFences.ORDER_HEADER_SLOT));
      for (int chunk = 0; chunk < chunkSweep(rowGroups); chunk++) {
        put(slots, "fenceChunk[" + chunk + "]", blob(storage, ProjectionIndexFences.CHUNK_SLOT_BASE + chunk));
      }

      // Bloom manifests + chunks, and set summaries, per column.
      for (int column = 0; column < columns; column++) {
        put(slots, "bloomManifest[" + column + "]", blob(storage, ProjectionIndexHOTStorage.bloomBlockSlotKey(column)));
        put(slots, "setSummary[" + column + "]", blob(storage, ProjectionSetSummaryChunks.slotKey(column)));
        for (int chunk = 0; chunk < chunkSweep(rowGroups); chunk++) {
          put(slots, "bloomChunk[" + column + "][" + chunk + "]",
              blob(storage, ProjectionBloomChunks.chunkSlotKey(column, chunk)));
        }
      }

      // Value dictionaries: the header blobs the metadata points at, plus a window above each in
      // case a dictionary spilled into continuation slots.
      final long[] dictionaryHeaderKeys = metadata.valueDictionaryHeaderKeys();
      if (dictionaryHeaderKeys != null) {
        for (int column = 0; column < dictionaryHeaderKeys.length; column++) {
          final long headerKey = dictionaryHeaderKeys[column];
          if (headerKey <= 0) {
            continue;
          }
          for (int offset = 0; offset < 64; offset++) {
            put(slots, "dictionary[" + column + "][+" + offset + "]", blob(storage, headerKey + offset));
          }
        }
      }

      // Record locator + structural-order directory, per record key. Both are keyed by DOCUMENT
      // node key, and the two arms load the identical corpus with the identical importer, so the
      // node keys are the same on both sides by construction.
      for (final long recordKey : recordKeys(wtx)) {
        final int locator = ProjectionRecordLocator.read(writer, INDEX_NUMBER, recordKey);
        if (locator != 0) {
          slots.put("recordLocator[" + recordKey + "]",
              new byte[] {(byte) locator, (byte) (locator >>> 8), (byte) (locator >>> 16), (byte) (locator >>> 24)});
        }
        put(slots, "structuralOrder[" + recordKey + "]",
            structuralOrderSlot(storage, ProjectionStructuralOrderDirectory.BASE + recordKey));
      }

      return new ProjectionStorageSnapshot(metadata.rootPath(), metadata.fieldPaths(), metadata.fieldNames(),
          metadata.columnKinds(), rowGroups, metadata.isStale(), dictionaryHeaderKeys, metadata.setValueRowCounts(),
          slots);
    }
  }

  /** Fence and Bloom chunks each cover a fixed window of leaves; sweep generously past the last. */
  private static int chunkSweep(final int rowGroupCount) {
    return Math.max(8, rowGroupCount / 8 + 8);
  }

  /**
   * A blob slot's bytes, or {@code null} when the slot holds nothing OR holds something that is not a
   * blob. The sweep deliberately probes past the end of every family, and a slot key one past a
   * family's last entry can land on an unrelated raw slot; that is "absent" for this comparison, and
   * both arms are probed identically, so a real divergence still shows.
   */
  private static byte[] blob(final ProjectionIndexHOTStorage storage, final long slotKey) {
    try {
      return storage.getBlob(slotKey);
    } catch (final RuntimeException notABlob) {
      return null;
    }
  }

  /** One structural-order directory slot (a raw, positive-namespace slot), or {@code null}. */
  private static byte[] structuralOrderSlot(final ProjectionIndexHOTStorage storage, final long slotKey) {
    try {
      return storage.getStructuralOrderSlot(slotKey);
    } catch (final RuntimeException absent) {
      return null;
    }
  }

  private static void put(final Map<String, byte[]> slots, final String name, final byte[] value) {
    if (value != null) {
      slots.put(name, value);
    }
  }

  private static long[] recordKeys(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    assertTrue(rtx.moveToFirstChild(), "the resource must expose its top-level array");
    final long arrayKey = rtx.getNodeKey();
    if (!rtx.moveToFirstChild()) {
      return new long[] {arrayKey};
    }
    final java.util.ArrayList<Long> keys = new java.util.ArrayList<>();
    keys.add(arrayKey);
    do {
      keys.add(rtx.getNodeKey());
    } while (rtx.moveToRightSibling());
    final long[] out = new long[keys.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = keys.get(i);
    }
    return out;
  }

  private static void assertEquivalent(final ProjectionStorageSnapshot onePass,
      final ProjectionStorageSnapshot postPass) {
    assertEquals(postPass.rootPath(), onePass.rootPath(), "root path");
    assertArrayEquals(postPass.fieldPaths(), onePass.fieldPaths(), "field paths");
    assertArrayEquals(postPass.fieldNames(), onePass.fieldNames(), "field names");
    assertArrayEquals(postPass.columnKinds(), onePass.columnKinds(),
        "column kinds (per-column dictionary decisions included)");
    assertEquals(postPass.rowGroupCount(), onePass.rowGroupCount(), "row-group count");
    assertEquals(postPass.stale(), onePass.stale(), "stale flag");
    assertEquals(postPass.setValueRowCounts(), onePass.setValueRowCounts(), "set-value row counts");
    assertArrayEquals(postPass.valueDictionaryHeaderKeys(), onePass.valueDictionaryHeaderKeys(),
        "value-dictionary header keys");

    final Set<String> onlyInOnePass = new java.util.TreeSet<>(onePass.slots().keySet());
    onlyInOnePass.removeAll(postPass.slots().keySet());
    final Set<String> onlyInPostPass = new java.util.TreeSet<>(postPass.slots().keySet());
    onlyInPostPass.removeAll(onePass.slots().keySet());
    assertTrue(onlyInOnePass.isEmpty(), "slots present only in the one-pass build: " + onlyInOnePass);
    assertTrue(onlyInPostPass.isEmpty(), "slots present only in the post-pass build: " + onlyInPostPass);

    for (final Map.Entry<String, byte[]> entry : postPass.slots().entrySet()) {
      final byte[] expected = entry.getValue();
      final byte[] actual = onePass.slots().get(entry.getKey());
      if (!Arrays.equals(expected, actual)) {
        throw new AssertionError("projection slot " + entry.getKey() + " differs: post-pass " + expected.length
            + " bytes, one-pass " + (actual == null
                ? "absent"
                : actual.length + " bytes"));
      }
    }
    // Non-vacuity: name what was actually compared, and refuse to pass on a sweep that found only
    // a couple of families. A differential that silently compared two empty maps proves nothing.
    final Map<String, Integer> census = new java.util.TreeMap<>();
    for (final String name : onePass.slots().keySet()) {
      final int bracket = name.indexOf('[');
      census.merge(bracket < 0
          ? name
          : name.substring(0, bracket), 1, Integer::sum);
    }
    System.out.println("PROJECTION DIFFERENTIAL: " + onePass.slots().size() + " slots compared byte-for-byte across "
        + census.size() + " families " + census + "; rowGroups=" + onePass.rowGroupCount() + ", columnKinds="
        + Arrays.toString(onePass.columnKinds()));
    for (final String required : List.of("leaf", "rowGroupDescriptor", "structuralOrder")) {
      assertTrue(census.containsKey(required), "the sweep found no " + required + " slots: " + census);
    }
    assertTrue(census.containsKey("fenceChunk") || onePass.slots().containsKey("fenceOrderHeader"),
        "the sweep found no fence slots: " + census);
  }

  @SuppressWarnings("unused")
  private static long sizeOf(final java.nio.file.Path path) throws IOException {
    return Files.size(path);
  }
}
