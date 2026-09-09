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
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.settings.VersioningType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;
import io.sirix.node.NodeKind;
import io.sirix.node.SegmentDictionaryDirectoryNode;
import io.sirix.node.SegmentDictionaryDirectoryNode.SlotTable;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.NamePage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The segment dictionary lane through the real load: a parallel bulk import with a projection index
 * over one string field, the lane armed, no pre-pass and no closed corpus. The document pages mint
 * their string values into the open segment as they encode; at the end of the load every segment is
 * sealed into a rank-ordered dictionary under a rank table, the anchors land in the index metadata,
 * the directory lands at key 1 — and a reader, which knows none of this, gets every string value
 * back byte for byte through the ids the pages carry.
 *
 * <p>
 * The values arrive in DESCENDING order with repeats across pages, so the mints are not the
 * collation ranks and the table is exercised on every decode; the field's distinct count is the
 * sealed entry count, which proves every page converted through the lane rather than keeping bytes.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class SegmentDictionaryLaneEndToEndTest {

  private static final JsonTestHelper.PATHS DATABASE = JsonTestHelper.PATHS.PATH1;

  private static final int INDEX_NUMBER = 0;

  /** Several 256-value blocks, more than one separator range, well inside one interner generation. */
  private static final int DISTINCT_CODES = 2_000;

  /** Each code appears on this many consecutive records, so repeats hit the segment's table. */
  private static final int CODE_RUN = 3;

  private static final int RECORDS = DISTINCT_CODES * CODE_RUN;

  private String savedLane;

  private String savedGlobalDict;

  private boolean savedChunked;

  private int savedChunkTarget;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
    savedLane = System.getProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    savedGlobalDict = System.getProperty("sirix.projection.globalDict");
    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    // The projection's own column dictionary is not under test; keep it out of the resource so the
    // value-dictionary sub-trie holds nothing but what the lane writes.
    System.setProperty("sirix.projection.globalDict", "never");
    // A converted page is readable only on the lazy route, and the lazy route needs a chunk-framed
    // body; the lane refuses to arm without one. A small target so a page of this size still yields
    // several chunks, which is what makes the point reads below expand a fraction of it.
    savedChunked = ChunkedBodyConfig.setEnabledForTesting(true);
    savedChunkTarget = ChunkedBodyConfig.setTargetChunkBytesForTesting(4096);
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setTargetChunkBytesForTesting(savedChunkTarget);
    ChunkedBodyConfig.setEnabledForTesting(savedChunked);
    restore(SegmentDictionaryLane.ENABLED_PROPERTY, savedLane);
    restore("sirix.projection.globalDict", savedGlobalDict);
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private static void restore(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  @Test
  @DisplayName("a parallel load with the lane armed seals its segment, anchors it, files it, and reads back")
  void aLoadSealsAnchorsAndReadsBack() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE.getFile()));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE.getFile())) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(2048, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          // Read something BEFORE the lane arms. A load does this constantly — the importer reads
          // pages back as it stitches — and the reader's "may these pages carry dictionary ids"
          // answer is NO at this moment and YES a line later, because arming the lane is what
          // creates the dictionary sub-trie. A reader that remembered the no would expand every
          // converted page eagerly for the rest of the load and refuse the first one it met.
          assertTrue(wtx.moveToDocumentRoot());
          final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, RECORDS);
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus()), 1 << 20, 4);
          wtx.commit();
        }

        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader reader = rtx.getStorageEngineReader();

          // The anchors the reader resolves through.
          final byte[] blob = ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L);
          assertNotNull(blob, "slot 0 holds the index metadata");
          final SegmentAnchor[] anchors = ProjectionIndexMetadata.parse(blob).segmentAnchors();
          assertNotNull(anchors, "the lane anchored its dictionaries");
          assertEquals(1, anchors.length, "one segment, one string column");
          final SegmentAnchor anchor = anchors[0];
          assertEquals(0L, anchor.segment());
          assertEquals(0, anchor.column());
          assertEquals(DISTINCT_CODES, anchor.sealedEntryCount(), "every page minted through the lane");
          assertTrue(anchor.headerKey() > SegmentDictionaryDirectoryNode.RESERVED_KEYS,
              "the dictionary lies beyond the directory's reserved range");

          // The dictionary itself: rank-ordered storage under a table, one generation.
          final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(anchor.headerKey(), reader);
          assertNotNull(header);
          assertEquals(DISTINCT_CODES, header.getEntryCount());
          assertTrue(header.isFullyOrdered(), "storage is in collation order");
          assertTrue(header.hasRankTable(), "descending arrival: the mints are not the ranks");
          assertFalse(header.idsAreCollationOrdered());
          assertEquals(0L, header.getForwardRootKey(), "no forward radix in a segment dictionary");
          assertEquals(0, header.getGeneration(), "2,000 values fit one interner generation");

          // The directory at key 1, naming the same dictionary under the field's path class.
          final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
          final DataRecord atKeyOne = namePage.getProjectionValueDictionaryRecord(
              SegmentDictionaryDirectoryNode.DIRECTORY_KEY, DatabaseType.JSON, reader);
          final SegmentDictionaryDirectoryNode directory =
              assertInstanceOf(SegmentDictionaryDirectoryNode.class, atKeyOne, "the directory lives at key 1");
          assertArrayEquals(new long[] {0L}, directory.segmentStarts());
          final SlotTable slots = directory.slots(0);
          assertEquals(1, slots.slotCount());
          assertEquals(1, slots.tags(0).length, "one path class feeds the column");
          assertEquals(anchor.headerKey(), slots.headerKey(0));
          assertEquals(DISTINCT_CODES, slots.entryCount(0));

          // And the document: every string value, in order, through the ids the pages carry.
          assertTrue(rtx.moveToDocumentRoot());
          assertTrue(rtx.moveToFirstChild(), "the record-set array");
          assertTrue(rtx.moveToFirstChild(), "the first record");
          for (int record = 0; record < RECORDS; record++) {
            assertTrue(rtx.moveToFirstChild(), "the record's only key, record " + record);
            // The importer fuses a key with its string value into one node; a resource that did not
            // fuse would hold the value in the key's child. Either way the value is what was loaded.
            final boolean fused = rtx.getKind() == NodeKind.OBJECT_NAMED_STRING;
            if (!fused) {
              assertTrue(rtx.moveToFirstChild(), "the key's string value, record " + record);
            }
            assertEquals(codeOf(record), rtx.getValue(), "record " + record);
            if (!fused) {
              assertTrue(rtx.moveToParent());
            }
            assertTrue(rtx.moveToParent());
            if (record + 1 < RECORDS) {
              assertTrue(rtx.moveToRightSibling(), "record " + (record + 1) + " follows");
            }
          }
          assertFalse(rtx.moveToRightSibling(), "no record beyond the last");
        }

        // And again from a WRITE transaction, off a COLD cache. A read-only point lookup takes the
        // lazy route on its own (no intent log, point lookup), so it never asks whether this
        // resource's pages may carry dictionary ids; a writer's reads do ask. The cache clear is what
        // makes the question reachable at all — a page the previous pass already expanded is served
        // from memory and never deserialized again — and a wrong answer then expands a converted page
        // eagerly, where no dictionary is reachable and the page is refused outright.
        Databases.getGlobalBufferManager().clearAllCaches();
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveToDocumentRoot());
          assertTrue(wtx.moveToFirstChild(), "the record-set array");
          assertTrue(wtx.moveToFirstChild(), "the first record");
          for (int record = 0; record < RECORDS; record++) {
            assertTrue(wtx.moveToFirstChild(), "the record's only key, record " + record);
            final boolean fused = wtx.getKind() == NodeKind.OBJECT_NAMED_STRING;
            if (!fused) {
              assertTrue(wtx.moveToFirstChild(), "the key's string value, record " + record);
            }
            assertEquals(codeOf(record), wtx.getValue(), "record " + record + " read through a writer");
            if (!fused) {
              assertTrue(wtx.moveToParent());
            }
            assertTrue(wtx.moveToParent());
            if (record + 1 < RECORDS) {
              assertTrue(wtx.moveToRightSibling(), "record " + (record + 1) + " follows");
            }
          }
        }
      }
    }
  }

  private static ResourceConfiguration resourceConfig() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                .useDeweyIDs(false)
                                .hashKind(HashType.NONE)
                                .storeNodeHistory(false)
                                .buildPathSummary(true)
                                // A page whose values are dictionary ids can only be expanded where a
                                // reader is reachable, which is the LAZY route; the fragment-combine
                                // route reads every slot of every fragment eagerly and refuses such a
                                // page outright (PageKind#refuseGlobalTagsOnEagerPath). FULL keeps one
                                // complete page per revision, so there is no combine — the same
                                // configuration the ClickBench arms run, and the same constraint the
                                // trie lane has always had.
                                .versioningApproach(VersioningType.FULL)
                                .build();
  }

  private static IndexDef projectionDef() {
    final List<Path<QNm>> fieldPaths = List.of(Path.parse("/[]/code", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths, List.of(Type.STR),
        INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  /** Codes DESCEND with the record number: the first page mints the highest-sorting values. */
  private static String codeOf(final int record) {
    return "code-" + (DISTINCT_CODES - 1 - record / CODE_RUN);
  }

  private static byte[] corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 24);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"code\":\"").append(codeOf(record)).append("\"}");
    }
    json.append(']');
    return json.toString().getBytes(StandardCharsets.UTF_8);
  }
}
