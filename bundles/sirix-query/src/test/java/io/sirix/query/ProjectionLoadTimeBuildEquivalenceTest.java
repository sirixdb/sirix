package io.sirix.query;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ProjectionSpec;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * A projection built BY the load must be the same index as one derived from the finished resource.
 *
 * <p>
 * The two routes share the extraction engine and the leaf machinery but reach them differently: the
 * post-pass build walks a complete resource in one transaction, while the load-time build is fed
 * one record at a time by change notifications, across as many auto-commits as the shred fires,
 * against a path summary that is still growing. Any of those differences could quietly change what
 * lands on disk — a stale field-path resolution alone would turn present columns into absent ones —
 * so this test compares the two stores BYTE FOR BYTE rather than only checking that both answer the
 * same questions.
 *
 * <p>
 * The corpus deliberately crosses several row-group boundaries and fires several auto-commits, and
 * one field appears only in later records: that is the case a build whose field paths were resolved
 * once, at the start, gets wrong.
 */
public final class ProjectionLoadTimeBuildEquivalenceTest {

  /** Enough records for several 1024-row leaves. */
  private static final int RECORDS = 5000;

  /** Small enough that the shred auto-commits many times over the corpus. */
  private static final int AUTO_COMMIT_NODES = 4096;

  /** The record index at which {@code latecomer} starts appearing. */
  private static final int LATECOMER_FROM = 3000;

  /** Distinct values of the unprojected {@code note} field. */
  private static final int NOTE_BUCKETS = 13;

  private static final int INDEX_NUMBER = 0;

  private static final String GLOBAL_DICTIONARY_PROPERTY = "sirix.projection.globalDict";

  private static final String ROOT_PATH = "/[]";

  private static final List<String> FIELD_PATHS =
      List.of("/[]/age", "/[]/active", "/[]/dept", "/[]/name", "/[]/latecomer");

  private static final List<String> FIELD_TYPES = List.of("long", "boolean", "string", "string", "string");

  private static final String[] SOURCE_PATH = {"[]"};

  private Path root;

  @BeforeEach
  public void setUp() throws IOException {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    ProjectionBulkLoad.clearActive();
    root = Files.createTempDirectory("projection-load-equivalence");
  }

  @AfterEach
  public void tearDown() throws IOException {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    ProjectionBulkLoad.clearActive();
    deleteRecursively(root);
  }

  private static void deleteRecursively(final Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(dir)) {
      for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  private static long ageOf(final int i) {
    return (i % 50) + 1;
  }

  /**
   * Records whose {@code dept} repeats heavily (so the dictionary decision has something to measure)
   * and whose {@code name} is near-unique (so the other branch of that decision is exercised too).
   */
  private static String dataset() {
    final StringBuilder sb = new StringBuilder(RECORDS * 72).append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"age\":")
        .append(ageOf(i))
        .append(",\"active\":")
        .append((i & 1) == 0)
        .append(",\"dept\":\"d")
        .append(i % 7)
        .append("\",\"name\":\"employee-")
        .append(i)
        .append('"');
      if (i >= LATECOMER_FROM) {
        sb.append(",\"latecomer\":\"late-").append(i % 3).append('"');
      }
      // NOT projected. The index cannot vouch for this field, so it is what proves the SHRED itself
      // came through the interleaved drains intact rather than only the columns the index copied.
      sb.append(",\"note\":\"n").append(i % NOTE_BUCKETS).append('"');
      sb.append('}');
    }
    return sb.append(']').toString();
  }

  private BasicJsonDBStore openStore(final String dbName) {
    return openStore(dbName, VersioningType.FULL);
  }

  private BasicJsonDBStore openStore(final String dbName, final VersioningType versioningType) {
    return BasicJsonDBStore.newBuilder()
                           .location(root.resolve(dbName))
                           .numberOfNodesBeforeAutoCommit(AUTO_COMMIT_NODES)
                           .buildPathSummary(true)
                           .buildPathStatistics(false)
                           .hashType(HashType.NONE)
                           .storeNodeHistory(false)
                           .versioningType(versioningType)
                           .build();
  }

  /** Load with the projection maintained BY the shred — one pass. */
  private void loadIncremental(final String dbName) throws IOException {
    try (final BasicJsonDBStore store = openStore(dbName);
        final JsonReader reader = new JsonReader(new StringReader(dataset()))) {
      store.create("coll", "res.jn", reader, new ProjectionSpec(ROOT_PATH, FIELD_PATHS, FIELD_TYPES));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  public void parallelBulkLoadPublishesTheOnePassProjectionForEveryVersioningType(final VersioningType versioningType)
      throws IOException {
    final String dbName = "parallel-" + versioningType.name().toLowerCase();
    try (BasicJsonDBStore store = openStore(dbName, versioningType); StringReader input = new StringReader(dataset())) {
      store.createParallel("coll", "res.jn", input, new ProjectionSpec(ROOT_PATH, FIELD_PATHS, FIELD_TYPES));
    }

    final Snapshot snapshot = snapshot(dbName);
    Assertions.assertEquals((RECORDS + 1023) / 1024, snapshot.rowGroupCount());
    Assertions.assertEquals(String.valueOf(RECORDS), queryOne(dbName, "count($d[])"));
  }

  /** Load, then derive the projection by walking the finished resource — two passes. */
  private void loadPostPass(final String dbName) throws IOException {
    try (final BasicJsonDBStore store = openStore(dbName);
        final JsonReader reader = new JsonReader(new StringReader(dataset()))) {
      store.create("coll", "res.jn", reader);
    }
    try (final BasicJsonDBStore store = openStore(dbName);
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final StringBuilder paths = new StringBuilder();
      final StringBuilder types = new StringBuilder();
      for (int i = 0; i < FIELD_PATHS.size(); i++) {
        if (i > 0) {
          paths.append(", ");
          types.append(", ");
        }
        paths.append('\'').append(FIELD_PATHS.get(i)).append('\'');
        types.append('\'').append(FIELD_TYPES.get(i)).append('\'');
      }
      new Query(chain,
          "let $doc := jn:doc('coll','res.jn')\n" + "let $stats := jn:create-projection-index($doc, '" + ROOT_PATH
              + "',\n    (" + paths + "),\n    (" + types + "))\nreturn {\"revision\": sdb:commit($doc)}").evaluate(
                  ctx);
    }
  }

  /** Everything the persisted store says about itself, for a two-route comparison. */
  private record Snapshot(String rootPath, String[] fieldNames, byte[] columnKinds, int rowGroupCount,
      List<byte[]> rowGroups) {
  }

  private static void assertEquivalent(final Snapshot incremental, final Snapshot postPass) {
    Assertions.assertEquals(postPass.rootPath(), incremental.rootPath());
    Assertions.assertArrayEquals(postPass.fieldNames(), incremental.fieldNames());
    Assertions.assertArrayEquals(postPass.columnKinds(), incremental.columnKinds(),
        "the two builds must reach the same per-column dictionary decision");
    Assertions.assertEquals(postPass.rowGroupCount(), incremental.rowGroupCount(),
        "the two builds must pack the records into the same number of row groups");
    for (int i = 0; i < postPass.rowGroupCount(); i++) {
      Assertions.assertArrayEquals(postPass.rowGroups().get(i), incremental.rowGroups().get(i),
          "row group " + (i + 1) + " differs between the load-time and post-pass builds");
    }
  }

  private Snapshot snapshot(final String dbName) {
    final Path dbPath = root.resolve(dbName).resolve("coll");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("res.jn");
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata meta = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0L));
      Assertions.assertNotNull(meta, "projection metadata must be present for " + dbName);
      Assertions.assertFalse(meta.isStale(), "projection of " + dbName + " must not be tombstoned");
      final List<byte[]> rowGroups = new ArrayList<>(meta.rowGroupCount());
      for (int slot = 1; slot <= meta.rowGroupCount(); slot++) {
        final byte[] raw = ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(),
            INDEX_NUMBER, slot);
        Assertions.assertNotNull(raw, "row group " + slot + " of " + dbName + " must be readable");
        rowGroups.add(raw);
      }
      return new Snapshot(meta.rootPath(), meta.fieldNames(), meta.columnKinds(), meta.rowGroupCount(), rowGroups);
    }
  }

  private static long aggregate(final String dbNameRoot, final Path root, final String function, final String column) {
    final Path dbPath = root.resolve(dbNameRoot).resolve("coll");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("res.jn")) {
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        final Sequence result = executor.executeAggregate(null, SOURCE_PATH, function, column);
        Assertions.assertNotNull(result, function + '(' + column + ") must be SERVED from the projection");
        return ((Int64) result).longValue();
      } finally {
        executor.close();
      }
    }
  }

  @Test
  public void loadTimeBuildProducesTheSameIndexAsThePostPassBuild() throws IOException {
    loadIncremental("incremental");
    loadPostPass("postpass");

    final Snapshot incremental = snapshot("incremental");
    final Snapshot postPass = snapshot("postpass");

    assertEquivalent(incremental, postPass);
  }

  @Test
  public void neverModeStreamsAndPersistsSeveralLeavesByteForByte() throws IOException {
    final String previousMode = System.getProperty(GLOBAL_DICTIONARY_PROPERTY);
    System.setProperty(GLOBAL_DICTIONARY_PROPERTY, "never");
    try {
      loadIncremental("incremental-never");
      loadPostPass("postpass-never");

      final Snapshot incremental = snapshot("incremental-never");
      final Snapshot postPass = snapshot("postpass-never");
      assertEquivalent(incremental, postPass);
      Assertions.assertEquals(
          (RECORDS + ProjectionIndexRowGroupPage.MAX_ROWS - 1) / ProjectionIndexRowGroupPage.MAX_ROWS,
          incremental.rowGroupCount(), "NEVER mode must persist every row across several immediately emitted leaves");
      Assertions.assertEquals(0, ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
          "a successfully completed NEVER build must publish a zero global-column diagnostic");
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      Databases.clearGlobalCaches();
      Assertions.assertEquals(RECORDS, aggregate("incremental-never", root, "count", "age"),
          "the cold-open projection must serve every immediately streamed row");
    } finally {
      if (previousMode == null) {
        System.clearProperty(GLOBAL_DICTIONARY_PROPERTY);
      } else {
        System.setProperty(GLOBAL_DICTIONARY_PROPERTY, previousMode);
      }
    }
  }

  @Test
  public void loadTimeBuiltProjectionServesTheSameAnswers() throws IOException {
    loadIncremental("incremental");
    loadPostPass("postpass");

    long expectedSum = 0;
    for (int i = 0; i < RECORDS; i++) {
      expectedSum += ageOf(i);
    }
    Assertions.assertEquals(expectedSum, aggregate("incremental", root, "sum", "age"));
    Assertions.assertEquals(expectedSum, aggregate("postpass", root, "sum", "age"));
    Assertions.assertEquals(RECORDS, aggregate("incremental", root, "count", "age"));
    Assertions.assertEquals(RECORDS, aggregate("postpass", root, "count", "age"));

    // The field that only appears from record 3000 on. A build whose field paths were resolved once,
    // before any record existed, records it as absent on EVERY row and this count comes out zero.
    Assertions.assertEquals(RECORDS - LATECOMER_FROM, aggregate("incremental", root, "count", "latecomer"),
        "a field first seen mid-load must be projected for the records that have it");
    Assertions.assertEquals(aggregate("postpass", root, "count", "latecomer"),
        aggregate("incremental", root, "count", "latecomer"));
  }

  @Test
  public void objectRootAllowsDescendantColumnsAtDifferentDepths() throws IOException {
    final String json = """
        {"root":{"bla":{"blubb":{"b":{"a":{"c":17}},"c":{"d":23}}}}}
        """;
    try (final BasicJsonDBStore store = openStore("nested-columns");
        final JsonReader reader = new JsonReader(new StringReader(json))) {
      store.create("coll", "res.jn", reader, new ProjectionSpec("/root/bla/blubb",
          List.of("/root/bla/blubb/b/a/c", "/root/bla/blubb/c/d"), List.of("long", "long")));
    }

    final Snapshot snapshot = snapshot("nested-columns");
    Assertions.assertEquals(1, snapshot.rowGroupCount(), "the exact object root must produce one row group");
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(snapshot.rowGroups().get(0));
    Assertions.assertEquals(1, leaf.getRowCount(), "the exact object root denotes one projected row");
    Assertions.assertTrue((leaf.presenceColumnBits(0)[0] & 1L) != 0, "the deeper b/a/c descendant must be present");
    Assertions.assertTrue((leaf.presenceColumnBits(1)[0] & 1L) != 0, "the shallower c/d descendant must be present");
    Assertions.assertEquals(17L, leaf.numericColumn(0)[0]);
    Assertions.assertEquals(23L, leaf.numericColumn(1)[0]);
  }

  @Test
  public void aSirixLoadFailureRetainsItsIdentityAndSuppressedCleanup() throws IOException {
    final SirixException injected = new SirixException("injected loader failure");
    final RuntimeException suppressedCleanup = new RuntimeException("injected cleanup failure");
    injected.addSuppressed(suppressedCleanup);

    // create() arms ProjectionBulkLoad before asking the reader for its first token. Throwing here is
    // therefore a load-boundary SirixException, not a setup failure handled by the outer store adapter.
    try (final BasicJsonDBStore store = openStore("sirix-failure");
        final JsonReader reader = new JsonReader(new StringReader("[]")) {
          @Override
          public JsonToken peek() {
            throw injected;
          }
        }) {
      final DocumentException translated = Assertions.assertThrows(DocumentException.class,
          () -> store.create("coll", "res.jn", reader, new ProjectionSpec(ROOT_PATH, FIELD_PATHS, FIELD_TYPES)));
      Assertions.assertSame(injected, translated.getCause(),
          "DocumentException must retain the exact load failure, not only its nested cause");
      Assertions.assertEquals(1, translated.getCause().getSuppressed().length);
      Assertions.assertSame(suppressedCleanup, translated.getCause().getSuppressed()[0],
          "cleanup suppressed on the primary failure was lost during store exception translation");
    }
    Assertions.assertTrue(ProjectionBulkLoad.activeKeys().isEmpty(),
        "the translated load failure must still retire its exact bulk-load owner");
  }

  @Test
  public void aFailedLoadRetiresItsBulkLoadAndTheSameResourceCanBeRetried() throws IOException {
    // The root array and first record are deliberately left open. ProjectionBulkLoad is armed before
    // the reader is consumed, so this parser failure happens after the process-global ACTIVE entry was
    // published and exercises the real create() failure boundary rather than simulating interruption.
    final String malformed = "[{\"age\":1,\"active\":true,\"dept\":\"d0\",\"name\":\"broken\"";
    try (final BasicJsonDBStore store = openStore("retry");
        final JsonReader reader = new JsonReader(new StringReader(malformed))) {
      Assertions.assertThrows(RuntimeException.class,
          () -> store.create("coll", "res.jn", reader, new ProjectionSpec(ROOT_PATH, FIELD_PATHS, FIELD_TYPES)));
    }
    Assertions.assertTrue(ProjectionBulkLoad.activeKeys().isEmpty(),
        "a failed parser must retire the exact bulk load it armed");

    // Recreate the exact same database/resource key. A leaked ACTIVE entry fails this call at
    // createProjectionIndexesAtLoadStart with "already active", even though the old write transaction
    // was rolled back and the old database was removed.
    final String retryDataset =
        "[{\"age\":7,\"active\":true,\"dept\":\"d0\",\"name\":\"retry\",\"latecomer\":\"yes\"}]";
    try (final BasicJsonDBStore store = openStore("retry");
        final JsonReader reader = new JsonReader(new StringReader(retryDataset))) {
      store.create("coll", "res.jn", reader, new ProjectionSpec(ROOT_PATH, FIELD_PATHS, FIELD_TYPES));
    }
    Assertions.assertTrue(ProjectionBulkLoad.activeKeys().isEmpty(),
        "the successful retry must finish and retire its replacement bulk load");

    final Path dbPath = root.resolve("retry").resolve("coll");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("res.jn");
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata meta = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0L));
      Assertions.assertNotNull(meta);
      Assertions.assertFalse(meta.isStale(), "the retry must replace its tombstone");
      Assertions.assertEquals(1, meta.rowGroupCount());
    }
  }

  /**
   * Evaluate {@code queryBody} over the named database's resource and return the single atomic
   * result.
   */
  private String queryOne(final String dbName, final String queryBody) {
    try (final BasicJsonDBStore store = openStore(dbName);
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final StringWriter out = new StringWriter();
      try (PrintWriter pw = new PrintWriter(out)) {
        new StringSerializer(pw).serialize(
            new Query(chain, "let $d := jn:doc('coll','res.jn')\nreturn " + queryBody).execute(ctx));
      }
      return out.toString().trim();
    }
  }

  /**
   * The shred itself must survive the drains that fire in the middle of it.
   *
   * <p>
   * Extraction moves the cursor, and the flush hook that triggers it fires from the node-count check
   * at the TOP of an insert — mid-record, with the insert about to read the cursor again. The drain
   * saves and restores the cursor for exactly this reason; if that restore were wrong, the shredder
   * would resume from the wrong node and the document would come out malformed.
   *
   * <p>
   * Checked through the ROW path on the {@code note} field, which the projection does not carry: the
   * index cannot answer for it, so a correct answer here can only come from an intact resource. The
   * byte-identical row groups in the sibling test already cover the projected columns — this covers
   * the rest of the document.
   */
  @Test
  public void theShredSurvivesDrainsFiredMidRecord() throws IOException {
    loadIncremental("incremental");

    Assertions.assertEquals(String.valueOf(RECORDS), queryOne("incremental", "count($d[])"),
        "the shred must hold every record");

    for (int bucket = 0; bucket < NOTE_BUCKETS; bucket++) {
      int expected = 0;
      for (int i = 0; i < RECORDS; i++) {
        if (i % NOTE_BUCKETS == bucket) {
          expected++;
        }
      }
      Assertions.assertEquals(String.valueOf(expected),
          queryOne("incremental", "count(for $r in $d[] where $r.note = 'n" + bucket + "' return 1)"),
          "unprojected field 'note' must be intact for bucket " + bucket
              + " — a disturbed shredder cursor would misplace fields");
    }

    // Field-to-record association, not just field presence: a cursor left on the wrong node would
    // attach a record's fields to its neighbour, which per-field counts alone would not notice.
    Assertions.assertEquals("1",
        queryOne("incremental",
            "count(for $r in $d[] where $r.name = 'employee-4999' and $r.note = 'n" + (4999 % NOTE_BUCKETS)
                + "' and $r.age = " + ageOf(4999) + " return 1)"),
        "the last record's projected and unprojected fields must belong to the same record");
  }

  @Test
  public void everyRecordIsProjected() throws IOException {
    loadIncremental("incremental");
    final Snapshot snapshot = snapshot("incremental");
    Assertions.assertEquals((RECORDS + 1023) / 1024, snapshot.rowGroupCount(),
        "every record must land in a row group — a missed record root would shorten the index silently");
    Assertions.assertEquals(FIELD_PATHS.size(), snapshot.columnKinds().length,
        "every declared field must have a column");
  }
}
