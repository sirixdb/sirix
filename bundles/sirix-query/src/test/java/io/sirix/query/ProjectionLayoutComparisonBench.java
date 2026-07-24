package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Head-to-head performance comparison of the two persisted projection-index layouts — the default
 * DESCRIPTOR layout (segments packed inline in the descriptor slot or in side-map pages) versus the
 * SEGMENT-SLOT layout (one bare HOT slot per column segment). The SAME dataset is built into two
 * fresh resources (the layout is a sticky per-store property), and each dimension is measured on
 * both: build time, projection storage bytes (isolated as the sirix.data size delta across the build
 * commit), whole-leaf decode CPU, descriptor-tier count, and end-to-end aggregate serving.
 *
 * <p>Not an assertion test — it prints a report to stdout. Run with:
 * {@code ./gradlew :sirix-query:test --tests io.sirix.query.ProjectionLayoutComparisonBench}.
 */
public final class ProjectionLayoutComparisonBench extends AbstractJsonTest {

  private static final int RECORDS = 40_000; // ~40 leaves at MAX_ROWS=1024
  private static final int WARMUP = 3;
  private static final int MEASURE = 9;

  // Configurable so a column-count sweep needs no recompile; the root build forwards every sirix.*
  // gradle-JVM property to the test JVM (build.gradle ~307). E.g. -Dsirix.bench.wideCols=64.
  private static final int WIDE_COLS = Integer.getInteger("sirix.bench.wideCols", 16);
  private static final int WIDE_RECORDS = Integer.getInteger("sirix.bench.wideRecords", 20_000); // ~20 leaves

  @BeforeEach
  public void clearBefore() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearAfter() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void compareLayouts() throws IOException {
    final String json = buildDataset();
    // jn:store recreates the collection, so each database holds ONE resource — use two databases
    // (same store location, different DB names) to keep both layouts' identical dataset side by side.
    query("jn:store('json-path1','data.jn','" + json + "')");
    query("jn:store('json-path2','data.jn','" + json + "')");

    final Result descriptor = measureLayout("json-path1", "data.jn", false);
    final Result segmentSlot = measureLayout("json-path2", "data.jn", true);

    report(descriptor, segmentSlot);
    // Sanity: both must have produced the layout we asked for and agree on the row count.
    Assertions.assertFalse(descriptor.segmentSlotLayout, "json-path1 must be descriptor layout");
    Assertions.assertTrue(segmentSlot.segmentSlotLayout, "json-path2 must be segment-slot layout");
    Assertions.assertEquals(descriptor.rowCount, segmentSlot.rowCount, "row counts must match");
  }

  /**
   * The point of the {@code (rowGroupId, columnSegmentId)} keying: a single-column aggregate over a WIDE
   * table should touch only that column's segment slots and skip the rest. This measures, per layout,
   * the whole-row read (all {@value #WIDE_COLS} columns) vs a column-pruned read of ONE column's BODY
   * segments across all row groups — proving the trie can decode just those segments.
   *
   * <p>Caveat this exposes: the segment-slot SERVING path does not prune yet (it falls through to
   * whole-leaf), so today it pays the "whole-row" cost for a single-column aggregate. The pruned
   * numbers here are what a column-pruned segment-slot reader WOULD deliver — the descriptor layout
   * already prunes via its column-lazy handle, so its pruned column is the reference.
   */
  @Test
  public void columnPruningWideTable() throws IOException {
    final String json = buildWideDataset();
    query("jn:store('json-path1','wide.jn','" + json + "')");
    query("jn:store('json-path2','wide.jn','" + json + "')");

    final Pruning descriptor = measurePruning("json-path1", "wide.jn", false);
    final Pruning segmentSlot = measurePruning("json-path2", "wide.jn", true);

    reportPruning(descriptor, segmentSlot);
    Assertions.assertFalse(descriptor.segmentSlotLayout, "json-path1 must be descriptor layout");
    Assertions.assertTrue(segmentSlot.segmentSlotLayout, "json-path2 must be segment-slot layout");
  }

  private Pruning measurePruning(final String dbName, final String resource,
      final boolean segmentSlotLayout) throws IOException {
    final StringBuilder fields = new StringBuilder();
    final StringBuilder types = new StringBuilder();
    for (int c = 0; c < WIDE_COLS; c++) {
      if (c > 0) {
        fields.append(", ");
        types.append(", ");
      }
      fields.append("'/[]/c").append(c).append('\'');
      types.append("'long'");
    }
    final String prior = System.getProperty("sirix.projection.segmentSlotLayout");
    System.setProperty("sirix.projection.segmentSlotLayout", Boolean.toString(segmentSlotLayout));
    try {
      query(("""
            let $doc := jn:doc('%s','%s')
            let $stats := jn:create-projection-index($doc, '/[]', (%s), (%s))
            return {"revision": sdb:commit($doc)}
          """).formatted(dbName, resource, fields, types));
    } finally {
      if (prior == null) {
        System.clearProperty("sirix.projection.segmentSlotLayout");
      } else {
        System.setProperty("sirix.projection.segmentSlotLayout", prior);
      }
    }
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final Pruning p = new Pruning();
    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(dbName);
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(resource);
      final int revision = session.getMostRecentRevisionNumber();
      final int indexNumber = 0;
      final int rowGroupCount;
      try (final JsonNodeReadOnlyTrx pin = session.beginNodeReadOnlyTrx(revision)) {
        final ProjectionIndexMetadata meta = ProjectionIndexMetadata.parse(
            ProjectionIndexHOTStorage.readBlob(pin.getStorageEngineReader(), indexNumber, 0L));
        Assertions.assertNotNull(meta, "projection metadata must be present");
        p.segmentSlotLayout = meta.isColumnSegmentSlotLayout();
        rowGroupCount = meta.rowGroupCount();
        p.rowGroupCount = rowGroupCount;
      }
      final int bodySeg = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0); // column c0's BODY segment

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final var reader = rtx.getStorageEngineReader();

        // Whole-row read: assemble every leaf (ALL columns) — the segment-slot serving path today.
        final long[] wholeBytes = {0};
        p.wholeMs = medianMs(() -> {
          final List<byte[]> leaves = p.segmentSlotLayout
              ? ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(reader, indexNumber, rowGroupCount)
              : ProjectionIndexHOTStorage.readAllRowGroups(reader, indexNumber);
          long b = 0;
          for (final byte[] leaf : leaves) {
            b += leaf.length;
          }
          wholeBytes[0] = b;
        });
        p.wholeBytes = wholeBytes[0];

        // Column-pruned read: ONLY column c0's BODY page, per row group — the rest is never touched.
        // Descriptor keys the page under (rowGroupId, bodySeg); segment-slot under the bare segment
        // slot's own page ref ((rg<<16)|(bodySeg+1), BLOB_SEGMENT_ID=0) — the 16-bit slotKind must
        // match ProjectionIndexHOTStorage.columnSegmentSlotKey. Both via readSegmentPageBytes.
        final long[] prunedBytes = {0};
        p.prunedMs = medianMs(() -> {
          long b = 0;
          for (long rg = 1; rg <= rowGroupCount; rg++) {
            final long ownerSlot = p.segmentSlotLayout ? ((rg << 16) | (bodySeg + 1)) : rg;
            final int columnSegmentId = p.segmentSlotLayout ? 0 : bodySeg;
            final byte[] bytes = ProjectionIndexHOTStorage.readSegmentPageBytes(
                reader, indexNumber, ownerSlot, columnSegmentId);
            if (bytes == null) {
              throw new IllegalStateException("c0 BODY is inline at row group " + rg
                  + " — the column compressed below the page threshold; raise WIDE_RECORDS/entropy");
            }
            b += bytes.length;
          }
          prunedBytes[0] = b;
        });
        p.prunedBytes = prunedBytes[0];
      }

      // End-to-end: sum(c0) through the vectorized executor. Segment-slot now builds a column-pruned
      // handle, so this SERVES from c0's slices only — before the pruned reader it went whole-leaf.
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try (final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
           final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        final String q = "let $doc := jn:doc('" + dbName + "','" + resource + "')\n"
            + "return sum(for $r in $doc[] return $r.c0)";
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        final String[] out = {null};
        p.servedMs = medianMs(() -> out[0] = evaluate(chain, ctx, q));
        p.served = ProjectionIndexCatalog.servedCount() > servedBefore;
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
    return p;
  }

  private Result measureLayout(final String dbName, final String resource,
      final boolean segmentSlotLayout) throws IOException {
    final Path dbDir = JsonTestHelper.PATHS.PATH1.getFile().getParent().resolve(dbName);
    final Path resourceDir = dbDir.resolve("resources").resolve(resource);
    final long sizeBeforeBuild = dirSize(resourceDir);

    final String prior = System.getProperty("sirix.projection.segmentSlotLayout");
    System.setProperty("sirix.projection.segmentSlotLayout", Boolean.toString(segmentSlotLayout));
    final long buildNanos;
    try {
      final long t0 = System.nanoTime();
      query("""
            let $doc := jn:doc('%s','%s')
            let $stats := jn:create-projection-index($doc, '/[]',
                ('/[]/age', '/[]/active', '/[]/dept'),
                ('long', 'boolean', 'string'))
            return {"revision": sdb:commit($doc)}
          """.formatted(dbName, resource));
      buildNanos = System.nanoTime() - t0;
    } finally {
      if (prior == null) {
        System.clearProperty("sirix.projection.segmentSlotLayout");
      } else {
        System.setProperty("sirix.projection.segmentSlotLayout", prior);
      }
    }
    final long projectionBytes = dirSize(resourceDir) - sizeBeforeBuild;

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final Result r = new Result();
    r.resource = resource;
    r.buildMs = buildNanos / 1e6;
    r.projectionBytes = projectionBytes;

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(dbName);
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(resource);
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();

      // Each DB has exactly one projection index → storage indexNumber 0. (The whole-leaf handle a
      // segment-slot store returns carries defId=-1, so read the metadata by indexNumber directly.)
      final int indexNumber = 0;
      final int rowGroupCount;
      try (final JsonNodeReadOnlyTrx pin = session.beginNodeReadOnlyTrx(revision)) {
        final ProjectionIndexMetadata meta = ProjectionIndexMetadata.parse(
            ProjectionIndexHOTStorage.readBlob(pin.getStorageEngineReader(), indexNumber, 0L));
        Assertions.assertNotNull(meta, "projection metadata must be present at indexNumber 0");
        r.segmentSlotLayout = meta.isColumnSegmentSlotLayout();
        rowGroupCount = meta.rowGroupCount();
        r.rowGroupCount = rowGroupCount;
      }

      // --- Descriptor-tier count (no hydrate) ---
      r.countMs = medianMs(() -> {
        ProjectionIndexCatalog.clearCache();
        final long n = ProjectionIndexCatalog.countRowsFromDescriptors(
            session, resourceKey, revision, new String[] {"[]"});
        r.rowCount = n;
      });

      // --- Whole-leaf decode CPU (warm reader): the layout's reassembly cost ---
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final var reader = rtx.getStorageEngineReader();
        r.decodeMs = medianMs(() -> {
          final List<byte[]> leaves = r.segmentSlotLayout
              ? ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(reader, indexNumber, rowGroupCount)
              : ProjectionIndexHOTStorage.readAllRowGroups(reader, indexNumber);
          if (leaves.size() != rowGroupCount) {
            throw new IllegalStateException("decoded " + leaves.size() + " != " + rowGroupCount);
          }
        });
      }

      // --- End-to-end aggregate serving (sum over a numeric column) ---
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try (final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
           final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        final String sumQuery =
            "let $doc := jn:doc('" + dbName + "','" + resource + "')\n"
            + "return sum(for $r in $doc[] return $r.age)";
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        final String[] out = new String[1];
        r.aggregateMs = medianMs(() -> out[0] = evaluate(chain, ctx, sumQuery));
        r.served = ProjectionIndexCatalog.servedCount() > servedBefore;
        r.aggregateResult = out[0];
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
    return r;
  }

  /** Warmup + median-of-N wall time for a repeatable op. */
  private static double medianMs(final Runnable op) {
    for (int i = 0; i < WARMUP; i++) {
      op.run();
    }
    final double[] samples = new double[MEASURE];
    for (int i = 0; i < MEASURE; i++) {
      final long t0 = System.nanoTime();
      op.run();
      samples[i] = (System.nanoTime() - t0) / 1e6;
    }
    java.util.Arrays.sort(samples);
    return samples[MEASURE / 2];
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String q) {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
         final PrintWriter pw = new PrintWriter(out)) {
      new Query(chain, q).serialize(ctx, pw);
      pw.flush();
      return out.toString();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@value #WIDE_COLS}-column long table with high-entropy values so every BODY segment is a page. */
  private static String buildWideDataset() {
    final StringBuilder sb = new StringBuilder(WIDE_RECORDS * WIDE_COLS * 12).append('[');
    for (int i = 0; i < WIDE_RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('{');
      for (int c = 0; c < WIDE_COLS; c++) {
        if (c > 0) {
          sb.append(',');
        }
        // Distinct, wide-range values → poorly compressible → a referenced BODY page per row group.
        sb.append("\"c").append(c).append("\":").append((long) i * 1_000_003L + c * 97L);
      }
      sb.append('}');
    }
    return sb.append(']').toString();
  }

  private static void reportPruning(final Pruning d, final Pruning s) {
    final StringBuilder b = new StringBuilder();
    b.append("\n======== Column pruning on a WIDE table (").append(WIDE_RECORDS).append(" records, ")
     .append(WIDE_COLS).append(" long columns, ").append(d.rowGroupCount).append(" row groups) ========\n");
    b.append("A single-column aggregate needs ONLY column c0's segments. 'whole-row' reads all ")
     .append(WIDE_COLS).append(" columns; 'pruned' reads just c0's BODY per row group.\n\n");
    b.append(String.format("%-30s %14s %14s%n", "", "DESCRIPTOR", "SEGMENT-SLOT"));
    b.append(String.format("%-30s %14.3f %14.3f%n", "whole-row read (ms)", d.wholeMs, s.wholeMs));
    b.append(String.format("%-30s %14.3f %14.3f%n", "pruned c0 read (ms)", d.prunedMs, s.prunedMs));
    b.append(String.format("%-30s %14d %14d%n", "whole-row bytes", d.wholeBytes, s.wholeBytes));
    b.append(String.format("%-30s %14d %14d%n", "pruned c0 bytes", d.prunedBytes, s.prunedBytes));
    b.append(String.format("%-30s %13.1f%% %13.1f%%%n", "pruned/whole bytes",
        100.0 * d.prunedBytes / d.wholeBytes, 100.0 * s.prunedBytes / s.wholeBytes));
    b.append(String.format("%-30s %13.1fx %13.1fx%n", "pruning speedup (whole/pruned)",
        d.wholeMs / d.prunedMs, s.wholeMs / s.prunedMs));
    b.append(String.format("%-30s %14.3f %14.3f%n", "end-to-end sum(c0) serve (ms)", d.servedMs, s.servedMs));
    b.append(String.format("%-30s %14s %14s%n", "served from projection", d.served, s.served));
    b.append("\nBoth layouts now build a column-pruned handle, so the end-to-end single-column\n");
    b.append("aggregate serves from ONLY c0's segments — segment-slot no longer pays the whole-row cost.\n");
    b.append("================================================================================\n");
    System.out.println(b);
  }

  private static String buildDataset() {
    final String[] depts = {"Eng", "Sales", "HR", "Ops", "QA", "Legal", "Finance"};
    final StringBuilder sb = new StringBuilder(RECORDS * 48).append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"age\":").append(18 + (i % 48))
        .append(",\"active\":").append((i & 1) == 0)
        .append(",\"dept\":\"").append(depts[i % depts.length]).append("\"}");
    }
    return sb.append(']').toString();
  }

  /** Recursive sum of regular-file sizes under {@code dir}; 0 if the dir is absent. */
  private static long dirSize(final Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return 0L;
    }
    try (var walk = Files.walk(dir)) {
      return walk.filter(Files::isRegularFile).mapToLong(p -> {
        try {
          return Files.size(p);
        } catch (final IOException e) {
          return 0L;
        }
      }).sum();
    }
  }

  private static void report(final Result d, final Result s) {
    final StringBuilder b = new StringBuilder();
    b.append("\n================ Projection layout comparison (").append(RECORDS)
     .append(" records, ").append(d.rowGroupCount).append(" leaves) ================\n");
    b.append(String.format("%-34s %16s %16s %12s%n", "metric", "DESCRIPTOR", "SEGMENT-SLOT", "seg/descr"));
    row(b, "build time (ms)", d.buildMs, s.buildMs, "%.1f");
    row(b, "projection storage (bytes)", d.projectionBytes, s.projectionBytes);
    row(b, "descriptor-tier count (ms)", d.countMs, s.countMs, "%.3f");
    row(b, "whole-leaf decode, warm (ms)", d.decodeMs, s.decodeMs, "%.3f");
    row(b, "aggregate sum serving (ms)", d.aggregateMs, s.aggregateMs, "%.3f");
    b.append(String.format("%-34s %16s %16s%n", "aggregate served-from-projection",
        d.served, s.served));
    b.append(String.format("%-34s %16s %16s%n", "aggregate result", d.aggregateResult, s.aggregateResult));
    b.append("================================================================================\n");
    System.out.println(b);
  }

  private static void row(final StringBuilder b, final String label, final double dv, final double sv,
      final String fmt) {
    b.append(String.format("%-34s " + pad(fmt) + " " + pad(fmt) + " %11.2fx%n", label, dv, sv,
        dv == 0 ? Double.NaN : sv / dv));
  }

  private static void row(final StringBuilder b, final String label, final long dv, final long sv) {
    b.append(String.format("%-34s %16d %16d %11.2fx%n", label, dv, sv,
        dv == 0 ? Double.NaN : (double) sv / dv));
  }

  private static String pad(final String fmt) {
    return "%16" + fmt.substring(1);
  }

  private static final class Pruning {
    private boolean segmentSlotLayout;
    private int rowGroupCount;
    private double wholeMs;
    private double prunedMs;
    private long wholeBytes;
    private long prunedBytes;
    private double servedMs;
    private boolean served;
  }

  private static final class Result {
    private String resource;
    private boolean segmentSlotLayout;
    private int rowGroupCount;
    private long rowCount;
    private double buildMs;
    private long projectionBytes;
    private double countMs;
    private double decodeMs;
    private double aggregateMs;
    private boolean served;
    private String aggregateResult;
  }
}
