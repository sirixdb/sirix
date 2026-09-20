/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.budget;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.SortedViewReadProbe;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.ProjectionSpec;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budgets for the projection query routes: each query shape is answered by the route meant for
 * it, reads no more of the index than that route needs, and answers exactly what the interpreter
 * answers.
 *
 * <p>
 * The three shapes are the ones a group-by workload leans on, over a schema that has nothing to do
 * with any benchmark (an inventory event log):
 *
 * <ul>
 * <li>a count per key with no filter, answered from the per-value row counts the <em>load</em>
 * maintains, without touching a row group;</li>
 * <li>a filtered count per key, answered from column slices;</li>
 * <li>a grouped earliest- or latest-value top-K under an equality prefix, answered from a sorted
 * view's group summaries.</li>
 * </ul>
 *
 * A route that stops being taken still returns the right answer, from a slower route, so only the
 * work shows it. Every query here runs cache-cold, because a warm buffer cache answers from memory
 * and reads zero leaves whatever the route does.
 *
 * <p>
 * <b>The fixture is built so the interpreter is an oracle.</b> Product {@code k} owns
 * {@code 32 * (k + 1)} rows, of which {@code 28 * (k + 1)} are active, so no two counts tie and
 * {@code order by $count} has one right answer; every {@code ts} is unique, so no two group minima
 * or maxima tie either. One row, deep inside the {@code (active, r2)} range of the sorted view, has
 * no {@code ts} at all: the aggregate field is optional.
 */
@Isolated
final class ProjectionQueryWorkBudgetTest {

  private static final String DATABASE = "inventory";

  private static final String RESOURCE = "events";

  private static final int PRODUCTS = 24;

  /** Rows of product {@code k}: {@code ROWS_PER_PRODUCT_STEP * (k + 1)}. */
  private static final int ROWS_PER_PRODUCT_STEP = 32;

  private static final int RECORDS = ROWS_PER_PRODUCT_STEP * PRODUCTS * (PRODUCTS + 1) / 2;

  /** Coprime with {@link #RECORDS}, so stepping by it visits every row once, products interleaved. */
  private static final int DOCUMENT_ORDER_STRIDE = 4099;

  /** The product whose first {@code r2} row carries no {@code ts}; mid-range, so mid-leaf-run. */
  private static final int PRODUCT_WITHOUT_A_TIMESTAMP = 11;

  private static final String REGION_WITHOUT_A_TIMESTAMP = "r2";

  /** A sorted data leaf holds at most this many rows ({@code ProjectionSortedLeaf.MAX_ROWS}). */
  private static final int SORTED_LEAF_ROWS = 256;

  /** Active rows of one region that is not {@code r3}: 8 per product step, over all products. */
  private static final int ACTIVE_ROWS_PER_FULL_REGION = 8 * PRODUCTS * (PRODUCTS + 1) / 2;

  /**
   * Group summaries a walk of one full region's range may read. Densely packed the range is ten
   * leaves, and a walk reads one summary per leaf it visits (8 and 10 measured, for the two orders).
   * Twice that leaves the builder free to pack leaves more loosely, and is still far below the 38
   * leaves of the whole view, which is what a walk that ran past its prefix would approach.
   */
  private static final int MAX_SUMMARY_READS_PER_FULL_REGION =
      2 * ((ACTIVE_ROWS_PER_FULL_REGION + SORTED_LEAF_ROWS - 1) / SORTED_LEAF_ROWS);

  private static final List<String> FIELDS = List.of("/[]/region", "/[]/product", "/[]/state", "/[]/qty", "/[]/ts");

  private static final List<String> TYPES = List.of("string", "string", "string", "long", "long");

  /** state, region, product, ts: an equality prefix, the group key, then the aggregated field. */
  private static final ProjectionSortedSpec SORT_ORDER = new ProjectionSortedSpec(List.of(2, 0, 1, 4));

  private static final String STREAMING_LOAD = "streaming load";

  private static final String PARALLEL_LOAD = "parallel load";

  private static final String INDEXED_AFTER_LOAD = "index declared after the load";

  /**
   * The same projection and sorted view as {@link #SORT_ORDER}, declared on an already loaded
   * resource.
   */
  private static final String DECLARE_PROJECTION = """
      let $doc := jn:doc('inventory','events')
      let $stats := jn:create-projection-index($doc, '/[]',
          ('/[]/region', '/[]/product', '/[]/state', '/[]/qty', '/[]/ts'),
          ('string', 'string', 'string', 'long', 'long'),
          ('/[]/state', '/[]/region', '/[]/product', '/[]/ts'))
      return sdb:commit($doc)
      """;

  private static final String COUNT_PER_PRODUCT = """
      for $e in jn:doc('inventory','events')[]
      let $product := $e.product
      group by $product
      let $count := count($e)
      order by $count descending
      return {"product": $product, "count": $count}
      """;

  private static final String ACTIVE_COUNT_PER_PRODUCT = """
      for $e in jn:doc('inventory','events')[]
      where $e.state = "active"
      let $product := $e.product
      group by $product
      let $count := count($e)
      order by $count descending
      return {"product": $product, "count": $count}
      """;

  /**
   * Grouped top-K over a sorted view, in the two orders that reach it by different doors. A
   * {@code min} ordered ascending is tried against the leaf bounds first and only then against the
   * group summaries; a {@code max} ordered descending goes to the summaries directly. Both must serve
   * a clean range and decline a range holding a row without the aggregate, from summaries alone.
   */
  private enum TopK {
    EARLIEST("""
        subsequence(
          for $e in jn:doc('inventory','events')[]
          where $e.state = "active" and $e.region = "%s"
          let $product := $e.product
          group by $product
          let $first := min($e.ts)
          order by $first
          return {"product": $product, "first": $first}, 1, 3)
        """),

    LATEST("""
        subsequence(
          for $e in jn:doc('inventory','events')[]
          where $e.state = "active" and $e.region = "%s"
          let $product := $e.product
          group by $product
          let $last := max($e.ts)
          order by $last descending
          return {"product": $product, "last": $last}, 1, 3)
        """);

    private final String template;

    TopK(final String template) {
      this.template = template;
    }

    String over(final String region) {
      return template.formatted(region);
    }
  }

  @TempDir
  private Path root;

  @BeforeEach
  @AfterEach
  void clearServingState() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    goCold();
  }

  /**
   * Catches: a build path that stops maintaining the per-value row counts, and a count-only group-by
   * that stops being answered from them. The counts are accumulated leaf by leaf by whichever path
   * builds the projection: {@code ProjectionBulkLoad} for both load-time builds, streaming and
   * parallel, and {@code ProjectionIndexBuilder.buildAndPersist} for an index declared afterwards.
   *
   * <p>
   * Measured by mutation, the two failures look nothing alike, which is why both the answer and the
   * work are asserted. A path that stops <em>accumulating</em> still publishes the summary, empty,
   * and the route still serves from it: {@code groupSummary} stays 1 and the answer comes back empty,
   * so only the comparison with the interpreter fails. A query that stops <em>qualifying</em> for the
   * route is answered correctly from the column slices instead: {@code groupSummary} goes 1 to 0 and
   * the leaf loads from at most 2 to 27 on this fixture, growing with the row count where the
   * summary's do not.
   */
  @ParameterizedTest(name = "{0}: a count-only group-by is answered from the value-count summary")
  @ValueSource(strings = {STREAMING_LOAD, PARALLEL_LOAD, INDEXED_AFTER_LOAD})
  void aCountOnlyGroupByIsAnsweredFromTheValueCountSummary(final String build) throws Exception {
    load(build);
    final String expected = interpreterAnswer(COUNT_PER_PRODUCT);
    assertTrue(expected.startsWith("{\"product\":\"p23\",\"count\":768}"),
        "the fixture's largest product must lead, or the oracle is not the one this test reasons about: " + expected);

    final WorkCapture.Captured<String> query = served(routesAndReads(), COUNT_PER_PRODUCT);

    assertEquals(expected, query.result(), "the summary must answer exactly what the interpreter answers");
    final WorkReport work = query.work();
    work.assertExactly(QueryWorkCounters.GROUP_SUMMARY, 1,
        "the count-only group-by was not answered from the value-count summary (" + build + "): no summary was "
            + "published for the key column, or the route's shape test stopped accepting this query");
    work.assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1,
        "a summary serve is one group-aggregate serve; more means the query was answered twice");
    work.assertZero(QueryWorkCounters.GROUP_SLICED, "the summary route scanned column slices as well");
    work.assertZero(EngineWorkCounters.EAGER_FALLBACKS,
        "a count the summary already holds materialized the whole projection");
    // The summary is one blob beside the index metadata: a couple of leaves, whatever the row count.
    work.assertBetween(EngineWorkCounters.HOT_LEAF_LOADS, 1, 8,
        "answering from the summary reads the metadata and one summary blob (1 or 2 leaf loads measured); answering "
            + "from the column slices instead loads 27 on this fixture and grows with the data");
    assertEquals(0L, ProjectionIndexCatalog.dataCacheSize(),
        "serving from the summary must not hydrate a row-group handle");
  }

  /**
   * Catches: the filtered group-by leaving the sliced route. Declining to the whole-leaf route
   * materializes every column of every leaf for a query that needs two ({@code eagerFallbacks} 0 to
   * 1); declining to the generic pipeline walks every record ({@code groupAggregates} 1 to 0).
   */
  @Test
  void aFilteredGroupByReadsColumnSlicesAndNeverTheWholeProjection() throws Exception {
    load(PARALLEL_LOAD);
    final String expected = interpreterAnswer(ACTIVE_COUNT_PER_PRODUCT);
    assertTrue(expected.startsWith("{\"product\":\"p23\",\"count\":672}"),
        "the fixture's largest product must lead with its active rows: " + expected);

    final WorkCapture.Captured<String> query = served(routesAndReads(), ACTIVE_COUNT_PER_PRODUCT);

    assertEquals(expected, query.result(), "the sliced route must answer exactly what the interpreter answers");
    final WorkReport work = query.work();
    work.assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1,
        "the filtered group-by fell through to the generic pipeline, which walks every record");
    work.assertExactly(QueryWorkCounters.GROUP_SLICED, 1,
        "the filtered group-by was served from whole leaves instead of the two column slices it needs");
    work.assertZero(QueryWorkCounters.GROUP_SUMMARY,
        "a filtered count cannot come from the unfiltered summary; serving it from there would be a wrong answer");
    work.assertZero(EngineWorkCounters.EAGER_FALLBACKS,
        "the sliced route materialized the whole projection: at scale that is the entire index in memory");
    // Two executor threads that miss the cache on the same leaf may each load it, so a healthy read
    // can approach twice the 27 to 30 measured here; the ceiling sits at four times.
    work.assertBetween(EngineWorkCounters.HOT_LEAF_LOADS, 1, 120,
        "a cache-cold sliced group-by over this fixture loads 27 to 30 HOT leaves; several times that means the "
            + "route reads its leaves over and over instead of once");
  }

  /**
   * Catches: a clean range leaving the sorted view, or reading more of it than the range. Every row
   * of {@code (active, r1)} carries the aggregate, so the group summaries alone answer it: one
   * summary per leaf of the range, and not one data leaf.
   */
  @ParameterizedTest(name = "{0}: a grouped top-K over a clean range is answered from the sorted view's summaries")
  @EnumSource(TopK.class)
  void aGroupedTopKOverACleanRangeIsAnsweredFromTheSortedViewsSummaries(final TopK topK) throws Exception {
    load(PARALLEL_LOAD);
    final String cleanRange = topK.over("r1");
    final String expected = interpreterAnswer(cleanRange);

    final SortedViewReadProbe sortedView = new SortedViewReadProbe();
    final WorkCapture.Captured<String> query = served(routesAndReads().with(sortedView), cleanRange);

    assertEquals(expected, query.result(), "the sorted view must answer exactly what the interpreter answers");
    final WorkReport work = query.work();
    work.assertExactly(QueryWorkCounters.SORTED_GROUP_BYS, 1,
        "a grouped top-K under an equality prefix was not answered from the sorted view");
    work.assertZero(QueryWorkCounters.GROUP_SLICED, "the sorted view served the query and a slice scan ran as well");
    work.assertBetween(sortedView.summaryReads(), 1, MAX_SUMMARY_READS_PER_FULL_REGION,
        "the summaries walk read more group summaries than the range has leaves: it is walking past its prefix");
    work.assertZero(sortedView.dataLeafReads(),
        "a range whose leaves all have summaries needs no data leaf; reading one means the full-key walk ran too");
    work.assertZero(EngineWorkCounters.EAGER_FALLBACKS, "the sorted view materialized the whole projection");
  }

  /**
   * Catches: the double walk over an optional aggregate field. One row of {@code (active, r2)} has no
   * {@code ts}, so its leaf has no group summary and the sorted view cannot answer the range. It must
   * find that out in <em>one</em> walk: the leaf lies entirely inside the range, which the
   * directory's fence keys prove, so seeking the range again to walk its data leaves can only reach
   * the same row and decline. That second walk used to run, reading the range's data leaves for
   * nothing.
   *
   * <p>
   * Both halves of the property are asserted: the range declines without a data-leaf read, and a
   * range of the same view whose rows all carry a value is still served from it. Declining the whole
   * view instead would pass the first half and fail the second.
   */
  @ParameterizedTest(name = "{0}: a range holding a row without the aggregate declines the sorted view after one walk")
  @EnumSource(TopK.class)
  void aRangeHoldingARowWithoutTheAggregateDeclinesTheSortedViewAfterOneWalk(final TopK topK) throws Exception {
    load(PARALLEL_LOAD);
    final String withoutAValue = topK.over(REGION_WITHOUT_A_TIMESTAMP);
    final String expected = interpreterAnswer(withoutAValue);

    final SortedViewReadProbe sortedView = new SortedViewReadProbe();
    final WorkCapture.Captured<String> declined = served(routesAndReads().with(sortedView), withoutAValue);

    assertEquals(expected, declined.result(),
        "a declined sorted view changes which route answers, never the answer: the row without a ts is skipped "
            + "by the aggregate, as the interpreter skips it");
    final WorkReport work = declined.work();
    work.assertZero(QueryWorkCounters.SORTED_GROUP_BYS,
        "the sorted view served a range holding a row without the aggregate value; its answer cannot be trusted");
    work.assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1,
        "after the sorted view declines, the group route must still answer from the projection");
    work.assertBetween(sortedView.summaryReads(), 1, MAX_SUMMARY_READS_PER_FULL_REGION,
        "the decline is proven by the summaries walk, which stops at the first leaf without a summary");
    work.assertZero(sortedView.dataLeafReads(),
        "the sorted view walked its key range twice before declining: the summaries walk had already proven, from "
            + "a summary-less leaf lying entirely inside the range, that the full-key walk could only decline too");

    goCold();
    final String clean = topK.over("r1");
    final String expectedClean = interpreterAnswer(clean);
    final SortedViewReadProbe cleanRange = new SortedViewReadProbe();
    final WorkCapture.Captured<String> servedClean = served(routesAndReads().with(cleanRange), clean);
    assertEquals(expectedClean, servedClean.result());
    servedClean.work()
               .assertExactly(QueryWorkCounters.SORTED_GROUP_BYS, 1,
                   "one row without a value, elsewhere in the view, stopped the sorted view from serving a range whose own "
                       + "rows all carry one: the decline must be proven per range, not taken view-wide");
  }

  private static WorkCapture routesAndReads() {
    return WorkCapture.of(QueryWorkCounters.ROUTES)
                      .and(EngineWorkCounters.CHUNKED_BODIES)
                      .and(EngineWorkCounters.HOT_LEAVES);
  }

  /** Answers {@code query} cache-cold through a bound executor, capturing the work it takes. */
  private WorkCapture.Captured<String> served(final WorkCapture capture, final String query) throws Exception {
    goCold();
    try (BasicJsonDBStore store = openStore();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(DATABASE);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession(RESOURCE)) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          return capture.call(() -> evaluate(chain, context, query));
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  /** The interpreter's answer: no executor bound and none auto-wired, so no route under test runs. */
  private String interpreterAnswer(final String query) throws IOException {
    try (BasicJsonDBStore store = openStore();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      return evaluate(chain, context, query);
    }
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream(); PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      return out.toString();
    }
  }

  /** Empties every cache between the file and the query, so the work counted is work done. */
  private static void goCold() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  /** Builds the fixture and its projection through one of the three paths that can build one. */
  private void load(final String build) throws IOException {
    final ProjectionSpec projection = new ProjectionSpec("/[]", FIELDS, TYPES, SORT_ORDER);
    try (BasicJsonDBStore store = openStore()) {
      switch (build) {
        case PARALLEL_LOAD -> {
          try (StringReader input = new StringReader(dataset())) {
            store.createParallel(DATABASE, RESOURCE, input, projection);
          }
        }
        case STREAMING_LOAD -> {
          try (JsonReader input = new JsonReader(new StringReader(dataset()))) {
            store.create(DATABASE, RESOURCE, input, projection);
          }
        }
        case INDEXED_AFTER_LOAD -> {
          try (JsonReader input = new JsonReader(new StringReader(dataset()));
              SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
              SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
            store.create(DATABASE, RESOURCE, input);
            new Query(chain, DECLARE_PROJECTION).evaluate(context);
          }
        }
        default -> throw new IllegalArgumentException("unknown build path: " + build);
      }
    }
  }

  /** FILE_CHANNEL explicitly: the store's default backend differs by platform, a budget must not. */
  private BasicJsonDBStore openStore() {
    return BasicJsonDBStore.newBuilder()
                           .location(root)
                           .storageType(StorageType.FILE_CHANNEL)
                           .hashType(HashType.NONE)
                           .storeNodeHistory(false)
                           .buildPathSummary(true)
                           .buildPathStatistics(false)
                           .build();
  }

  private static String dataset() {
    final StringBuilder json = new StringBuilder(RECORDS * 88).append('[');
    for (int position = 0; position < RECORDS; position++) {
      if (position != 0) {
        json.append(',');
      }
      appendRow(json, (int) ((long) position * DOCUMENT_ORDER_STRIDE % RECORDS));
    }
    return json.append(']').toString();
  }

  /**
   * Row {@code row} of the fixture. Rows {@code [first(k), first(k + 1))} belong to product
   * {@code k}; within a product, row {@code j} is in region {@code j % 4} and retired when
   * {@code j % 8 == 7}, which retires every other {@code r3} row and none elsewhere.
   */
  private static void appendRow(final StringBuilder json, final int row) {
    int product = 0;
    int firstOfProduct = 0;
    while (row >= firstOfProduct + ROWS_PER_PRODUCT_STEP * (product + 1)) {
      firstOfProduct += ROWS_PER_PRODUCT_STEP * (product + 1);
      product++;
    }
    final int withinProduct = row - firstOfProduct;
    final String region = "r" + withinProduct % 4;
    json.append("{\"region\":\"")
        .append(region)
        .append("\",\"product\":\"p")
        .append(product < 10
            ? "0"
            : "")
        .append(product)
        .append("\",\"state\":\"")
        .append(withinProduct % 8 == 7
            ? "retired"
            : "active")
        .append("\",\"qty\":")
        .append(row % 11);
    // withinProduct 2 is this product's first r2 row; it is the one row with no aggregate value.
    final boolean withoutTimestamp = product == PRODUCT_WITHOUT_A_TIMESTAMP && withinProduct == 2;
    if (!withoutTimestamp) {
      json.append(",\"ts\":").append(1_000_000L + (long) row * 7919L % 1_000_003L);
    }
    json.append('}');
  }
}
