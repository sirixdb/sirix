package io.sirix.query.bench.clickbench;

import io.brackit.query.Query;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ProjectionSpec;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The projection index the 43 ClickBench queries can actually be served from.
 *
 * <h2>Why this exists</h2> Without it the benchmark measures the row path and nothing else: every
 * run reported {@code # served: predicateCounts=0 groupAggregates=0 numericGroupBys=0}, because no
 * projection was ever created over the corpus. Column kernels, group-by kernels and every serving
 * gate were simply unreachable, so no ClickBench number said anything about them.
 *
 * <h2>Column choice</h2> The projected set is exactly the columns the 43 queries dereference —
 * {@link #PROJECTED_COLUMNS}, derived here rather than hand-listed so a query edit that reaches for
 * a new column shows up as a missing projection column instead of a silent decline. Projecting all
 * 105 would pay ingest and space for 80 columns no query reads.
 *
 * <h2>Types</h2> The projection's type vocabulary is narrower than SQL's: {@code INT} and
 * {@code LONG} both become {@code long}. {@code DATE} and {@code DATETIME} are ISO-8601 JSON
 * strings by the loader's own encoding contract, so they are DECLARED as {@code date} and
 * {@code timestamp}: the projection then stores each as an epoch in its numeric lane — one
 * bit-packed integer per row instead of a per-leaf string dictionary — and reproduces the exact
 * original text on emission. That contract is enforced at load time by
 * {@code ClickBenchLoadMain#validate}, which is what makes the declaration safe to state here; a
 * value that violated it fails the projection build loudly instead of being indexed as something
 * the queries cannot compare.
 */
public final class ClickBenchProjection {

  /** Root path of the record set: the top-level array's elements. */
  public static final String ROOT_PATH = "/[]";

  /**
   * Columns the 43 queries dereference, in {@code create.sql} order.
   *
   * <p>
   * Kept in schema order rather than discovery order so the projection's column indexes are stable
   * across query-set edits — a reordering would invalidate every persisted store.
   */
  public static final List<String> PROJECTED_COLUMNS;

  static {
    final Set<String> referenced = ClickBenchQueries.referencedColumns();
    final List<String> ordered = new ArrayList<>(referenced.size());
    for (final String column : ClickBenchSchema.COLUMNS) {
      if (referenced.contains(column)) {
        ordered.add(column);
      }
    }
    // Every referenced name must BE a column: a typo in a query would otherwise quietly shrink the
    // projection and turn into a decline at run time rather than an error here.
    final Set<String> unknown = new LinkedHashSet<>(referenced);
    unknown.removeAll(ClickBenchSchema.COLUMNS);
    if (!unknown.isEmpty()) {
      throw new IllegalStateException("queries dereference non-columns: " + unknown);
    }
    PROJECTED_COLUMNS = List.copyOf(ordered);
  }

  private ClickBenchProjection() {
    throw new AssertionError("no instances");
  }

  /**
   * The projection type for {@code column}: {@code long}, {@code string}, {@code date} or
   * {@code timestamp}.
   */
  static String projectionType(final String column) {
    return switch (ClickBenchSchema.typeOf(column)) {
      case INT, LONG -> "long";
      case STRING -> "string";
      // Declared temporal columns. The loader's own encoding contract guarantees exactly the
      // canonical shapes these types accept ('YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS' — the loader
      // rewrites the separating space to 'T'), enforced at load time by ClickBenchLoadMain#validate,
      // and a value that ever violated it would now fail the BUILD rather than be indexed as text.
      case DATE -> "date";
      case DATETIME -> "timestamp";
    };
  }

  /**
   * The same declaration as {@link #createQuery()}, in the form the LOAD-TIME build takes: the index
   * is catalogued before the shred and maintained by it, so the corpus is walked once instead of
   * twice. Derived from the same {@link #PROJECTED_COLUMNS} list, so the two routes cannot declare
   * different projections.
   */
  public static ProjectionSpec spec() {
    return spec(-1L);
  }

  /**
   * As above, carrying an expected record count so the resource-wide dictionary's election can
   * decline a column whose dictionary would not fit. It matters here specifically: URL, Referer and
   * Title are long and near-unique, and at 100M rows their dictionaries do not fit any heap this
   * benchmark runs on.
   *
   * @param expectedRows the record count, or {@code -1} when unknown
   */
  public static ProjectionSpec spec(final long expectedRows) {
    final List<String> paths = new ArrayList<>(PROJECTED_COLUMNS.size());
    final List<String> types = new ArrayList<>(PROJECTED_COLUMNS.size());
    for (final String column : PROJECTED_COLUMNS) {
      paths.add(ROOT_PATH + '/' + column);
      types.add(projectionType(column));
    }
    return new ProjectionSpec(ROOT_PATH, paths, types, expectedRows);
  }

  /** The {@code jn:create-projection-index} call for the projected columns. */
  public static String createQuery() {
    return createQuery(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE);
  }

  /** Same call against an explicit database/resource — the per-partition composite entry point. */
  public static String createQuery(final String database, final String resource) {
    final StringBuilder paths = new StringBuilder(PROJECTED_COLUMNS.size() * 24);
    final StringBuilder types = new StringBuilder(PROJECTED_COLUMNS.size() * 10);
    for (int i = 0; i < PROJECTED_COLUMNS.size(); i++) {
      if (i > 0) {
        paths.append(", ");
        types.append(", ");
      }
      final String column = PROJECTED_COLUMNS.get(i);
      paths.append('\'').append(ROOT_PATH).append('/').append(column).append('\'');
      types.append('\'').append(projectionType(column)).append('\'');
    }
    return "let $doc := jn:doc('" + database + "','" + resource + "')\n"
        + "let $stats := jn:create-projection-index($doc, '" + ROOT_PATH + "',\n" + "    (" + paths + "),\n" + "    ("
        + types + "))\n" + "return {\"revision\": sdb:commit($doc)}";
  }

  /**
   * Build and persist the projection over an already-loaded corpus.
   *
   * @param dbDir the database directory holding {@code clickbench/hits.jn}
   * @return wall-clock seconds spent building and committing
   */
  public static double create(final Path dbDir) {
    return create(dbDir, ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE);
  }

  /**
   * Build and persist the projection for one explicit database/resource pair — the per-partition
   * entry point for partitioned (composite) corpora.
   *
   * @param location the store location holding {@code <database>/<resource>}
   * @param database the database directory name under {@code location}
   * @param resource the resource to project
   * @return wall-clock seconds spent building and committing
   */
  public static double create(final Path location, final String database, final String resource) {
    final long start = System.nanoTime();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, createQuery(database, resource)).evaluate(ctx);
    }
    return (System.nanoTime() - start) / 1e9;
  }
}
