package io.sirix.query.bench.jsonbench;

import io.brackit.query.Query;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * The projection index the five JSONBench queries can be served from.
 *
 * <h2>Column choice</h2> Exactly the five fields the queries touch, and no more: the Bluesky corpus
 * has hundreds of distinct paths below {@code commit.record}, none of which any query reads.
 * ClickHouse's reference schema makes the same five choices — {@code kind},
 * {@code commit.operation}, {@code commit.collection} as {@code LowCardinality(String)},
 * {@code did} as {@code String} and {@code time_us} as {@code UInt64} — so the two systems index
 * the same thing.
 *
 * <p>
 * Unlike {@code ClickBenchProjection}, the set is declared here rather than read off the query
 * text: ClickBench's columns are the top-level fields of a flat record and a regex over
 * {@code $h.Column} finds them all, while these are <em>nested</em> paths
 * ({@code /[]/commit/collection}) whose spelling in a query is a chained deref. Deriving nested
 * paths from query text would be guesswork; five hand-declared paths that the loader verifies is
 * not.
 *
 * <h2>Nested paths and the ambiguity guard</h2> {@code jn:create-projection-index} accepts
 * multi-step field paths and names the resulting column after the trailing step, so
 * {@code /[]/commit/collection} is the column {@code collection}. Two of these names recur
 * elsewhere in the corpus — {@code did} at six paths below {@code commit.record} (2285 events carry
 * {@code commit.record.facets[].features[].did}) and {@code collection} at
 * {@code commit.record.skyfeedBuilder.blocks[].collection}. See
 * {@code CreateProjectionIndex#assertUnambiguousFieldNames} for what that means for creation, and
 * {@code bench/jsonbench/README.md} for the measurement it forces.
 */
public final class JsonBenchProjection {

  /** The projected field paths, in declaration order; the trailing step names each column. */
  public static final List<String> COLUMN_PATHS =
      List.of("/[]/kind", "/[]/did", "/[]/time_us", "/[]/commit/collection", "/[]/commit/operation");

  /** Declared column types, keyed by path. Only {@code time_us} is numeric. */
  private static final Map<String, String> COLUMN_TYPES = Map.of("/[]/kind", "string", "/[]/did", "string",
      "/[]/time_us", "long", "/[]/commit/collection", "string", "/[]/commit/operation", "string");

  private JsonBenchProjection() {
    throw new AssertionError("no instances");
  }

  /**
   * The declared type of {@code path}; {@code long} for {@code time_us}, {@code string} otherwise.
   */
  static String projectionType(final String path) {
    final String type = COLUMN_TYPES.get(path);
    if (type == null) {
      throw new IllegalArgumentException("no declared type for projection path " + path);
    }
    return type;
  }

  /** The {@code jn:create-projection-index} call for the projected columns. */
  public static String createQuery() {
    final StringBuilder paths = new StringBuilder(COLUMN_PATHS.size() * 28);
    final StringBuilder types = new StringBuilder(COLUMN_PATHS.size() * 10);
    for (int i = 0; i < COLUMN_PATHS.size(); i++) {
      if (i > 0) {
        paths.append(", ");
        types.append(", ");
      }
      final String path = COLUMN_PATHS.get(i);
      paths.append('\'').append(path).append('\'');
      types.append('\'').append(projectionType(path)).append('\'');
    }
    return "let $doc := jn:doc('" + JsonBenchSchema.DATABASE + "','" + JsonBenchSchema.RESOURCE + "')\n"
        + "let $stats := jn:create-projection-index($doc, '" + JsonBenchSchema.ROOT_PATH + "',\n" + "    (" + paths
        + "),\n" + "    (" + types + "))\n" + "return {\"revision\": sdb:commit($doc)}";
  }

  /**
   * Build and persist the projection over an already-loaded corpus.
   *
   * @param dbDir the database directory holding {@code jsonbench/bluesky.jn}
   * @return wall-clock seconds spent building and committing
   */
  public static double create(final Path dbDir) {
    final long start = System.nanoTime();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, createQuery()).evaluate(ctx);
    }
    return (System.nanoTime() - start) / 1e9;
  }
}
