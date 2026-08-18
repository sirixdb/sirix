package io.sirix.query.bench.jsonbench;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.trx.node.HashType;
import io.sirix.cache.Allocators;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.clickbench.ClickBenchSource;
import io.sirix.query.json.BasicJsonDBStore;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * JSONBench loader: shreds the Bluesky firehose events into a SirixDB JSON resource and reports the
 * two figures the JSONBench harness asks a system for — {@code Load time} in seconds and
 * {@code Data size} in bytes.
 *
 * <pre>
 *   JsonBenchLoadMain &lt;dbDir&gt; &lt;source&gt;
 * </pre>
 *
 * where {@code source} is a path to the NDJSON event file, optionally gzipped
 * ({@code file_0001.json.gz}). The reader comes from {@code ClickBenchSource}: it is
 * ClickBench-named but format-generic — it fabricates the enclosing array around a JSON-lines
 * stream on the fly, which is exactly what SirixDB's shredder consumes and what keeps the 480 MB
 * uncompressed corpus from ever being buffered whole.
 *
 * <p>
 * Tunables, all system properties, defaulted to the same fast-ingest configuration
 * {@code ClickBenchLoadMain} uses:
 * <ul>
 * <li>{@code -Dsirix.offheap.bytes} (default 24 GiB) — page buffer pool;</li>
 * <li>{@code -Dsirix.autoCommit.nodes} (default 131072) — auto-commit window in nodes. The corpus
 * shreds to roughly 31M nodes, i.e. ~237 windows, which is what bounds ingest memory;</li>
 * <li>{@code -Djsonbench.projection} (default true) — build the projection index over the five
 * columns the queries touch, as part of the load;</li>
 * <li>{@code -Djsonbench.projection.required} (default false) — make a failed projection build
 * fatal. Off by default deliberately: the ingest is the expensive half and a projection can be
 * added in place afterwards ({@code JsonBenchRunMain --build-projection}), so an index problem must
 * not throw a completed shred away. The failure is still reported loudly;</li>
 * <li>{@code -DbuildPathSummary} (defaults to {@code jsonbench.projection}) — the projection
 * builder resolves its field paths through the summary, so the two cannot disagree;</li>
 * <li>{@code -DbuildPathStatistics} (default false), {@code -DhashType} (default NONE);</li>
 * <li>{@code -Djsonbench.validate} (default true) — post-load type check, see below.</li>
 * </ul>
 *
 * <p>
 * The validation is not ceremony. {@code time_us} has to arrive as an unquoted JSON integer: as a
 * string node Q3's {@code idiv} arithmetic throws and Q4/Q5's {@code min}/{@code max} silently
 * compare lexicographically, which for equal-length microsecond stamps even looks plausible. Only
 * {@code kind = "commit"} events are checked for the {@code commit.*} fields — {@code identity} and
 * {@code account} events legitimately have none, and validating them for {@code commit.collection}
 * would fail a perfectly good load.
 */
public final class JsonBenchLoadMain {

  /** How many leading records validation pulls back while looking for a {@code commit} event. */
  private static final int VALIDATION_SAMPLE = 64;

  private JsonBenchLoadMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JsonBenchLoadMain <dbDir> <source>");
      System.err.println("  source: <file.json[.gz]> | <file.jsonl[.gz]> — NDJSON Bluesky events");
      System.exit(2);
      return;
    }
    final Path dbDir = Path.of(args[0]);
    final String source = args[1];

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    Allocators.getInstance().init(offheap);

    final int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "131072"));
    final boolean projection = Boolean.parseBoolean(System.getProperty("jsonbench.projection", "true"));
    final boolean projectionRequired =
        Boolean.parseBoolean(System.getProperty("jsonbench.projection.required", "false"));
    // Not independent options: a corpus loaded without a summary can never have a projection added,
    // and discovering that only when jn:create-projection-index throws costs a whole re-ingest.
    final boolean pathSummary =
        Boolean.parseBoolean(System.getProperty("buildPathSummary", String.valueOf(projection)));
    if (projection && !pathSummary) {
      System.err.println("buildPathSummary=false cannot be combined with jsonbench.projection=true — the "
          + "projection builder resolves its paths through the summary. Set one of them.");
      System.exit(2);
      return;
    }
    final boolean pathStatistics = Boolean.parseBoolean(System.getProperty("buildPathStatistics", "false"));
    final HashType hashType = HashType.fromString(System.getProperty("hashType", "NONE"));

    Files.createDirectories(dbDir);
    System.out.printf("# JSONBench load: db=%s source=%s%n", dbDir, source);
    System.out.printf("# offheap=%d MB autoCommit=%d pathSummary=%s pathStatistics=%s hash=%s%n", offheap / (1L << 20),
        autoCommit, pathSummary, pathStatistics, hashType);

    final long start = System.nanoTime();
    try (var store = BasicJsonDBStore.newBuilder()
                                     .location(dbDir)
                                     .numberOfNodesBeforeAutoCommit(autoCommit)
                                     .buildPathSummary(pathSummary)
                                     .buildPathStatistics(pathStatistics)
                                     .hashType(hashType)
                                     .build()) {
      try (Reader src = ClickBenchSource.open(source); JsonReader jsonReader = new JsonReader(src)) {
        store.create(JsonBenchSchema.DATABASE, JsonBenchSchema.RESOURCE, jsonReader);
      }
    }
    double loadSeconds = (System.nanoTime() - start) / 1e9;
    System.out.printf("# shred: %.3f s%n", loadSeconds);

    // The projection index is part of LOADING, the same way ClickHouse builds its typed subcolumns
    // while ingesting: a run without it measures the row path and can say nothing about any column
    // or group-by kernel.
    if (projection) {
      try {
        final double projectionSeconds = JsonBenchProjection.create(dbDir);
        // Which string columns the build gave a resource-wide dictionary. Worth printing because it
        // is a property of the DATA, decided from a sample at build time: the same query is a
        // dictionary-id group-by on one corpus and a per-leaf-dict one on another, and nothing else
        // in the output says which run this was.
        System.out.printf("# projection: columns=%d globalDictColumns=%d built in %.3f s%n",
            JsonBenchProjection.COLUMN_PATHS.size(), ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
            projectionSeconds);
        loadSeconds += projectionSeconds;
      } catch (final RuntimeException e) {
        System.out.printf("# projection: FAILED — %s%n", e);
        System.out.println("# projection: no query will be served from columns; the shred itself is intact and the "
            + "index can be added in place with JsonBenchRunMain --build-projection");
        if (projectionRequired) {
          throw e;
        }
      }
    } else {
      System.out.println("# projection: DISABLED (-Djsonbench.projection=false) — nothing will be served");
    }

    // The store's close() already flushed; this makes the page-cache write-back explicit, so "load
    // time" means "the data is on disk" the way the JSONBench driver intends.
    sync();

    final long bytes = directorySize(dbDir);
    System.out.printf("Load time: %.3f%n", loadSeconds);
    System.out.printf("Data size: %d%n", bytes);

    if (Boolean.parseBoolean(System.getProperty("jsonbench.validate", "true"))) {
      validate(dbDir);
    }
    System.exit(0);
  }

  /**
   * Reads the leading records back and fails loudly if the encoding is not the one the queries
   * assume.
   *
   * @param dbDir the loaded database directory
   */
  private static void validate(final Path dbDir) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String query = JsonBenchQueries.wrap(JsonBenchSchema.DATABASE, JsonBenchSchema.RESOURCE,
          "subsequence(for $e in $events[] return $e, 1, " + VALIDATION_SAMPLE + ")");
      final Sequence result = new Query(chain, query).execute(ctx);
      final StringWriter out = new StringWriter();
      try (PrintWriter pw = new PrintWriter(out)) {
        new StringSerializer(pw).serialize(result);
      }
      final List<JsonObject> sample = parseObjects(out.toString());
      if (sample.isEmpty()) {
        throw new IllegalStateException("validation failed: the resource holds no records");
      }
      final List<String> problems = new ArrayList<>();
      final JsonObject first = sample.getFirst();
      for (final String field : JsonBenchSchema.UNIVERSAL_FIELDS) {
        if (!first.has(field)) {
          problems.add("first record is missing the universal field " + field);
        }
      }
      // time_us drives Q3's integer arithmetic and Q4/Q5's min/max: a quoted value shreds as a
      // string node and turns those into a lexicographic comparison that still looks plausible.
      final JsonElement timeUs = first.get("time_us");
      if (timeUs != null && timeUs.isJsonPrimitive() && !((JsonPrimitive) timeUs).isNumber()) {
        problems.add("time_us is not a JSON number (" + timeUs
            + ") — microsecond stamps must not be quoted, or Q3/Q4/Q5 compare them as text");
      }
      // Only commit events carry commit.*; identity/account events legitimately do not, so the
      // check has to find a commit event rather than assume record 1 is one.
      final JsonObject commitEvent = firstCommitEvent(sample);
      if (commitEvent == null) {
        problems.add("no kind='commit' record among the first " + VALIDATION_SAMPLE
            + " — cannot verify the commit sub-object encoding");
      } else {
        final JsonElement commit = commitEvent.get("commit");
        if (commit == null || !commit.isJsonObject()) {
          problems.add("a kind='commit' record has no commit object: " + commitEvent);
        } else {
          final JsonObject commitObject = commit.getAsJsonObject();
          for (final String field : List.of("operation", "collection")) {
            final JsonElement value = commitObject.get(field);
            if (value == null) {
              problems.add("commit." + field + " is missing on a kind='commit' record");
            } else if (!value.isJsonPrimitive() || !((JsonPrimitive) value).isString()) {
              problems.add("commit." + field + " is not a JSON string (" + value + ")");
            }
          }
        }
      }
      if (!problems.isEmpty()) {
        throw new IllegalStateException("JSONBench encoding validation failed:\n  " + String.join("\n  ", problems));
      }
      System.out.printf("# validation OK: universal fields present, time_us is an exact integer (%s), "
          + "commit.* strings on commit events%n", timeUs);
    }
  }

  /** The first {@code kind = "commit"} object in the sample, or {@code null} if there is none. */
  private static JsonObject firstCommitEvent(final List<JsonObject> sample) {
    for (final JsonObject event : sample) {
      final JsonElement kind = event.get("kind");
      if (kind != null && kind.isJsonPrimitive() && JsonBenchSchema.KIND_COMMIT.equals(kind.getAsString())) {
        return event;
      }
    }
    return null;
  }

  /**
   * brackit serializes a sequence as whitespace-separated values; a lenient reader takes them one at
   * a time.
   */
  private static List<JsonObject> parseObjects(final String serialized) throws IOException {
    final List<JsonObject> objects = new ArrayList<>(VALIDATION_SAMPLE);
    if (serialized.isBlank()) {
      return objects;
    }
    try (JsonReader reader = new JsonReader(new StringReader(serialized))) {
      reader.setStrictness(Strictness.LENIENT);
      while (reader.peek() != JsonToken.END_DOCUMENT) {
        final JsonElement item = JsonParser.parseReader(reader);
        if (!item.isJsonObject()) {
          throw new IllegalStateException("validation failed: a record is not an object: " + item);
        }
        objects.add(item.getAsJsonObject());
      }
    }
    return objects;
  }

  /**
   * Sums every file below {@code dir}; JSONBench counts indexes and logs, i.e. everything.
   *
   * @param dir the database directory
   * @return total bytes on disk
   */
  public static long directorySize(final Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).mapToLong(path -> {
        try {
          return Files.size(path);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }).sum();
    }
  }

  /** Best-effort {@code sync(1)}; ignored where it is not available. */
  private static void sync() {
    try {
      new ProcessBuilder("sync").inheritIO().start().waitFor();
    } catch (final IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      System.out.println("# note: could not run sync(1): " + e.getMessage());
    }
  }
}
