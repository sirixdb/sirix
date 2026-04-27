package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks Brackit's query path against a Sirix-stored JSON dataset.
 *
 * <p>Tests the same query shapes that Brackit 0.9 vectorizes for the
 * file-scan path (filter-count, group-by, sum, avg) but here against
 * Sirix's persistent storage. The measurements baseline what SirixDB
 * needs to beat once it plugs into Brackit's
 * {@link io.brackit.query.compiler.optimizer.VectorizedExecutor}
 * interface (e.g. via {@code DirectPageScanner}).
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes=BrackitQueryOnSirixBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(value = 1,
    jvmArgs = { "--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED" })
@State(Scope.Benchmark)
public class BrackitQueryOnSirixBenchmark {

  @Param({ "10000", "100000" })
  public int recordCount;

  /** {@code true} → register {@link SirixVectorizedExecutor} for the query. */
  @Param({ "false", "true" })
  public boolean vectorized;

  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };
  private static final String JSON_DB = "bench-db";
  private static final String JSON_RESOURCE = "records.jn";

  private static final QNm DOC_VAR = new QNm("doc");

  private Path dbDir;
  private BasicJsonDBStore store;
  private SirixCompileChain chain;
  private SirixQueryContext ctx;
  private Database<JsonResourceSession> directDatabase;
  private JsonResourceSession resourceSession;
  private SirixVectorizedExecutor vecExecutor;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(ch.qos.logback.classic.Level.WARN);

    dbDir = Files.createTempDirectory("sirix-jmh-brackit");

    store = BasicJsonDBStore.newBuilder().location(dbDir).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    // Stream-shred via a generated Reader so that recordCount > 100k stays out
    // of memory. For 1B records the JSON would be ~64 GB if materialized.
    try (Reader src = new GeneratedRecordsReader(recordCount);
         JsonReader jsonReader = new JsonReader(src)) {
      store.create(JSON_DB, JSON_RESOURCE, jsonReader);
    }

    if (vectorized) {
      // Reuse the store's existing Database handle (don't open a second one
      // — Sirix tracks transactions per database, and a duplicate handle
      // exhausts the RevisionEpochTracker quickly).
      var coll = store.lookup(JSON_DB);
      resourceSession = coll.getDatabase().beginResourceSession(JSON_RESOURCE);
      int latestRev = resourceSession.getMostRecentRevisionNumber();
      // Parallel: ThreadLocal per-worker trx, opened lazily inside worker
      // threads to avoid cross-thread session affinity issues.
      vecExecutor = new SirixVectorizedExecutor(resourceSession, latestRev);
      SequentialPipelineStrategy.setVectorizedExecutor(vecExecutor);
    } else {
      SequentialPipelineStrategy.setVectorizedExecutor(null);
    }

    // Pre-bind $doc as an external variable (one trx for the whole trial)
    // to avoid jn:doc() opening a fresh trx per iteration — that exhausts
    // the RevisionEpochTracker (4096 slots) within the warmup window for
    // sub-millisecond vectorized queries.
    JsonDBCollection coll = (JsonDBCollection) store.lookup(JSON_DB);
    JsonDBItem docItem = (JsonDBItem) coll.getDocument();
    ctx.bind(DOC_VAR, (Sequence) docItem);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (vecExecutor != null) vecExecutor.close();
    if (resourceSession != null) resourceSession.close();
    if (directDatabase != null) directDatabase.close();
    if (chain != null) chain.close();
    if (store != null) store.close();
    Databases.removeDatabase(dbDir);
  }

  private void runQuery(Blackhole bh, String body) {
    String wrapped = "declare variable $doc external; " + body;
    var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, wrapped).execute(ctx));
    }
    bh.consume(buf);
  }

  /**
   * Generates a JSON array of {@code [{"id":i,"age":..,"dept":..,"city":..,"active":..}, ...]}
   * lazily into a char buffer so that callers (Gson's {@link JsonReader}) can pull
   * arbitrarily large datasets without materializing the JSON string.
   */
  private static final class GeneratedRecordsReader extends Reader {
    private final long total;
    private final Random rng = new Random(42);
    private final StringBuilder line = new StringBuilder(96);
    private long produced = 0;
    private int pos = 0;
    private boolean opened = false;
    private boolean closed = false;

    GeneratedRecordsReader(long total) {
      this.total = total;
    }

    private void refill() {
      line.setLength(0);
      pos = 0;
      if (!opened) {
        line.append('[');
        opened = true;
        return;
      }
      if (produced < total) {
        if (produced > 0) line.append(',');
        line.append("{\"id\":").append(produced)
            .append(",\"age\":").append(18 + rng.nextInt(48))
            .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
            .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
            .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
            .append('}');
        produced++;
        return;
      }
      if (!closed) {
        line.append(']');
        closed = true;
      }
    }

    @Override
    public int read(char[] cbuf, int off, int len) {
      if (pos >= line.length()) {
        if (closed) return -1;
        refill();
        if (pos >= line.length()) return -1;
      }
      int n = Math.min(len, line.length() - pos);
      line.getChars(pos, pos + n, cbuf, off);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      // no-op
    }
  }

  @Benchmark
  public void filterCount(Blackhole bh) {
    runQuery(bh, "count(for $u in $doc[] where $u.age > 40 and $u.active return $u)");
  }

  @Benchmark
  public void groupByDept(Blackhole bh) {
    runQuery(bh, "for $u in $doc[] let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
  }

  @Benchmark
  public void sumAge(Blackhole bh) {
    runQuery(bh, "sum(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void avgAge(Blackhole bh) {
    runQuery(bh, "avg(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void minMaxAge(Blackhole bh) {
    runQuery(bh, "{\"min\": min(for $u in $doc[] return $u.age), \"max\": max(for $u in $doc[] return $u.age)}");
  }

  @Benchmark
  public void groupBy2Keys(Blackhole bh) {
    runQuery(bh, "for $u in $doc[] let $d := $u.dept, $c := $u.city group by $d, $c return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Benchmark
  public void filterGroupBy(Blackhole bh) {
    runQuery(bh, "for $u in $doc[] where $u.active let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
  }

  @Benchmark
  public void countDistinct(Blackhole bh) {
    runQuery(bh, "count(for $u in $doc[] let $d := $u.dept group by $d return $d)");
  }

  @Benchmark
  public void compoundAndFilterCount(Blackhole bh) {
    runQuery(bh, "count(for $u in $doc[] where $u.age > 30 and $u.age < 50 and $u.active return $u)");
  }
}
