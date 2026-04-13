package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
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

  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };

  private Path dbDir;
  private BasicJsonDBStore store;
  private SirixCompileChain chain;
  private SirixQueryContext ctx;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(ch.qos.logback.classic.Level.WARN);

    dbDir = Files.createTempDirectory("sirix-jmh-brackit");

    // Build a JSON array of records and shred it into a Sirix resource.
    Random rng = new Random(42);
    StringBuilder sb = new StringBuilder(recordCount * 64);
    sb.append('[');
    for (int i = 0; i < recordCount; i++) {
      if (i > 0) sb.append(',');
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(18 + rng.nextInt(48))
        .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
        .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
        .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
        .append('}');
    }
    sb.append(']');

    store = BasicJsonDBStore.newBuilder().location(dbDir).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    String storeQuery = "jn:store('bench-db','records.jn','" + sb.toString().replace("'", "''") + "')";
    new Query(chain, storeQuery).evaluate(ctx);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (chain != null) chain.close();
    if (store != null) store.close();
    Databases.removeDatabase(dbDir);
  }

  private void runQuery(Blackhole bh, String body) {
    String wrapped = "let $doc := jn:doc('bench-db','records.jn') " + body;
    var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, wrapped).execute(ctx));
    }
    bh.consume(buf);
  }

  @Benchmark
  public void filterCount(Blackhole bh) {
    runQuery(bh, "return count(for $u in $doc[] where $u.age > 40 and $u.active return $u)");
  }

  @Benchmark
  public void groupByDept(Blackhole bh) {
    runQuery(bh, "for $u in $doc[] let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
  }

  @Benchmark
  public void sumAge(Blackhole bh) {
    runQuery(bh, "return sum(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void avgAge(Blackhole bh) {
    runQuery(bh, "return avg(for $u in $doc[] return $u.age)");
  }

  @Benchmark
  public void minMaxAge(Blackhole bh) {
    runQuery(bh, "return {\"min\": min(for $u in $doc[] return $u.age), \"max\": max(for $u in $doc[] return $u.age)}");
  }
}
