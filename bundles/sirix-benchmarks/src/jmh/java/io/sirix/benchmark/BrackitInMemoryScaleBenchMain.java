package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.function.json.FastJSONParser;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Pure-Brackit (in-memory, no Sirix storage) baseline for the aggregate
 * scale-bench query set.
 * <p>
 * Generates the exact same synthetic dataset as
 * {@link BrackitQueryOnSirixScaleMain} — {@code Random(42)}, ages
 * {@code 18 + nextInt(48)}, identical DEPTS/CITIES pools — parses it with
 * Brackit's {@link FastJSONParser} into Brackit's own in-memory JSON items and
 * runs the identical query set through a plain {@link CompileChain} with no
 * vectorized executor installed. This measures Brackit's stock interpreted
 * aggregate/group-by pipeline, giving an apples-to-apples baseline against:
 * <ul>
 *   <li>Sirix + vectorized executor (generic columnar scan), and</li>
 *   <li>Sirix + vectorized executor + covering projection index.</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   java io.sirix.benchmark.BrackitInMemoryScaleBenchMain &lt;recordCount&gt; [iters=N]
 * </pre>
 */
public final class BrackitInMemoryScaleBenchMain {

  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };
  private static final QNm DOC_VAR = new QNm("doc");

  private static final Map<String, String> QUERIES = new LinkedHashMap<>();
  static {
    QUERIES.put("filterCount",            "count(for $u in $doc[] where $u.age > 40 and $u.active return $u)");
    QUERIES.put("groupByDept",            "for $u in $doc[] let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
    QUERIES.put("sumAge",                 "sum(for $u in $doc[] return $u.age)");
    QUERIES.put("avgAge",                 "avg(for $u in $doc[] return $u.age)");
    QUERIES.put("minMaxAge",              "{\"min\": min(for $u in $doc[] return $u.age), \"max\": max(for $u in $doc[] return $u.age)}");
    QUERIES.put("groupBy2Keys",           "for $u in $doc[] let $d := $u.dept, $c := $u.city group by $d, $c return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
    QUERIES.put("filterGroupBy",          "for $u in $doc[] where $u.active let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
    QUERIES.put("countDistinct",          "count(for $u in $doc[] let $d := $u.dept group by $d return $d)");
    QUERIES.put("compoundAndFilterCount", "count(for $u in $doc[] where $u.age > 30 and $u.age < 50 and $u.active return $u)");
    QUERIES.put("filterGroupByAge",       "for $u in $doc[] where $u.age > 40 let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
  }

  private BrackitInMemoryScaleBenchMain() {
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: BrackitInMemoryScaleBenchMain <recordCount> [iters=N]");
      System.exit(1);
    }
    final long recordCount = Long.parseLong(args[0]);
    final int iters = args.length < 2 ? 3 : Integer.parseInt(args[1]);

    final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(ch.qos.logback.classic.Level.WARN);

    System.out.printf("# Engine: pure Brackit (in-memory items, no Sirix)   Records: %,d   Iters: %d%n",
                      recordCount, iters);

    // Generate the dataset as UTF-8 bytes and hand it to FastJSONParser.
    // ASCII-only payload, so bytes == chars; pre-sized to avoid growth copies.
    final long tGen = System.nanoTime();
    final byte[] json = generateJson(recordCount);
    System.out.printf("# Generated JSON: %,d bytes in %,d ms%n",
                      json.length, (System.nanoTime() - tGen) / 1_000_000L);

    final long tParse = System.nanoTime();
    final Item doc = new FastJSONParser(json, 0, json.length).parse();
    System.out.printf("# Parsed to in-memory Brackit items in %,d ms%n",
                      (System.nanoTime() - tParse) / 1_000_000L);

    final CompileChain chain = new CompileChain();
    final QueryContext ctx = new BrackitQueryContext();
    ctx.bind(DOC_VAR, (Sequence) doc);

    System.out.printf("%-26s | %10s | %10s | %10s | %10s%n", "query", "min(ms)", "avg(ms)", "max(ms)", "result_bytes");
    System.out.printf("%-26s + %10s + %10s + %10s + %10s%n", "--------------------------",
                      "----------", "----------", "----------", "------------");

    for (final Map.Entry<String, String> e : QUERIES.entrySet()) {
      runQueryRepeated(chain, ctx, e.getKey(), e.getValue(), iters);
    }
  }

  private static byte[] generateJson(final long recordCount) throws Exception {
    if (recordCount > 20_000_000L) {
      // ~90 bytes/record: beyond ~20 M the byte[] would push past 1.8 GB and
      // the parsed item tree past typical heap sizes for this baseline.
      throw new IllegalArgumentException("recordCount too large for the in-memory baseline: " + recordCount);
    }
    final Random rng = new Random(42);
    final int sizeHint = (int) Math.min(Integer.MAX_VALUE - 16L, recordCount * 92L + 16L);
    final ByteArrayOutputStream bos = new ByteArrayOutputStream(sizeHint);
    try (Writer w = new OutputStreamWriter(bos, StandardCharsets.US_ASCII)) {
      w.write('[');
      final StringBuilder line = new StringBuilder(96);
      for (long i = 0; i < recordCount; i++) {
        line.setLength(0);
        if (i > 0) {
          line.append(',');
        }
        line.append("{\"id\":").append(i)
            .append(",\"age\":").append(18 + rng.nextInt(48))
            .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
            .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
            .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
            .append('}');
        w.write(line.toString());
      }
      w.write(']');
    }
    return bos.toByteArray();
  }

  private static void runQueryRepeated(final CompileChain chain, final QueryContext ctx,
                                       final String name, final String body, final int iters) {
    final String wrapped = "declare variable $doc external; " + body;

    // Warm up enough for HotSpot to tier-up the interpreted pipeline; capped
    // by a 5 s budget so very slow query shapes don't stall the run.
    // Brackit has no result caches, so warm iterations only remove JIT noise.
    final boolean noWarmup = Boolean.getBoolean("sirix.noWarmup");
    final int warmupCount = noWarmup ? 0 : Math.max(3, Math.min(20, iters));
    final long warmDeadline = System.nanoTime() + 5_000_000_000L;
    try {
      for (int i = 0; i < warmupCount && System.nanoTime() < warmDeadline; i++) {
        runOnce(chain, ctx, wrapped);
      }
    } catch (final RuntimeException re) {
      System.out.printf("%-26s | (skipped: %s)%n", name, re.getMessage());
      return;
    }

    long min = Long.MAX_VALUE;
    long max = 0;
    long sum = 0;
    int bytes = 0;
    try {
      for (int i = 0; i < iters; i++) {
        final long t0 = System.nanoTime();
        bytes = runOnce(chain, ctx, wrapped);
        final long elapsed = System.nanoTime() - t0;
        sum += elapsed;
        if (elapsed < min) {
          min = elapsed;
        }
        if (elapsed > max) {
          max = elapsed;
        }
      }
    } catch (final RuntimeException re) {
      System.out.printf("%-26s | (aborted iter: %s)%n", name, re.getMessage());
      return;
    }
    final double minMs = min / 1e6;
    final double maxMs = max / 1e6;
    final double avgMs = (sum / (double) iters) / 1e6;
    System.out.printf("%-26s | %10.3f | %10.3f | %10.3f | %,10d%n",
                      name, minMs, avgMs, maxMs, bytes);
  }

  private static int runOnce(final CompileChain chain, final QueryContext ctx, final String wrapped) {
    final var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, wrapped).execute(ctx));
    }
    return buf.toString().length();
  }
}
