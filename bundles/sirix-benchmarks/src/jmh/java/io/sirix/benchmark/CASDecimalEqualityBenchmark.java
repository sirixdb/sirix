/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * What the {@code xs:decimal} key encoding costs a HOT CAS equality lookup, measured against the
 * types whose bytes are free.
 *
 * <h2>Why this exists</h2>
 * <p>
 * A trie has nowhere to put type semantics but the <em>encoding</em>. For a type whose natural
 * bytes already sort correctly — raw UTF-8, a sign-flipped integer, a canonicalized instant — that
 * costs nothing: {@link io.sirix.index.hot.CASKeySerializer} writes the bytes it would have written
 * anyway. {@code xs:decimal} is where it stops being free. Arbitrary-precision values do not fit a
 * fixed-width order-preserving word, so the encoder writes an eight-byte double prefix <em>and</em>
 * an exact normalized-digit suffix behind it, sign-complemented and terminated, so that
 * {@code 1.50} and {@code 1.5} share a key while {@code -0.5} still outranks
 * {@code -0.5000000000000000001}.
 *
 * <p>
 * That suffix is per-probe work: {@link BigDecimal#stripTrailingZeros()},
 * {@link BigDecimal#toPlainString()}, an ASCII copy. An earlier attempt at exactness made decimal
 * equality materially slower and was rejected on those grounds, so the current encoding needs a
 * number rather than an argument — this is that number, and it is the thing to point at if the
 * encoder is ever changed again.
 *
 * <h2>Reading the result</h2>
 * <p>
 * The arms are a 2x2: the same literal text is indexed twice, once as {@code xs:decimal} and once
 * as {@code xs:string}. The string arm is not a separate question — it is the control. It walks the
 * same document, the same index shape, the same query and the same key <em>width</em>, and its
 * encoder does nothing but copy bytes. So {@code DEC} minus {@code STR} at a fixed value width is
 * the decimal encoding's cost, with descent, in-leaf search and posting materialization subtracted
 * out by construction.
 *
 * <p>
 * The width axis is there because the suffix is the mechanism under suspicion and its cost is
 * proportional to its length: {@code WIDE} carries 25 significant digits against {@code NARROW}'s
 * money-shaped two decimal places. Reading the width axis alone would be a trap — a wider value
 * also gives the trie a longer common prefix to discriminate through, which the string control pays
 * too. Only the DEC-minus-STR gap, and how that gap moves with width, says anything about the
 * encoder.
 *
 * <p>
 * Run it:
 *
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes=CASDecimalEqualityBenchmark \
 *       -Pjmh.warmupIterations=5 -Pjmh.iterations=10
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
public class CASDecimalEqualityBenchmark {

  /** Distinct indexed values — enough that the trie has real depth rather than one leaf. */
  private static final int DISTINCT_VALUES = 10_000;

  /**
   * Every value appears twice, so each equality answer is a posting list rather than a singleton. A
   * benchmark whose answer is empty measures the failure path at full speed and looks excellent, so
   * {@link #setUp} asserts this exact cardinality before any timing runs.
   */
  private static final int POSTINGS_PER_VALUE = 2;

  private static final String VALUE_PATH = "/[]/v";

  private static final String RESOURCE = "cas-decimal-equality";

  /** The literal text the document holds — decimal spellings at two widths. */
  @Param({"NARROW", "WIDE"})
  public String width;

  /** The content type that same text is indexed under. {@code STR} is the free-encoding control. */
  @Param({"DEC", "STR"})
  public String contentType;

  private File databaseDirectory;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private JsonNodeReadOnlyTrx rtx;
  private IndexController<?, ?> controller;
  private IndexDef indexDef;
  private Set<String> queriedPaths;

  /** Pre-built so the timed region measures the index, not {@code new BigDecimal(String)}. */
  private Atomic[] probes;

  private int probeIndex;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    final ValueWidth valueWidth = ValueWidth.valueOf(width);
    final Encoding encoding = Encoding.valueOf(contentType);

    final String[] values = new String[DISTINCT_VALUES];
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      values[i] = valueWidth.literal(i);
    }

    // createTempDirectory creates the directory; Sirix insists on creating it itself.
    databaseDirectory = Files.createTempDirectory("sirix-cas-decimal-bench").toFile();
    Files.delete(databaseDirectory.toPath());
    Databases.createJsonDatabase(new DatabaseConfiguration(databaseDirectory.toPath()));
    database = Databases.openJsonDatabase(databaseDirectory.toPath());
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());

    queriedPaths = Set.of(VALUE_PATH);
    indexDef = buildIndex(values, encoding);

    session = database.beginResourceSession(RESOURCE);
    rtx = session.beginNodeReadOnlyTrx();
    controller = session.getRtxIndexController(rtx.getRevisionNumber());

    probes = new Atomic[DISTINCT_VALUES];
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      probes[i] = encoding.probe(values[i]);
    }
    probeIndex = 0;

    // Check every probe once, not a sample: an encoding that resolved 9,999 of 10,000 would pass a
    // spot check and then be timed largely on its miss path, which is the cheaper one.
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      final long cardinality = totalCardinality(probes[i]);
      if (cardinality != POSTINGS_PER_VALUE) {
        throw new IllegalStateException("width=" + width + " contentType=" + contentType + " probe=" + values[i]
            + " resolved to " + cardinality + " postings, expected " + POSTINGS_PER_VALUE
            + " — the benchmark would be timing a lookup that does not find its value");
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (rtx != null) {
      rtx.close();
    }
    if (session != null) {
      session.close();
    }
    if (database != null) {
      database.close();
    }
    if (databaseDirectory != null && databaseDirectory.exists()) {
      Databases.removeDatabase(databaseDirectory.toPath());
      deleteRecursively(databaseDirectory);
    }
  }

  /**
   * One equality lookup, drained to its postings. Summing the cardinality rather than returning the
   * iterator keeps the drain from being optimized away and keeps the arms comparable: each must
   * materialize the same number of node keys.
   */
  @Benchmark
  public long equalityLookup() {
    final Atomic probe = probes[probeIndex];
    probeIndex = probeIndex + 1 == DISTINCT_VALUES
        ? 0
        : probeIndex + 1;
    return totalCardinality(probe);
  }

  private long totalCardinality(final Atomic probe) {
    final Iterator<NodeReferences> hits = controller.openCASIndex(rtx.getStorageEngineReader(), indexDef,
        controller.createCASFilter(queriedPaths, probe, SearchMode.EQUAL, new JsonPCRCollector(rtx)));
    long cardinality = 0;
    while (hits.hasNext()) {
      cardinality += hits.next().cardinality();
    }
    return cardinality;
  }

  // ==================== fixture ====================

  /**
   * How wide the decimal spellings are. Both are valid {@code xs:decimal} lexical forms, so the same
   * text feeds both encodings unchanged — which is what makes the DEC-minus-STR difference
   * attributable to the encoder and nothing else.
   */
  private enum ValueWidth {

    /** Money-shaped: {@code 0.00} to {@code 99.99}, a short exact suffix. */
    NARROW {
      @Override
      String literal(final int i) {
        return (i / 100) + "." + (i % 100 < 10
            ? "0"
            : "") + (i % 100);
      }
    },

    /** 25 significant digits, well past what a {@code double} prefix can discriminate. */
    WIDE {
      @Override
      String literal(final int i) {
        return "12345678901234." + String.format("%010d", i) + "56789";
      }
    };

    abstract String literal(int i);
  }

  /**
   * The content type the text is indexed under, and the probe atomic that goes with it. The document
   * always holds JSON <em>strings</em>, for both: the CAS builder converts to the declared content
   * type either way, and holding the spelling constant keeps the wide arm from being silently
   * truncated by JSON number parsing — which would turn the arm that exists to stress the suffix into
   * one that has no suffix at all.
   */
  private enum Encoding {

    /** The subject: an eight-byte double prefix plus an exact normalized-digit suffix. */
    DEC(Type.DEC) {
      @Override
      Atomic probe(final String literal) {
        return new Dec(new BigDecimal(literal));
      }
    },

    /** The control: raw UTF-8, where byte order already is value order and the encoder just copies. */
    STR(Type.STR) {
      @Override
      Atomic probe(final String literal) {
        return new Str(literal);
      }
    };

    private final Type indexType;

    Encoding(final Type indexType) {
      this.indexType = indexType;
    }

    Type indexType() {
      return indexType;
    }

    abstract Atomic probe(String literal);
  }

  private IndexDef buildIndex(final String[] values, final Encoding encoding) {
    try (final var writeSession = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx trx = writeSession.beginNodeTrx()) {
      final var wtxController = writeSession.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef def =
          IndexDefs.createCASIdxDef(false, encoding.indexType(), parsePaths(queriedPaths), 0, IndexDef.DbType.JSON);
      wtxController.createIndexes(Set.of(def), trx);
      new JsonShredder.Builder(trx, JsonShredder.createStringReader(json(values)),
          InsertPosition.AS_FIRST_CHILD).build().call();
      trx.commit();
      return def;
    }
  }

  /**
   * The fixture as a JSON array of <code>{"v": "&lt;literal&gt;"}</code>, each literal repeated
   * {@link #POSTINGS_PER_VALUE} times. Pre-sized because the wide-decimal arm builds roughly 800 kB
   * of text and growing a {@link StringBuilder} into that is pure setup waste.
   */
  private static String json(final String[] values) {
    final int perEntry = values[0].length() + 16;
    final StringBuilder sb = new StringBuilder(values.length * POSTINGS_PER_VALUE * perEntry + 2);
    sb.append('[');
    for (int copy = 0; copy < POSTINGS_PER_VALUE; copy++) {
      for (final String value : values) {
        if (sb.length() > 1) {
          sb.append(',');
        }
        sb.append("{\"v\":\"").append(value).append("\"}");
      }
    }
    return sb.append(']').toString();
  }

  private static Set<Path<QNm>> parsePaths(final Set<String> paths) {
    final Set<Path<QNm>> parsed = new HashSet<>(paths.size());
    for (final String path : paths) {
      parsed.add(Path.parse(path, PathParser.Type.JSON));
    }
    return parsed;
  }

  private static void deleteRecursively(final File root) throws IOException {
    if (!root.exists()) {
      return;
    }
    try (final var walk = Files.walk(root.toPath())) {
      walk.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }
  }
}
