package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Baseline hot-path benchmark for JSON/XML insert, update, traversal and commit operations.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgs = {
    "--add-modules=jdk.incubator.vector",
    "--enable-preview",
    "--enable-native-access=ALL-UNNAMED"
})
public class HotPathBaselineBenchmark {

  private static final String JSON_RESOURCE = "jsonResource";
  private static final String XML_RESOURCE = "xmlResource";
  private static final int SEED_CHILD_COUNT = 64;
  private static final int UPDATE_VALUE_VARIANTS = 32;
  private static final int UPDATE_NAME_VARIANTS = 16;
  private static final QNm XML_ROOT = new QNm("root");
  private static final QNm XML_CHILD = new QNm("n");
  private static final String XML_TEXT = "x";

  private static void clampLoggingForBenchmarks() {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    rootLogger.setLevel(ch.qos.logback.classic.Level.WARN);
  }

  @State(Scope.Thread)
  public static class JsonState {
    private Path databasePath;
    private Database<JsonResourceSession> database;
    private final Integer[] cachedValues = new Integer[UPDATE_VALUE_VARIANTS];
    private int valueIndex;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      clampLoggingForBenchmarks();

      for (int i = 0; i < cachedValues.length; i++) {
        cachedValues[i] = Integer.valueOf(i);
      }

      databasePath = Files.createTempDirectory("sirix-jmh-json-hotpath");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);
      database.createResource(ResourceConfiguration.newBuilder(JSON_RESOURCE).build());

      try (var session = database.beginResourceSession(JSON_RESOURCE);
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        for (int i = 0; i < SEED_CHILD_COUNT; i++) {
          wtx.insertNumberValueAsFirstChild(cachedValues[i & (UPDATE_VALUE_VARIANTS - 1)]);
          wtx.moveToParent();
        }
        wtx.commit();
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      if (database != null) {
        database.close();
      }
      if (databasePath != null) {
        Databases.removeDatabase(databasePath);
      }
    }

    private Integer nextValue() {
      return cachedValues[(valueIndex++) & (UPDATE_VALUE_VARIANTS - 1)];
    }
  }

  @State(Scope.Thread)
  public static class XmlState {
    private Path databasePath;
    private Database<XmlResourceSession> database;
    private final QNm[] cachedNames = new QNm[UPDATE_NAME_VARIANTS];
    private int nameIndex;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      clampLoggingForBenchmarks();

      for (int i = 0; i < cachedNames.length; i++) {
        cachedNames[i] = new QNm("n" + i);
      }

      databasePath = Files.createTempDirectory("sirix-jmh-xml-hotpath");
      Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openXmlDatabase(databasePath);
      database.createResource(ResourceConfiguration.newBuilder(XML_RESOURCE).build());

      try (var session = database.beginResourceSession(XML_RESOURCE);
           XmlNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertElementAsFirstChild(XML_ROOT);
        for (int i = 0; i < SEED_CHILD_COUNT; i++) {
          wtx.insertElementAsFirstChild(XML_CHILD);
          wtx.insertTextAsFirstChild(XML_TEXT);
          wtx.moveToParent();
          wtx.moveToParent();
        }
        wtx.commit();
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      if (database != null) {
        database.close();
      }
      if (databasePath != null) {
        Databases.removeDatabase(databasePath);
      }
    }

    private QNm nextName() {
      return cachedNames[(nameIndex++) & (UPDATE_NAME_VARIANTS - 1)];
    }
  }

  @Benchmark
  public void jsonInsertCommit(final JsonState state) {
    try (var session = state.database.beginResourceSession(JSON_RESOURCE);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertArrayAsFirstChild();
      }
      wtx.insertNumberValueAsFirstChild(state.nextValue());
      wtx.commit();
    }
  }

  @Benchmark
  public void jsonUpdateCommit(final JsonState state) {
    try (var session = state.database.beginResourceSession(JSON_RESOURCE);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertArrayAsFirstChild();
      }
      if (!wtx.moveToFirstChild()) {
        wtx.insertNumberValueAsFirstChild(state.nextValue());
      } else {
        wtx.setNumberValue(state.nextValue());
      }
      wtx.commit();
    }
  }

  @Benchmark
  public void jsonTraverse(final JsonState state, final Blackhole blackhole) {
    long checksum = 0L;
    int traversed = 0;

    try (var session = state.database.beginResourceSession(JSON_RESOURCE);
         var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      if (rtx.moveToFirstChild() && rtx.moveToFirstChild()) {
        do {
          checksum += rtx.getNodeKey();
          traversed++;
        } while (traversed < SEED_CHILD_COUNT && rtx.moveToRightSibling());
      }
    }

    blackhole.consume(checksum);
    blackhole.consume(traversed);
  }

  @Benchmark
  public void xmlInsertCommit(final XmlState state) {
    try (var session = state.database.beginResourceSession(XML_RESOURCE);
         XmlNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertElementAsFirstChild(XML_ROOT);
      }
      wtx.insertElementAsFirstChild(XML_CHILD);
      wtx.insertTextAsFirstChild(XML_TEXT);
      wtx.commit();
    }
  }

  @Benchmark
  public void xmlUpdateCommit(final XmlState state) {
    try (var session = state.database.beginResourceSession(XML_RESOURCE);
         XmlNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertElementAsFirstChild(XML_ROOT);
      }
      if (!wtx.moveToFirstChild()) {
        wtx.insertElementAsFirstChild(XML_CHILD);
      } else {
        wtx.setName(state.nextName());
      }
      wtx.commit();
    }
  }

  @Benchmark
  public void xmlTraverse(final XmlState state, final Blackhole blackhole) {
    long checksum = 0L;
    int traversed = 0;

    try (var session = state.database.beginResourceSession(XML_RESOURCE);
         var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      if (rtx.moveToFirstChild() && rtx.moveToFirstChild()) {
        do {
          checksum += rtx.getNodeKey();
          traversed++;
        } while (traversed < SEED_CHILD_COUNT && rtx.moveToRightSibling());
      }
    }

    blackhole.consume(checksum);
    blackhole.consume(traversed);
  }
}
