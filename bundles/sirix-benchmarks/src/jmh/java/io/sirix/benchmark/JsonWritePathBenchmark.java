package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.IndexBackendType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.io.bytepipe.LZ4Compressor;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
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
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Focused write-path benchmarks for current JSON hot spots.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*JsonWritePathBenchmark.*'
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
public class JsonWritePathBenchmark {

  private static final String RESOURCE = "jsonWritePath";
  private static final int VALUE_VARIANTS = 256;
  private static final int OBJECT_KEY_VARIANTS = 128;
  private static final int STRING_LENGTH = 192;
  private static final String HIGH_ENTROPY_ALPHABET =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  private static final int MOVE_KEYS = 2048;

  public enum CompressionPipeline {
    NONE,
    LZ4,
    FFI_LZ4
  }

  private static void clampLoggingForBenchmarks() {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    rootLogger.setLevel(ch.qos.logback.classic.Level.WARN);
  }

  @State(Scope.Thread)
  public abstract static class ResourceParamState {
    @Param({"FILE_CHANNEL"})
    public StorageType storageType;

    @Param({"SLIDING_SNAPSHOT"})
    public VersioningType versioningType;

    @Param({"ROLLING"})
    public HashType hashKind;

    @Param({"3"})
    public int maxRevisionsToRestore;

    @Param({"true"})
    public boolean buildPathSummary;

    @Param({"true"})
    public boolean storeDiffs;

    @Param({"false"})
    public boolean storeNodeHistory;

    @Param({"true"})
    public boolean storeChildCount;

    @Param({"false"})
    public boolean useTextCompression;

    @Param({"false"})
    public boolean useDeweyIDs;

    @Param({"16"})
    public int deweyIdSiblingDistance;

    @Param({"NONE"})
    public StringCompressionType stringCompressionType;

    @Param({"HOT"})
    public IndexBackendType indexBackendType;

    @Param({"FFI_LZ4"})
    public CompressionPipeline compressionPipeline;

    private ByteHandlerPipeline newByteHandlerPipeline() {
      return switch (compressionPipeline) {
        case NONE -> new ByteHandlerPipeline();
        case LZ4 -> new ByteHandlerPipeline(new LZ4Compressor());
        case FFI_LZ4 -> new ByteHandlerPipeline(new FFILz4Compressor());
      };
    }

    protected ResourceConfiguration.Builder newResourceConfig(final String resource) {
      return ResourceConfiguration.newBuilder(resource)
                                  .storageType(storageType)
                                  .versioningApproach(versioningType)
                                  .hashKind(hashKind)
                                  .maxNumberOfRevisionsToRestore(maxRevisionsToRestore)
                                  .storeDiffs(storeDiffs)
                                  .storeNodeHistory(storeNodeHistory)
                                  .storeChildCount(storeChildCount)
                                  .useTextCompression(useTextCompression)
                                  .buildPathSummary(buildPathSummary)
                                  .useDeweyIDs(useDeweyIDs)
                                  .deweyIdSiblingDistance(deweyIdSiblingDistance)
                                  .stringCompressionType(stringCompressionType)
                                  .indexBackendType(indexBackendType)
                                  .byteHandlerPipeline(newByteHandlerPipeline());
    }
  }

  @State(Scope.Thread)
  public static class InsertState extends ResourceParamState {
    private Path databasePath;
    private Database<JsonResourceSession> database;
    private final Integer[] cachedValues = new Integer[VALUE_VARIANTS];
    private int valueIndex;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      clampLoggingForBenchmarks();

      for (int i = 0; i < cachedValues.length; i++) {
        cachedValues[i] = Integer.valueOf(i);
      }

      databasePath = Files.createTempDirectory("sirix-jmh-json-write-insert");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);

      database.createResource(newResourceConfig(RESOURCE).build());

      // Seed one array with one value so benchmarked op always takes the sibling insert path.
      try (var session = database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        wtx.insertNumberValueAsFirstChild(cachedValues[0]);
        wtx.moveToParent();
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
      return cachedValues[(valueIndex++) & (VALUE_VARIANTS - 1)];
    }
  }

  @Benchmark
  public void insertNumberAsRightSiblingCommit(final InsertState state) {
    try (var session = state.database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertArrayAsFirstChild();
      }

      if (!wtx.moveToFirstChild()) {
        wtx.insertNumberValueAsFirstChild(state.nextValue());
      } else {
        wtx.insertNumberValueAsRightSibling(state.nextValue());
      }
      wtx.commit();
    }
  }

  @Benchmark
  public void insertNumberAsRightSiblingRollback(final InsertState state) {
    try (var session = state.database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertArrayAsFirstChild();
      }

      if (!wtx.moveToFirstChild()) {
        wtx.insertNumberValueAsFirstChild(state.nextValue());
      } else {
        wtx.insertNumberValueAsRightSibling(state.nextValue());
      }
      wtx.rollback();
    }
  }

  @State(Scope.Thread)
  public static class StringInsertState extends ResourceParamState {
    @Param({"LOW_ENTROPY", "HIGH_ENTROPY"})
    public String payloadKind;

    private Path databasePath;
    private Database<JsonResourceSession> database;
    private final String[] cachedValues = new String[VALUE_VARIANTS];
    private int valueIndex;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      clampLoggingForBenchmarks();

      final boolean highEntropy = "HIGH_ENTROPY".equals(payloadKind);
      for (int i = 0; i < cachedValues.length; i++) {
        cachedValues[i] = highEntropy ? highEntropyValue(i) : lowEntropyValue(i);
      }

      databasePath = Files.createTempDirectory("sirix-jmh-json-write-string");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);

      database.createResource(newResourceConfig(RESOURCE).build());

      // Seed one array with one value so benchmarked op always takes the sibling insert path.
      try (var session = database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        wtx.insertStringValueAsFirstChild(cachedValues[0]);
        wtx.moveToParent();
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

    private static String lowEntropyValue(final int index) {
      final StringBuilder builder = new StringBuilder(STRING_LENGTH + 32);
      builder.append("msg-").append(index).append(':');
      while (builder.length() < STRING_LENGTH) {
        builder.append("common prefix chicago sirix ");
      }
      builder.setLength(STRING_LENGTH);
      return builder.toString();
    }

    private static String highEntropyValue(final int index) {
      final char[] chars = new char[STRING_LENGTH];
      int state = index * 0x9E3779B9 + 0x7F4A7C15;
      for (int i = 0; i < chars.length; i++) {
        state = state * 1664525 + 1013904223;
        final int alphabetIndex = (state >>> 24) & 63;
        chars[i] = HIGH_ENTROPY_ALPHABET.charAt(alphabetIndex);
      }
      return new String(chars);
    }

    private String nextValue() {
      return cachedValues[(valueIndex++) & (VALUE_VARIANTS - 1)];
    }
  }

  @Benchmark
  public void insertStringAsRightSiblingCommit(final StringInsertState state) {
    try (var session = state.database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertArrayAsFirstChild();
      }

      if (!wtx.moveToFirstChild()) {
        wtx.insertStringValueAsFirstChild(state.nextValue());
      } else {
        wtx.insertStringValueAsRightSibling(state.nextValue());
      }
      wtx.commit();
    }
  }

  @Benchmark
  public void insertStringAsRightSiblingRollback(final StringInsertState state) {
    try (var session = state.database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertArrayAsFirstChild();
      }

      if (!wtx.moveToFirstChild()) {
        wtx.insertStringValueAsFirstChild(state.nextValue());
      } else {
        wtx.insertStringValueAsRightSibling(state.nextValue());
      }
      wtx.rollback();
    }
  }

  @State(Scope.Thread)
  public static class ObjectRecordStringInsertState extends ResourceParamState {
    @Param({"LOW_ENTROPY", "HIGH_ENTROPY"})
    public String payloadKind;

    private Path databasePath;
    private Database<JsonResourceSession> database;
    private final String[] cachedValues = new String[VALUE_VARIANTS];
    private final StringValue[] cachedObjectValues = new StringValue[VALUE_VARIANTS];
    private final String[] cachedKeys = new String[OBJECT_KEY_VARIANTS];
    private int valueIndex;
    private int keyIndex;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      clampLoggingForBenchmarks();

      final boolean highEntropy = "HIGH_ENTROPY".equals(payloadKind);
      for (int i = 0; i < cachedValues.length; i++) {
        final String value = highEntropy ? StringInsertState.highEntropyValue(i) : StringInsertState.lowEntropyValue(i);
        cachedValues[i] = value;
        cachedObjectValues[i] = new StringValue(value);
      }
      for (int i = 0; i < cachedKeys.length; i++) {
        cachedKeys[i] = "field_" + i;
      }

      databasePath = Files.createTempDirectory("sirix-jmh-json-write-object-record-string");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);

      database.createResource(newResourceConfig(RESOURCE).build());

      // Seed one object with one object-record so benchmarked op always takes the right sibling path.
      try (var session = database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        wtx.insertObjectRecordAsFirstChild(cachedKeys[0], cachedObjectValues[0]);
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

    private String nextKey() {
      return cachedKeys[(keyIndex++) & (OBJECT_KEY_VARIANTS - 1)];
    }

    private StringValue nextObjectValue() {
      return cachedObjectValues[(valueIndex++) & (VALUE_VARIANTS - 1)];
    }
  }

  @Benchmark
  public void insertObjectRecordStringAsRightSiblingCommit(final ObjectRecordStringInsertState state) {
    try (var session = state.database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertObjectAsFirstChild();
      }

      if (!wtx.moveToFirstChild()) {
        wtx.insertObjectRecordAsFirstChild(state.nextKey(), state.nextObjectValue());
      } else {
        wtx.insertObjectRecordAsRightSibling(state.nextKey(), state.nextObjectValue());
      }
      wtx.commit();
    }
  }

  @Benchmark
  public void insertObjectRecordStringAsRightSiblingRollback(final ObjectRecordStringInsertState state) {
    try (var session = state.database.beginResourceSession(RESOURCE); JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        wtx.insertObjectAsFirstChild();
      }

      if (!wtx.moveToFirstChild()) {
        wtx.insertObjectRecordAsFirstChild(state.nextKey(), state.nextObjectValue());
      } else {
        wtx.insertObjectRecordAsRightSibling(state.nextKey(), state.nextObjectValue());
      }
      wtx.rollback();
    }
  }

  @State(Scope.Thread)
  public static class MoveState extends ResourceParamState {
    @Param({"SEQUENTIAL", "RANDOM"})
    public String accessPattern;

    private Path databasePath;
    private Database<JsonResourceSession> database;
    private JsonResourceSession session;
    private JsonNodeTrx wtx;
    private long[] nodeKeys;
    private int keyIndex;
    private int randomState;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
      clampLoggingForBenchmarks();

      databasePath = Files.createTempDirectory("sirix-jmh-json-write-move");
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      database = Databases.openJsonDatabase(databasePath);
      database.createResource(newResourceConfig(RESOURCE).build());

      try (var seedSession = database.beginResourceSession(RESOURCE); JsonNodeTrx seedWtx = seedSession.beginNodeTrx()) {
        seedWtx.insertArrayAsFirstChild();
        for (int i = 0; i < MOVE_KEYS; i++) {
          seedWtx.insertNumberValueAsFirstChild(Integer.valueOf(i & (VALUE_VARIANTS - 1)));
          seedWtx.moveToParent();
        }
        seedWtx.commit();
      }

      final long[] tmpKeys = new long[MOVE_KEYS];
      int count = 0;
      try (var readSession = database.beginResourceSession(RESOURCE); var rtx = readSession.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        if (rtx.moveToFirstChild() && rtx.moveToFirstChild()) {
          do {
            tmpKeys[count++] = rtx.getNodeKey();
          } while (count < MOVE_KEYS && rtx.moveToRightSibling());
        }
      }
      nodeKeys = count == tmpKeys.length ? tmpKeys : Arrays.copyOf(tmpKeys, count);
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
      session = database.beginResourceSession(RESOURCE);
      wtx = session.beginNodeTrx();
      keyIndex = 0;
      randomState = 0x9E3779B9;
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      if (wtx != null) {
        wtx.close();
      }
      if (session != null) {
        session.close();
      }
      wtx = null;
      session = null;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
      if (database != null) {
        database.close();
      }
      if (databasePath != null) {
        Databases.removeDatabase(databasePath);
      }
    }

    private long nextNodeKey() {
      if ("RANDOM".equals(accessPattern)) {
        randomState = randomState * 1664525 + 1013904223;
        return nodeKeys[Math.floorMod(randomState, nodeKeys.length)];
      }
      return nodeKeys[(keyIndex++) % nodeKeys.length];
    }
  }

  @Benchmark
  public long writeTrxMoveToKnownNode(final MoveState state) {
    final long key = state.nextNodeKey();
    state.wtx.moveTo(key);
    return state.wtx.getNodeKey();
  }
}
