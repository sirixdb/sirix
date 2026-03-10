package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.IndexBackendType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.service.json.shredder.JsonShredder;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end Chicago shredder benchmark with fully configurable resource settings.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes='.*ChicagoShredderBenchmark.*'
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
public class ChicagoShredderBenchmark {

  private static final String RESOURCE = "chicagoResource";

  public enum CompressionPipeline {
    NONE,
    FFI_LZ4
  }

  public enum TraversalMode {
    NONE,
    DESCENDANT,
    POSTORDER,
    BOTH
  }

  private static void clampLoggingForBenchmarks() {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    rootLogger.setLevel(ch.qos.logback.classic.Level.WARN);
  }

  @State(Scope.Thread)
  public static class ChicagoState {
    @Param({"bundles/sirix-core/src/test/resources/json/cityofchicago.json"})
    public String inputJsonPath;

    @Param({"NONE"})
    public TraversalMode traversalMode;

    @Param({"4456448"})
    public int nodeTrxPageBufferSize;

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

    private Path inputPath;

    @Setup(Level.Trial)
    public void setupTrial() {
      clampLoggingForBenchmarks();
      inputPath = resolveInputPath(inputJsonPath);
    }

    private static Path resolveInputPath(final String configuredPath) {
      final Path rawPath = Path.of(configuredPath);
      if (rawPath.isAbsolute() && Files.exists(rawPath)) {
        return rawPath.normalize();
      }

      final Path workingDirectory = Path.of("").toAbsolutePath().normalize();
      for (Path current = workingDirectory; current != null; current = current.getParent()) {
        final Path candidate = current.resolve(rawPath).normalize();
        if (Files.exists(candidate)) {
          return candidate;
        }
      }

      throw new IllegalStateException("Input JSON file does not exist: " + configuredPath
          + " (checked from working directory " + workingDirectory + " and its parents)");
    }

    private ByteHandlerPipeline newByteHandlerPipeline() {
      return switch (compressionPipeline) {
        case NONE -> new ByteHandlerPipeline();
        case FFI_LZ4 -> new ByteHandlerPipeline(new FFILz4Compressor());
      };
    }

    private ResourceConfiguration.Builder newResourceConfig() {
      return ResourceConfiguration.newBuilder(RESOURCE)
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

  @Benchmark
  public long chicagoShredder(final ChicagoState state, final Blackhole blackhole) throws Exception {
    final Path databasePath = Files.createTempDirectory("sirix-jmh-chicago");
    long traversedNodes = 0L;

    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(state.newResourceConfig().build());

        try (var manager = database.beginResourceSession(RESOURCE);
            JsonNodeTrx trx = manager.beginNodeTrx(state.nodeTrxPageBufferSize)) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(state.inputPath));

          if (state.traversalMode != TraversalMode.NONE) {
            trx.moveToDocumentRoot();
          }

          if (state.traversalMode == TraversalMode.DESCENDANT || state.traversalMode == TraversalMode.BOTH) {
            final Axis descendantAxis = new DescendantAxis(trx);
            while (descendantAxis.hasNext()) {
              descendantAxis.nextLong();
              traversedNodes++;
            }
          }

          if (state.traversalMode == TraversalMode.POSTORDER || state.traversalMode == TraversalMode.BOTH) {
            final Axis postOrderAxis = new PostOrderAxis(trx);
            while (postOrderAxis.hasNext()) {
              postOrderAxis.nextLong();
              traversedNodes++;
            }
          }
        }
      }
    } finally {
      Databases.removeDatabase(databasePath);
    }

    blackhole.consume(traversedNodes);
    return traversedNodes;
  }
}
