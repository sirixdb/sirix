package io.sirix.index.aggregate;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.path.Path;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.changelog.ChangeLogGroupReader;
import io.sirix.settings.VersioningType;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageProvider;
import io.sirix.io.StorageProviders;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Process death before/after the derived beacon must replay from the last durable mapping. */
final class DerivedIndexCrashRecoveryTest {
  @TempDir
  File directory;

  static Stream<Arguments> boundaries() {
    return Stream.of("source", "before-beacon", "after-beacon")
                 .flatMap(boundary -> Stream.of(VersioningType.values())
                                            .map(versioning -> Arguments.of(boundary, versioning)));
  }

  @ParameterizedTest
  @MethodSource("boundaries")
  void restartsAtDurableDerivedFrontier(final String boundary, final VersioningType versioning) throws Exception {
    assumeTrue(Boolean.getBoolean("sirix.crash.run"));
    final var databasePath = directory.toPath().resolve("database");
    final var log = directory.toPath().resolve("writer.log");
    final Process child = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").toString(),
        "-Xmx512m", "--enable-preview", "--enable-native-access=ALL-UNNAMED", "--add-modules=jdk.incubator.vector",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp",
        System.getProperty("java.class.path"), WriterMain.class.getName(), databasePath.toString(), boundary,
        versioning.name()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(child.waitFor(60, TimeUnit.SECONDS), "Crash writer did not reach its boundary");
      assertEquals(87, child.exitValue(), () -> {
        try {
          return Files.readString(log);
        } catch (final Exception failure) {
          return failure.toString();
        }
      });
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor(30, TimeUnit.SECONDS);
      }
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession source = database.beginResourceSession("records")) {
      assertEquals(4, source.getMostRecentRevisionNumber());
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) source).getDerivedResource());
      resource.pause();
      assertHistory(source, resource);
      resource.drain();
      assertHistory(source, resource);
    }
  }

  private static void assertHistory(final JsonResourceSession source, final DerivedIndexMaintainer.Resource resource) {
    for (int revision = 1; revision <= 4; revision++) {
      final int expected = revision * 10;
      try (StorageEngineReader reader = source.createStorageEngineReader(revision);
          DerivedIndexSnapshot snapshot = resource.openCatchUpSnapshot(reader)) {
        assertNotNull(snapshot);
        final var indexes = source.getRtxIndexController(revision).getIndexes();
        final IndexDef definition = Objects.requireNonNull(indexes.getIndexDef(0, IndexType.TEMPORAL_AGGREGATE));
        final IndexDef projection = Objects.requireNonNull(indexes.getIndexDef(0, IndexType.PROJECTION));
        final TemporalAggregateIndexes.CatchUp tail = TemporalAggregateIndexes.catchUp(reader,
            snapshot.sourceRevision(), definition, projection, new ChangeLogGroupReader.Budget());
        try (TemporalAggregateStorage.Reader storage = new TemporalAggregateStorage.Reader(snapshot.reader(), 0)) {
          final int[] groups = {0};
          storage.groups(1, (key, head) -> {
            groups[0]++;
            final AggregateSummary summary = tail.point(storage.parts(), head.parts(), key,
                new TemporalAggregateDomain().point(Instant.parse("2024-06-01T00:00:00Z")));
            assertEquals(1, summary.count());
            assertEquals(BigInteger.valueOf(expected), summary.sum(0));
          });
          assertEquals(1, groups[0]);
        }
      }
    }
  }

  public static final class WriterMain {
    private static volatile boolean armed;

    private WriterMain() {}

    public static void main(final String[] arguments) throws Exception {
      final var databasePath = new File(arguments[0]).toPath();
      final String boundary = arguments[1];
      installCrashProvider(boundary);
      Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder("records")
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(VersioningType.valueOf(arguments[2]))
                                                     .validTimePaths("lo", "hi")
                                                     .customCommitTimestamps(true)
                                                     .storeDiffs(false)
                                                     .build());
        try (JsonResourceSession source = database.beginResourceSession("records");
            JsonNodeTrx writer = source.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
              """), JsonNodeTrx.Commit.NO);
          final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
              List.of(field("g"), field("n"), field("lo"), field("hi")),
              List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
          source.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
          TemporalAggregateDeclarations.declare(source.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
              List.of("g"), List.of("sum(n)"));
          final DerivedIndexMaintainer.Resource resource =
              Objects.requireNonNull(((AbstractResourceSession<?, ?>) source).getDerivedResource());
          resource.pause();
          writer.commit("first", Instant.parse("2024-01-01T00:00:00Z"));
          resource.drain();
          writer.moveToDocumentRoot();
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          writer.moveToRightSibling();
          writer.moveToFirstChild();
          writer.setNumberValue(20);
          writer.commit("second", Instant.parse("2024-02-01T00:00:00Z"));
          for (int revision = 3; revision <= 4; revision++) {
            writer.setNumberValue(revision * 10);
            writer.commit("tail " + revision, Instant.parse("2024-0" + revision + "-01T00:00:00Z"));
          }
          if (boundary.equals("source")) {
            Runtime.getRuntime().halt(87);
          }
          armed = true;
          resource.drain();
          throw new AssertionError("Crash boundary was not reached");
        }
      }
    }

    private static Path<QNm> field(final String name) {
      return Path.parse("/[]/" + name, PathParser.Type.JSON);
    }

    @SuppressWarnings("unchecked")
    private static void installCrashProvider(final String boundary) throws ReflectiveOperationException {
      final Field registry = StorageProviders.class.getDeclaredField("PROVIDERS");
      registry.setAccessible(true);
      final Map<String, StorageProvider> providers = (Map<String, StorageProvider>) registry.get(null);
      final StorageProvider previous = providers.putIfAbsent("FILE_CHANNEL", new StorageProvider() {
        @Override
        public String getName() {
          return "FILE_CHANNEL";
        }

        @Override
        public boolean isAvailable() {
          return true;
        }

        @Override
        public IOStorage createStorage(final ResourceConfiguration configuration) {
          final IOStorage storage = StorageType.FILE_CHANNEL.getInstance(configuration);
          if (!configuration.getResource().getFileName().toString().endsWith(DerivedIndexMaintainer.SUFFIX)) {
            return storage;
          }
          return (IOStorage) Proxy.newProxyInstance(IOStorage.class.getClassLoader(), new Class<?>[] {IOStorage.class},
              (proxy, method, args) -> {
                final Object result = invoke(method, storage, args);
                if (!method.getName().equals("createWriter")) {
                  return result;
                }
                return Proxy.newProxyInstance(Writer.class.getClassLoader(), new Class<?>[] {Writer.class},
                    (writerProxy, writerMethod, writerArgs) -> {
                      final boolean beacon = armed && writerMethod.getName().equals("writeUberPageReference");
                      if (beacon && boundary.equals("before-beacon")) {
                        Runtime.getRuntime().halt(87);
                      }
                      final Object value = invoke(writerMethod, result, writerArgs);
                      if (beacon && boundary.equals("after-beacon")) {
                        Runtime.getRuntime().halt(87);
                      }
                      return value;
                    });
              });
        }
      });
      if (previous != null) {
        throw new IllegalStateException("A FILE_CHANNEL provider is already installed");
      }
    }

    private static Object invoke(final Method method, final Object target, final Object[] arguments) throws Throwable {
      try {
        return method.invoke(target, arguments);
      } catch (final InvocationTargetException failure) {
        throw failure.getCause();
      }
    }
  }
}
