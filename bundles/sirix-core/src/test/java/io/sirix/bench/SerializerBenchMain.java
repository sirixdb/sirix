package io.sirix.bench;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Times whole-document JSON serialization (the REST "get all data" / query-result hot path) over
 * an imported document. Args: {@code <json-file> <iterations>}. Prints per-iteration latency and
 * MB/s; meant to run under JFR/perf for hotspot attribution.
 */
public final class SerializerBenchMain {

  private SerializerBenchMain() {
  }

  public static void main(final String[] args) throws Exception {
    final Path json = Paths.get(args[0]);
    final int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 30;
    final Path dbPath = Files.createTempDirectory("sirix-serbench-").resolve("db");
    final String resource = "bench";

    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .buildPathSummary(true)
                                                   .storeChildCount(true)
                                                   .hashKind(HashType.ROLLING)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(resource);
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(json));
      }

      try (final JsonResourceSession session = database.beginResourceSession(resource)) {
        long bytes = 0;
        // Warmup
        for (int i = 0; i < 5; i++) {
          bytes = serializeOnce(session);
        }
        final long[] times = new long[iterations];
        for (int i = 0; i < iterations; i++) {
          final long t0 = System.nanoTime();
          bytes = serializeOnce(session);
          times[i] = System.nanoTime() - t0;
        }
        java.util.Arrays.sort(times);
        final long p50 = times[iterations / 2];
        final long p95 = times[(int) (iterations * 0.95)];
        final double mbPerSec = bytes / (p50 / 1e9) / (1024 * 1024);
        System.out.printf("serialized %d bytes: p50=%dms p95=%dms throughput=%.1f MB/s%n", bytes,
                          TimeUnit.NANOSECONDS.toMillis(p50), TimeUnit.NANOSECONDS.toMillis(p95), mbPerSec);
      }
    }
  }

  private static final boolean BYTE_MODE = Boolean.getBoolean("bench.bytes");

  private static long serializeOnce(final JsonResourceSession session) {
    if (BYTE_MODE) {
      final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(4 << 20);
      final JsonSerializer serializer = JsonSerializer.newBuilder(session, out).build();
      serializer.call();
      return out.size();
    }
    final StringWriter out = new StringWriter(4 << 20);
    final JsonSerializer serializer = JsonSerializer.newBuilder(session, out).build();
    serializer.call();
    return out.getBuffer().length();
  }
}
