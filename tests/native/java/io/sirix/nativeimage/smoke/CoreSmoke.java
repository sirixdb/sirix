package io.sirix.nativeimage.smoke;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.StorageEngineWriterFactory;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.exception.SirixUsageException;
import io.sirix.service.json.shredder.JsonShredder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

/** Test-only entrypoints, deliberately separate from the future public ABI. */
public final class CoreSmoke {
  @CEntryPoint(name = "smoke_core")
  public static int run(final IsolateThread thread, final CCharPointer path) {
    try {
      final Path location = Path.of(CTypeConversion.toJavaString(path));
      final DatabaseConfiguration config = new DatabaseConfiguration(location);
      config.setMaxSegmentAllocationSize(64L * 1024 * 1024);
      if (!Databases.createJsonDatabase(config)) {
        throw new AssertionError("Fresh create failed");
      }
      if (Databases.createJsonDatabase(config)) {
        throw new AssertionError("Duplicate create succeeded");
      }
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(location)) {
        if (!database.isOpen() || !database.createResource(
            ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).build())) {
          throw new AssertionError("Database/resource open failed");
        }
        try (final JsonResourceSession session = database.beginResourceSession("data");
             final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"answer\":42}"), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
      }
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(location);
           final JsonResourceSession session = database.beginResourceSession("data");
           final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        if (!reader.moveToDocumentRoot() || !reader.moveToFirstChild() || !reader.moveToFirstChild()
            || reader.getNumberValue().intValue() != 42) {
          throw new AssertionError("Persisted value differs from 42");
        }
      }
      final Path missing = location.resolveSibling(location.getFileName() + "-missing");
      try (final Database<JsonResourceSession> ignored = Databases.openJsonDatabase(missing)) {
        throw new AssertionError("Missing open succeeded");
      } catch (final SirixUsageException expected) {
        if (Files.exists(missing)) {
          throw new AssertionError("Missing open created a database", expected);
        }
      }
      exerciseCaches();
      return 42;
    } catch (final Throwable failure) {
      failure.printStackTrace();
      return -1;
    }
  }

  // Covers core's size/weight policies and PageCache's listener/statistics combination.
  private static void exerciseCaches() {
    for (int shape = 0; shape < 16; shape++) {
      final Caffeine<Object, Object> builder = Caffeine.newBuilder().executor(Runnable::run);
      if ((shape & 1) == 0) {
        builder.maximumSize(2);
      } else {
        builder.maximumWeight(2).weigher((key, value) -> 1);
      }
      if ((shape & 2) != 0) {
        builder.recordStats();
      }
      if ((shape & 4) != 0) {
        builder.removalListener((key, value, cause) -> {});
      }
      final Cache<Object, Object> cache = (shape & 8) == 0 ? builder.build() : builder.buildAsync().synchronous();
      cache.put("answer", 42);
      if (!Integer.valueOf(42).equals(cache.getIfPresent("answer"))) {
        throw new AssertionError("Cache lookup failed for shape " + shape);
      }
      cache.put("second", 2);
      cache.put("third", 3);
      cache.cleanUp();
      if (cache.estimatedSize() > 2) {
        throw new AssertionError("Cache bound failed for shape " + shape);
      }
      cache.invalidateAll();
      cache.cleanUp();
      if (cache.estimatedSize() != 0) {
        throw new AssertionError("Cache clear failed for shape " + shape);
      }
      System.out.println("cache_shape=" + shape + " passed");
    }
  }

  @CEntryPoint(name = "smoke_shutdown")
  public static int shutdown(final IsolateThread thread) {
    try {
      StorageEngineWriterFactory.shutdownSharedExecutors();
      return 0;
    } catch (final Throwable failure) {
      failure.printStackTrace();
      return -1;
    }
  }

  @CEntryPoint(name = "smoke_threads")
  public static void threads(final IsolateThread thread) {
    for (final Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      final Thread live = entry.getKey();
      System.err.println("THREAD " + live.getName() + " daemon=" + live.isDaemon() + " state=" + live.getState());
      for (final StackTraceElement frame : entry.getValue()) {
        System.err.println("  at " + frame);
      }
    }
  }
}
