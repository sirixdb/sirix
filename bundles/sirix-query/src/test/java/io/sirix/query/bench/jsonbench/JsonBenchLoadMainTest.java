package io.sirix.query.bench.jsonbench;

import io.sirix.access.trx.node.HashType;
import io.sirix.access.Databases;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonBenchLoadMainTest {

  @Test
  void durabilitySyncMustExitSuccessfully() {
    assertDoesNotThrow(() -> JsonBenchLoadMain.awaitSuccessfulSync(new CompletedProcess(0)));
    assertThrows(IOException.class, () -> JsonBenchLoadMain.awaitSuccessfulSync(new CompletedProcess(7)));
  }

  @Test
  void interruptedDurabilitySyncRestoresTheInterrupt() {
    Thread.interrupted();
    try {
      assertThrows(IOException.class, () -> JsonBenchLoadMain.awaitSuccessfulSync(new InterruptedProcess()));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void benchmarkRequiresItsProjectionUnlessDiagnosticFallbackIsExplicit(@TempDir final Path directory) {
    try {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build()) {
        final var collection = store.create(JsonBenchSchema.DATABASE, JsonBenchSchema.RESOURCE, "[{}]");
        try (final var session = collection.getDatabase().beginResourceSession(JsonBenchSchema.RESOURCE)) {
          final int revision = session.getMostRecentRevisionNumber();
          final IllegalStateException failure = assertThrows(IllegalStateException.class,
              () -> JsonBenchRunMain.warmCatalog(session, revision, false));
          assertTrue(failure.getMessage().contains("usable projection"));
          assertDoesNotThrow(() -> JsonBenchRunMain.warmCatalog(session, revision, true));
        }
      }
    } finally {
      Databases.removeDatabase(directory.resolve(JsonBenchSchema.DATABASE));
    }
  }

  @Test
  void bothBenchmarkArmsLoadWithoutDeweyIds() {
    try (final var projectionStore = JsonBenchLoadMain.newLoadStoreBuilder(
        Path.of("."), 1024, true, false, HashType.NONE).build();
         final var genericStore = JsonBenchLoadMain.newLoadStoreBuilder(
             Path.of("."), 1024, false, false, HashType.NONE).build()) {
      assertFalse(projectionStore.options().useDeweyIDs());
      assertFalse(genericStore.options().useDeweyIDs());
    }
  }

  private static class CompletedProcess extends Process {
    private final int exitCode;

    CompletedProcess(final int exitCode) {
      this.exitCode = exitCode;
    }

    @Override
    public OutputStream getOutputStream() {
      return new ByteArrayOutputStream();
    }

    @Override
    public InputStream getInputStream() {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public InputStream getErrorStream() {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public int waitFor() throws InterruptedException {
      return exitCode;
    }

    @Override
    public int exitValue() {
      return exitCode;
    }

    @Override
    public void destroy() {
    }
  }

  private static final class InterruptedProcess extends CompletedProcess {
    InterruptedProcess() {
      super(0);
    }

    @Override
    public int waitFor() throws InterruptedException {
      throw new InterruptedException("interrupted");
    }
  }
}
