package io.sirix.access.trx.page;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner.Cleanable;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WriterCleanerLifetimeTest {
  @TempDir
  Path directory;

  @Test
  void normalCloseDeregistersWhileTheWriterStaysReachable() throws Exception {
    final Path path = directory.resolve("writer-cleaner");
    final Logger logger = (Logger) LoggerFactory.getLogger(NodeStorageEngineWriter.class);
    final Level previousLevel = logger.getLevel();
    final ListAppender<ILoggingEvent> events = new ListAppender<>();
    events.start();
    logger.setLevel(Level.WARN);
    logger.addAppender(events);
    try {
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(
            ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build()));
        try (final JsonResourceSession session = database.beginResourceSession("resource");
            final JsonNodeTrx transaction = session.beginNodeTrx()) {
          final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) transaction.getStorageEngineWriter();
          final ReferenceQueue<Cleanable> collected = new ReferenceQueue<>();
          final WeakReference<Cleanable> registration = observeRegistration(writer, collected);
          try {
            transaction.close();
            // GC is an ownership oracle here; the bounded retry is only a collector/hang guard.
            for (int round = 0; round < 30 && registration.get() != null; round++) {
              System.gc();
              collected.remove(100);
            }
            assertNull(registration.get(), "a closed live writer must not retain its Cleaner registration");
            final String identity = "instance=" + System.identityHashCode(writer) + " ";
            assertTrue(
                events.list.stream()
                           .noneMatch(event -> event.getFormattedMessage().contains("FINALIZED WITHOUT CLOSE")
                               && event.getFormattedMessage().contains(identity)),
                "normal close must suppress the abandoned-writer diagnostic");
          } finally {
            Reference.reachabilityFence(writer);
          }
        }
      }
    } finally {
      logger.detachAppender(events);
      logger.setLevel(previousLevel);
      events.stop();
      Databases.removeDatabase(path);
    }
  }

  private static WeakReference<Cleanable> observeRegistration(final NodeStorageEngineWriter writer,
      final ReferenceQueue<Cleanable> collected) throws ReflectiveOperationException {
    // Observe the registered resource, not source text or a mock of the close implementation.
    final Field stateField = NodeStorageEngineWriter.class.getDeclaredField("leakDetectorState");
    stateField.setAccessible(true);
    final Object state = stateField.get(writer);
    final Field registrationField = state.getClass().getDeclaredField("cleanable");
    registrationField.setAccessible(true);
    final Cleanable registration = (Cleanable) registrationField.get(state);
    assertNotNull(registration);
    return new WeakReference<>(registration, collected);
  }
}
