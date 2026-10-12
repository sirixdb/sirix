package io.sirix.query.function.jn.temporal;

import io.sirix.access.ValidTimeConfig;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.json.JsonDBItem;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ValidTimeColdInitializationTest {
  @Test
  void decliningMutableSourceDoesNotLoadValidTimeStore() throws IOException, ReflectiveOperationException {
    final StorageEngineReader storage = mock(StorageEngineReader.class);
    when(storage.hasTrxIntentLog()).thenReturn(true);
    final JsonNodeReadOnlyTrx transaction = mock(JsonNodeReadOnlyTrx.class);
    when(transaction.getStorageEngineReader()).thenReturn(storage);
    final JsonDBItem document = mock(JsonDBItem.class);
    when(document.getTrx()).thenReturn(transaction);
    final AtomicInteger intervalLoads = new AtomicInteger();
    final String indexClass = ValidTimeIntervalIndex.class.getName();
    final URL classes = ValidTimeIntervalIndex.class.getProtectionDomain().getCodeSource().getLocation();
    try (final URLClassLoader loader = new URLClassLoader(new URL[] {classes}, getClass().getClassLoader()) {
      @Override
      protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        if (name.equals("io.sirix.index.interval.HotOrderedStore")) {
          intervalLoads.incrementAndGet();
        }
        if (!name.equals(indexClass) && !name.startsWith(indexClass + "$")) {
          return super.loadClass(name, resolve);
        }
        synchronized (getClassLoadingLock(name)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded == null) {
            loaded = findClass(name);
          }
          if (resolve) {
            resolveClass(loaded);
          }
          return loaded;
        }
      }
    }) {
      final var sequence = loader.loadClass(indexClass)
                                 .getMethod("sequence", JsonDBItem.class, Instant.class, ValidTimeConfig.class,
                                     boolean.class, boolean.class);
      assertNull(sequence.invoke(null, document, Instant.EPOCH, new ValidTimeConfig("vf", "vt"), false, true));
      assertEquals(0, intervalLoads.get(), "declining an index path must not initialize its store");
    }
  }
}
