package io.sirix.utils;

import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertNull;

/** Runs alone: the first registration must not transfer application context into the daemon. */
final class CleanerStartupTest {
  @Test
  void firstRegistrationDoesNotInheritApplicationThreadLocals() throws Exception {
    final InheritableThreadLocal<String> context = new InheritableThreadLocal<>();
    final CompletableFuture<String> observed = new CompletableFuture<>();
    context.set("registering-application-context");
    try {
      final WeakReference<Object> owner = abandonOwner(context, observed);
      for (int round = 0; round < 30 && !observed.isDone(); round++) {
        System.gc();
        try {
          observed.get(100, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException awaitingCollection) {
          // Bounded collector scheduling guard, not a latency assertion.
        }
      }
      assertNull(observed.get(1, TimeUnit.SECONDS), "the service must not retain its creator's context");
      assertNull(owner.get());
    } finally {
      context.remove();
    }
  }

  private static WeakReference<Object> abandonOwner(final InheritableThreadLocal<String> context,
      final CompletableFuture<String> observed) {
    final Object owner = new Object();
    SirixCleaner.register(owner, () -> observed.complete(context.get()));
    return new WeakReference<>(owner);
  }
}
