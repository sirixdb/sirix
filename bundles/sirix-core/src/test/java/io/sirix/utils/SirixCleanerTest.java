package io.sirix.utils;

import org.junit.jupiter.api.Test;

import java.lang.ref.Cleaner.Cleanable;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SirixCleanerTest {
  @Test
  void explicitCloseIsSynchronousAndExactlyOnce() {
    final Object owner = new Object();
    final AtomicInteger calls = new AtomicInteger();
    final AtomicReference<Thread> thread = new AtomicReference<>();
    final Cleanable cleanup = SirixCleaner.register(owner, () -> {
      calls.incrementAndGet();
      thread.set(Thread.currentThread());
    });
    try {
      cleanup.clean();
      cleanup.clean();
      assertEquals(1, calls.get());
      assertSame(Thread.currentThread(), thread.get());
    } finally {
      Reference.reachabilityFence(owner);
    }
  }

  @Test
  void concurrentExplicitCloseReleasesExactlyOnce() throws Exception {
    try (final ExecutorService workers = Executors.newFixedThreadPool(2)) {
      for (int round = 0; round < 32; round++) {
        final Object owner = new Object();
        final AtomicInteger calls = new AtomicInteger();
        final Cleanable cleanup = SirixCleaner.register(owner, calls::incrementAndGet);
        final CountDownLatch start = new CountDownLatch(1);
        try {
          final Future<?> first = workers.submit(() -> {
            start.await();
            cleanup.clean();
            return null;
          });
          final Future<?> second = workers.submit(() -> {
            start.await();
            cleanup.clean();
            return null;
          });
          start.countDown();
          first.get(10, TimeUnit.SECONDS);
          second.get(10, TimeUnit.SECONDS);
          assertEquals(1, calls.get(), "two closing threads must return the resource exactly once");
        } finally {
          start.countDown();
          Reference.reachabilityFence(owner);
        }
      }
    }
  }

  @Test
  void failureIsPropagatedByExplicitCloseWithoutRunningTwice() {
    final Object owner = new Object();
    final IllegalStateException expected = new IllegalStateException("close failed");
    final AtomicInteger calls = new AtomicInteger();
    final Cleanable cleanup = SirixCleaner.register(owner, () -> {
      calls.incrementAndGet();
      throw expected;
    });
    try {
      assertSame(expected, assertThrows(IllegalStateException.class, cleanup::clean));
      cleanup.clean();
      assertEquals(1, calls.get());
    } finally {
      Reference.reachabilityFence(owner);
    }
  }

  @Test
  void blockedFallbackCannotHoldUpAnotherCleanup() throws InterruptedException {
    final Object firstOwner = new Object();
    final Object secondOwner = new Object();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch unblock = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final CountDownLatch second = new CountDownLatch(1);
    final SirixCleaner.Cleanup firstCleanup = (SirixCleaner.Cleanup) SirixCleaner.register(firstOwner, () -> {
      started.countDown();
      try {
        unblock.await();
      } catch (final InterruptedException failure) {
        Thread.currentThread().interrupt();
      } finally {
        finished.countDown();
      }
    });
    final SirixCleaner.Cleanup secondCleanup = (SirixCleaner.Cleanup) SirixCleaner.register(secondOwner, second::countDown);
    try {
      // Invoke precisely the callback the Cleaner invokes, without depending on GC scheduling.
      firstCleanup.run();
      assertTrue(started.await(10, TimeUnit.SECONDS));
      secondCleanup.run();
      assertTrue(second.await(10, TimeUnit.SECONDS));
      assertEquals(1, unblock.getCount(), "the first action is still blocked when the second finishes");
    } finally {
      unblock.countDown();
      assertTrue(finished.await(10, TimeUnit.SECONDS));
      firstCleanup.clean();
      secondCleanup.clean();
      Reference.reachabilityFence(firstOwner);
      Reference.reachabilityFence(secondOwner);
    }
  }

  @Test
  void closedRegistrationIsCollectibleWhileItsOwnerStaysLive() throws InterruptedException {
    final Object owner = new Object();
    final AtomicInteger calls = new AtomicInteger();
    final ReferenceQueue<Cleanable> collected = new ReferenceQueue<>();
    final WeakReference<Cleanable> registration = closeAndForgetRegistration(owner, calls, collected);
    try {
      for (int round = 0; round < 30 && registration.get() != null; round++) {
        System.gc();
        collected.remove(100);
      }
      assertEquals(1, calls.get());
      assertNull(registration.get(), "normal close must deregister the callback before its owner is collected");
    } finally {
      Reference.reachabilityFence(owner);
    }
  }

  private static WeakReference<Cleanable> closeAndForgetRegistration(final Object owner,
      final AtomicInteger calls, final ReferenceQueue<Cleanable> collected) {
    final Cleanable cleanup = SirixCleaner.register(owner, calls::incrementAndGet);
    cleanup.clean();
    return new WeakReference<>(cleanup, collected);
  }

  @Test
  void collectionRunsTheFallbackWithoutRetainingTheOwner() throws InterruptedException {
    final CountDownLatch cleaned = new CountDownLatch(1);
    final WeakReference<Object> owner = abandonOwner(cleaned);
    for (int round = 0; round < 30 && cleaned.getCount() != 0; round++) {
      System.gc();
      cleaned.await(100, TimeUnit.MILLISECONDS);
    }
    assertEquals(0, cleaned.getCount(), "the registered GC fallback must run");
    assertNull(owner.get());
  }

  private static WeakReference<Object> abandonOwner(final CountDownLatch cleaned) {
    final Object owner = new Object();
    SirixCleaner.register(owner, cleaned::countDown);
    return new WeakReference<>(owner);
  }
}
