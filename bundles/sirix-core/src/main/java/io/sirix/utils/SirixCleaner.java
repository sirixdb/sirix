package io.sirix.utils;

import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.lang.ref.Reference;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * One lazy Cleaner service for core and query owners. Explicit close remains synchronous. GC
 * fallbacks run on independent virtual threads so a stalled close, allocator lock or diagnostic
 * logger cannot hold up the shared reference-processing daemon. Actions must not retain owners.
 */
public final class SirixCleaner {
  private SirixCleaner() {}

  private static final class Service {
    private static final Cleaner CLEANER = Cleaner.create(runnable -> {
      final Thread thread = new Thread(runnable, "sirix-cleaner");
      thread.setDaemon(true);
      return thread;
    });

    private Service() {}
  }

  /** Register an owner-independent action, invoked at most once, by close or by GC. */
  public static Cleanable register(final Object owner, final Runnable action) {
    requireNonNull(owner);
    final Cleanup cleanup = new Cleanup(requireNonNull(action));
    cleanup.registration = Service.CLEANER.register(owner, cleanup);
    Reference.reachabilityFence(owner);
    return cleanup;
  }

  /** Kept separate from the referent; also the action the JDK Cleaner invokes after collection. */
  static final class Cleanup extends AtomicReference<Runnable> implements Runnable, Cleanable {
    private static final long serialVersionUID = 1L;
    private Cleanable registration;

    private Cleanup(final Runnable action) {
      super(action);
    }

    @Override
    public void clean() {
      final Runnable action = getAndSet(null);
      try {
        if (action != null) {
          action.run();
        }
      } finally {
        registration.clean();
      }
    }

    @Override
    public void run() {
      final Runnable action = getAndSet(null);
      if (action != null) {
        Thread.ofVirtual().name("sirix-cleanup").start(action);
      }
    }
  }
}
