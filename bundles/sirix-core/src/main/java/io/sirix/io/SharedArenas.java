package io.sirix.io;

import java.lang.foreign.Arena;
import java.util.Locale;

/**
 * Central choice point for arenas that need <em>shared</em> (cross-thread) access semantics.
 *
 * <p>On HotSpot the natural choice is {@link Arena#ofShared()} with an explicit
 * {@link Arena#close()} once the last user is done — close deterministically unmaps/frees the
 * backing memory, which matters for tests that delete resource files right after closing a
 * storage, and for long-running servers that remap a growing data file on every commit.
 *
 * <p>In a GraalVM native image, however, closing a shared arena requires building the image with
 * {@code -H:+SharedArenaSupport}. That flag makes the compiler insert session checks into every
 * scoped memory access and, as of GraalVM 25 (verified through 25.0.3), it is incompatible with
 * {@code jdk.incubator.vector}: combining both aborts the build with
 * {@code GraalError: ... was not inlined and could access a session} in
 * {@code SubstrateOptimizeSharedArenaAccessPhase}. Since the SIMD kernels are non-negotiable for
 * query performance, native images instead default to {@link Arena#ofAuto()}: identical
 * cross-thread access semantics, but reclamation is GC-driven (a {@code Cleaner} unmaps the
 * memory once the arena becomes unreachable) and {@link #close(Arena)} becomes a no-op.
 *
 * <p>The strategy can be forced via {@code -Dsirix.arena.strategy=shared|auto|global}:
 *
 * <ul>
 *   <li>{@code shared} — {@link Arena#ofShared()}, explicit close (HotSpot default);</li>
 *   <li>{@code auto} — {@link Arena#ofAuto()}, GC-reclaimed (native-image default);</li>
 *   <li>{@code global} — {@link Arena#global()}, never reclaimed (diagnostic last resort).</li>
 * </ul>
 *
 * <p>All call sites must pair {@link #newSharedArena()} with {@link #close(Arena)} instead of
 * calling {@code arena.close()} directly — auto/global arenas throw
 * {@link UnsupportedOperationException} on {@code close()}.
 */
public final class SharedArenas {

  /** System property selecting the arena strategy: {@code shared}, {@code auto} or {@code global}. */
  public static final String STRATEGY_PROPERTY = "sirix.arena.strategy";

  /** How "shared" arenas are materialized. */
  public enum Strategy {
    /** {@link Arena#ofShared()} — explicitly closeable, deterministic reclamation. */
    SHARED,
    /** {@link Arena#ofAuto()} — cross-thread accessible, GC-reclaimed, not closeable. */
    AUTO,
    /** {@link Arena#global()} — cross-thread accessible, lives until process exit. */
    GLOBAL;
  }

  private static final Strategy STRATEGY =
      determineStrategy(System.getProperty(STRATEGY_PROPERTY), inNativeImage());

  private SharedArenas() {
    throw new AssertionError("no instances");
  }

  /**
   * Pure strategy resolution — separated from the static initializer for testability.
   *
   * @param explicit value of {@link #STRATEGY_PROPERTY}, or {@code null} if unset
   * @param nativeImage whether we are building/running a GraalVM native image
   * @return the strategy to use
   */
  static Strategy determineStrategy(final String explicit, final boolean nativeImage) {
    if (explicit != null && !explicit.isBlank()) {
      return switch (explicit.trim().toLowerCase(Locale.ROOT)) {
        case "shared" -> Strategy.SHARED;
        case "auto" -> Strategy.AUTO;
        case "global" -> Strategy.GLOBAL;
        default -> throw new IllegalArgumentException(
            STRATEGY_PROPERTY + " must be one of shared|auto|global, but was: " + explicit);
      };
    }
    return nativeImage ? Strategy.AUTO : Strategy.SHARED;
  }

  /**
   * Detect GraalVM native image (build time or run time). The property is set to
   * {@code "buildtime"} by the image builder and {@code "runtime"} inside the image; it is never
   * set on HotSpot. Checking for presence (not a specific value) keeps the answer correct even if
   * this class is initialized at image build time.
   */
  private static boolean inNativeImage() {
    return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
  }

  /** The active strategy. */
  public static Strategy strategy() {
    return STRATEGY;
  }

  /**
   * Create an arena whose memory is accessible from any thread. Pair with {@link #close(Arena)} —
   * never call {@code arena.close()} directly on the result.
   */
  public static Arena newSharedArena() {
    return switch (STRATEGY) {
      case SHARED -> Arena.ofShared();
      case AUTO -> Arena.ofAuto();
      case GLOBAL -> Arena.global();
    };
  }

  /**
   * Release an arena obtained from {@link #newSharedArena()}. Closes it under the {@code shared}
   * strategy; a no-op otherwise (auto arenas are reclaimed by the GC once unreachable, global
   * arenas live for the process lifetime).
   *
   * @param arena the arena to release
   */
  public static void close(final Arena arena) {
    if (STRATEGY == Strategy.SHARED) {
      arena.close();
    }
  }
}
