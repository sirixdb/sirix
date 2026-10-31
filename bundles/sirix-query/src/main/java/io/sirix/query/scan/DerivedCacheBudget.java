package io.sirix.query.scan;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.sirix.cache.Allocators;
import io.sirix.cache.HeapCacheBudget;
import io.sirix.cache.HeapCacheBudget.Pool;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One runtime heap allocation for decoded numeric snapshots and packed join sides. The fixed
 * partitions include retained entries and reservations for cache construction. Query workspace
 * needed even with caching disabled is separate; an optional background build must reserve first.
 */
public final class DerivedCacheBudget {
  public static final String MAX_BYTES_PROPERTY = "sirix.derivedCache.maxBytes";
  public static final String HEAP_RESERVE_BYTES_PROPERTY = "sirix.derivedCache.heapReserveBytes";

  private final long configuredBytes;
  private final long allocatorBytes;
  private final long heapBytes;
  private final long heapReserveBytes;
  private final long effectiveBytes;
  private final Partition numeric;
  private final Partition packed;

  private static volatile @Nullable DerivedCacheBudget runtime;
  private static volatile @Nullable DerivedCacheBudget testing;

  /** Initialized on first use, after the allocator has received its effective persisted allowance. */
  public static DerivedCacheBudget global() {
    final DerivedCacheBudget override = testing;
    if (override != null) {
      return override;
    }
    DerivedCacheBudget result = runtime;
    if (result == null) {
      synchronized (DerivedCacheBudget.class) {
        result = runtime;
        if (result == null) {
          if (!Allocators.getInstance().isInitialized()) {
            throw new IllegalStateException("Initialize the persisted allocator before using derived caches");
          }
          result = fromRuntime();
          runtime = result;
        }
      }
    }
    return result;
  }

  private static DerivedCacheBudget fromRuntime() {
    final long heap = Runtime.getRuntime().maxMemory();
    return new DerivedCacheBudget(Allocators.getInstance().getMaxBufferSize(), heap,
        HeapCacheBudget.global().bytes(Pool.DERIVED), property(MAX_BYTES_PROPERTY, -1));
  }

  private static long property(final String name, final long fallback) {
    final String value = System.getProperty(name);
    if (value == null) {
      return fallback;
    }
    final long bytes;
    try {
      bytes = Long.parseLong(value.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a non-negative byte count", e);
    }
    if (bytes < 0) {
      throw new IllegalArgumentException(name + " must be a non-negative byte count");
    }
    return bytes;
  }

  /**
   * @param allocatorBytes effective allocator allowance, not a second allocation from that arena
   * @param heapBytes effective runtime maximum heap
   * @param heapReserveBytes operator-declared allocation left for these caches
   * @param configuredBytes explicit target, or -1 to derive min(allocator / 8, heap / 8, reserve)
   */
  public DerivedCacheBudget(final long allocatorBytes, final long heapBytes, final long heapReserveBytes,
      final long configuredBytes) {
    if (allocatorBytes < 0 || heapBytes < 0 || heapReserveBytes < 0 || configuredBytes < -1) {
      throw new IllegalArgumentException("Memory allowances must be non-negative (target -1 means automatic)");
    }
    this.allocatorBytes = allocatorBytes;
    this.heapBytes = heapBytes;
    this.heapReserveBytes = heapReserveBytes;
    this.configuredBytes = configuredBytes;
    final long target = configuredBytes < 0 ? Math.min(allocatorBytes / 8, heapBytes / 8) : configuredBytes;
    effectiveBytes = Math.min(target, Math.min(heapBytes, heapReserveBytes));
    final long packedBytes = effectiveBytes / 4;
    // Round down both shares; at most one byte remains unassigned. Never multiply D by three.
    numeric = new Partition(effectiveBytes / 4 * 3 + effectiveBytes % 4 * 3 / 4);
    packed = new Partition(packedBytes);
  }

  public Partition numeric() {
    return numeric;
  }

  public Partition packed() {
    return packed;
  }

  /** Cold-path observability, in bytes; configured=-1 denotes automatic sizing. */
  public Metrics metrics() {
    final PartitionMetrics n = numeric.metrics();
    final PartitionMetrics p = packed.metrics();
    return new Metrics(configuredBytes, allocatorBytes, heapBytes, heapReserveBytes, effectiveBytes,
        n, p, n.retainedBytes() + p.retainedBytes(), n.inFlightBytes() + p.inFlightBytes(),
        saturatedAdd(n.rejectedBytes(), p.rejectedBytes()));
  }

  public record Metrics(long configuredBytes, long allocatorBytes, long heapBytes, long heapReserveBytes,
      long effectiveBytes, PartitionMetrics numeric, PartitionMetrics packed, long retainedBytes,
      long inFlightBytes, long rejectedBytes) {}

  public record PartitionMetrics(long maximumBytes, long retainedBytes, long inFlightBytes, long rejectedBytes) {}

  /** Test seam for isolated integration fixtures. Callers must restore it after all packing finishes. */
  public static @Nullable DerivedCacheBudget setForTesting(final @Nullable DerivedCacheBudget budget) {
    final DerivedCacheBudget previous = testing;
    testing = budget;
    return previous;
  }

  /** Non-negative sizes saturate on overflow, so an unrepresentable allocation is always rejected. */
  public static long saturatedAdd(final long left, final long right) {
    if (left < 0 || right < 0) {
      throw new IllegalArgumentException("Negative byte weight");
    }
    return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  /** Non-negative byte multiplication, saturating rather than wrapping into an admissible size. */
  public static long saturatedMultiply(final long left, final long right) {
    if (left < 0 || right < 0) {
      throw new IllegalArgumentException("Negative byte weight");
    }
    return right != 0 && left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
  }

  /** Optional index cleanup. Implementations must not retain the weak cache owner. */
  interface EvictionKey {
    void evicted();
  }

  /**
   * A weak-identity weighted cache. Hits never take the admission monitor. Admission reserves exact
   * long byte weights before construction; Caffeine's replacement order supplies victims when
   * needed. Its integer weights use rounded-up units, never a narrowing byte cast.
   */
  public static final class Partition {
    private final long maximumBytes;
    private final long weightUnit;
    private final AtomicLong occupied = new AtomicLong();
    private final AtomicLong inFlight = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final Cache<Object, Entry> cache;

    private record Entry(Object value, long bytes, Reservation reservation) {}

    private Partition(final long maximumBytes) {
      this.maximumBytes = maximumBytes;
      weightUnit = Math.max(1, maximumBytes / Integer.MAX_VALUE
          + (maximumBytes % Integer.MAX_VALUE == 0 ? 0 : 1));
      cache = Caffeine.newBuilder().weakKeys().maximumWeight(maximumBytes / weightUnit)
          .executor(Runnable::run)
          .weigher((Object key, Entry entry) -> weight(entry.bytes))
          .removalListener((Object key, Entry entry, RemovalCause cause) -> {
            if (entry != null) {
              entry.reservation.release();
            }
            if (key instanceof EvictionKey indexed) {
              indexed.evicted();
            }
          }).build();
    }

    private int weight(final long bytes) {
      return Math.toIntExact(bytes / weightUnit + (bytes % weightUnit == 0 ? 0 : 1));
    }

    public @Nullable Object get(final Object key) {
      final Entry entry = cache.getIfPresent(Objects.requireNonNull(key));
      return entry == null ? null : entry.value;
    }

    /** Returns null without building when the entry or simultaneous builds cannot fit the share. */
    public synchronized @Nullable Reservation reserve(final long bytes) {
      if (bytes <= 0) {
        throw new IllegalArgumentException("Construction weight must be positive");
      }
      if (bytes > maximumBytes) {
        reject(bytes);
        return null;
      }
      cache.cleanUp();
      while (bytes > maximumBytes - occupied.get()) {
        final var victims = cache.policy().eviction().orElseThrow().coldest(1);
        if (victims.isEmpty()) {
          reject(bytes);
          return null;
        }
        cache.invalidate(victims.keySet().iterator().next());
        cache.cleanUp();
      }
      synchronized (occupied) {
        occupied.addAndGet(bytes);
        inFlight.addAndGet(bytes);
      }
      return new Reservation(this, bytes);
    }

    private void reject(final long bytes) {
      rejected.getAndUpdate(previous -> saturatedAdd(previous, bytes));
    }

    public PartitionMetrics metrics() {
      // Reservation transitions and listener releases use this same monitor for coherent metrics.
      synchronized (occupied) {
        final long building = inFlight.get();
        return new PartitionMetrics(maximumBytes, occupied.get() - building, building, rejected.get());
      }
    }

    public void clear() {
      cache.invalidateAll();
      cache.cleanUp();
    }
  }

  /** Exactly one owner, with idempotent release on failed construction, eviction or publication failure. */
  public static final class Reservation implements AutoCloseable {
    private final Partition partition;
    private final AtomicBoolean released = new AtomicBoolean();
    private long bytes;
    private boolean published;

    private Reservation(final Partition partition, final long bytes) {
      this.partition = partition;
      this.bytes = bytes;
    }

    public void publish(final Object key, final Object value, final long retainedBytes) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(value);
      if (published || released.get() || retainedBytes <= 0 || retainedBytes > bytes) {
        throw new IllegalArgumentException("Publication must fit its live construction reservation");
      }
      synchronized (partition.occupied) {
        partition.inFlight.addAndGet(-bytes);
        partition.occupied.addAndGet(retainedBytes - bytes);
        bytes = retainedBytes;
        published = true;
      }
      try {
        partition.cache.put(key, new Partition.Entry(value, bytes, this));
      } catch (final RuntimeException | Error e) {
        release();
        throw e;
      }
    }

    private void release() {
      if (released.compareAndSet(false, true)) {
        synchronized (partition.occupied) {
          partition.occupied.addAndGet(-bytes);
          if (!published) {
            partition.inFlight.addAndGet(-bytes);
          }
        }
      }
    }

    @Override
    public void close() {
      if (!published) {
        release();
      }
    }
  }
}
