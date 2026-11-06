package io.sirix.query.scan;

import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.projection.NumericLane;
import io.sirix.index.projection.ProjectionColumnStore;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * One allowance for cached masked selections and packed join sides, carved from the persisted-page
 * allocator's physical budget rather than the Java heap. An entry owns at most one allocator slab:
 * a global-scope {@link MemorySegment} whose bytes are reserved before mapping and refunded exactly
 * once when the last lease drops (an entry over a store's resident columns owns none and pins those
 * columns instead, until the same moment). Removing an entry from lookup never frees its storage; a
 * reader that acquired a lease before the eviction keeps reading valid memory until it releases.
 *
 * <p>
 * The default target is {@code D = A / 8} of the allocator's effective allowance {@code A}; an
 * explicit byte override ({@value #MAX_BYTES_PROPERTY}) replaces it within {@code A}, and zero
 * disables both caches. Numeric snapshots receive three quarters, packed sides one quarter. Within
 * a share, retained bytes plus in-flight construction plus retired-but-leased bytes never exceed
 * the share, and a single entry must fit it. Admission and eviction take a short partition lock;
 * copies and packing run outside it.
 */
public final class DerivedCacheBudget {
  public static final String MAX_BYTES_PROPERTY = "sirix.derivedCache.maxBytes";

  private final MemorySegmentAllocator allocator;
  private final long configuredBytes;
  private final long allocatorBytes;
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
          final MemorySegmentAllocator allocator = Allocators.getInstance();
          if (!allocator.isInitialized()) {
            throw new IllegalStateException("Initialize the persisted allocator before using derived caches");
          }
          result = new DerivedCacheBudget(allocator, allocator.getMaxBufferSize(), property(MAX_BYTES_PROPERTY, -1));
          runtime = result;
        }
      }
    }
    return result;
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
   * @param allocator the allocator whose physical budget every slab is charged to
   * @param allocatorBytes the allocator's effective allowance, the scaling input of the default
   * @param configuredBytes an explicit target, or -1 to derive one eighth of the allowance
   */
  public DerivedCacheBudget(final MemorySegmentAllocator allocator, final long allocatorBytes,
      final long configuredBytes) {
    this.allocator = Objects.requireNonNull(allocator);
    if (allocatorBytes < 0 || configuredBytes < -1) {
      throw new IllegalArgumentException("Memory allowances must be non-negative (target -1 means automatic)");
    }
    this.allocatorBytes = allocatorBytes;
    this.configuredBytes = configuredBytes;
    effectiveBytes = configuredBytes < 0
        ? allocatorBytes / 8
        : Math.min(configuredBytes, allocatorBytes);
    final long packedBytes = effectiveBytes / 4;
    // Round down both shares; at most one byte remains unassigned. Never multiply D by three.
    numeric = new Partition(this, effectiveBytes / 4 * 3 + effectiveBytes % 4 * 3 / 4);
    packed = new Partition(this, packedBytes);
  }

  public MemorySegmentAllocator allocator() {
    return allocator;
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
    return new Metrics(configuredBytes, allocatorBytes, effectiveBytes, n, p, n.retainedBytes() + p.retainedBytes(),
        n.inFlightBytes() + p.inFlightBytes(), n.retiredLeasedBytes() + p.retiredLeasedBytes(),
        saturatedAdd(n.rejectedBytes(), p.rejectedBytes()),
        new LaneMetrics(allocator.getSlabBytes(), ProjectionColumnStore.slabFillsDeclinedCount(),
            NumericLane.materializedCount(), NumericLane.materializedBytes()));
  }

  public record Metrics(long configuredBytes, long allocatorBytes, long effectiveBytes, PartitionMetrics numeric,
      PartitionMetrics packed, long retainedBytes, long inFlightBytes, long retiredLeasedBytes, long rejectedBytes,
      LaneMetrics lanes) {
  }

  /**
   * The off-heap column lanes of the projection stores: slab bytes the allocator currently commits,
   * fills the allocator refused (decoded to the heap instead), and lanes a not-yet-converted consumer
   * copied to the heap ({@link NumericLane#values()}).
   */
  public record LaneMetrics(long slabBytes, long fillsDeclined, long materializedLanes, long materializedBytes) {
  }

  public record PartitionMetrics(long maximumBytes, long retainedBytes, long inFlightBytes, long retiredLeasedBytes,
      long rejectedBytes, int entries) {
  }

  /**
   * Test seam for isolated integration fixtures. Callers must restore it after all packing finishes.
   */
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
    return left > Long.MAX_VALUE - right
        ? Long.MAX_VALUE
        : left + right;
  }

  /** Non-negative byte multiplication, saturating rather than wrapping into an admissible size. */
  public static long saturatedMultiply(final long left, final long right) {
    if (left < 0 || right < 0) {
      throw new IllegalArgumentException("Negative byte weight");
    }
    return right != 0 && left > Long.MAX_VALUE / right
        ? Long.MAX_VALUE
        : left * right;
  }

  /** Round a byte offset up to the next multiple of 64, saturating. */
  public static long alignUp(final long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Negative byte offset");
    }
    return bytes > Long.MAX_VALUE - 63
        ? Long.MAX_VALUE
        : (bytes + 63) & -64L;
  }

  /**
   * A slab-owning cache entry. One lease belongs to the cache while the entry is published, one to
   * each reader view and background task, and the publisher receives one on publication. Leases are
   * counted per view, never per element. When the count reaches zero the slab is released and its
   * bytes refunded; a reader can only acquire a lease while the count is positive, so a freed slab is
   * never resurrected.
   */
  public abstract static class Entry {
    private @Nullable Partition partition;
    private @Nullable MemorySegment slab;
    private long bytes;
    private final AtomicInteger leases = new AtomicInteger(1);
    private long lastUse;
    private boolean published;

    /** The slab this entry owns; valid only while a lease is held. */
    public final MemorySegment slab() {
      return Objects.requireNonNull(slab, "entry is not published");
    }

    public final long bytes() {
      return bytes;
    }

    /** The partition that published this entry. */
    protected final Partition partition() {
      return Objects.requireNonNull(partition, "entry is not published");
    }

    /** Acquire a reader lease; fails once the final lease has dropped. */
    public final boolean tryAcquire() {
      for (;;) {
        final int current = leases.get();
        if (current == 0) {
          return false;
        }
        if (leases.compareAndSet(current, current + 1)) {
          return true;
        }
      }
    }

    /** Drop one lease; the final drop frees the slab and refunds the share. */
    public final void release() {
      final int remaining = leases.decrementAndGet();
      if (remaining == 0) {
        Objects.requireNonNull(partition).freed(this);
      } else if (remaining < 0) {
        throw new IllegalStateException("lease released more often than acquired");
      }
    }

    /** Whether a weakly held owner this entry depends on is gone; stale entries are reaped. */
    protected boolean isStale() {
      return false;
    }

    /** Whether this entry derives from {@code identity}; such entries are evicted with it. */
    protected boolean dependsOn(final Object identity) {
      return false;
    }

    /** Runs under the partition lock right after this entry left the lookup set. */
    protected void evicted() {}

    /**
     * Runs once, outside the partition lock, after the final lease dropped and the slab (if any) was
     * released: nothing reads this entry any more.
     */
    protected void released() {}
  }

  /**
   * One reader's hold on an entry; idempotent, so an explicit close and a cleaner backstop cannot
   * double-release. The lease, never the view that owns it, is the cleaning action.
   */
  public static final class Lease implements Runnable {
    private final Entry entry;
    private final AtomicBoolean released = new AtomicBoolean();

    public Lease(final Entry entry) {
      this.entry = Objects.requireNonNull(entry);
    }

    @Override
    public void run() {
      if (released.compareAndSet(false, true)) {
        entry.release();
      }
    }
  }

  /** A share of the allowance: a short list of published entries under one lock. */
  public static final class Partition {
    private final DerivedCacheBudget budget;
    private final long maximumBytes;
    private final ArrayList<Entry> entries = new ArrayList<>();
    private long retained;
    private long inFlight;
    private long retiredLeased;
    private long rejected;
    private long clock;

    private Partition(final DerivedCacheBudget budget, final long maximumBytes) {
      this.budget = budget;
      this.maximumBytes = maximumBytes;
    }

    public DerivedCacheBudget budget() {
      return budget;
    }

    public long maximumBytes() {
      return maximumBytes;
    }

    /**
     * Reserve {@code bytes} of this share and map a slab for them. The coldest entries no reader holds
     * are evicted first; an entry under a lease stays resident because evicting it would free nothing
     * yet. Returns {@code null} without building when the entry cannot fit the share even then, or when
     * the allocator refuses the slab.
     */
    public @Nullable Reservation reserve(final long bytes) {
      if (bytes <= 0) {
        throw new IllegalArgumentException("Construction weight must be positive");
      }
      synchronized (this) {
        if (bytes > maximumBytes) {
          reject(bytes);
          return null;
        }
        reapStale();
        while (bytes > maximumBytes - retained - inFlight - retiredLeased) {
          final Entry victim = coldestUnleased();
          if (victim == null) {
            reject(bytes);
            return null;
          }
          evictLocked(victim);
        }
        inFlight += bytes;
      }
      final MemorySegment slab = budget.allocator.allocateSlab(bytes);
      if (slab == null) {
        synchronized (this) {
          inFlight -= bytes;
          reject(bytes);
        }
        return null;
      }
      return new Reservation(this, slab, bytes);
    }

    private void reject(final long bytes) {
      rejected = saturatedAdd(rejected, bytes);
    }

    private void reapStale() {
      for (int i = entries.size() - 1; i >= 0; i--) {
        final Entry entry = entries.get(i);
        if (entry.isStale()) {
          evictLocked(entry);
        }
      }
    }

    /** The least recently acquired entry whose only lease is the cache's own. */
    private @Nullable Entry coldestUnleased() {
      Entry coldest = null;
      for (final Entry entry : entries) {
        if (entry.leases.get() == 1 && (coldest == null || entry.lastUse < coldest.lastUse)) {
          coldest = entry;
        }
      }
      return coldest;
    }

    private void evictLocked(final Entry entry) {
      if (!entries.remove(entry)) {
        return;
      }
      entry.published = false;
      retained -= entry.bytes;
      retiredLeased += entry.bytes;
      entry.evicted();
      entry.release();
    }

    /** The final lease dropped: free the slab (a resident entry owns none) and refund the share. */
    private void freed(final Entry entry) {
      final MemorySegment slab = entry.slab;
      if (slab != null) {
        budget.allocator.releaseSlab(slab);
      }
      synchronized (this) {
        retiredLeased -= entry.bytes;
      }
      entry.released();
    }

    /**
     * Publish an entry that owns no slab: its lanes are the store's resident column slabs, which the
     * entry pins for its lifetime; {@code bytes} is what the entry itself retains (its heap keys and
     * masks), charged to this share like any other entry. Fails when the share cannot hold it.
     */
    public boolean publishResident(final Entry entry, final long bytes) {
      Objects.requireNonNull(entry);
      if (bytes < 0) {
        throw new IllegalArgumentException("Negative resident weight");
      }
      synchronized (this) {
        if (bytes > maximumBytes) {
          reject(bytes);
          return false;
        }
        reapStale();
        while (bytes > maximumBytes - retained - inFlight - retiredLeased) {
          final Entry victim = coldestUnleased();
          if (victim == null) {
            reject(bytes);
            return false;
          }
          evictLocked(victim);
        }
        if (!entry.tryAcquire()) {
          throw new IllegalStateException("a fresh entry must accept the publisher's lease");
        }
        retained += bytes;
        entry.partition = this;
        entry.slab = null;
        entry.bytes = bytes;
        entry.published = true;
        entry.lastUse = ++clock;
        entries.add(entry);
        return true;
      }
    }

    private void publish(final Reservation reservation, final Entry entry) {
      synchronized (this) {
        inFlight -= reservation.bytes;
        retained += reservation.bytes;
        entry.partition = this;
        entry.slab = reservation.slab;
        entry.bytes = reservation.bytes;
        entry.published = true;
        entry.lastUse = ++clock;
        entries.add(entry);
      }
    }

    private void abort(final Reservation reservation) {
      synchronized (this) {
        inFlight -= reservation.bytes;
      }
      budget.allocator.releaseSlab(reservation.slab);
    }

    /**
     * The first published entry matching {@code matcher}, with a lease acquired for the caller, or
     * {@code null}. The caller must release that lease.
     */
    public synchronized @Nullable Entry acquire(final Predicate<Entry> matcher) {
      for (final Entry entry : entries) {
        if (matcher.test(entry) && entry.tryAcquire()) {
          entry.lastUse = ++clock;
          return entry;
        }
      }
      return null;
    }

    /** Whether a published entry matches, without acquiring it. */
    public synchronized boolean contains(final Predicate<Entry> matcher) {
      for (final Entry entry : entries) {
        if (matcher.test(entry)) {
          return true;
        }
      }
      return false;
    }

    /** Evict every entry that derives from {@code identity}. */
    public synchronized void evictDependents(final Object identity) {
      for (int i = entries.size() - 1; i >= 0; i--) {
        final Entry entry = entries.get(i);
        if (entry.dependsOn(identity)) {
          evictLocked(entry);
        }
      }
    }

    /** Evict every published entry; leased slabs free when their readers finish. */
    public synchronized void clear() {
      for (int i = entries.size() - 1; i >= 0; i--) {
        evictLocked(entries.get(i));
      }
    }

    public synchronized PartitionMetrics metrics() {
      return new PartitionMetrics(maximumBytes, retained, inFlight, retiredLeased, rejected, entries.size());
    }
  }

  /**
   * An in-flight construction: the slab is mapped and charged; {@link #publish} turns it into a
   * leased entry and {@link #close()} without publication releases it.
   */
  public static final class Reservation implements AutoCloseable {
    private final Partition partition;
    private final MemorySegment slab;
    private final long bytes;
    private boolean published;

    private Reservation(final Partition partition, final MemorySegment slab, final long bytes) {
      this.partition = partition;
      this.slab = slab;
      this.bytes = bytes;
    }

    public MemorySegment slab() {
      return slab;
    }

    public long bytes() {
      return bytes;
    }

    /**
     * Publish the completed entry over this slab. The publisher holds one lease afterwards and must
     * release it; the cache holds its own.
     */
    public void publish(final Entry entry) {
      Objects.requireNonNull(entry);
      if (published) {
        throw new IllegalStateException("reservation already published");
      }
      if (!entry.tryAcquire()) {
        throw new IllegalStateException("a fresh entry must accept the publisher's lease");
      }
      published = true;
      partition.publish(this, entry);
    }

    @Override
    public void close() {
      if (!published) {
        published = true;
        partition.abort(this);
      }
    }
  }
}
