/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;

/**
 * Reusable native blocks for the immutable read images a page decoder owns outright — today the
 * compact HOT-leaf fragment image.
 *
 * <h2>Why this exists</h2>
 *
 * A compact fragment image used to take its slot bytes from {@code Arena.ofAuto().allocate(...)}:
 * one automatic arena per decoded fragment, on the per-fragment read path. That arena is a Java
 * allocation (a 24-byte {@code ArenaImpl} whenever escape analysis fails to remove it, which is
 * decided per compilation) plus a session and a {@code Cleaner} registration whose cost follows GC
 * timing — about 230 bytes of heap per decode, measured, for memory whose exact lifetime the
 * decoder already knows. The project's low-latency rules forbid a per-element allocation on a read
 * path.
 *
 * <h2>What a lease is</h2>
 *
 * {@link #acquire(long)} hands out a {@link Block}: a native segment of the smallest size class
 * that holds the request, following {@link #blockBytesFor(int)} — carved from a slab charged to
 * this pool's own ceiling. The block IS its own releaser ({@link Block#run()}), so the decoder hands it
 * to the page as the frame owner the page already supports and nothing is allocated for the
 * ownership handle either. In steady state the free-stack pop and push, admission counters and
 * lease state changes allocate no Java heap.
 *
 * <h2>Lifetime, and why recycling is safe here</h2>
 *
 * <ul>
 * <li><b>One owner.</b> A leased block belongs to exactly one page. The page releases it from
 * {@code HOTLeafPage.releaseMemory()}, which runs once (won by a {@code closed} compare-and-set)
 * and only with no live guard, so eviction can never recycle a block under a guarded reader.</li>
 * <li><b>Optimistic readers.</b> A page publishes {@code closed == true} and makes its stamp
 * binding odd <em>before</em> invoking this releaser, so a reader that snapshotted the page's stamp
 * before the close cannot validate reads taken over bytes a later lease overwrote: validation sees
 * the odd binding or the closed flag and re-resolves. That ordering is the whole safety argument
 * for giving these images a recyclable owner, and it is the argument the frame-slot allocator's
 * version counters make for ordinary page frames.</li>
 * <li><b>Dirty on reuse.</b> Nothing zeroes a recycled block, exactly as for frame slots: the
 * decoder overwrites the first {@code used} bytes and no consumer may read a compact image outside
 * {@code [0, used)}. A compact image is immutable, so a block's slack is never written.</li>
 * </ul>
 *
 * <h2>Bounds</h2>
 *
 * A compact image that is never closed now leaks its block, where the collector used to reclaim it:
 * the same contract as every other HOT leaf, whose allocator frame leaks just as a missing close
 * leaks a file handle, and {@link #leasedBlockCount()} makes it observable. The pool grows only when
 * a decode finds its size class empty, so its mapped bytes stay at the classes' peak until
 * shutdown, and an idle slab is retained capacity the next decode of that class reuses.
 *
 * <p>
 * That peak is bounded by {@link #maxSlabBytes()}: the share that already bounds what the fragment
 * cache's compact half retains, which is the cache these blocks hold storage for. The pool therefore
 * has exactly one owner and one budget — it charges NOTHING of the allocator's frame commitment, so
 * cached compact images cannot starve a page frame, and nothing has to be handed back under
 * pressure. Memory comes back the way the owning cache's memory always comes back: the cache evicts
 * an image, the image closes, its block returns to the free stack and the next decode reuses it.
 * When the ceiling is reached (or the allocator refuses the mapping) {@link #acquire(long)} returns
 * {@code null} and the decoder keeps its own GC-owned image, which is the fallback it already had.
 * A page's native cache charge is its block's size.
 * </p>
 */
public final class CompactImageStore {

  /** Eight steps per power of two bounds a class's slack at an eighth of its block. */
  private static final int CLASSES_PER_OCTAVE = 8;

  /**
   * Block granularity below {@link #SUBDIVIDED_FROM}, and the smallest block. A compact image's cache
   * charge is its block, so the slack a class rounds up to is footprint: eight classes per octave
   * hold it under an eighth of the block, and 64 bytes at the bottom, where an eighth would be a
   * handful of bytes and the page's own heap is orders of magnitude larger anyway.
   */
  static final int MIN_BLOCK_BYTES = 64;

  /** Above this, each octave is divided into {@link #CLASSES_PER_OCTAVE} classes. */
  private static final int SUBDIVIDED_FROM = MIN_BLOCK_BYTES * CLASSES_PER_OCTAVE;

  /**
   * Largest block: the capacity of a full HOT leaf ({@code HOTLeafPage.DEFAULT_SIZE}), which bounds
   * every legal compact image. A larger request is refused and left to the caller's fallback.
   */
  static final int MAX_BLOCK_BYTES = 64 * 1024;

  private static final int OCTAVE_SHIFT = Integer.numberOfTrailingZeros(CLASSES_PER_OCTAVE);

  private static final int FIRST_OCTAVE = Integer.numberOfTrailingZeros(SUBDIVIDED_FROM);

  private static final int LAST_OCTAVE = Integer.numberOfTrailingZeros(MAX_BLOCK_BYTES) - 1;

  private static final int CLASS_COUNT = CLASSES_PER_OCTAVE * (LAST_OCTAVE - FIRST_OCTAVE + 2);

  /** Slab granularity: enough blocks per slab that the cold growth path is rare. */
  private static final int MIN_SLAB_BYTES = 16 * 1024;

  /** Blocks per slab for a class whose blocks are at least a quarter of {@link #MIN_SLAB_BYTES}. */
  private static final int MIN_BLOCKS_PER_SLAB = 4;

  private static final CompactImageStore INSTANCE = new CompactImageStore(Allocators.getInstance());

  private final MemorySegmentAllocator allocator;

  /**
   * The ceiling this pool's mapped slab bytes may not pass: the share that already bounds what the
   * fragment cache's compact half retains. One budget, one owner — the pool holds storage on that
   * cache's behalf, so it answers to that cache's capacity and to no other consumer's.
   */
  // Volatile only for the test seam below; growth reads it once per mapped slab, never per lease.
  private volatile long maxSlabBytes;

  /** Mapped slab bytes, admitted against {@link #maxSlabBytes} before a slab is mapped. */
  private final AtomicLong slabBytes = new AtomicLong();

  private final SizeClass[] classes;

  private final AtomicInteger leasedBlocks = new AtomicInteger();

  private final AtomicInteger acquisitions = new AtomicInteger();

  private volatile boolean stopped;

  // Test callbacks, in atomic holders rather than volatile reference fields: the publication is the
  // only ordering they need, and an AtomicReference says so in the type instead of in a comment.
  private final AtomicReference<@Nullable Runnable> leasePublicationHook = new AtomicReference<>();

  private final AtomicReference<@Nullable Runnable> slabReleaseHook = new AtomicReference<>();

  private final AtomicReference<@Nullable Runnable> slabPublicationHook = new AtomicReference<>();

  /** Test-only: refuse every lease, so the caller's fallback path runs. */
  private volatile boolean refuseLeases;

  /**
   * Test-only: fill a released block with {@code 0x5A}, so a read of a block whose page has been
   * closed returns poison instead of the stale image it would otherwise still find. Mirrors
   * {@code FrameSlotAllocator.setPoisonOnReleaseForTesting}: a use-after-free that is occasional by
   * luck becomes certain.
   */
  private volatile boolean poisonOnRelease;

  CompactImageStore(final MemorySegmentAllocator allocator) {
    this(allocator, HeapCacheBudget.global().bytes(HeapCacheBudget.Pool.HOT_IMAGES));
  }

  CompactImageStore(final MemorySegmentAllocator allocator, final long maxSlabBytes) {
    if (maxSlabBytes < 0L) {
      throw new IllegalArgumentException("maxSlabBytes=" + maxSlabBytes);
    }
    this.allocator = allocator;
    this.maxSlabBytes = maxSlabBytes;
    classes = new SizeClass[CLASS_COUNT];
    for (int i = 0; i < CLASS_COUNT; i++) {
      classes[i] = new SizeClass(this, blockBytesOfClass(i));
    }
    if (allocator instanceof FrameSlotAllocator frameAllocator) {
      // Last statement of the constructor, and the registration only stores the reference: the
      // allocator reaches back into this store from its pressure path, which cannot run before a
      // decode has leased a block from it.
      frameAllocator.registerCompactImages(this);
    }
  }

  /** The process-wide store, drawing its slabs from the process-wide allocator. */
  public static CompactImageStore getInstance() {
    return INSTANCE;
  }

  /**
   * Lease a reusable native block of at least {@code byteSize} bytes.
   *
   * @param byteSize the image size the caller is about to write
   * @return the leased block, or {@code null} when this store cannot serve the request — it exceeds
   *         {@link #MAX_BLOCK_BYTES}, admission is stopped, or the allocator refuses another slab.
   *         The caller then falls back to its own allocation.
   * @throws IllegalArgumentException if {@code byteSize} is negative
   */
  public @Nullable Block acquire(final long byteSize) {
    if (byteSize < 0L) {
      throw new IllegalArgumentException("byteSize=" + byteSize);
    }
    if (byteSize > MAX_BLOCK_BYTES || refuseLeases || stopped) {
      return null;
    }
    acquisitions.incrementAndGet();
    try {
      if (stopped) {
        return null;
      }
      final SizeClass sizeClass = classes[classIndexFor((int) byteSize)];
      final Block block = sizeClass.take();
      if (block == null) {
        return null;
      }
      final Runnable hook = leasePublicationHook.get();
      if (hook != null) {
        hook.run();
      }
      // After admission, acquisitions keeps every slab mapped until this handout completes.
      block.lease();
      leasedBlocks.incrementAndGet();
      return block;
    } finally {
      acquisitions.decrementAndGet();
    }
  }

  /**
   * The block {@code byteSize} is served from: {@code byteSize} rounded up to this class table's
   * granularity, which is {@link #MIN_BLOCK_BYTES} up to {@link #SUBDIVIDED_FROM} and an eighth of
   * the enclosing power of two above it.
   */
  static int blockBytesFor(final int byteSize) {
    if (byteSize <= SUBDIVIDED_FROM) {
      return Math.max(MIN_BLOCK_BYTES, (byteSize + MIN_BLOCK_BYTES - 1) & -MIN_BLOCK_BYTES);
    }
    // byteSize lies in (2^octave, 2^(octave+1)], whose classes step by an eighth of 2^octave.
    final int octave = Integer.SIZE - 1 - Integer.numberOfLeadingZeros(byteSize - 1);
    final int step = 1 << (octave - OCTAVE_SHIFT);
    return (byteSize + step - 1) & -step;
  }

  /** The class serving {@code byteSize}. */
  static int classIndexFor(final int byteSize) {
    if (byteSize <= SUBDIVIDED_FROM) {
      return blockBytesFor(byteSize) / MIN_BLOCK_BYTES - 1;
    }
    final int octave = Integer.SIZE - 1 - Integer.numberOfLeadingZeros(byteSize - 1);
    final int step = 1 << (octave - OCTAVE_SHIFT);
    // Blocks of this octave are the multiples of step from 9 * step to 16 * step.
    final int withinOctave = blockBytesFor(byteSize) / step - CLASSES_PER_OCTAVE - 1;
    return CLASSES_PER_OCTAVE * (octave - FIRST_OCTAVE + 1) + withinOctave;
  }

  /** The block size of class {@code index}; the inverse of {@link #classIndexFor(int)}. */
  static int blockBytesOfClass(final int index) {
    if (index < CLASSES_PER_OCTAVE) {
      return MIN_BLOCK_BYTES * (index + 1);
    }
    final int octave = FIRST_OCTAVE + index / CLASSES_PER_OCTAVE - 1;
    final int step = 1 << (octave - OCTAVE_SHIFT);
    return step * (CLASSES_PER_OCTAVE + 1 + index % CLASSES_PER_OCTAVE);
  }

  /**
   * Admit {@code bytes} of fresh slab against this pool's own ceiling.
   *
   * <p>
   * This is the whole of the pool's memory policy: it may map what its owner's share allows and not
   * one byte more, so no other consumer of the machine's memory — above all a page frame — can be
   * starved by cached compact images, and nothing ever has to be handed back under pressure. A
   * refusal reaches the decoder as a {@code null} block, which falls back to its own GC-owned image.
   * </p>
   *
   * @return whether the bytes were admitted
   */
  private boolean tryAdmitSlabBytes(final long bytes) {
    long current = slabBytes.getAcquire();
    while (current + bytes <= maxSlabBytes) {
      if (slabBytes.compareAndSet(current, current + bytes)) {
        return true;
      }
      current = slabBytes.getAcquire();
    }
    return false;
  }

  private void refundSlabBytes(final long bytes) {
    slabBytes.addAndGet(-bytes);
  }

  /** The ceiling this pool's mapped slab bytes may not pass. */
  public long maxSlabBytes() {
    return maxSlabBytes;
  }

  /**
   * Test-only: move this pool's ceiling, so a test can reach the refusal the production ceiling
   * would need hundreds of megabytes to reach. Lowering it below what is mapped refuses growth; it
   * never takes a mapped slab away.
   */
  void setMaxSlabBytesForTesting(final long bytes) {
    if (bytes < 0L) {
      throw new IllegalArgumentException("bytes=" + bytes);
    }
    maxSlabBytes = bytes;
  }

  /** Mapped slab bytes, leased blocks, free blocks and per-slab slack alike. */
  public long mappedSlabBytes() {
    return slabBytes.get();
  }

  /** Why a cleanup refused, for a diagnosable failure rather than a bare "in flight". */
  public String describeInFlight() {
    return "leases=" + leasedBlocks.get() + " acquisitions=" + acquisitions.get() + " blocks=" + blockCount();
  }

  /** Blocks currently leased to a page. */
  public int leasedBlockCount() {
    return leasedBlocks.get();
  }

  /** Blocks this store has carved, leased and free alike. */
  public int blockCount() {
    int total = 0;
    for (final SizeClass sizeClass : classes) {
      total += sizeClass.blockCount();
    }
    return total;
  }

  /** Native bytes this store has carved into blocks. */
  public long pooledBytes() {
    long total = 0L;
    for (final SizeClass sizeClass : classes) {
      total += sizeClass.slabBytes();
    }
    return total;
  }

  /**
   * Stop lease admission and give every slab back to the allocator for shutdown.
   *
   * @return {@code true} if the store is now empty and stopped, {@code false} if a lease or
   *         acquisition still holds it; a refusal restores admission so shutdown can be retried
   */
  public synchronized boolean releaseIdleSlabs() {
    stopped = true;
    if (acquisitions.get() != 0 || leasedBlocks.get() != 0) {
      stopped = false;
      return false;
    }
    final Runnable hook = slabReleaseHook.get();
    if (hook != null) {
      hook.run();
    }
    Throwable cleanupFailure = null;
    for (final SizeClass sizeClass : classes) {
      try {
        sizeClass.releaseSlabs(allocator);
      } catch (final RuntimeException | Error failure) {
        cleanupFailure = FrameSlotAllocator.retainCleanupFailure(cleanupFailure, failure);
      }
    }
    if (cleanupFailure != null) {
      FrameSlotAllocator.rethrowCleanupFailure(cleanupFailure);
    }
    return true;
  }

  synchronized void resumeAfterRejectedShutdown() {
    stopped = false;
  }

  void setLeasePublicationHookForTesting(final @Nullable Runnable hook) {
    leasePublicationHook.set(hook);
  }

  void setSlabReleaseHookForTesting(final @Nullable Runnable hook) {
    slabReleaseHook.set(hook);
  }

  void setSlabPublicationHookForTesting(final @Nullable Runnable hook) {
    slabPublicationHook.set(hook);
  }

  int directoryGrowthCount() {
    int total = 0;
    for (final SizeClass sizeClass : classes) {
      total += sizeClass.directoryGrowths.get();
    }
    return total;
  }

  /** Test-only: make every {@link #acquire(long)} refuse, exercising the caller's fallback. */
  public void setRefuseLeasesForTesting(final boolean refuse) {
    refuseLeases = refuse;
  }

  /** Test-only: poison a block's bytes when its page releases it. */
  public void setPoisonOnReleaseForTesting(final boolean poison) {
    poisonOnRelease = poison;
  }

  /**
   * One leased native block, and its own releaser.
   *
   * <p>
   * The decoder hands this object to the page as the frame owner the page already releases exactly
   * once. Reusing the block's own object as the releaser is what keeps a lease free of Java
   * allocation; the lease flag catches a release of a block that is not leased, which is the
   * detectable half of a double free.
   * </p>
   */
  public static final class Block implements Runnable {

    private static final int FREE = 0;

    private static final int LEASED = 1;

    private final SizeClass sizeClass;

    private final MemorySegment segment;

    /** Index in {@link SizeClass#blocks}; also this block's identity in the free list. */
    private final int index;

    /**
     * Next free block of the same class, or -1 at the bottom of the stack. Written before the push
     * compare-and-set and read after the pop's, both on {@link SizeClass#freeStack}, which orders the
     * two.
     */
    private int nextFree = -1;

    /**
     * {@link #LEASED} while a page owns this block. An {@link AtomicInteger} rather than a
     * {@code VarHandle} on a field of this class: its compare-and-set reaches {@code Unsafe} directly,
     * where an interpreted {@code VarHandle} access boxes its arguments — which would put an allocation
     * on the lease path for as long as it takes the JIT to compile it.
     */
    private final AtomicInteger state = new AtomicInteger(FREE);

    Block(final SizeClass sizeClass, final MemorySegment segment, final int index) {
      this.sizeClass = sizeClass;
      this.segment = segment;
      this.index = index;
    }

    /** The native bytes to write the image into; at least the requested size. */
    public MemorySegment segment() {
      return segment;
    }

    /** Return this block to its pool. Runs exactly once per lease. */
    @Override
    public void run() {
      if (!state.compareAndSet(LEASED, FREE)) {
        throw new IllegalStateException("Compact-image block " + index + " of the " + sizeClass.blockBytes
            + "-byte class was released while not leased");
      }
      if (sizeClass.store.poisonOnRelease) {
        segment.fill((byte) 0x5A);
      }
      sizeClass.give(this);
    }

    /** Whether a page currently owns this block. */
    public boolean isLeased() {
      return state.get() == LEASED;
    }

    void lease() {
      state.set(LEASED);
    }
  }

  /**
   * One fixed block size: its slabs, its blocks, and a lock-free stack of the free ones.
   *
   * <p>
   * The stack is a Treiber stack over block indices with a counter in the high half of the head word,
   * so a pop cannot be fooled by a block that was taken and given back between its read of the head
   * and its compare-and-set. Growth is the only path that takes the monitor, and only when a decode
   * found the stack empty.
   * </p>
   */
  static final class SizeClass {

    /** No block is free; the low half holds {@code index + 1}, so zero cannot name a block. */
    private static final long EMPTY_TOP = 0L;

    private static final long INDEX_MASK = 0xFFFF_FFFFL;

    private static final Block[] EMPTY_BLOCKS = new Block[0];

    private final CompactImageStore store;

    private final int blockBytes;

    private final int blocksPerSlab;

    private final AtomicLong freeStack = new AtomicLong(EMPTY_TOP);

    /** Index to block. Grows under the monitor; shutdown detaches it after stopping admission. */
    // A published entry never changes: growth publishes new entries before pushing them onto the
    // free stack, and only shutdown replaces the directory, after stopping admission.
    // The holder is atomic rather than a volatile array field: a get() is the same single load the
    // volatile field was, and the type no longer invites the reading that the ARRAY is thread-safe.
    private final AtomicReference<Block[]> blocks = new AtomicReference<>(EMPTY_BLOCKS);

    /** Where the next slab's blocks start. */
    private final AtomicInteger nextIndex = new AtomicInteger();

    /** Blocks this class holds right now. Written under this monitor. */
    private final AtomicInteger liveBlocks = new AtomicInteger();

    /** How often growth had to copy the directory. Diagnostic; same writer and readers. */
    private final AtomicInteger directoryGrowths = new AtomicInteger();

    /** One mapped slab and the directory index its first block took. */
    private record SlabSpan(MemorySegment segment, int baseIndex) {
    }

    // Read and written only under this class's monitor: growth appends, shutdown iterates.
    private final List<SlabSpan> slabs = new ArrayList<>();

    SizeClass(final CompactImageStore store, final int blockBytes) {
      this.store = store;
      this.blockBytes = blockBytes;
      final int slabBytes = Math.max(MIN_SLAB_BYTES, blockBytes * MIN_BLOCKS_PER_SLAB);
      blocksPerSlab = slabBytes / blockBytes;
    }

    int blockCount() {
      return liveBlocks.get();
    }

    long slabBytes() {
      return (long) liveBlocks.get() * blockBytes;
    }

    /** Pop a free block, growing the class once when none is free. */
    @Nullable
    Block take() {
      Block block = pop();
      if (block == null) {
        synchronized (this) {
          // A waiter reuses blocks returned or published while it waited for growth.
          block = pop();
          if (block == null) {
            block = grow();
          }
        }
      }
      return block;
    }

    void give(final Block block) {
      // Only Block.run calls this, on the final SizeClass that constructed that block. A block that
      // is not this class's current directory entry could only arrive through a double release or a
      // release after teardown.
      final Block[] current = blocks.get();
      if (block.index >= current.length || current[block.index] != block) {
        store.leasedBlocks.decrementAndGet();
        throw new IllegalStateException(
            "Compact-image block " + block.index + " of the " + blockBytes + "-byte class is not this class's");
      }
      push(block);
      store.leasedBlocks.decrementAndGet();
    }

    private @Nullable Block pop() {
      for (;;) {
        final long head = freeStack.get();
        final int top = (int) (head & INDEX_MASK) - 1;
        if (top < 0) {
          return null;
        }
        // A block never leaves the array while the class lives, so an index on the stack resolves.
        final Block candidate = blocks.get()[top];
        if (freeStack.compareAndSet(head, nextHead(head, candidate.nextFree))) {
          return candidate;
        }
      }
    }

    private void push(final Block block) {
      for (;;) {
        final long head = freeStack.get();
        block.nextFree = (int) (head & INDEX_MASK) - 1;
        if (freeStack.compareAndSet(head, nextHead(head, block.index))) {
          return;
        }
      }
    }

    /** The head word naming {@code topIndex}, with the counter that defeats ABA advanced. */
    private static long nextHead(final long head, final int topIndex) {
      return ((head + 0x1_0000_0000L) & ~INDEX_MASK) | ((topIndex + 1) & INDEX_MASK);
    }

    /**
     * Map one more slab, push all but one of its blocks and return that one.
     *
     * @return the block to lease, or {@code null} when this pool's own ceiling or the allocator has
     *         no room for another slab
     */
    private @Nullable Block grow() {
      final Block[] current = blocks.get();
      final int base = nextIndex.get();
      final int required = Math.addExact(base, blocksPerSlab);
      final long slabBytes = (long) blocksPerSlab * blockBytes;
      if (!store.tryAdmitSlabBytes(slabBytes)) {
        return null;
      }
      final MemorySegment slab = store.allocator.allocateOwnerBudgetedSlab(slabBytes);
      if (slab == null) {
        store.refundSlabBytes(slabBytes);
        return null;
      }
      final Block[] grown;
      try {
        grown = required > current.length
            ? Arrays.copyOf(current, (int) Math.max(required, Math.min(Integer.MAX_VALUE, current.length * 2L)))
            : current;
        for (int i = 0; i < blocksPerSlab; i++) {
          grown[base + i] = new Block(this, slab.asSlice((long) i * blockBytes, blockBytes), base + i);
        }
        final Runnable hook = store.slabPublicationHook.get();
        if (hook != null) {
          hook.run();
        }
        slabs.add(new SlabSpan(slab, base));
      } catch (final RuntimeException | Error failure) {
        // Only [0, nextIndex) is published. A retry overwrites the unpublished suffix before
        // pushing any new block, and these global-scope views do not own the native mapping.
        try {
          store.allocator.releaseSlab(slab);
        } catch (final RuntimeException | Error cleanupFailure) {
          FrameSlotAllocator.retainCleanupFailure(failure, cleanupFailure);
        } finally {
          store.refundSlabBytes(slabBytes);
        }
        throw failure;
      }
      blocks.set(grown);
      nextIndex.set(required);
      liveBlocks.addAndGet(blocksPerSlab);
      if (grown != current) {
        directoryGrowths.incrementAndGet();
      }
      for (int i = 1; i < blocksPerSlab; i++) {
        push(grown[base + i]);
      }
      return grown[base];
    }

    synchronized void releaseSlabs(final MemorySegmentAllocator allocator) {
      freeStack.set(EMPTY_TOP);
      blocks.set(EMPTY_BLOCKS);
      nextIndex.set(0);
      liveBlocks.set(0);
      // This class gives up every slab here, so its bytes leave the pool's account whatever a
      // release reports: a mapping a release failed to free is the allocator's leak to report, not
      // capacity this pool may still hand out.
      final long given = (long) slabs.size() * blocksPerSlab * blockBytes;
      Throwable cleanupFailure = null;
      try {
        for (int i = 0; i < slabs.size(); i++) {
          try {
            allocator.releaseSlab(slabs.get(i).segment());
          } catch (final RuntimeException | Error failure) {
            cleanupFailure = FrameSlotAllocator.retainCleanupFailure(cleanupFailure, failure);
          }
        }
      } finally {
        slabs.clear();
        store.refundSlabBytes(given);
      }
      if (cleanupFailure != null) {
        FrameSlotAllocator.rethrowCleanupFailure(cleanupFailure);
      }
    }
  }
}
