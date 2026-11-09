/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * An exclusive borrow of one scratch buffer from the allocator: the memory a read lands in, a page
 * is serialized into, a codec stages its output in. The allocator is the only owner of such memory;
 * nothing else keeps spare buffers.
 *
 * <p>
 * <b>What a borrow gets.</b> A buffer of at least the requested size, rounded up to the allocator's
 * next capacity: a frame slot up to 256 KiB, a mapped region of 512 KiB to 16 MiB above that, an
 * exactly sized region beyond. It is charged to the allocator's one physical budget, is never an
 * eviction candidate while lent, and is <em>not zeroed</em>: it holds whatever its last borrower
 * left, so a borrower that needs zeros writes them.
 *
 * <p>
 * <b>Lifetime.</b> The lease is the borrow's close authority. {@link #close()} returns the buffer
 * exactly once, from any thread, however often it is called; after it the {@link #segment()} and
 * {@link #buffer()} handed out before must not be touched, because the next borrower owns the
 * bytes. Whatever outlives the borrow (a decoded page, a cached image) must own a copy or its own
 * allocation before the lease closes.
 *
 * <p>
 * <b>Reuse.</b> A lease is a small handle and may be borrowed with again after it was closed, so an
 * owner that borrows repeatedly (a serializer, a scanner) keeps one lease instead of creating one
 * per borrow. A lease is used by one thread at a time; handing it to another thread hands over the
 * borrow, and only {@code close()} may race with itself.
 *
 * <pre>{@code
 * try (ScratchLease lease = ScratchLease.of(length, ScratchUse.READ)) {
 *   final ByteBuffer buffer = lease.buffer().limit(length);
 *   channel.read(buffer, offset);
 *   decode(lease.segment(), length); // copies what it keeps
 * }
 * }</pre>
 */
public final class ScratchLease implements AutoCloseable {

  private final MemorySegmentAllocator allocator;

  /**
   * The buffer this lease holds, or {@code null}. Accessed through
   * {@link ScratchHandles#LEASE_CARRIER}: a close takes it with one atomic exchange.
   */
  @Nullable
  ScratchCarrier carrier;

  /** The era the held buffer was lent in; named by the return. */
  private long era;

  /** What the held buffer was borrowed for; a replacement taken by {@link #grow} keeps it. */
  private ScratchUse use = ScratchUse.READ;

  /** A lease on the process allocator ({@link Allocators#getInstance()}), holding nothing yet. */
  public ScratchLease() {
    this(Allocators.getInstance());
  }

  /**
   * A lease on {@code allocator}, holding nothing yet.
   *
   * @param allocator the allocator to borrow from
   */
  public ScratchLease(final MemorySegmentAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  /**
   * Borrow a buffer of at least {@code minBytes} from the process allocator.
   *
   * @param minBytes the bytes needed; positive
   * @param use what the buffer is borrowed for
   * @return a lease holding the buffer
   * @throws OutOfMemoryError when the budget cannot serve the borrow
   */
  public static ScratchLease of(final long minBytes, final ScratchUse use) {
    return new ScratchLease().acquire(minBytes, use);
  }

  /**
   * Borrow a buffer of at least {@code minBytes}. The borrow is mandatory: when the budget has no
   * room it asks the page caches for relief and waits as a page frame does before it fails.
   *
   * @param minBytes the bytes needed; positive
   * @param use what the buffer is borrowed for
   * @return this lease, holding the buffer
   * @throws IllegalStateException when this lease already holds a buffer
   * @throws OutOfMemoryError when the budget cannot serve the borrow
   */
  public ScratchLease acquire(final long minBytes, final ScratchUse use) {
    bind(minBytes, use, true);
    return this;
  }

  /**
   * Borrow a buffer of at least {@code minBytes} if the budget has room right now. For work with a
   * cheaper fallback, such as a coalesced read that can be split into single-page reads: a refusal
   * costs nothing and evicts nothing.
   *
   * @param minBytes the bytes needed; positive
   * @param use what the buffer is borrowed for
   * @return whether this lease holds a buffer now
   * @throws IllegalStateException when this lease already holds a buffer
   */
  public boolean tryAcquire(final long minBytes, final ScratchUse use) {
    return bind(minBytes, use, false);
  }

  private boolean bind(final long minBytes, final ScratchUse use, final boolean mandatory) {
    Objects.requireNonNull(use, "use");
    if (minBytes <= 0) {
      throw new IllegalArgumentException("scratch size must be positive: " + minBytes);
    }
    if (carrier != null) {
      throw new IllegalStateException("this lease already holds a scratch buffer; close it before borrowing again");
    }
    final ScratchCarrier lent = ScratchSource.of(allocator).acquire(minBytes, use, mandatory);
    if (lent == null) {
      return false;
    }
    // The era is read after the source lent the buffer to this thread alone, and written before the
    // carrier: a close on another thread that takes the carrier sees its era.
    this.era = lent.era;
    this.use = use;
    ScratchHandles.LEASE_CARRIER.setRelease(this, lent);
    return true;
  }

  /** Whether this lease holds a buffer. */
  public boolean isHeld() {
    return carrier != null;
  }

  /**
   * The whole buffer: {@link #capacity()} bytes, at least the bytes asked for. The view carries no
   * per-access liveness check, so it is as cheap to read as memory gets and exactly as unsafe to keep
   * after {@link #close()}.
   *
   * @throws IllegalStateException when this lease holds no buffer
   */
  public MemorySegment segment() {
    return held().segment;
  }

  /**
   * A little-endian direct {@link ByteBuffer} over the whole buffer, for the I/O calls that take one.
   * It is the same instance for the whole borrow, cleared when the borrow began, and its position,
   * limit and byte order are this lease's to change.
   *
   * @throws IllegalStateException when this lease holds no buffer
   * @throws UnsupportedOperationException when the buffer is larger than a {@code ByteBuffer} can
   *         address
   */
  public ByteBuffer buffer() {
    return held().buffer();
  }

  /**
   * Bytes of the held buffer.
   *
   * @throws IllegalStateException when this lease holds no buffer
   */
  public long capacity() {
    return held().capacity;
  }

  /**
   * Make the held buffer at least {@code minBytes} large, keeping its first {@code preserveBytes}
   * bytes. A buffer that is large enough stays. Otherwise the replacement is borrowed first, the
   * prefix copied, and the old buffer returned, so both are charged while they overlap; views of the
   * old buffer are dead afterwards.
   *
   * @param minBytes the bytes needed
   * @param preserveBytes the leading bytes to carry over; at most the current capacity
   * @return whether the buffer was replaced
   * @throws IllegalStateException when this lease holds no buffer
   * @throws OutOfMemoryError when the budget cannot serve the larger borrow; the lease keeps its
   *         buffer
   */
  public boolean grow(final long minBytes, final long preserveBytes) {
    final ScratchCarrier current = held();
    if (preserveBytes < 0 || preserveBytes > current.capacity) {
      throw new IllegalArgumentException(
          "cannot preserve " + preserveBytes + " bytes of a " + current.capacity + "-byte scratch buffer");
    }
    if (minBytes <= current.capacity) {
      return false;
    }
    final ScratchSource source = ScratchSource.of(allocator);
    final ScratchCarrier grown = Objects.requireNonNull(source.acquire(minBytes, use, true));
    if (preserveBytes > 0) {
      MemorySegment.copy(current.segment, 0L, grown.segment, 0L, preserveBytes);
    }
    final long currentEra = era;
    this.era = grown.era;
    ScratchHandles.LEASE_CARRIER.setRelease(this, grown);
    current.source.release(current, currentEra);
    return true;
  }

  /**
   * Return the buffer. Idempotent and safe from any thread: exactly one call returns it, the others
   * do nothing.
   */
  @Override
  public void close() {
    final ScratchCarrier held = (ScratchCarrier) ScratchHandles.LEASE_CARRIER.getAndSet(this, null);
    if (held != null) {
      held.source.release(held, era);
    }
  }

  private ScratchCarrier held() {
    final ScratchCarrier held = carrier;
    if (held == null) {
      throw new IllegalStateException("this lease holds no scratch buffer");
    }
    return held;
  }
}
