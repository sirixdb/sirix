package io.sirix.access.trx;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Tracks the minimum active revision across all transactions (epoch-based MVCC).
 * <p>
 * This enables safe page eviction: a page can only be evicted when its revision is less than the
 * minimum active revision (no transaction needs it anymore).
 * <p>
 * Inspired by LeanStore/CedarDB epoch-based reclamation, adapted to Sirix's revision-based MVCC
 * model.
 *
 * <h2>HFT-grade design</h2>
 * <ul>
 *   <li>Slot state is packed into a single {@code long} per slot — bit 63 is the active flag,
 *       bits 0..31 hold the revision number. Replaces the previous
 *       {@code AtomicReferenceArray<Slot>} (one volatile object per slot, two volatile fields
 *       per slot, three pointer hops to read state). The packed layout is one cache-line
 *       prefetch every eight slots and one volatile read per state access via a {@link VarHandle}.</li>
 *   <li>{@link #register} is O(1): a free-list of slot indices in a primitive {@code int[]}
 *       is popped under a single intrinsic monitor. The lock holds for ~10 ns; outside it,
 *       the volatile slot write is lock-free.</li>
 *   <li>{@link #deregister} is O(1): clears the slot's volatile state then pushes the index
 *       back. Idempotent on a {@code null} ticket.</li>
 *   <li>{@link #minActiveRevision} is lock-free: walks the {@code long[]} with one volatile
 *       read per slot. At the default 65,536 slot count one full scan is ~30 µs, well under
 *       the typical 100 ms ClockSweeper interval.</li>
 *   <li>Default capacity is 65,536 slots — {@code ~512 KB} of permanent heap (one
 *       {@code long} per slot in the state array plus an {@code int} per slot in the free
 *       stack), enough for query engines fronting Sirix with thousands of concurrent
 *       readers. Override via {@code -D}{@value #SLOT_COUNT_PROPERTY}{@code =N}.</li>
 * </ul>
 *
 * <h2>Memory ordering</h2>
 * <p>{@code register} writes the packed state via {@link VarHandle#setVolatile} <em>after</em>
 * popping the index under the lock; {@code deregister} clears the active bit via a
 * {@code compareAndSet} (which also rejects stale tickets — see {@link #deregister})
 * <em>before</em> pushing the index back. {@code minActiveRevision} reads each slot via
 * {@link VarHandle#getVolatile}. The volatile write/read pair establishes happens-before from
 * the registering transaction's revision assignment to any subsequent sweeper observation,
 * which is the safety the eviction watermark requires (see docs/formal-verification.md, Inv 8).
 *
 * @author Johannes Lichtenberger
 */
public final class RevisionEpochTracker {

  /** System property to override the slot count. Must be a positive integer. */
  public static final String SLOT_COUNT_PROPERTY = "sirix.epoch.tracker.slots";

  /**
   * Default slot count, applied when {@link #defaultSlotCount()} is used by callers that don't
   * pick an explicit count. 65,536 is enough for all typical bitemporal Sirix workloads and
   * costs ~768 KB of permanent heap (state long[] + free-stack int[]).
   */
  public static final int DEFAULT_SLOT_COUNT = 65_536;

  /** Bit set in the packed state when the slot is active. The low 32 bits hold the revision. */
  private static final long ACTIVE_BIT = 1L << 63;

  /**
   * Bits 32..62 of the packed state hold a per-slot generation counter (31 bits, wraps).
   * The generation is bumped on every {@link #register} and echoed in the returned
   * {@link Ticket}, which makes {@link #deregister} ABA-safe: a stale ticket (double
   * deregister, or a deregister racing a slot that was already released and re-registered)
   * no longer matches the slot's current generation and is ignored instead of corrupting
   * the free stack / clearing another transaction's slot (issue #1102).
   */
  private static final int GENERATION_SHIFT = 32;
  private static final long GENERATION_MASK = 0x7FFF_FFFFL;

  /** Volatile-access handle for the slot-state array. */
  private static final VarHandle SLOT_VH = MethodHandles.arrayElementVarHandle(long[].class);

  /**
   * Ticket returned when registering a transaction. Used to deregister the transaction later in
   * O(1). The fields are final; one Ticket per register call — escape-analysis-friendly and,
   * since the fields are primitive, cheap to allocate. The generation binds the ticket to this
   * specific registration of the slot, so stale tickets are rejected on deregistration.
   */
  public static final class Ticket {
    private final int slotIndex;
    private final int generation;

    private Ticket(final int slotIndex, final int generation) {
      this.slotIndex = slotIndex;
      this.generation = generation;
    }

    public int getSlotIndex() {
      return slotIndex;
    }
  }

  /** Packed (active, revision) state per slot. Accessed via {@link #SLOT_VH}. */
  private final long[] slotStates;
  private final int slotCount;
  /** Stack of free slot indices. {@link #freeTop} is the count of free slots. */
  private final int[] freeStack;
  private int freeTop;
  /** Used only for {@link #getDiagnostics} reporting; updated under {@link #lock}. */
  private int highWaterMark;
  private final Object lock = new Object();
  private volatile int lastCommittedRevision;

  /**
   * Resolve the configured slot count from the {@value #SLOT_COUNT_PROPERTY} system property,
   * falling back to {@link #DEFAULT_SLOT_COUNT}. Invalid or non-positive values fall back to
   * the default.
   */
  public static int defaultSlotCount() {
    final String prop = System.getProperty(SLOT_COUNT_PROPERTY);
    if (prop == null || prop.isEmpty()) {
      return DEFAULT_SLOT_COUNT;
    }
    try {
      final int parsed = Integer.parseInt(prop.trim());
      return parsed > 0 ? parsed : DEFAULT_SLOT_COUNT;
    } catch (final NumberFormatException ignored) {
      return DEFAULT_SLOT_COUNT;
    }
  }

  /**
   * Create a new RevisionEpochTracker.
   *
   * @param slotCount number of concurrent transaction slots (power of 2 recommended)
   */
  public RevisionEpochTracker(final int slotCount) {
    if (slotCount <= 0) {
      throw new IllegalArgumentException("slotCount must be > 0, got " + slotCount);
    }
    this.slotCount = slotCount;
    this.slotStates = new long[slotCount];
    this.freeStack = new int[slotCount];
    // Push all indices onto the free stack in reverse order so register() pops 0, 1, 2, ...
    for (int i = 0; i < slotCount; i++) {
      freeStack[i] = slotCount - 1 - i;
    }
    this.freeTop = slotCount;
    this.highWaterMark = 0;
    this.lastCommittedRevision = 0;
  }

  /**
   * Register a transaction at the given revision. Returns a ticket that must be used to deregister
   * later.
   *
   * @param revision the revision number the transaction is reading
   * @return ticket for deregistration
   * @throws IllegalStateException if no free slots available
   */
  public Ticket register(final int revision) {
    final int slotIndex;
    synchronized (lock) {
      if (freeTop == 0) {
        throw new IllegalStateException("No free slots in RevisionEpochTracker (max=" + slotCount + "). "
            + "Too many concurrent transactions. Raise the cap via -D" + SLOT_COUNT_PROPERTY + "=N.");
      }
      slotIndex = freeStack[--freeTop];
      final int active = slotCount - freeTop;
      if (active > highWaterMark) {
        highWaterMark = active;
      }
    }
    // The slot is exclusively ours between pop and push-back, so a plain read of the retired
    // generation is safe; bump it so tickets from earlier registrations of this slot go stale.
    final long previousState = (long) SLOT_VH.getVolatile(slotStates, slotIndex);
    final int generation = (int) (((previousState >>> GENERATION_SHIFT) + 1) & GENERATION_MASK);
    // Volatile write: bit 63 set (active) + generation in bits 32..62 + revision in the low
    // 32 bits. After this returns, any subsequent volatile read of the slot observes the
    // active state with the right revision.
    SLOT_VH.setVolatile(slotStates, slotIndex,
        ACTIVE_BIT | ((long) generation << GENERATION_SHIFT) | (revision & 0xFFFF_FFFFL));
    return new Ticket(slotIndex, generation);
  }

  /**
   * Deregister a transaction. Idempotent: a ticket that was already deregistered — or whose slot
   * has since been released and handed to another transaction — no longer matches the slot's
   * current generation and is ignored. Before this ABA guard existed, a double-deregister pushed
   * the slot index onto an already-full free stack; the failed store still incremented
   * {@code freeTop} (the index expression is evaluated before the bounds check), permanently
   * poisoning the tracker so every later {@link #register} threw (issue #1102).
   *
   * @param ticket the ticket from registration
   */
  public void deregister(final Ticket ticket) {
    if (ticket == null) {
      return;
    }
    // Clear the slot first so a concurrent minActiveRevision can no longer see it as active —
    // we only re-push the index onto the free stack after the volatile clear has been published.
    // CAS from the exact registered state: only the caller that actually owns this registration
    // (active bit set, matching generation) wins the release; stale or duplicate tickets fail
    // the compare and return without touching the free stack or another transaction's state.
    final long expectedActiveState = (long) SLOT_VH.getVolatile(slotStates, ticket.slotIndex);
    if ((expectedActiveState & ACTIVE_BIT) == 0L
        || (int) ((expectedActiveState >>> GENERATION_SHIFT) & GENERATION_MASK) != ticket.generation) {
      return;
    }
    // Release keeps the generation (inactive), so the next register of this slot still bumps
    // past every ticket ever issued for it.
    final long releasedState = expectedActiveState & ~ACTIVE_BIT & ~0xFFFF_FFFFL;
    if (!SLOT_VH.compareAndSet(slotStates, ticket.slotIndex, expectedActiveState, releasedState)) {
      return;
    }
    synchronized (lock) {
      // Defense in depth: never mutate freeTop past capacity even if an invariant breaks —
      // a full stack here means a bug elsewhere, and corrupting the tracker would turn one
      // bad close into a process-wide inability to open transactions.
      if (freeTop == slotCount) {
        throw new IllegalStateException(
            "RevisionEpochTracker free stack overflow: slot " + ticket.slotIndex
                + " released while all " + slotCount + " slots are already free");
      }
      freeStack[freeTop++] = ticket.slotIndex;
    }
  }

  /**
   * Get the minimum active revision across all registered transactions.
   * <p>
   * Returns the oldest revision that any active transaction is currently reading. If no transactions
   * are active, returns the last committed revision.
   * <p>
   * This is the watermark for safe eviction: pages with revision &lt; minActiveRevision are no
   * longer needed by any active transaction. Lock-free; eventually consistent with the latest
   * register/deregister.
   *
   * @return minimum active revision, or lastCommittedRevision if none active
   */
  public int minActiveRevision() {
    int min = Integer.MAX_VALUE;
    boolean anyActive = false;

    for (int i = 0; i < slotCount; i++) {
      final long state = (long) SLOT_VH.getVolatile(slotStates, i);
      if ((state & ACTIVE_BIT) != 0L) {
        anyActive = true;
        final int rev = (int) state; // low 32 bits
        if (rev < min) {
          min = rev;
        }
      }
    }

    return anyActive
        ? min
        : lastCommittedRevision;
  }

  /**
   * Update the last committed revision. Should be called after each successful commit.
   *
   * @param revision the newly committed revision number
   */
  public void setLastCommittedRevision(final int revision) {
    this.lastCommittedRevision = revision;
  }

  /**
   * Get the last committed revision.
   *
   * @return last committed revision number
   */
  public int getLastCommittedRevision() {
    return lastCommittedRevision;
  }

  /** Capacity of the tracker (max concurrent active transactions). */
  public int slotCount() {
    return slotCount;
  }

  /**
   * Get diagnostic information about active transactions.
   *
   * @return diagnostic string
   */
  public String getDiagnostics() {
    int activeCount = 0;
    int minRev = Integer.MAX_VALUE;
    int maxRev = 0;

    for (int i = 0; i < slotCount; i++) {
      final long state = (long) SLOT_VH.getVolatile(slotStates, i);
      if ((state & ACTIVE_BIT) != 0L) {
        activeCount++;
        final int rev = (int) state;
        if (rev < minRev) {
          minRev = rev;
        }
        if (rev > maxRev) {
          maxRev = rev;
        }
      }
    }

    final int high;
    synchronized (lock) {
      high = highWaterMark;
    }

    if (activeCount == 0) {
      return String.format("RevisionEpochTracker: no active transactions (capacity=%d, highWater=%d, lastCommitted=%d)",
                           slotCount, high, lastCommittedRevision);
    } else {
      return String.format(
          "RevisionEpochTracker: %d active txns, revisions [%d..%d] (capacity=%d, highWater=%d, lastCommitted=%d)",
          activeCount, minRev, maxRev, slotCount, high, lastCommittedRevision);
    }
  }
}
