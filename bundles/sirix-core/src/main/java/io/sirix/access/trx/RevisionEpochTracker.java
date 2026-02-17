package io.sirix.access.trx;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Tracks the minimum active revision across all transactions (epoch-based MVCC).
 * <p>
 * This enables safe page eviction: a page can only be evicted when its revision is less than the
 * minimum active revision (no transaction needs it anymore).
 * <p>
 * Inspired by LeanStore/CedarDB epoch-based reclamation, adapted to Sirix's revision-based MVCC
 * model.
 *
 * @author Johannes Lichtenberger
 */
public final class RevisionEpochTracker {

  /**
   * Slot in the tracker array.
   */
  private static final class Slot {
    volatile int revision;
    volatile boolean active;

    Slot() {
      this.revision = 0;
      this.active = false;
    }
  }

  /**
   * Ticket returned when registering a transaction. Used to efficiently deregister the transaction
   * later.
   */
  public static final class Ticket {
    private final int slotIndex;

    private Ticket(int slotIndex) {
      this.slotIndex = slotIndex;
    }

    public int getSlotIndex() {
      return slotIndex;
    }
  }

  private final AtomicReferenceArray<Slot> slots;
  private final int slotCount;
  private volatile int lastCommittedRevision;

  /**
   * Create a new RevisionEpochTracker.
   *
   * @param slotCount number of concurrent transaction slots (power of 2 recommended)
   */
  public RevisionEpochTracker(int slotCount) {
    this.slotCount = slotCount;
    this.slots = new AtomicReferenceArray<>(slotCount);
    for (int i = 0; i < slotCount; i++) {
      slots.set(i, new Slot());
    }
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
  public Ticket register(int revision) {
    // Try to find a free slot using simple linear search
    for (int i = 0; i < slotCount; i++) {
      Slot slot = slots.get(i);
      if (!slot.active) {
        synchronized (slot) {
          if (!slot.active) {
            slot.revision = revision;
            slot.active = true;
            return new Ticket(i);
          }
        }
      }
    }

    throw new IllegalStateException("No free slots in RevisionEpochTracker (max=" + slotCount + "). "
        + "Too many concurrent transactions. Consider increasing slot count.");
  }

  /**
   * Deregister a transaction.
   *
   * @param ticket the ticket from registration
   */
  public void deregister(Ticket ticket) {
    if (ticket == null) {
      return;
    }

    Slot slot = slots.get(ticket.slotIndex);
    synchronized (slot) {
      slot.active = false;
      slot.revision = 0;
    }
  }

  /**
   * Get the minimum active revision across all registered transactions.
   * <p>
   * Returns the oldest revision that any active transaction is currently reading. If no transactions
   * are active, returns the last committed revision.
   * <p>
   * This is the watermark for safe eviction: pages with revision < minActiveRevision are no longer
   * needed by any active transaction.
   *
   * @return minimum active revision, or lastCommittedRevision if none active
   */
  public int minActiveRevision() {
    int min = Integer.MAX_VALUE;
    boolean anyActive = false;

    for (int i = 0; i < slotCount; i++) {
      Slot slot = slots.get(i);
      if (slot.active) {
        anyActive = true;
        min = Math.min(min, slot.revision);
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
  public void setLastCommittedRevision(int revision) {
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
      Slot slot = slots.get(i);
      if (slot.active) {
        activeCount++;
        minRev = Math.min(minRev, slot.revision);
        maxRev = Math.max(maxRev, slot.revision);
      }
    }

    if (activeCount == 0) {
      return String.format("RevisionEpochTracker: no active transactions (lastCommitted=%d)", lastCommittedRevision);
    } else {
      return String.format("RevisionEpochTracker: %d active txns, revisions [%d..%d], lastCommitted=%d", activeCount,
          minRev, maxRev, lastCommittedRevision);
    }
  }
}

