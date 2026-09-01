package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An open-addressing set of 128-bit keys for exact distinct counting over hashed values.
 *
 * <p>
 * The two halves of a key sit side by side in ONE {@code long[]} (slot {@code i} at {@code 2i} and
 * {@code 2i + 1}), so a probe touches one cache line and an insert allocates nothing. The all-zero key
 * is the empty-slot marker; a real all-zero key is kept in a flag. Linear probing at a load of at most
 * one half: the low bits of the low half index the table, so a caller that partitions keys by the
 * HIGH half ({@link #partitionOf(long, int)}, as {@link SharedDistinctHash128Set} does) keeps the two
 * decisions independent.
 * </p>
 *
 * <p>
 * Every array the set allocates is charged to a shared byte {@code budget} (bytes remaining, shared by
 * the workers of one operation); a growth the budget refuses throws
 * {@link ByteBudgetExceededException} and the caller declines the whole operation. Single-threaded by
 * contract; {@link SharedDistinctHash128Set} is the form several workers fill together.
 * </p>
 */
public final class DistinctHash128Set implements DistinctHash128Sink {

  /** The shared byte budget refused an array this set needed — the operation declines as a whole. */
  public static final class ByteBudgetExceededException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    ByteBudgetExceededException(final String message) {
      super(message);
    }
  }

  /** Bytes one slot occupies: the two halves of a key. */
  static final int BYTES_PER_SLOT = 2 * Long.BYTES;
  private static final int MIN_CAPACITY = 1 << 8;
  /** Slots; twice this many longs is the largest array the set will ask for. */
  private static final int MAX_CAPACITY = 1 << 29;

  private final @Nullable AtomicLong budget;
  private long[] slots;
  private int mask;
  private int size;
  private int growAt;
  private boolean hasZero;
  private long chargedBytes;

  /**
   * A set sized for {@code expectedKeys} keys without growing, its arrays charged to {@code budget}.
   *
   * @param expectedKeys the keys the set should hold before its first growth, at least zero
   * @param budget the shared bytes remaining, or {@code null} for an unbounded set
   * @throws ByteBudgetExceededException when the budget refuses the initial array
   */
  public DistinctHash128Set(final int expectedKeys, final @Nullable AtomicLong budget) {
    if (expectedKeys < 0) {
      throw new IllegalArgumentException("expectedKeys must not be negative: " + expectedKeys);
    }
    this.budget = budget;
    final int capacity = capacityFor(expectedKeys);
    charge((long) capacity * BYTES_PER_SLOT);
    this.slots = new long[capacity << 1];
    this.mask = capacity - 1;
    this.growAt = capacity >>> 1;
  }

  /** The smallest power-of-two capacity (at least {@link #MIN_CAPACITY}) holding {@code keys} at half load. */
  static int capacityFor(final long keys) {
    long capacity = MIN_CAPACITY;
    while (capacity < 2L * keys) {
      capacity <<= 1;
    }
    if (capacity > MAX_CAPACITY) {
      throw new IllegalArgumentException("distinct set cannot hold " + keys + " keys");
    }
    return (int) capacity;
  }

  /**
   * The partition of a key by the top {@code partitionBits} bits of its high half — {@code 0} for
   * {@code partitionBits == 0}.
   */
  public static int partitionOf(final long hi, final int partitionBits) {
    return partitionBits == 0
        ? 0
        : (int) (hi >>> (Long.SIZE - partitionBits));
  }

  @Override
  public void put(final long lo, final long hi) {
    add(lo, hi);
  }

  /**
   * Add the key {@code (lo, hi)}.
   *
   * @return {@code true} when the key was not in the set before
   * @throws ByteBudgetExceededException when the insert needs a growth the budget refuses
   */
  public boolean add(final long lo, final long hi) {
    if ((lo | hi) == 0L) {
      if (hasZero) {
        return false;
      }
      hasZero = true;
      return true;
    }
    final long[] s = slots;
    final int m = mask;
    int i = (int) lo & m;
    while (true) {
      final int at = i << 1;
      final long slotLo = s[at];
      final long slotHi = s[at + 1];
      if (slotLo == lo && slotHi == hi) {
        return false;
      }
      if ((slotLo | slotHi) == 0L) {
        s[at] = lo;
        s[at + 1] = hi;
        if (++size > growAt) {
          growTo(capacityFor(size));
        }
        return true;
      }
      i = (i + 1) & m;
    }
  }

  /** Whether the key {@code (lo, hi)} is in the set. */
  public boolean contains(final long lo, final long hi) {
    if ((lo | hi) == 0L) {
      return hasZero;
    }
    final long[] s = slots;
    final int m = mask;
    int i = (int) lo & m;
    while (true) {
      final int at = i << 1;
      final long slotLo = s[at];
      final long slotHi = s[at + 1];
      if (slotLo == lo && slotHi == hi) {
        return true;
      }
      if ((slotLo | slotHi) == 0L) {
        return false;
      }
      i = (i + 1) & m;
    }
  }

  /**
   * Add every key of {@code other}, growing once up front to the combined size so the merge rehashes
   * this set at most once.
   *
   * @throws ByteBudgetExceededException when the growth the merge needs is refused
   */
  public void addAll(final DistinctHash128Set other) {
    if (other == this) {
      return;
    }
    final int combined = size + other.size;
    if (combined > growAt) {
      growTo(capacityFor(combined));
    }
    if (other.hasZero) {
      hasZero = true;
    }
    final long[] s = other.slots;
    for (int at = 0; at < s.length; at += 2) {
      final long lo = s[at];
      final long hi = s[at + 1];
      if ((lo | hi) != 0L) {
        add(lo, hi);
      }
    }
  }

  /** The distinct keys in the set. */
  public int size() {
    return hasZero
        ? size + 1
        : size;
  }

  /** Slots in the table. */
  public int capacity() {
    return mask + 1;
  }

  /** Bytes this set currently holds against its budget. */
  public long chargedBytes() {
    return chargedBytes;
  }

  private void growTo(final int newCapacity) {
    final long[] old = slots;
    final long newBytes = (long) newCapacity * BYTES_PER_SLOT;
    charge(newBytes);
    final long[] grown = new long[newCapacity << 1];
    final int m = newCapacity - 1;
    for (int at = 0; at < old.length; at += 2) {
      final long lo = old[at];
      final long hi = old[at + 1];
      if ((lo | hi) == 0L) {
        continue;
      }
      int i = (int) lo & m;
      while (grown[i << 1] != 0L || grown[(i << 1) + 1] != 0L) {
        i = (i + 1) & m;
      }
      grown[i << 1] = lo;
      grown[(i << 1) + 1] = hi;
    }
    slots = grown;
    mask = m;
    growAt = newCapacity >>> 1;
    release((long) old.length * Long.BYTES);
  }

  private void charge(final long bytes) {
    if (budget != null && budget.addAndGet(-bytes) < 0L) {
      budget.addAndGet(bytes);
      throw new ByteBudgetExceededException("distinct set refused " + bytes + " bytes: " + Math.max(0L, budget.get())
          + " bytes of the shared budget remain");
    }
    chargedBytes += bytes;
  }

  private void release(final long bytes) {
    if (budget != null) {
      budget.addAndGet(bytes);
    }
    chargedBytes -= bytes;
  }
}
