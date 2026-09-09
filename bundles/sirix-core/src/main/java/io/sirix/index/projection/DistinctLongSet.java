package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An open-addressing set of 64-bit values for exact distinct counting.
 *
 * <p>
 * One {@code long} per slot; {@code 0L} is the empty-slot marker and a real zero is kept in a flag.
 * Slots are indexed by the LOW bits of {@link #mix(long)} and a caller that partitions values by
 * the HIGH bits of the same mix ({@link #partitionOf(long, int)}, as {@link SharedDistinctLongSet}
 * does) keeps the two decisions independent — the values themselves (user ids, dictionary ids,
 * timestamps) are anything but uniform in their low bits. Linear probing at a load of at most three
 * quarters: a probe run touches one or two cache lines and the table stays half the size the
 * 128-bit form ({@link DistinctHash128Set}) would need for the same keys.
 * </p>
 *
 * <p>
 * Every array the set allocates is charged to a shared byte {@code budget} (bytes remaining, shared
 * by the workers of one operation); a growth the budget refuses throws
 * {@link DistinctHash128Set.ByteBudgetExceededException} and the caller declines the whole
 * operation. Single-threaded by contract; {@link SharedDistinctLongSet} is the form several workers
 * fill together.
 * </p>
 */
public final class DistinctLongSet implements DistinctLongSink {

  private static final int MIN_CAPACITY = 1 << 8;
  /** Slots; this many longs is the largest array the set will ask for. */
  private static final int MAX_CAPACITY = 1 << 30;

  private final @Nullable AtomicLong budget;
  private long[] slots;
  private int mask;
  private int size;
  private int growAt;
  private boolean hasZero;
  private long chargedBytes;

  /**
   * A set sized for {@code expectedKeys} values without growing, its arrays charged to
   * {@code budget}.
   *
   * @param expectedKeys the values the set should hold before its first growth, at least zero
   * @param budget the shared bytes remaining, or {@code null} for an unbounded set
   * @throws DistinctHash128Set.ByteBudgetExceededException when the budget refuses the initial array
   */
  public DistinctLongSet(final int expectedKeys, final @Nullable AtomicLong budget) {
    if (expectedKeys < 0) {
      throw new IllegalArgumentException("expectedKeys must not be negative: " + expectedKeys);
    }
    this.budget = budget;
    final int capacity = capacityFor(expectedKeys);
    charge((long) capacity * Long.BYTES);
    this.slots = new long[capacity];
    this.mask = capacity - 1;
    this.growAt = growAtFor(capacity);
  }

  /**
   * The smallest power-of-two capacity (at least {@link #MIN_CAPACITY}) holding {@code keys} at
   * three-quarter load.
   */
  static int capacityFor(final long keys) {
    long capacity = MIN_CAPACITY;
    while (growAtFor(capacity) < keys) {
      capacity <<= 1;
      if (capacity > MAX_CAPACITY) {
        throw new IllegalArgumentException("distinct set cannot hold " + keys + " keys");
      }
    }
    return (int) capacity;
  }

  private static int growAtFor(final long capacity) {
    return (int) (capacity - (capacity >>> 2));
  }

  /**
   * MurmurHash3's 64-bit finaliser: a bijection whose every output bit depends on every input bit.
   */
  public static long mix(long v) {
    v ^= v >>> 33;
    v *= 0xff51afd7ed558ccdL;
    v ^= v >>> 33;
    v *= 0xc4ceb9fe1a85ec53L;
    v ^= v >>> 33;
    return v;
  }

  /**
   * The partition of a value by the top {@code partitionBits} bits of its {@link #mix(long) mix} —
   * {@code 0} for {@code partitionBits == 0}.
   */
  public static int partitionOf(final long mixed, final int partitionBits) {
    return partitionBits == 0
        ? 0
        : (int) (mixed >>> (Long.SIZE - partitionBits));
  }

  @Override
  public void put(final long value) {
    add(value);
  }

  /**
   * Add {@code value}.
   *
   * @return {@code true} when the value was not in the set before
   * @throws DistinctHash128Set.ByteBudgetExceededException when the insert needs a growth the budget
   *         refuses
   */
  public boolean add(final long value) {
    if (value == 0L) {
      if (hasZero) {
        return false;
      }
      hasZero = true;
      return true;
    }
    final long[] s = slots;
    final int m = mask;
    int i = (int) mix(value) & m;
    while (true) {
      final long slot = s[i];
      if (slot == value) {
        return false;
      }
      if (slot == 0L) {
        s[i] = value;
        if (++size > growAt) {
          growTo(capacityFor(size));
        }
        return true;
      }
      i = (i + 1) & m;
    }
  }

  /** Whether {@code value} is in the set. */
  public boolean contains(final long value) {
    if (value == 0L) {
      return hasZero;
    }
    final long[] s = slots;
    final int m = mask;
    int i = (int) mix(value) & m;
    while (true) {
      final long slot = s[i];
      if (slot == value) {
        return true;
      }
      if (slot == 0L) {
        return false;
      }
      i = (i + 1) & m;
    }
  }

  /** The distinct values in the set. */
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
    charge((long) newCapacity * Long.BYTES);
    final long[] grown = new long[newCapacity];
    final int m = newCapacity - 1;
    for (final long value : old) {
      if (value == 0L) {
        continue;
      }
      int i = (int) mix(value) & m;
      while (grown[i] != 0L) {
        i = (i + 1) & m;
      }
      grown[i] = value;
    }
    slots = grown;
    mask = m;
    growAt = growAtFor(newCapacity);
    release((long) old.length * Long.BYTES);
  }

  private void charge(final long bytes) {
    if (budget != null && budget.addAndGet(-bytes) < 0L) {
      budget.addAndGet(bytes);
      throw new DistinctHash128Set.ByteBudgetExceededException("distinct set refused " + bytes + " bytes: "
          + Math.max(0L, budget.get()) + " bytes of the shared budget remain");
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
