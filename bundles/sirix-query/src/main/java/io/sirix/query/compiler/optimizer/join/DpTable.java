package io.sirix.query.compiler.optimizer.join;

/**
 * Open-addressing hash table mapping relation-set bitmasks to optimal join plans.
 *
 * <p>Uses Fibonacci hashing for excellent distribution of bitmask keys.
 * The golden ratio constant {@code 0x9E3779B97F4A7C15L} provides near-perfect
 * avalanche properties for the multiplicative hash.</p>
 *
 * <p>Empty slots are indicated by a sentinel key of 0 (the empty set is never
 * a valid relation set). Linear probing resolves collisions.</p>
 *
 * <p>Resizes at 75% load factor to keep probe sequences short.</p>
 */
public final class DpTable {

  /** Fibonacci hashing constant: floor(2^64 / φ) where φ = (1 + √5) / 2. */
  private static final long FIBONACCI_HASH = 0x9E3779B97F4A7C15L;

  /** Sentinel: 0 is never a valid relation set (always at least one relation). */
  private static final long EMPTY_KEY = 0L;

  private long[] keys;
  private JoinPlan[] values;
  private int size;
  private int capacity;
  private int mask;
  private int shift;
  /** Pre-computed resize threshold: capacity * 3/4. Avoids float multiply on every insert. */
  private int threshold;

  /**
   * @param initialCapacity initial number of slots (rounded up to power of 2)
   */
  public DpTable(int initialCapacity) {
    this.capacity = Integer.highestOneBit(Math.max(16, initialCapacity - 1)) << 1;
    this.mask = capacity - 1;
    this.shift = Long.SIZE - Integer.numberOfTrailingZeros(capacity);
    this.threshold = capacity - (capacity >>> 2); // 75% load factor
    this.keys = new long[capacity];
    this.values = new JoinPlan[capacity];
    this.size = 0;
  }

  /**
   * Store a plan for the given relation set (replaces any existing entry).
   *
   * @param key  relation-set bitmask (must be non-zero)
   * @param plan the join plan
   */
  public void put(long key, JoinPlan plan) {
    if (key == EMPTY_KEY) {
      throw new IllegalArgumentException("Key must be non-zero");
    }

    int idx = fibHash(key);
    while (true) {
      final long existing = keys[idx];
      if (existing == EMPTY_KEY) {
        keys[idx] = key;
        values[idx] = plan;
        size++;
        if (size > threshold) {
          resize();
        }
        return;
      }
      if (existing == key) {
        values[idx] = plan;
        return;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Retrieve the best plan for the given relation set, or null if none exists.
   */
  public JoinPlan get(long key) {
    if (key == EMPTY_KEY) {
      return null;
    }

    int idx = fibHash(key);
    while (true) {
      final long existing = keys[idx];
      if (existing == EMPTY_KEY) {
        return null;
      }
      if (existing == key) {
        return values[idx];
      }
      idx = (idx + 1) & mask;
    }
  }

  public int size() {
    return size;
  }

  /**
   * Fibonacci hash: multiply by golden ratio constant, then shift to table range.
   */
  private int fibHash(long key) {
    return (int) ((key * FIBONACCI_HASH) >>> shift);
  }

  private void resize() {
    final int newCapacity = capacity << 1;
    final long[] oldKeys = keys;
    final JoinPlan[] oldValues = values;

    capacity = newCapacity;
    mask = newCapacity - 1;
    shift = Long.SIZE - Integer.numberOfTrailingZeros(newCapacity);
    threshold = newCapacity - (newCapacity >>> 2);
    keys = new long[newCapacity];
    values = new JoinPlan[newCapacity];
    size = 0;

    for (int i = 0; i < oldKeys.length; i++) {
      if (oldKeys[i] != EMPTY_KEY) {
        put(oldKeys[i], oldValues[i]);
      }
    }
  }
}
