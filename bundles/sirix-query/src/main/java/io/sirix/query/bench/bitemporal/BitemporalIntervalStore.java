package io.sirix.query.bench.bitemporal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;

/**
 * Load-owned native slabs. No per-identity or per-interval heap objects escape into retained state.
 */
final class BitemporalIntervalStore implements AutoCloseable {
  static final int SLAB_SHIFT = 12;
  static final int SLAB_SIZE = 1 << SLAB_SHIFT;
  private static final int SLAB_MASK = SLAB_SIZE - 1;
  private static final int RECORD_BYTES = 40;
  private final Arena arena;
  private final long limit;
  private long reservedBytes;

  BitemporalIntervalStore(final long limit) {
    if (limit < (long) SLAB_SIZE * RECORD_BYTES) {
      throw new IllegalArgumentException("native interval budget is smaller than one slab");
    }
    this.limit = limit;
    arena = Arena.ofConfined();
  }

  long reservedBytes() {
    return reservedBytes;
  }

  private MemorySegment allocate(final long bytes) {
    if (bytes > limit - reservedBytes) {
      throw new IllegalStateException("native interval budget exhausted: " + limit);
    }
    final MemorySegment segment = arena.allocate(bytes, Long.BYTES);
    reservedBytes += bytes;
    return segment;
  }

  Relation relation(final int identities) {
    if (identities < 1) {
      throw new IllegalArgumentException("identity count must be positive");
    }
    return new Relation(identities);
  }

  @Override
  public void close() {
    arena.close();
  }

  final class Relation {
    private final int identities;
    private final MemorySegment[] heads;
    private final ArrayList<MemorySegment> slabs = new ArrayList<>();
    private int allocated = 1; // Zero is the end-of-chain sentinel in zero-filled native slabs.
    private int free;

    private Relation(final int identities) {
      this.identities = identities;
      heads = new MemorySegment[(identities >>> SLAB_SHIFT) + 1];
      for (int i = 0; i < heads.length; i++) {
        heads[i] = allocate((long) SLAB_SIZE * Integer.BYTES);
      }
    }

    private int head(final int id) {
      checkId(id);
      return heads[id >>> SLAB_SHIFT].getAtIndex(ValueLayout.JAVA_INT, id & SLAB_MASK);
    }

    private void checkId(final int id) {
      if (id < 1 || id > identities) {
        throw new IllegalArgumentException("identity out of range: " + id);
      }
    }

    void read(final int id, final Intervals target) {
      target.size = 0;
      for (int slot = head(id); slot != 0; slot = next(slot)) {
        final MemorySegment slab = slab(slot);
        final long offset = offset(slot);
        final int row = target.size++;
        for (int column = 0; column < Intervals.COLUMNS; column++) {
          target.values[row * Intervals.COLUMNS + column] = slab.get(ValueLayout.JAVA_INT, offset + column * 4L);
        }
        target.keys[row] = slab.get(ValueLayout.JAVA_LONG, offset + 32);
      }
    }

    void write(final int id, final Intervals source) {
      int slot = head(id);
      int first = 0;
      int previous = 0;
      for (int row = 0; row < source.size; row++) {
        final int current = slot == 0
            ? acquire()
            : slot;
        slot = next(current);
        if (previous != 0) {
          next(previous, current);
        } else {
          first = current;
        }
        final MemorySegment slab = slab(current);
        final long offset = offset(current);
        for (int column = 0; column < Intervals.COLUMNS; column++) {
          slab.set(ValueLayout.JAVA_INT, offset + column * 4L, source.values[row * Intervals.COLUMNS + column]);
        }
        slab.set(ValueLayout.JAVA_LONG, offset + 32, source.keys[row]);
        previous = current;
      }
      if (previous != 0) {
        next(previous, 0);
      }
      while (slot != 0) {
        final int following = next(slot);
        next(slot, free);
        free = slot;
        slot = following;
      }
      heads[id >>> SLAB_SHIFT].setAtIndex(ValueLayout.JAVA_INT, id & SLAB_MASK, first);
    }

    private int acquire() {
      if (free != 0) {
        final int result = free;
        free = next(result);
        next(result, 0);
        return result;
      }
      if ((allocated >>> SLAB_SHIFT) == slabs.size()) {
        slabs.add(allocate((long) SLAB_SIZE * RECORD_BYTES));
      }
      return allocated++;
    }

    private MemorySegment slab(final int slot) {
      return slabs.get(slot >>> SLAB_SHIFT);
    }

    private long offset(final int slot) {
      return (long) (slot & SLAB_MASK) * RECORD_BYTES;
    }

    private int next(final int slot) {
      return slab(slot).get(ValueLayout.JAVA_INT, offset(slot) + 28);
    }

    private void next(final int slot, final int value) {
      slab(slot).set(ValueLayout.JAVA_INT, offset(slot) + 28, value);
    }
  }

  /** One day is the minimum interval width, so this scratch bound is independent of input scale. */
  static final class Intervals {
    static final int COLUMNS = 7;
    final int[] values = new int[(BitemporalSchema.HORIZON_DAYS + 2) * COLUMNS];
    final long[] keys = new long[BitemporalSchema.HORIZON_DAYS + 2];
    int size;

    int get(final int row, final int column) {
      return values[row * COLUMNS + column];
    }

    void set(final int row, final int column, final int value) {
      values[row * COLUMNS + column] = value;
    }

    void copy(final Intervals source, final int row, final int from, final int to) {
      System.arraycopy(source.values, row * COLUMNS, values, size * COLUMNS, COLUMNS);
      set(size, 0, from);
      set(size, 1, to);
      keys[size++] = -1;
    }

    int find(final int from) {
      for (int row = 0; row < size; row++) {
        if (get(row, 0) == from) {
          return row;
        }
      }
      return -1;
    }

    void replace(final BitemporalEventReader events, final Intervals target) {
      target.size = 0;
      boolean inserted = !events.put;
      for (int row = 0; row < size; row++) {
        final int from = get(row, 0);
        final int to = get(row, 1);
        if (from < events.from) {
          target.copy(this, row, from, Math.min(to, events.from));
        }
        if (!inserted && to > events.from) {
          target.append(events);
          inserted = true;
        }
        if (to > events.to) {
          target.copy(this, row, Math.max(from, events.to), to);
        }
      }
      if (!inserted) {
        target.append(events);
      }
      target.coalesce();
    }

    void append(final BitemporalEventReader event) {
      set(size, 0, event.from);
      set(size, 1, event.to);
      System.arraycopy(event.payload, 0, values, size * COLUMNS + 2, 5);
      keys[size++] = -1;
    }

    private void coalesce() {
      int write = 0;
      for (int read = 0; read < size; read++) {
        boolean same = write != 0 && get(write - 1, 1) == get(read, 0);
        for (int column = 2; same && column < COLUMNS; column++) {
          same = get(write - 1, column) == get(read, column);
        }
        if (same) {
          set(write - 1, 1, get(read, 1));
        } else {
          System.arraycopy(values, read * COLUMNS, values, write * COLUMNS, COLUMNS);
          keys[write++] = -1;
        }
      }
      size = write;
    }
  }
}
