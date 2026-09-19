package io.sirix.access.trx.page;

import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.PageFragmentKey;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.List;
import java.util.Objects;

/** Bounded, writer-owned cache of immutable pages read back from uncommitted durable offsets. */
final class WriterRecordPageCache implements AutoCloseable {
  static final int MIN_CAPACITY = 8;
  static final int MAX_CAPACITY = 256;
  private static final long[] NO_FRAGMENTS = new long[0];

  private final NodeStorageEngineReader reader;
  private final Long2IntOpenHashMap slots;
  private final long[] offsets;
  private final long[] hashes;
  private final boolean[] hashPresent;
  private final long[][] fragments;
  private final KeyValueLeafPage[] pages;
  private final int mask;
  private int next;

  WriterRecordPageCache(final NodeStorageEngineReader reader) {
    this(reader, capacityForBudget(Allocators.getInstance().getMaxBufferSize()));
  }

  WriterRecordPageCache(final NodeStorageEngineReader reader, final int capacity) {
    this.reader = Objects.requireNonNull(reader, "reader");
    if (capacity < MIN_CAPACITY || capacity > MAX_CAPACITY || (capacity & (capacity - 1)) != 0) {
      throw new IllegalArgumentException("writer readback cache capacity must be a power of two in [8, 256]");
    }
    // One primitive entry per occupied slot. Eviction precedes insertion, so this never rehashes.
    slots = new Long2IntOpenHashMap(capacity, 0.5f);
    slots.defaultReturnValue(-1);
    offsets = new long[capacity];
    hashes = new long[capacity];
    hashPresent = new boolean[capacity];
    fragments = new long[capacity][];
    pages = new KeyValueLeafPage[capacity];
    mask = capacity - 1;
  }

  /**
   * At most 1/32 of the configured arena in maximum-size frames, with the original eight-page floor.
   */
  static int capacityForBudget(final long arenaBytes) {
    if (arenaBytes < 0) {
      throw new IllegalArgumentException("negative off-heap budget");
    }
    final long frames = arenaBytes / (32L * MemorySegmentAllocator.TWO_FIFTYSIX_KB);
    return Integer.highestOneBit((int) Math.max(MIN_CAPACITY, Math.min(MAX_CAPACITY, frames)));
  }

  KeyValueLeafPage get(final PageReference reference) {
    final long offset = reference.getKey();
    final long hash = reference.getHashAsLong();
    final boolean present = reference.hasHash();
    final List<PageFragmentKey> history = reference.getPageFragments();
    final int cached = slots.get(offset);
    if (cached >= 0 && hashes[cached] == hash && hashPresent[cached] == present
        && sameFragments(fragments[cached], history) && !pages[cached].isClosed()) {
      return pages[cached];
    }
    // Evict before allocating the replacement, so the native-page count never exceeds the cap.
    // A changed hash/history at the same offset replaces that entry, never aliases its old page.
    final int at = cached >= 0
        ? cached
        : next;
    final KeyValueLeafPage previous = pages[at];
    pages[at] = null;
    fragments[at] = null;
    if (previous != null) {
      slots.remove(offsets[at]);
      previous.retire();
    }
    final long[] captured = history.isEmpty()
        ? NO_FRAGMENTS
        : new long[Math.multiplyExact(history.size(), 2)];
    for (int i = 0; i < history.size(); i++) {
      captured[i * 2] = history.get(i).key();
      captured[i * 2 + 1] = history.get(i).revision();
    }
    final KeyValueLeafPage loaded = reader.readRecordPageFromExactReference(reference);
    offsets[at] = offset;
    hashes[at] = hash;
    hashPresent[at] = present;
    fragments[at] = captured;
    pages[at] = loaded;
    slots.put(offset, at);
    if (cached < 0) {
      next = (at + 1) & mask;
    }
    return loaded;
  }

  private static boolean sameFragments(final long[] captured, final List<PageFragmentKey> history) {
    if (captured.length != history.size() * 2) {
      return false;
    }
    for (int i = 0; i < history.size(); i++) {
      final PageFragmentKey fragment = history.get(i);
      if (captured[i * 2] != fragment.key() || captured[i * 2 + 1] != fragment.revision()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    slots.clear();
    next = 0;
    Throwable failure = null;
    for (int i = 0; i < pages.length; i++) {
      final KeyValueLeafPage page = pages[i];
      pages[i] = null;
      fragments[i] = null;
      if (page != null) {
        try {
          page.retire();
        } catch (final RuntimeException | Error exception) {
          if (failure == null) {
            failure = exception;
          } else if (failure != exception) {
            failure.addSuppressed(exception);
          }
        }
      }
    }
    if (failure instanceof RuntimeException exception) {
      throw exception;
    }
    if (failure instanceof Error error) {
      throw error;
    }
  }
}
