/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.diff;

import io.sirix.budget.WorkCounter;
import io.sirix.budget.WorkProbe;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Observes the actual primitive backing arrays of each completed revision cache. These arrays only
 * grow during serialization, so their final capacities also bound their peak retained payload.
 * Array headers and the fixed number of collection objects are deliberately excluded: payload bytes
 * are exact on every JVM, unlike heap-usage or thread-allocation estimates.
 *
 * <p>
 * The reflective access is confined to this probe: fastutil exposes a list's backing array but not
 * a map's. Counting entries alone would miss eager preallocation of an otherwise empty map.
 */
public final class ArrayPositionCacheProbe implements WorkProbe {

  private static final Field KEYS = backingField("key");
  private static final Field VALUES = backingField("value");

  private long observedCaches;
  private long entries;
  private long backingBytes;
  private boolean open;
  private @Nullable BiConsumer<Long2IntOpenHashMap, LongArrayList> displaced;

  private final WorkCounter cachesCounter = WorkCounter.alwaysOn("arrayPositionCaches",
      "one completed revision cache observed, including the unused revision", () -> observedCaches);
  private final WorkCounter entriesCounter =
      WorkCounter.alwaysOn("arrayPositionCacheEntries", "one node ordinal retained by a revision cache", () -> entries);
  private final WorkCounter bytesCounter = WorkCounter.alwaysOn("arrayPositionBackingBytes",
      "one allocated payload byte in the cache's key, ordinal, or walk-stack backing arrays", () -> backingBytes);

  public WorkCounter caches() {
    return cachesCounter;
  }

  public WorkCounter entries() {
    return entriesCounter;
  }

  public WorkCounter backingBytes() {
    return bytesCounter;
  }

  @Override
  public List<WorkCounter> counters() {
    return List.of(cachesCounter, entriesCounter, bytesCounter);
  }

  @Override
  public void open() {
    if (open) {
      throw new IllegalStateException("the array-position cache probe is already open");
    }
    observedCaches = 0;
    entries = 0;
    backingBytes = 0;
    displaced = JsonDiffSerializer.setArrayPositionCacheObserverForTesting(this::observe);
    open = true;
  }

  @Override
  public void close() {
    if (open) {
      JsonDiffSerializer.setArrayPositionCacheObserverForTesting(displaced);
      displaced = null;
      open = false;
    }
  }

  private void observe(final Long2IntOpenHashMap positions, final LongArrayList walkedKeys) {
    observedCaches++;
    entries += positions.size();
    try {
      backingBytes += (long) ((long[]) KEYS.get(positions)).length * Long.BYTES
          + (long) ((int[]) VALUES.get(positions)).length * Integer.BYTES
          + (long) walkedKeys.elements().length * Long.BYTES;
    } catch (final IllegalAccessException e) {
      throw new AssertionError("cannot observe the ordinal cache's allocated backing arrays", e);
    }
    if (displaced != null) {
      displaced.accept(positions, walkedKeys);
    }
  }

  private static Field backingField(final String name) {
    try {
      final Field field = Long2IntOpenHashMap.class.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
}
