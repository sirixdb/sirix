/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.PageReference;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.cache.HOTMiniPageCache.ReadScope.ImageSnapshot;
import io.sirix.index.projection.ProjectionSortedReadView;
import org.jspecify.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Byte-bounded retention of verified sorted projection blobs and their committed routing identity.
 * Raw readers receive private copies; sorted readers share validated immutable views. Neither a
 * mutable array nor a native-page guard escapes the cache. Fixed associative sets keep hits
 * lock-free and bound eviction work. Admission locks only one set; resource invalidation fences
 * in-flight reads before clearing their entries, including reused offsets.
 */
public final class ProjectionBlobCache {
  private static final int WAYS = 16;
  private static final int MAX_PAYLOAD_BYTES = 32 * 1024;
  private static final long SET_OVERHEAD_BYTES = 256;
  private static final long ENTRY_OVERHEAD_BYTES = 192;
  private static final VarHandle TABLE = MethodHandles.arrayElementVarHandle(Entry[].class);
  private static final ProjectionBlobCache DISABLED = new ProjectionBlobCache();

  /** A committed root and revision uniquely resolve a logical slot, including its side reference. */
  public record Key(long databaseId, long resourceId, long rootOffset, long rootHash, int revision, long slot) {
    public Key {
      if (rootOffset < 0 || revision < 0 || slot < 0) {
        throw new IllegalArgumentException("projection cache requires committed routing and a valid slot");
      }
    }
  }

  @SuppressWarnings("ArrayRecordComponent") // Internal payload carrier; cache lookup compares Key, never Entry
                                            // equality.
  private record Entry(Key key, byte @Nullable [] bytes, @Nullable ProjectionSortedReadView view,
      @Nullable ImageSnapshot routing, long weight) {
  }

  private static final class Set {
    private final Entry[] entries = new Entry[WAYS];
    private long weight;
    private int victim;
  }

  private final Set @Nullable [] sets;
  private final long maxWeight;
  private final long setBudget;
  private final AtomicLong generation = new AtomicLong();

  public ProjectionBlobCache(final long maxWeightBytes) {
    if (maxWeightBytes < 1024) {
      throw new IllegalArgumentException("projection blob cache needs at least 1024 bytes; use disabled()");
    }
    maxWeight = maxWeightBytes;
    final int count = Integer.highestOneBit((int) Math.min(4096, Math.max(1, maxWeightBytes / (64 * 1024))));
    sets = new Set[count];
    for (int i = 0; i < count; i++) {
      sets[i] = new Set();
    }
    // Conservatively account for set objects, arrays, references and the cache object itself.
    setBudget = (maxWeightBytes - 128) / count - SET_OVERHEAD_BYTES;
  }

  private ProjectionBlobCache() {
    sets = null;
    maxWeight = 0;
    setBudget = 0;
  }

  public static ProjectionBlobCache disabled() {
    return DISABLED;
  }

  public boolean isEnabled() {
    return sets != null;
  }

  /** Capture before resolving the root, leaf marker or referenced payload. */
  public long generation() {
    return generation.get();
  }

  private Set set(final Key key) {
    long hash = key.rootOffset ^ Long.rotateLeft(key.slot, 23) ^ key.databaseId ^ Long.rotateLeft(key.resourceId, 41);
    hash ^= hash >>> 33;
    hash *= 0xff51afd7ed558ccdL;
    hash ^= hash >>> 33;
    final Set[] all = Objects.requireNonNull(sets);
    return all[(int) hash & (all.length - 1)];
  }

  /** A private payload copy, or a miss; callers may mutate the result freely. */
  public byte @Nullable [] get(final Key key) {
    return get(key, null, null);
  }

  /** Restore only immutable, identity-fenced routing; payload callers still receive private bytes. */
  public byte @Nullable [] get(final Key key, final @Nullable StorageEngineReader reader,
      final @Nullable ReadScope scope) {
    Objects.requireNonNull(key);
    if (sets == null) {
      return null;
    }
    final Entry[] entries = set(key).entries;
    for (int i = 0; i < WAYS; i++) {
      final Entry entry = (Entry) TABLE.getAcquire(entries, i);
      if (entry != null && entry.key.equals(key)) {
        if (entry.routing != null && reader != null && scope != null) {
          scope.restoreImage(reader, entry.routing);
        }
        return entry.view == null
            ? Objects.requireNonNull(entry.bytes).clone()
            : entry.view.copyBytes();
      }
    }
    return null;
  }

  /** Already validated, immutable descriptors; no array copy or repeated decode on a hit. */
  public @Nullable ProjectionSortedReadView getView(final Key key) {
    Objects.requireNonNull(key);
    if (sets == null) {
      return null;
    }
    final Entry[] entries = set(key).entries;
    for (int i = 0; i < WAYS; i++) {
      final Entry entry = (Entry) TABLE.getAcquire(entries, i);
      if (entry != null && entry.key.equals(key)) {
        return entry.view;
      }
    }
    return null;
  }

  public boolean canCache(final ProjectionSortedReadView view) {
    Objects.requireNonNull(view);
    return sets != null && view.encodedSize() <= MAX_PAYLOAD_BYTES
        && ENTRY_OVERHEAD_BYTES + view.retainedSizeBytes() <= setBudget;
  }

  public boolean canCache(final int length) {
    return sets != null && length >= 0 && length <= MAX_PAYLOAD_BYTES && weight(length) <= setBudget;
  }

  private static long weight(final int length) {
    return ENTRY_OVERHEAD_BYTES + ((length + 7L) & ~7L);
  }

  /** Admit only a fully verified payload, copying it before publication. */
  public boolean put(final Key key, final byte[] verified, final long expectedGeneration) {
    return put(key, verified, null, expectedGeneration);
  }

  /**
   * An admitted metadata record may own its already verified physical routing under the same budget.
   */
  public boolean put(final Key key, final byte[] verified, final @Nullable ImageSnapshot routing,
      final long expectedGeneration) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(verified);
    if (!canCache(verified.length)) {
      return false;
    }
    if (routing != null) {
      routing.prepareForCache();
    }
    final long weight = weight(verified.length) + (routing == null
        ? 0
        : routing.retainedSizeBytes());
    return weight <= setBudget && put(key, verified, null, routing, weight, expectedGeneration);
  }

  /** A view owns its immutable bytes and descriptors before it can be published. */
  public boolean putView(final Key key, final ProjectionSortedReadView view, final long expectedGeneration) {
    Objects.requireNonNull(key);
    final long weight = prepareViewWeight(view);
    return weight >= 0 && publishView(key, view, weight, expectedGeneration);
  }

  /** Resolve all descriptor/routing work before publishing its fixed, complete charge. */
  long prepareViewWeight(final ProjectionSortedReadView view) {
    if (!canCache(view)) {
      return -1;
    }
    view.prepareForCache();
    final long weight = ENTRY_OVERHEAD_BYTES + view.retainedSizeBytes();
    return weight <= setBudget
        ? weight
        : -1;
  }

  /** Internal callers must obtain the immutable view's charge through {@link #prepareViewWeight}. */
  boolean publishView(final Key key, final ProjectionSortedReadView view, final long weight,
      final long expectedGeneration) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(view);
    if (sets == null || weight < ENTRY_OVERHEAD_BYTES || weight > setBudget) {
      return false;
    }
    return put(key, null, view, null, weight, expectedGeneration);
  }

  private boolean put(final Key key, final byte @Nullable [] verified, final @Nullable ProjectionSortedReadView view,
      final @Nullable ImageSnapshot routing, final long weight, final long expectedGeneration) {
    Objects.requireNonNull(key);
    if (generation.get() != expectedGeneration) {
      return false;
    }
    final Set set = set(key);
    synchronized (set) {
      if (generation.get() != expectedGeneration) {
        return false;
      }
      final Entry[] entries = set.entries;
      int free = -1;
      for (int i = 0; i < WAYS; i++) {
        final Entry prior = entries[i];
        if (prior == null) {
          free = i;
        } else if (prior.key.equals(key)) {
          if (view == null || prior.view != null) {
            return true;
          }
          // Replace raw bytes with their validated view under the same byte ceiling and fence.
          TABLE.setRelease(entries, i, null);
          set.weight -= prior.weight;
          free = i;
        }
      }
      while (free < 0 || set.weight + weight > setBudget) {
        final int slot = set.victim++ & (WAYS - 1);
        final Entry prior = entries[slot];
        if (prior != null) {
          TABLE.setRelease(entries, slot, null);
          set.weight -= prior.weight;
          free = slot;
        }
      }
      TABLE.setRelease(entries, free, new Entry(key, view == null
          ? Objects.requireNonNull(verified).clone()
          : null, view, routing, weight));
      set.weight += weight;
      return true;
    }
  }

  public long getCurrentWeightBytes() {
    if (sets == null) {
      return 0;
    }
    long weight = 128 + sets.length * SET_OVERHEAD_BYTES;
    for (final Set set : sets) {
      synchronized (set) {
        weight += set.weight;
      }
    }
    return weight;
  }

  public long getMaxWeightBytes() {
    return maxWeight;
  }

  public void clear() {
    invalidate(ignored -> true);
  }

  /** Same identity and lifecycle as the buffer manager's ordinary page-cache invalidation. */
  public void invalidate(final Predicate<PageReference> matches) {
    Objects.requireNonNull(matches);
    if (sets == null) {
      return;
    }
    generation.incrementAndGet();
    Throwable failure = null;
    for (final Set set : sets) {
      synchronized (set) {
        for (int i = 0; i < WAYS; i++) {
          final Entry entry = set.entries[i];
          if (entry == null) {
            continue;
          }
          try {
            final Key key = entry.key;
            if (matches.test(new PageReference().setDatabaseId(key.databaseId)
                                                .setResourceId(key.resourceId)
                                                .setKey(key.rootOffset))) {
              TABLE.setRelease(set.entries, i, null);
              set.weight -= entry.weight;
            }
          } catch (final RuntimeException | Error e) {
            failure = ShardedPageCache.retainCleanupFailure(failure, e);
          }
        }
      }
    }
    ShardedPageCache.rethrowCleanupFailure(failure);
  }
}
