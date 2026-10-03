/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Buffer-manager-owned, byte-budgeted cache of immutable resolved HOT slots. Hits use the ordinary
 * page-cache guard. Only admissions lock (per resource); invalidation fences every in-flight
 * read-through admission so truncation cannot resurrect an answer under a reused durable offset.
 * The fence lives on the owning resource, so promoting one leaf to its complete image never
 * serialises or rejects the concurrent resolution of a leaf belonging to any other resource.
 */
public final class HOTMiniPageCache {
  /** Four distinct confirmed point misses justify one ordinary complete-view reconstruction. */
  public static final int POINT_PROMOTION_DISTINCT_KEYS = 4;
  private static final HOTMiniPageCache DISABLED = new HOTMiniPageCache();
  private final @Nullable ShardedPageCache<HOTMiniPage> pages;
  /** Copy-on-write, one entry per resource ever admitted; scanned without allocating. */
  private volatile AdmissionStripe @Nullable [] stripes;

  /**
   * One resource's admission fence: the monitor that serialises its admissions, and the invalidation
   * generation those admissions check. Keeping both on the same object is what publishes a bump
   * through the very monitor release the admitting thread acquires.
   *
   * <p>
   * Scoped to the resource rather than to a hash bucket of keys. A hash stripe made two unrelated
   * resources share a fence, so promoting a leaf in one could reject an in-flight admission in the
   * other; a resource is the smallest scope every bulk invalidation already works in.
   * </p>
   */
  private static final class AdmissionStripe {
    private final long databaseId;
    private final long resourceId;
    private volatile long generation;

    private AdmissionStripe(final long databaseId, final long resourceId) {
      this.databaseId = databaseId;
      this.resourceId = resourceId;
    }
  }

  public HOTMiniPageCache(final long maxWeightBytes) {
    if (maxWeightBytes <= 0) {
      throw new IllegalArgumentException("Mini-page cache budget must be positive; use disabled()");
    }
    pages = new ShardedPageCache<>(maxWeightBytes);
    stripes = new AdmissionStripe[0];
  }

  private HOTMiniPageCache() {
    pages = null;
    stripes = null;
  }

  public static HOTMiniPageCache disabled() {
    return DISABLED;
  }

  /**
   * The fence to capture BEFORE reading anything an admission for {@code key} will be derived from,
   * and to hand back to {@link #admit} or {@link #claimPointPromotion}. It is per resource, so a
   * discard may conservatively reject an admission for another leaf of the SAME resource; it never
   * touches another resource, and it can never let a stale admission through.
   */
  public long generation(final PageReference key) {
    Objects.requireNonNull(key);
    return stripes == null
        ? 0
        : stripeFor(key).generation;
  }

  private AdmissionStripe stripeFor(final PageReference key) {
    final long databaseId = key.getDatabaseId();
    final long resourceId = key.getResourceId();
    final AdmissionStripe[] existing = Objects.requireNonNull(stripes);
    for (final AdmissionStripe candidate : existing) {
      if (candidate.databaseId == databaseId && candidate.resourceId == resourceId) {
        return candidate;
      }
    }
    return addStripe(databaseId, resourceId);
  }

  private synchronized AdmissionStripe addStripe(final long databaseId, final long resourceId) {
    final AdmissionStripe[] existing = Objects.requireNonNull(stripes);
    for (final AdmissionStripe candidate : existing) {
      if (candidate.databaseId == databaseId && candidate.resourceId == resourceId) {
        return candidate;
      }
    }
    final AdmissionStripe created = new AdmissionStripe(databaseId, resourceId);
    final AdmissionStripe[] grown = Arrays.copyOf(existing, existing.length + 1);
    grown[existing.length] = created;
    stripes = grown;
    return created;
  }

  public @Nullable HOTMiniPage getAndGuard(final PageReference key) {
    return pages == null
        ? null
        : pages.getAndGuard(key);
  }

  /**
   * Admit a resolved result if the generation captured BEFORE reading fragments still holds. True
   * requests full-page promotion because an existing mini image would exceed its packed cap. A single
   * oversized record is returned uncached: its first access still resolves only one slot.
   */
  public boolean admit(final PageReference key, final long expectedGeneration, final int revision, final byte[] slotKey,
      final long sideReferenceKey, final @Nullable HOTLeafEntry entry) {
    if (pages == null || !HOTMiniPage.canCache(slotKey, entry)) {
      return false;
    }
    Objects.requireNonNull(key);
    if (key.getKey() < 0 || key.getLogKey() >= 0) {
      throw new IllegalArgumentException("Mini-page cache key must be a canonical durable reference");
    }
    final AdmissionStripe stripe = stripeFor(key);
    synchronized (stripe) {
      if (stripe.generation != expectedGeneration) {
        return false;
      }
      final HOTMiniPage previous = pages.getAndGuard(key);
      try {
        if (previous != null && previous.isPointPromotionRequested()) {
          return false;
        }
        final HOTMiniPage next = HOTMiniPage.append(previous, key.getKey(), revision, slotKey, sideReferenceKey, entry);
        if (next == null) {
          return previous != null;
        }
        if (next != previous) {
          if (next.getActualMemorySize() > pages.getMaxWeightBytes()) {
            next.retire();
            return false;
          }
          // Own the immutable canonical key; callers may reuse or mutate their reference later.
          final PageReference ownedKey = new PageReference().setKey(key.getKey())
                                                            .setDatabaseId(key.getDatabaseId())
                                                            .setResourceId(key.getResourceId());
          pages.put(ownedKey, next);
        }
        return false;
      } finally {
        if (previous != null) {
          previous.releaseGuard();
        }
      }
    }
  }

  /**
   * Claim one promotion on a new distinct POINT miss. The old subset stays guarded and immutable
   * while the ordinary loader runs. Concurrent misses cannot replace this claim with another mini
   * image; successful complete adoption discards it through the normal generation fence. A failed
   * attempt keeps serving the old subset without repeatedly attempting the same promotion.
   */
  public boolean claimPointPromotion(final PageReference key, final long expectedGeneration, final byte[] slotKey) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(slotKey);
    if (key.getKey() < 0 || key.getLogKey() >= 0) {
      throw new IllegalArgumentException("Point promotion requires a canonical durable reference");
    }
    if (pages == null) {
      return false;
    }
    final AdmissionStripe stripe = stripeFor(key);
    synchronized (stripe) {
      if (stripe.generation != expectedGeneration) {
        return false;
      }
      final HOTMiniPage previous = pages.getAndGuard(key);
      if (previous == null) {
        return false;
      }
      try {
        if (previous.getDistinctKeyCount() < POINT_PROMOTION_DISTINCT_KEYS - 1 || previous.containsKey(slotKey)
            || !previous.requestPointPromotion()) {
          return false;
        }
        return true;
      } finally {
        previous.releaseGuard();
      }
    }
  }

  /**
   * Drop the subset after complete-page promotion and reject earlier, unfinished admissions for this
   * key. Every complete-leaf reconstruction calls this, so it touches only its own resource: an
   * admission either publishes before the bump and is removed here, or sees the bump and declines.
   */
  public void discard(final PageReference key) {
    if (pages == null) {
      return;
    }
    Objects.requireNonNull(key);
    final AdmissionStripe stripe = stripeFor(key);
    synchronized (stripe) {
      stripe.generation++;
      retireRemoved(key);
    }
  }

  public void clear() {
    if (pages == null) {
      return;
    }
    fenceEveryStripe();
    pages.clear();
  }

  public void invalidate(final Predicate<PageReference> matches) {
    Objects.requireNonNull(matches);
    if (pages == null) {
      return;
    }
    fenceEveryStripe();
    for (final PageReference key : pages.asMap().keySet()) {
      if (matches.test(key)) {
        retireRemoved(key);
      }
    }
  }

  /**
   * A bulk removal cannot name the keys an in-flight admission is derived from, so it fences every
   * stripe first. An admission already inside its stripe publishes before the bump and is then
   * removed below; one that has not entered yet observes the bump and declines.
   */
  private void fenceEveryStripe() {
    for (final AdmissionStripe stripe : Objects.requireNonNull(stripes)) {
      synchronized (stripe) {
        stripe.generation++;
      }
    }
  }

  private void retireRemoved(final PageReference key) {
    final HOTMiniPage removed = Objects.requireNonNull(pages).removeAndGet(key);
    if (removed != null) {
      removed.retire();
    }
  }

  /** Buffer-manager integration only; mutations must preserve the admission fence above. */
  @Nullable
  ShardedPageCache<HOTMiniPage> pages() {
    return pages;
  }

  public long getCurrentWeightBytes() {
    return pages == null
        ? 0
        : pages.getCurrentWeightBytes();
  }

  public long getMaxWeightBytes() {
    return pages == null
        ? 0
        : pages.getMaxWeightBytes();
  }

  void evictUnderPressure() {
    if (pages != null) {
      pages.evictUnderPressure();
    }
  }
}
