/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Buffer-manager-owned, byte-budgeted cache of immutable resolved HOT slots. Hits use the ordinary
 * page-cache guard. Admissions and invalidations use shared stripe monitors; invalidation fences
 * in-flight read-through admission so truncation cannot resurrect an answer under a reused durable
 * offset. Fences use 1,024 shared hash stripes over database, resource and durable key. Promotion
 * normally touches only one leaf; a rare collision can serialize or reject another leaf's
 * admission.
 */
public final class HOTMiniPageCache {
  /** Four distinct confirmed point misses justify one ordinary complete-view reconstruction. */
  public static final int POINT_PROMOTION_DISTINCT_KEYS = 4;
  private static final HOTMiniPageCache DISABLED = new HOTMiniPageCache();
  private final @Nullable ShardedPageCache<HOTMiniPage> pages;
  /** Fixed, power-of-two fence array: an O(1) index, and nothing that grows with resource count. */
  private static final int ADMISSION_STRIPES = 1024;

  private final AdmissionStripe @Nullable [] stripes;

  /**
   * One admission fence: the monitor that serialises the admissions landing on it, and the
   * invalidation generation those admissions check. Keeping both on the same object is what publishes
   * a bump through the very monitor release the admitting thread acquires.
   *
   * <p>
   * Fences are striped by (databaseId, resourceId, durable key), so a discard normally touches only
   * the promoted leaf. {@value #ADMISSION_STRIPES} stripes make a collision rare, and a collision
   * costs only a rejected admission: the read still returns the right answer, it simply stays
   * uncached until the next attempt.
   * </p>
   */
  private static final class AdmissionStripe {
    private volatile long generation;
  }

  public HOTMiniPageCache(final long maxWeightBytes) {
    if (maxWeightBytes <= 0) {
      throw new IllegalArgumentException("Mini-page cache budget must be positive; use disabled()");
    }
    pages = new ShardedPageCache<>(maxWeightBytes);
    stripes = new AdmissionStripe[ADMISSION_STRIPES];
    for (int i = 0; i < stripes.length; i++) {
      stripes[i] = new AdmissionStripe();
    }
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
   * and to hand back to {@link #admit} or {@link #claimPointPromotion}. It is striped by resource and
   * durable key, so a discard rejects an admission for an unrelated leaf only on a hash collision,
   * and even then the answer is correct and merely uncached. It can never let a stale one through.
   */
  public long generation(final PageReference key) {
    Objects.requireNonNull(key);
    return stripes == null
        ? 0
        : stripeFor(key).generation;
  }

  private AdmissionStripe stripeFor(final PageReference key) {
    final AdmissionStripe[] all = Objects.requireNonNull(stripes);
    long mixed = key.getDatabaseId() * 0x9E3779B97F4A7C15L;
    mixed = (mixed ^ key.getResourceId()) * 0xC2B2AE3D27D4EB4FL;
    mixed = (mixed ^ key.getKey()) * 0x165667B19E3779F9L;
    return all[(int) (mixed >>> 48) & all.length - 1];
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
   * key. Every complete-leaf reconstruction bumps its shared hash stripe: an admission for this key
   * either publishes before the bump and is removed here, or sees the bump and declines. A colliding
   * leaf's admission can also be rejected, leaving its correct answer uncached.
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
    Throwable failure = null;
    for (final PageReference key : pages.asMap().keySet()) {
      try {
        if (matches.test(key)) {
          retireRemoved(key);
        }
      } catch (final RuntimeException | Error e) {
        failure = ShardedPageCache.retainCleanupFailure(failure, e);
      }
    }
    ShardedPageCache.rethrowCleanupFailure(failure);
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
