/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.List;
import java.util.function.Predicate;

/**
 * Buffer-manager-owned, byte-budgeted cache of immutable resolved HOT slots. Hits use the ordinary
 * page-cache guard. Only admissions lock (per key stripe); invalidation fences every in-flight
 * read-through admission so truncation cannot resurrect an answer under a reused durable offset.
 * The fence lives on the key's own stripe, so promoting one leaf to its complete image never
 * serialises or rejects the concurrent resolution of an unrelated leaf.
 */
public final class HOTMiniPageCache {
  /** Four distinct confirmed point misses justify one ordinary complete-view reconstruction. */
  public static final int POINT_PROMOTION_DISTINCT_KEYS = 4;
  private static final HOTMiniPageCache DISABLED = new HOTMiniPageCache();
  private final @Nullable ShardedPageCache<HOTMiniPage> pages;
  private final AdmissionStripe @Nullable [] stripes;

  /**
   * One admission stripe: the monitor that serialises admissions for its keys, and the invalidation
   * generation those admissions check. Keeping both on the same object is what makes the fence
   * key-scoped — a bump is published by the same monitor release the admitting thread acquires.
   */
  private static final class AdmissionStripe {
    private volatile long generation;
  }

  public HOTMiniPageCache(final long maxWeightBytes) {
    if (maxWeightBytes <= 0) {
      throw new IllegalArgumentException("Mini-page cache budget must be positive; use disabled()");
    }
    pages = new ShardedPageCache<>(maxWeightBytes);
    stripes = new AdmissionStripe[64];
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
   * and to hand back to {@link #admit} or {@link #claimPointPromotion}. It is striped, so a discard
   * may conservatively reject an admission for another key sharing the stripe; it can never let a
   * stale one through.
   */
  public long generation(final PageReference key) {
    Objects.requireNonNull(key);
    return stripes == null
        ? 0
        : stripeFor(key).generation;
  }

  private AdmissionStripe stripeFor(final PageReference key) {
    final AdmissionStripe[] all = Objects.requireNonNull(stripes);
    return all[key.hashCode() & (all.length - 1)];
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
    return admit(key, expectedGeneration, revision, slotKey, sideReferenceKey, entry, null);
  }

  /** The optional query scope shares one admission between its data lookup and metadata. */
  public boolean admit(final PageReference key, final long expectedGeneration, final int revision, final byte[] slotKey,
      final long sideReferenceKey, final @Nullable HOTLeafEntry entry, final @Nullable ReadScope scope) {
    if (pages == null || (scope != null && (scope.admitted || scope.finished))
        || !HOTMiniPage.canCache(slotKey, entry)) {
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
          if (scope != null) {
            scope.admitted = true;
          }
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
  public boolean claimPointPromotion(final PageReference key, final long expectedGeneration, final byte[] slotKey,
      final @Nullable ReadScope scope) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(slotKey);
    if (key.getKey() < 0 || key.getLogKey() >= 0) {
      throw new IllegalArgumentException("Point promotion requires a canonical durable reference");
    }
    if (pages == null || (scope != null && (scope.admitted || scope.finished))) {
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
        if (scope != null) {
          scope.admitted = true;
        }
        return true;
      } finally {
        previous.releaseGuard();
      }
    }
  }

  /**
   * One ambiguous seek may grow at most one mini entry. Its data slot has priority. Remember only the
   * first metadata miss, detached and bounded to one cacheable record; publish it only after a
   * successful point seek that did not admit data. A scan, failed seek or abandoned open publishes
   * nothing. This reader-confined object owns no guard, native page or unbounded collection.
   */
  public static final class ReadScope {
    private @Nullable HOTMiniPageCache cache;
    private @Nullable PageReference pageKey;
    private byte @Nullable [] slotKey;
    private @Nullable HOTLeafEntry entry;
    private long generation;
    private long sideReferenceKey;
    private int revision;
    private boolean admitted;
    private boolean finished;
    private @Nullable StorageEngineReader promotionReader;

    public void rememberMetadata(final HOTMiniPageCache owner, final PageReference key, final long expectedGeneration,
        final int revisionNumber, final byte[] slot, final long sideKey, final @Nullable HOTLeafEntry value) {
      rememberMetadata(owner, key, expectedGeneration, revisionNumber, slot, sideKey, value, null, null);
    }

    /** A metadata miss may promote only after this scope finishes as a successful POINT seek. */
    public void rememberMetadata(final HOTMiniPageCache owner, final PageReference key, final long expectedGeneration,
        final int revisionNumber, final byte[] slot, final long sideKey, final @Nullable HOTLeafEntry value,
        final @Nullable StorageEngineReader reader, final @Nullable PageReference chainReference) {
      if (finished || cache != null || owner.pages == null || !HOTMiniPage.fitsEmpty(slot, value)) {
        return;
      }
      // A caller can reuse its key buffer or mutate detached values before finishing the scope.
      slotKey = slot.clone();
      pageKey = new PageReference().setKey(key.getKey())
                                   .setDatabaseId(key.getDatabaseId())
                                   .setResourceId(key.getResourceId());
      if (reader != null && chainReference != null) {
        // The caller may reuse its reference or fragment list before finishing. Copy only durable
        // routing/checksum data; never retain a swizzled page without its guard.
        pageKey.setPageFragments(List.copyOf(chainReference.getPageFragments()));
        if (chainReference.hasHash()) {
          pageKey.setHash(chainReference.getHashAsLong());
        }
        promotionReader = reader;
      }
      if (value != null) {
        final PageReference source = value.sideReference();
        PageReference side = null;
        if (source != null) {
          side = new PageReference().setKey(source.getKey())
                                    .setDatabaseId(source.getDatabaseId())
                                    .setResourceId(source.getResourceId());
          if (source.hasHash()) {
            side.setHash(source.getHashAsLong());
          }
        }
        entry = new HOTLeafEntry(value.value().clone(), side);
      }
      generation = expectedGeneration;
      revision = revisionNumber;
      sideReferenceKey = sideKey;
      cache = owner;
    }

    /** Call once after a successful POINT seek, or with false to abandon a scan/failed lookup. */
    public void finish(final boolean point) {
      if (finished) {
        return;
      }
      finished = true;
      try {
        if (point && !admitted && cache != null) {
          if (promotionReader != null && cache.claimPointPromotion(Objects.requireNonNull(pageKey), generation,
              Objects.requireNonNull(slotKey), null)) {
            final Page page = promotionReader.loadHOTPageAndGuard(pageKey);
            if (!(page instanceof HOTLeafPage leaf)) {
              throw new SirixIOException("Point metadata promotion did not load a HOT leaf");
            }
            leaf.releaseGuard();
            return;
          }
          // The normal generation fence, immutable identity, guards and byte budget all apply.
          // A full mini cap simply leaves metadata uncached; it never forces another data merge.
          cache.admit(Objects.requireNonNull(pageKey), generation, revision, Objects.requireNonNull(slotKey),
              sideReferenceKey, entry);
        }
      } finally {
        cache = null;
        pageKey = null;
        slotKey = null;
        entry = null;
        promotionReader = null;
      }
    }
  }

  /**
   * Drop the subset after complete-page promotion and reject earlier, unfinished admissions for this
   * key. Every complete-leaf reconstruction calls this, so it touches only the key's own stripe: an
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
