/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.projection.ProjectionSortedReadView;
import io.sirix.cache.ProjectionBlobCache.Key;
import io.sirix.exception.SirixIOException;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.interfaces.Page;
import java.util.List;
import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTProjectionImage;
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
    return admit(key, expectedGeneration, revision, slotKey, sideReferenceKey, entry, null);
  }

  /** Share at most one admission between a point seek's data and metadata records. */
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
          if (previous != null && scope != null) {
            scope.admitted = true;
          }
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
  public boolean claimPointPromotion(final PageReference key, final long expectedGeneration, final byte[] slotKey) {
    return claimPointPromotion(key, expectedGeneration, slotKey, null);
  }

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
    private @Nullable ProjectionBlobCache blobCache;
    private @Nullable Key blobKey;
    private byte @Nullable [] blob;
    private @Nullable ProjectionSortedReadView blobView;
    private long blobGeneration;
    private boolean metadataScan;
    private boolean blobRetention;
    private @Nullable Object imageOwner;
    private @Nullable HOTProjectionImage image;
    private long imageOffset;
    private long imageHash;
    private boolean imageHasHash;
    private long imageGeneration;

    /** One bounded heap image may serve this query's related logical slots. */
    public @Nullable HOTProjectionImage image(final Object owner, final PageReference reference,
        final long currentGeneration) {
      return !finished && imageOwner == owner && imageOffset == reference.getKey()
          && imageHasHash == reference.hasHash() && (!imageHasHash || imageHash == reference.getHashAsLong())
          && imageGeneration == currentGeneration
              ? image
              : null;
    }

    public void rememberImage(final Object owner, final PageReference reference, final HOTProjectionImage verified,
        final long expectedGeneration) {
      if (!finished) {
        imageOwner = Objects.requireNonNull(owner);
        image = Objects.requireNonNull(verified);
        imageOffset = reference.getKey();
        imageHasHash = reference.hasHash();
        imageHash = imageHasHash
            ? reference.getHashAsLong()
            : 0;
        imageGeneration = expectedGeneration;
      }
    }

    public void discardImage() {
      imageOwner = null;
      image = null;
    }

    /** Heap ownership is one image (at most 64 KiB plus headers), independent of cache admission. */
    public long retainedImageBytes() {
      return image == null
          ? 0
          : image.retainedSizeBytes();
    }

    /** Metadata scans retain their existing cache policy while sharing the bounded physical read. */
    public void metadataScan() {
      if (finished) {
        throw new IllegalStateException("a completed scope cannot become a metadata scan");
      }
      metadataScan = true;
      blobCache = null;
      blobKey = null;
      blob = null;
      blobView = null;
      cache = null;
      pageKey = null;
      slotKey = null;
      entry = null;
      promotionReader = null;
    }

    public void useBlobRetention() {
      blobRetention = true;
    }

    public boolean limitsBlobAdmissions() {
      return !metadataScan;
    }

    /** Verified data wins the seek's sole admission; metadata waits for a successful point seek. */
    public void retainBlob(final ProjectionBlobCache owner, final Key key, final byte[] verified,
        final long expectedGeneration, final boolean data) {
      if (finished || admitted || !owner.canCache(verified.length)) {
        return;
      }
      if (data) {
        admitted = owner.put(key, verified, expectedGeneration);
      } else if (blobCache == null) {
        blobCache = owner;
        blobKey = key;
        blob = verified.clone();
        blobGeneration = expectedGeneration;
      }
    }


    /** Same one-entry admission rule, with no copy of an already immutable validated view. */
    public void retainView(final ProjectionBlobCache owner, final Key key, final ProjectionSortedReadView view,
        final long expectedGeneration, final boolean data) {
      if (finished || admitted || !owner.canCache(view)) {
        return;
      }
      if (data) {
        admitted = owner.putView(key, view, expectedGeneration);
      } else if (blobCache == null) {
        blobCache = owner;
        blobKey = key;
        blobView = view;
        blobGeneration = expectedGeneration;
      }
    }

    public void rememberMetadata(final HOTMiniPageCache owner, final PageReference key, final long expectedGeneration,
        final int revisionNumber, final byte[] slot, final long sideKey, final @Nullable HOTLeafEntry value) {
      rememberMetadata(owner, key, expectedGeneration, revisionNumber, slot, sideKey, value, null, null);
    }

    /** A metadata miss may promote only after this scope finishes as a successful POINT seek. */
    public void rememberMetadata(final HOTMiniPageCache owner, final PageReference key, final long expectedGeneration,
        final int revisionNumber, final byte[] slot, final long sideKey, final @Nullable HOTLeafEntry value,
        final @Nullable StorageEngineReader reader, final @Nullable PageReference chainReference) {
      if (finished || metadataScan || blobRetention || cache != null || owner.pages == null
          || !HOTMiniPage.fitsEmpty(slot, value)) {
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
        if (point && !admitted && blobCache != null) {
          admitted = blobView == null
              ? blobCache.put(Objects.requireNonNull(blobKey), Objects.requireNonNull(blob), blobGeneration)
              : blobCache.putView(Objects.requireNonNull(blobKey), blobView, blobGeneration);
        }
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
        blobCache = null;
        blobKey = null;
        blob = null;
        blobView = null;
        imageOwner = null;
        image = null;
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
