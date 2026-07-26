/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.pageskip;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.RoaringBitmap;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-resource "nameKey → pages-containing-nameKey" page-skip index.
 *
 * <p>Analytical scans that anchor on a single JSON field currently iterate
 * every leaf page in the resource, fetch each page, and call
 * {@code KeyValueLeafPage.getObjectKeySlotsForNameKey(anchorNameKey)},
 * bailing when the slot list is empty. At 100M records (~100K pages) any
 * non-universal field lives on a fraction of pages; the wasted fetches
 * dominate.
 *
 * <h2>Build strategy — opportunistic, zero install cost</h2>
 * A 100K-page eager build would itself fetch every page once, negating
 * the benefit on the first query. Instead the registry is
 * <em>populated as a side-effect of the first scan that anchors on a
 * given nameKey</em>. That scan has to fetch every page anyway to
 * produce its result; it emits the set of pages where
 * {@code anchorNameKey} matched via
 * {@link #recordCompleteScan(String, int, RoaringBitmap, long)}. Every
 * subsequent scan anchored on the same nameKey (and against the same
 * underlying page set — see {@code totalPagesAtScan}) reads the bitmap
 * in O(#matching pages) and skips the rest.
 *
 * <h2>Invalidation</h2>
 * A bitmap is valid only for the resource state it was built against.
 * We capture {@code totalPages} at record time and expose it via
 * {@link Handle#pagesForIfValid(int, long)} — callers pass the current
 * {@code totalPages} and get {@code null} back on mismatch (commit,
 * truncation, or any change to the page count). Simpler than a full
 * revision-based scheme and correct for the read-only analytical
 * workload the registry targets.
 *
 * <h2>Thread-safety</h2>
 * Backed by a {@link ConcurrentHashMap} of resource-key → per-resource
 * handle. Per-resource handles use a {@link ConcurrentHashMap} from
 * {@code nameKey} to {@link Entry}. First writer wins; once published an
 * entry is immutable.
 */
public final class PageSkipRegistry {

  /** Per-(resource, nameKey) record: the bitmap and the scan-time totalPages. */
  private static final class Entry {
    final RoaringBitmap bitmap;
    final long totalPagesAtScan;

    Entry(final RoaringBitmap bitmap, final long totalPagesAtScan) {
      this.bitmap = bitmap;
      this.totalPagesAtScan = totalPagesAtScan;
    }
  }

  /** Per-resource handle. One instance per {@code resourceKey}. */
  public static final class Handle {
    private final ConcurrentMap<Integer, Entry> byNameKey = new ConcurrentHashMap<>();

    /**
     * @param nameKey        the anchor nameKey to look up
     * @param currentTotalPages {@code totalPages} the caller is about to
     *                          scan against. If the cached bitmap was
     *                          built for a different page count it's
     *                          returned as {@code null} (invalidated by
     *                          a write since the bitmap was built).
     * @return sorted page keys that contain at least one slot with
     *         {@code nameKey}, or {@code null} if no cached bitmap is
     *         available (caller must fall back to a full scan and
     *         should call {@link #recordCompleteScan} at the end).
     */
    public int[] pagesForIfValid(final int nameKey, final long currentTotalPages) {
      final Entry e = byNameKey.get(nameKey);
      if (e == null) return null;
      if (e.totalPagesAtScan != currentTotalPages) return null;
      if (e.bitmap.isEmpty()) return EMPTY_INT_ARRAY;
      return e.bitmap.toArray();
    }

    /** Non-validating variant — use only when you've confirmed the totalPages yourself. */
    public int[] pagesForOrNull(final int nameKey) {
      final Entry e = byNameKey.get(nameKey);
      if (e == null || e.bitmap.isEmpty()) return null;
      return e.bitmap.toArray();
    }

    /**
     * Publish a page-skip bitmap built by a full scan. First writer
     * wins; subsequent calls with the same {@code nameKey} are ignored.
     *
     * @param nameKey the anchor nameKey of the completed scan.
     * @param matchingPages pages whose {@code getObjectKeySlotsForNameKey}
     *                      returned at least one match. The registry
     *                      takes ownership of the bitmap — callers must
     *                      not mutate it after this call. May be empty
     *                      (the nameKey isn't present anywhere); even
     *                      an empty bitmap is useful because it lets
     *                      future scans short-circuit to "no pages".
     * @param totalPagesAtScan the {@code totalPages} the scan was run
     *                         against. Checked on lookup to detect
     *                         stale bitmaps after writes.
     */
    public void record(final int nameKey, final RoaringBitmap matchingPages,
        final long totalPagesAtScan) {
      Objects.requireNonNull(matchingPages, "matchingPages");
      byNameKey.putIfAbsent(nameKey, new Entry(matchingPages, totalPagesAtScan));
    }

    /** @return distinct nameKey count recorded so far. */
    public int size() {
      return byNameKey.size();
    }
  }

  private static final ConcurrentMap<String, Handle> REGISTRY = new ConcurrentHashMap<>();
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  private PageSkipRegistry() {
  }

  /**
   * @return the handle for {@code resourceKey}, creating it on first
   *         access. The handle outlives any specific revision — it's
   *         keyed only by resource identity; staleness is detected at
   *         lookup time via the {@code totalPages} sanity check.
   */
  public static Handle handleFor(final String resourceKey) {
    if (resourceKey == null) return null;
    return REGISTRY.computeIfAbsent(resourceKey, k -> new Handle());
  }

  /** @return the installed handle without creating one. */
  public static Handle lookup(final String resourceKey) {
    if (resourceKey == null) return null;
    return REGISTRY.get(resourceKey);
  }

  /** Remove the handle for {@code resourceKey}. */
  public static void uninstall(final String resourceKey) {
    if (resourceKey == null) return;
    REGISTRY.remove(resourceKey);
  }

  /** Drop every entry — for test isolation. */
  public static void clear() {
    REGISTRY.clear();
  }

  /**
   * Convenience: record a completed scan's matching pages.
   * Equivalent to
   * {@code handleFor(resourceKey).record(nameKey, pages, totalPages)}.
   */
  public static void recordCompleteScan(final String resourceKey, final int nameKey,
      final RoaringBitmap matchingPages, final long totalPagesAtScan) {
    final Handle h = handleFor(resourceKey);
    if (h != null) h.record(nameKey, matchingPages, totalPagesAtScan);
  }

  // Package-private helper for diagnostic toString in tests.
  static int size() {
    return REGISTRY.size();
  }

  /** For tests: raw access to a handle's backing map for assertions. */
  static Int2ObjectMap<RoaringBitmap> snapshot(final Handle h) {
    if (h == null) return new Int2ObjectOpenHashMap<>();
    final Int2ObjectOpenHashMap<RoaringBitmap> out = new Int2ObjectOpenHashMap<>(h.byNameKey.size());
    for (final var e : h.byNameKey.entrySet()) {
      out.put(e.getKey().intValue(), e.getValue().bitmap);
    }
    return out;
  }
}
