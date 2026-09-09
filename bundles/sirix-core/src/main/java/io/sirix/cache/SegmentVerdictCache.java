/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * Settled-verdict tables for string predicates over SEGMENT-scoped projection dictionaries, one
 * table per {@code (segment dictionary, revision, op, literal)}, retained across queries.
 *
 * <p>
 * The global kind caches its verdict as a bitset filled by one sweep; a sealed segment dictionary
 * refuses that sweep (its ids are mints behind a rank table), so the segment kind settles ids one
 * dictionary read at a time, as rows reference them. Without this cache every query re-read every
 * referenced value: four {@code LIKE} queries over the same column at 100M rows each paid ~18M
 * dictionary reads for verdicts the previous query had already settled.
 * </p>
 *
 * <p>
 * <b>The cached table is SHARED, not copied</b> — the opposite of {@link GlobalVerdictCache}. A
 * table is a monotone three-state memo: every entry starts unsettled and is written at most to the
 * one verdict the dictionary determines for it, by whichever query settles it first. Two queries
 * settling the same entry concurrently write the same byte, and a byte store never tears, so a
 * reader sees either "unsettled" (and evaluates, idempotently) or the verdict. Sharing is the
 * point: a query that touches a segment inherits every entry an earlier query settled and adds its
 * own.
 * </p>
 *
 * <p>
 * Bounded by WEIGHT in bytes, one byte per dictionary id: ~18 MB for an 18M-entry column, and one
 * table per distinct {@code (op, literal)}. A count bound comfortable at a million rows would
 * silently cost gigabytes at a hundred million. Missing is always safe: the caller allocates a
 * fresh table and settles it as it would have anyway.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentVerdictCache implements Cache<GlobalVerdictCacheKey, byte[]> {

  private final com.github.benmanes.caffeine.cache.Cache<GlobalVerdictCacheKey, byte[]> cache;

  /**
   * @param maxWeightBytes total table bytes retained; entries beyond it are evicted
   */
  public SegmentVerdictCache(final long maxWeightBytes) {
    if (maxWeightBytes <= 0L) {
      throw new IllegalArgumentException("segment verdict cache budget must be positive, got " + maxWeightBytes);
    }
    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeightBytes)
                    .weigher((GlobalVerdictCacheKey key, byte[] table) -> table.length)
                    .scheduler(scheduler)
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  /**
   * The table for {@code key}, or {@code null} — the cached array ITSELF, so the caller's settled
   * entries land in the copy every later query reads. See the class comment for why that is sound.
   */
  @Override
  public byte[] get(final GlobalVerdictCacheKey key) {
    return cache.getIfPresent(key);
  }

  /**
   * REFUSED, for the reason {@link GlobalVerdictCache#get(GlobalVerdictCacheKey, BiFunction)} gives:
   * a mapping function under the bin lock stalls unrelated callers. Use {@link #get} then
   * {@link #put}; two callers allocating on the same miss costs one orphaned empty table at worst.
   */
  @Override
  public byte[] get(final GlobalVerdictCacheKey key,
      final BiFunction<? super GlobalVerdictCacheKey, ? super byte[], ? extends byte[]> mappingFunction) {
    throw new UnsupportedOperationException(
        "compute-under-bin-lock is refused for verdict tables: use get(key) then put(key, table)");
  }

  /** No second tier: a table is rebuilt by the query that misses it. */
  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<GlobalVerdictCacheKey, byte[]> getAll(final Iterable<? extends GlobalVerdictCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void put(final GlobalVerdictCacheKey key, final byte[] value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends GlobalVerdictCacheKey, ? extends byte[]> map) {
    cache.putAll(map);
  }

  @Override
  public void remove(final GlobalVerdictCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public ConcurrentMap<GlobalVerdictCacheKey, byte[]> asMap() {
    return cache.asMap();
  }

  @Override
  public void close() {
    cache.invalidateAll();
  }
}
