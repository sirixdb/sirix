package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.node.ValueDictionaryBlockIndexNode;
import io.sirix.node.ValueDictionaryDirectoryNode;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHashBucketNode;
import io.sirix.node.ValueDictionaryRadixNode;
import io.sirix.node.ValueDictionaryRankTableNode;
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.node.ValueDictionaryValueBucketNode;
import io.sirix.node.interfaces.DataRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * Decoded records of the global projection value dictionary, retained ACROSS transactions.
 *
 * <p>
 * A read view retains blocks for its own lifetime, but a view is built per query execution and dies
 * with it, so every execution re-fetched and re-decoded the dictionary material it touched.
 * Measured on a 43-query ClickBench leg: <b>26,300 LZ77 decode dispatches with three global columns
 * against 125 without any</b> — the whole difference is dictionary records being decoded again and
 * again.
 * </p>
 *
 * <p>
 * Bounded by WEIGHT in decoded bytes, because that is what actually occupies memory: a value block
 * carries up to {@link ValueDictionaryValueBlockNode#MAX_BLOCK_BYTES} of expanded values while a
 * radix node carries a handful of references, and a count bound would treat them alike. Missing is
 * always safe — the caller re-reads through the path that already exists.
 * </p>
 */
public final class GlobalDictionaryRecordCache implements Cache<GlobalDictionaryRecordCacheKey, DataRecord> {

  /** Charged for a record whose size is not known from its type; small by construction. */
  private static final int DEFAULT_RECORD_WEIGHT = 256;

  private final com.github.benmanes.caffeine.cache.Cache<GlobalDictionaryRecordCacheKey, DataRecord> cache;

  /**
   * @param maxWeightBytes decoded dictionary bytes retained across transactions
   */
  public GlobalDictionaryRecordCache(final long maxWeightBytes) {
    if (maxWeightBytes < 0L) {
      throw new IllegalArgumentException("dictionary record cache budget must be non-negative, got " + maxWeightBytes);
    }
    final long unit = HeapCacheBudget.weightUnit(maxWeightBytes);
    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeightBytes / unit)
                    .executor(Runnable::run)
                    .weigher((GlobalDictionaryRecordCacheKey key,
                        DataRecord record) -> HeapCacheBudget.weight(retainedBytes(record), unit))
                    .scheduler(scheduler)
                    .build();
  }

  /** Price every dictionary array without copying it or decoding its values. */
  private static long retainedBytes(final DataRecord record) {
    final long payload = switch (record) {
      case ValueDictionaryValueBlockNode block -> block.rawBytes().length + 4L * (block.size() + 1L);
      case ValueDictionaryValueBucketNode bucket -> 16L * bucket.blockCount() + 12L * bucket.spillCount();
      case ValueDictionaryRadixNode radix -> 9L * radix.childCount();
      case ValueDictionaryHashBucketNode bucket -> 12L * bucket.size();
      case ValueDictionaryDirectoryNode directory -> 12L * directory.size();
      case ValueDictionaryEntryNode entry -> entry.getValueLength();
      case ValueDictionaryRankTableNode ranks -> 8L * ranks.words().length;
      case ValueDictionaryBlockIndexNode blocks -> blocks.separatorBytes().length + 8L * blocks.size() + 4L;
      default -> 0L; // Header, collision and segment records contain only scalar fields.
    };
    return DEFAULT_RECORD_WEIGHT + payload;
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public DataRecord get(final GlobalDictionaryRecordCacheKey key) {
    return cache.getIfPresent(key);
  }

  @Override
  public DataRecord get(final GlobalDictionaryRecordCacheKey key,
      final BiFunction<? super GlobalDictionaryRecordCacheKey, ? super DataRecord, ? extends DataRecord> mappingFunction) {
    // getIfPresent first: asMap().compute() locks the bin and runs the mapping function even on a
    // hit, which here would re-read and re-decode the record the cache exists to keep. Same defect
    // already fixed in NamesCache and PageCache.
    final DataRecord hit = cache.getIfPresent(key);
    if (hit != null) {
      return hit;
    }
    return cache.asMap().compute(key, mappingFunction);
  }

  /** No second tier: a dictionary record is cheap to re-read relative to promoting it anywhere. */
  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<GlobalDictionaryRecordCacheKey, DataRecord> getAll(
      final Iterable<? extends GlobalDictionaryRecordCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void put(final GlobalDictionaryRecordCacheKey key, final DataRecord value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends GlobalDictionaryRecordCacheKey, ? extends DataRecord> map) {
    cache.putAll(map);
  }

  @Override
  public void remove(final GlobalDictionaryRecordCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public ConcurrentMap<GlobalDictionaryRecordCacheKey, DataRecord> asMap() {
    return cache.asMap();
  }

  @Override
  public void close() {
    cache.invalidateAll();
  }
}
