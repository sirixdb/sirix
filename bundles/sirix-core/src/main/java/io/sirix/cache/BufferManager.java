package io.sirix.cache;

import io.sirix.index.name.Names;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;

import io.sirix.node.interfaces.DataRecord;

public interface BufferManager extends AutoCloseable {
  Cache<PageReference, KeyValueLeafPage> getRecordPageCache();

  Cache<PageReference, KeyValueLeafPage> getRecordPageFragmentCache();

  Cache<PageReference, Page> getPageCache();

  Cache<PageReference, HOTLeafPage> getHOTLeafPageCache();

  /**
   * Cache of individual HOT leaf FRAGMENTS, keyed by each fragment's own durable offset — the HOT
   * analogue of {@link #getRecordPageFragmentCache()}.
   *
   * <p>
   * Deliberately separate from {@link #getHOTLeafPageCache()}: that one holds the COMBINED page under
   * a canonical key whose value is the newest fragment's offset, so a fragment stored there would
   * alias a merged page with one of its own sparse inputs. Fragments are immutable once written and
   * are re-read by every commit that copy-on-writes the same leaf while the versioning window still
   * spans them, which is what makes them worth caching.
   * </p>
   */
  Cache<PageReference, HOTLeafPage> getHOTLeafFragmentCache();

  Cache<RevisionRootPageCacheKey, RevisionRootPage> getRevisionRootPageCache();

  /**
   * Memoized HOT point lookups, keyed by committed revision.
   *
   * <p>
   * Not a {@link Cache} like its neighbours because it does not hold pages: it holds the ANSWER to a
   * lookup, so it has neither guards nor a second-level tier, and it needs an admission rule the page
   * caches do not (see {@link HOTLookupCache#MAX_CACHED_NODE_KEYS}).
   * </p>
   */
  HOTLookupCache getHOTLookupCache();

  Cache<NamesCacheKey, Names> getNamesCache();

  Cache<PathSummaryCacheKey, PathSummaryData> getPathSummaryCache();

  /**
   * Verdict bitsets for string predicates over global projection value dictionaries.
   *
   * <p>
   * Resource-scoped rather than executor-scoped by necessity: a verdict costs a sweep of every
   * distinct value, and the query engine builds a NEW executor per execution, so an executor-held
   * cache is measurably never reused. Missing is always safe -- the caller recomputes.
   * </p>
   */
  Cache<GlobalVerdictCacheKey, long[]> getGlobalVerdictCache();

  /**
   * Decoded global-dictionary records, retained across transactions.
   *
   * <p>
   * A read view retains blocks only for its own lifetime and is built per query execution, so
   * without this every execution re-decodes the dictionary material it touches. Missing is safe.
   * </p>
   */
  Cache<GlobalDictionaryRecordCacheKey, DataRecord> getGlobalDictionaryRecordCache();

  /**
   * Dictionaries already warmed, keyed with the dictionary's header key in the node-key slot.
   *
   * <p>
   * On the buffer manager rather than in a static so it inherits
   * {@link #clearCachesForResource(long, long)}: a resource deleted and recreated with the same ids
   * must not find a surviving marker and conclude its dictionaries are warm when the caches holding
   * them were just swept.
   * </p>
   */
  Cache<GlobalDictionaryRecordCacheKey, Boolean> getGlobalDictionaryWarmMarkers();

  void clearAllCaches();

  /**
   * Clear all cached pages for a specific database. CRITICAL: Must be called when a database is
   * closed to prevent cache pollution.
   * 
   * @param databaseId the database ID to clear pages for
   */
  void clearCachesForDatabase(long databaseId);

  /**
   * Clear all cached pages for a specific resource within a database. CRITICAL: Must be called when a
   * resource is closed to prevent cache pollution.
   * 
   * @param databaseId the database ID
   * @param resourceId the resource ID to clear pages for
   */
  void clearCachesForResource(long databaseId, long resourceId);
}
