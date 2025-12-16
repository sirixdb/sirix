package io.sirix.cache;

/**
 * Composite cache key for RevisionRootPage to support global BufferManager.
 * Includes database ID and resource ID to uniquely identify revision root pages
 * across all databases and resources.
 *
 * @param databaseId the unique database ID
 * @param resourceId the unique resource ID within the database
 * @param revision the revision number
 */
public record RevisionRootPageCacheKey(long databaseId, long resourceId, int revision) {
}





