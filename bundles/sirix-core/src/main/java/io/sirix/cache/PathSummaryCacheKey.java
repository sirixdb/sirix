package io.sirix.cache;

/**
 * Composite cache key for PathSummaryCache to support global BufferManager.
 * Includes database ID and resource ID to uniquely identify path summary data
 * across all databases and resources.
 *
 * @param databaseId the unique database ID
 * @param resourceId the unique resource ID within the database
 * @param pathNodeKey the path node key
 */
public record PathSummaryCacheKey(long databaseId, long resourceId, int pathNodeKey) {
}





