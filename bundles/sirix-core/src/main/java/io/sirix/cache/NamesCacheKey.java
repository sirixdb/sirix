package io.sirix.cache;

/**
 * Composite cache key for NamesCache to support global BufferManager.
 * Includes database ID and resource ID to uniquely identify names data
 * across all databases and resources.
 *
 * @param databaseId the unique database ID
 * @param resourceId the unique resource ID within the database
 * @param revision the revision number
 * @param indexNumber the index number
 */
public record NamesCacheKey(long databaseId, long resourceId, int revision, int indexNumber) {
}
