package io.sirix.io;

import java.nio.file.Path;

/**
 * Composite key for the global {@link com.github.benmanes.caffeine.cache.AsyncCache}
 * of {@link RevisionFileData}. Pairs a resource's path with a revision number so
 * one global cache instance can hold entries for every resource in the JVM —
 * the SaaS pattern (PostgreSQL {@code shared_buffers},
 * InnoDB {@code innodb_buffer_pool_size}). Each Sirix resource accesses the
 * shared cache through {@link PerResourceRevisionFileDataCache}, which translates
 * its {@code int}-keyed API to and from this composite at the boundary.
 */
public record RevisionFileDataCacheKey(Path resourcePath, int revision) {
}
