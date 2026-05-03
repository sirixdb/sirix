/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.io;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.file.FileStorage;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.io.iouring.IOUringStorage;
import io.sirix.io.memorymapped.MMStorage;
import io.sirix.io.ram.RAMStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.util.Optional;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Specific backend types are specified in this enum.
 *
 * <p>
 * External storage providers can be registered via the {@link StorageProvider} SPI. Use
 * {@link #fromString(String)} to resolve provider names, which checks both built-in types and
 * ServiceLoader-discovered providers.
 *
 * @author Johannes Lichtenberger
 */
public enum StorageType {
  /**
   * In memory backend.
   */
  IN_MEMORY {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      return new RAMStorage(resourceConf);
    }
  },

  /**
   * {@link RandomAccessFile} backend.
   */
  FILE {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      final AsyncCache<Integer, RevisionFileData> cache = getIntegerRevisionFileDataAsyncCache(resourceConf);
      final RevisionIndexHolder revisionIndexHolder = getRevisionIndexHolder(resourceConf);
      final var storage = new FileStorage(resourceConf, cache, revisionIndexHolder);
      storage.loadRevisionFileDataIntoMemory(cache);
      storage.loadRevisionIndex(revisionIndexHolder);
      return storage;
    }
  },

  /**
   * FileChannel backend.
   */
  FILE_CHANNEL {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      final AsyncCache<Integer, RevisionFileData> cache = getIntegerRevisionFileDataAsyncCache(resourceConf);
      final RevisionIndexHolder revisionIndexHolder = getRevisionIndexHolder(resourceConf);
      final var storage = new FileChannelStorage(resourceConf, cache, revisionIndexHolder);
      storage.loadRevisionFileDataIntoMemory(cache);
      storage.loadRevisionIndex(revisionIndexHolder);
      return storage;
    }
  },

  /**
   * Memory mapped backend.
   */
  MEMORY_MAPPED {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      final AsyncCache<Integer, RevisionFileData> cache = getIntegerRevisionFileDataAsyncCache(resourceConf);
      final RevisionIndexHolder revisionIndexHolder = getRevisionIndexHolder(resourceConf);
      final var storage = new MMStorage(resourceConf, cache, revisionIndexHolder);
      storage.loadRevisionFileDataIntoMemory(cache);
      storage.loadRevisionIndex(revisionIndexHolder);
      return storage;
    }
  },

  IO_URING {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      final AsyncCache<Integer, RevisionFileData> cache = getIntegerRevisionFileDataAsyncCache(resourceConf);
      final RevisionIndexHolder revisionIndexHolder = getRevisionIndexHolder(resourceConf);
      final var storage = new IOUringStorage(resourceConf, cache, revisionIndexHolder);
      storage.loadRevisionFileDataIntoMemory(cache);
      storage.loadRevisionIndex(revisionIndexHolder);
      return storage;
    }
  };

  /** System property to override the global RevisionFileData cache cap. */
  public static final String REVISION_FILE_DATA_CACHE_SIZE_PROPERTY = "sirix.revision.file.data.cache.size";

  /**
   * Default cap for the <em>global</em> {@code RevisionFileData} cache. Each entry maps
   * a {@code (resourcePath, revision)} pair to its on-disk (offset, timestamp) tuple; a
   * cache miss is a single 16-byte read from the file's revision index, so the cache is
   * a pure optimization for repeated point-in-time queries — not a correctness
   * requirement.
   *
   * <p>Without any upper bound this cache grew linearly in the number of committed
   * revisions (one entry per commit, never evicted), retaining ~2 KB of heap per commit.
   * Confirmed by a 30-minute soak: 77,153 commits → +77k {@link RevisionFileData}
   * records, +77k {@link java.time.Instant}, +77k {@link java.util.concurrent.CompletableFuture}
   * (the {@code AsyncCache} wrapper), +154k Caffeine map nodes — exactly one
   * never-evicted entry per commit.
   *
   * <p>The cache is <em>global</em> across all resources in the JVM. This is the
   * SaaS-friendly pattern PostgreSQL ({@code shared_buffers}) and InnoDB
   * ({@code innodb_buffer_pool_size}) follow: total memory is bounded by a single
   * operator dial regardless of how many resources are open. Caffeine's LRU
   * naturally redistributes the budget toward whichever resource is currently busy.
   * Each resource accesses the global cache through
   * {@link PerResourceRevisionFileDataCache}, a thin wrapper that translates the
   * resource's {@code Integer}-keyed API to and from {@link RevisionFileDataCacheKey}
   * composites at the boundary.
   *
   * <p>1,000,000 entries is the default — at ~200 bytes per Caffeine entry the
   * worst-case ceiling is ~200 MB regardless of resource count, well within typical
   * heap allocations. Override via
   * {@code -D}{@value #REVISION_FILE_DATA_CACHE_SIZE_PROPERTY}{@code =M}.
   */
  public static final long DEFAULT_REVISION_FILE_DATA_CACHE_MAX_SIZE = 1_000_000L;

  static long revisionFileDataCacheMaxSize() {
    final String prop = System.getProperty(REVISION_FILE_DATA_CACHE_SIZE_PROPERTY);
    if (prop == null || prop.isEmpty()) {
      return DEFAULT_REVISION_FILE_DATA_CACHE_MAX_SIZE;
    }
    try {
      final long parsed = Long.parseLong(prop.trim());
      return parsed > 0L ? parsed : DEFAULT_REVISION_FILE_DATA_CACHE_MAX_SIZE;
    } catch (final NumberFormatException ignored) {
      return DEFAULT_REVISION_FILE_DATA_CACHE_MAX_SIZE;
    }
  }

  /**
   * One global {@link AsyncCache} backs every per-resource {@link RevisionFileData}
   * lookup in this JVM. Keyed by {@link RevisionFileDataCacheKey} {@code (resourcePath,
   * revision)} so a single Caffeine instance is responsible for total memory; each
   * resource's {@link AsyncCache} surface is provided by {@link PerResourceRevisionFileDataCache},
   * a thin per-resource view that translates Integer keys to the composite at the
   * boundary. The {@code maximumSize(N)} cap on this cache is the operator's single
   * memory dial — same shape as PostgreSQL's {@code shared_buffers} or InnoDB's
   * {@code innodb_buffer_pool_size}.
   */
  static final AsyncCache<RevisionFileDataCacheKey, RevisionFileData> GLOBAL_REVISION_FILE_DATA_CACHE =
      Caffeine.newBuilder().maximumSize(revisionFileDataCacheMaxSize()).buildAsync();

  /**
   * Per-resource view registry. Each resource path maps to its own
   * {@link PerResourceRevisionFileDataCache}, all of which delegate storage to
   * {@link #GLOBAL_REVISION_FILE_DATA_CACHE}. Kept as a public field for backwards
   * compatibility with callers that hold references to the per-resource view.
   */
  public static final ConcurrentMap<Path, AsyncCache<Integer, RevisionFileData>> CACHE_REPOSITORY =
      new ConcurrentHashMap<>();

  /**
   * Repository for RevisionIndexHolder instances, keyed by resource path. Used for fast
   * timestamp-based revision lookups.
   */
  public static final ConcurrentMap<Path, RevisionIndexHolder> REVISION_INDEX_REPOSITORY = new ConcurrentHashMap<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageType.class);

  /**
   * Parse a storage type from string.
   *
   * <p>
   * First checks built-in types, then falls back to ServiceLoader-discovered providers.
   *
   * @param storageType the storage type name (case-insensitive)
   * @return the storage type
   * @throws IllegalArgumentException if type not found in built-in types or providers
   */
  public static StorageType fromString(String storageType) {
    // First check built-in types
    for (final var type : values()) {
      if (type.name().equalsIgnoreCase(storageType)) {
        return type;
      }
    }

    // Check if there's an external provider with this name
    if (StorageProviders.isAvailable(storageType)) {
      LOGGER.info("Storage type '{}' resolved to external provider", storageType);
      // Return a marker type - actual creation happens via getStorageWithProviders
      return FILE_CHANNEL; // Default fallback, but getStorageWithProviders handles it
    }

    throw new IllegalArgumentException("No storage type or provider with name '" + storageType + "' found. "
        + "Available types: " + java.util.Arrays.toString(values()) + ", Available providers: "
        + StorageProviders.getAvailableProviderNames());
  }

  /**
   * Check if a storage type name refers to an external provider.
   *
   * @param storageType the storage type name
   * @return true if this is handled by an external provider
   */
  public static boolean isExternalProvider(String storageType) {
    // Not a built-in type?
    for (final var type : values()) {
      if (type.name().equalsIgnoreCase(storageType)) {
        return false;
      }
    }
    // Check external providers
    return StorageProviders.isAvailable(storageType);
  }

  /**
   * Get an instance of the storage backend.
   *
   * @param resourceConf {@link ResourceConfiguration} reference
   * @return instance of a storage backend specified within the {@link ResourceConfiguration}
   * @throws SirixIOException if an IO-error occured
   */
  public abstract IOStorage getInstance(final ResourceConfiguration resourceConf);

  /**
   * Factory method to retrieve suitable {@link IOStorage} instances based upon the suitable
   * {@link ResourceConfiguration}.
   *
   * <p>
   * This method first checks for external providers (via ServiceLoader) that might override or
   * enhance the built-in storage type. This allows enterprise features like FFM-based io_uring to
   * transparently replace the default implementation.
   *
   * @param resourceConf determining the storage
   * @return an implementation of the {@link IOStorage} interface
   * @throws SirixIOException if an IO-exception occurs
   * @throws NullPointerException if {@code resourceConf} is {@code null}
   */
  public static IOStorage getStorage(final ResourceConfiguration resourceConf) {
    final String typeName = resourceConf.storageType.name();

    // Check if an external provider wants to handle this storage type
    // Enterprise providers can override built-in types with higher-performance implementations
    Optional<StorageProvider> provider = StorageProviders.get(typeName);
    if (provider.isPresent() && provider.get().isAvailable()) {
      StorageProvider p = provider.get();
      if (p.isEnterprise()) {
        LOGGER.info("Using enterprise storage provider for {}: {} (priority={})", typeName,
            p.getClass().getSimpleName(), p.getPriority());
      }
      return p.createStorage(resourceConf);
    }

    // Fall back to built-in implementation
    return resourceConf.storageType.getInstance(resourceConf);
  }

  private static AsyncCache<Integer, RevisionFileData> getIntegerRevisionFileDataAsyncCache(
      ResourceConfiguration resourceConf) {
    final var resourcePath = resourceConf.resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                                      .resolve(IOStorage.FILENAME);
    return StorageType.CACHE_REPOSITORY.computeIfAbsent(resourcePath,
        path -> new PerResourceRevisionFileDataCache(path, GLOBAL_REVISION_FILE_DATA_CACHE));
  }

  /**
   * Get or create the RevisionIndexHolder for a resource.
   * 
   * @param resourceConf the resource configuration
   * @return the RevisionIndexHolder for this resource
   */
  public static RevisionIndexHolder getRevisionIndexHolder(ResourceConfiguration resourceConf) {
    final var resourcePath = resourceConf.resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                                      .resolve(IOStorage.FILENAME);
    return StorageType.REVISION_INDEX_REPOSITORY.computeIfAbsent(resourcePath, path -> new RevisionIndexHolder());
  }
}
