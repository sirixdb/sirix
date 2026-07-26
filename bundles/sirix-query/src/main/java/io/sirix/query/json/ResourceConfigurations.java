package io.sirix.query.json;

import io.sirix.access.ResourceConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * Single translation point from resolved store-layer {@link Options} to a
 * {@link ResourceConfiguration}. Every JSONiq resource-creation path (fresh collection, add to
 * existing collection, parallel shredder) must go through this mapper so a new option cannot be
 * wired into one path and silently ignored in another.
 */
final class ResourceConfigurations {

  private ResourceConfigurations() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Build (but do not create) the {@link ResourceConfiguration} for {@code resourceName} from the
   * resolved {@code options}.
   *
   * @param resourceName the resource name
   * @param options the resolved options
   * @return the resource configuration
   */
  static ResourceConfiguration create(final String resourceName, final Options options) {
    requireNonNull(resourceName, "resourceName must not be null");
    requireNonNull(options, "options must not be null");
    return ResourceConfiguration.newBuilder(resourceName)
                                .useTextCompression(options.useTextCompression())
                                .buildPathSummary(options.buildPathSummary())
                                .buildPathStatistics(options.buildPathStatistics())
                                .customCommitTimestamps(options.commitTimestamp() != null)
                                .storageType(options.storageType())
                                .useDeweyIDs(options.useDeweyIDs())
                                .hashKind(options.hashType())
                                .versioningApproach(options.versioningType())
                                .storeNodeHistory(options.storeNodeHistory())
                                .validTimeConfig(options.validTimeConfig())
                                .build();
  }
}
