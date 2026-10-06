package io.sirix.query.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Immutable store-level resource settings shared by direct creation and collection additions. Each
 * resource gets its own configuration; commit timestamps remain specific to that resource.
 */
record XmlResourceOptions(StorageType storageType, boolean buildPathSummary, boolean buildPathStatistics,
    boolean storeDeweyIds, HashType hashType, VersioningType versioningType, boolean storeNodeHistory) {

  XmlResourceOptions {
    requireNonNull(storageType);
    requireNonNull(hashType);
    requireNonNull(versioningType);
  }

  ResourceConfiguration create(final String resourceName, final @Nullable Instant commitTimestamp) {
    return ResourceConfiguration.newBuilder(resourceName)
                                .useDeweyIDs(storeDeweyIds)
                                .useTextCompression(false)
                                .buildPathSummary(buildPathSummary)
                                .buildPathStatistics(buildPathStatistics)
                                .storageType(storageType)
                                .customCommitTimestamps(commitTimestamp != null)
                                .hashKind(hashType)
                                .versioningApproach(versioningType)
                                .storeNodeHistory(storeNodeHistory)
                                .build();
  }
}
