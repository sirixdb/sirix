package io.sirix.query.json;

import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.HashType;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;

import java.time.Instant;

/**
 * Resolved resource-creation options for the JSONiq store layer.
 *
 * @param commitMessage optional commit message for the initial commit
 * @param commitTimestamp optional custom commit timestamp for the initial commit
 * @param useTextCompression whether text values are compressed
 * @param buildPathSummary whether a path summary is built
 * @param buildPathStatistics whether per-path value statistics are maintained
 * @param storageType the storage backend
 * @param useDeweyIDs whether DeweyIDs are generated
 * @param hashType the hash type used for integrity hashes
 * @param versioningType the page versioning approach
 * @param numberOfNodesBeforeAutoCommit auto-commit threshold during imports
 * @param storeNodeHistory whether the record-to-revisions index is maintained (store-level setting,
 *        not exposed as a query option)
 * @param validTimeConfig optional valid-time (bitemporal) field configuration; {@code null} if the
 *        resource has no valid-time support
 * @param autoCreateValidTimeIndex whether the valid-time interval index is created automatically
 *        when {@code validTimeConfig} is set (default {@code true})
 */
public record Options(String commitMessage, Instant commitTimestamp, boolean useTextCompression,
    boolean buildPathSummary, boolean buildPathStatistics,
    StorageType storageType, boolean useDeweyIDs, HashType hashType,
    VersioningType versioningType, int numberOfNodesBeforeAutoCommit, boolean storeNodeHistory,
    ValidTimeConfig validTimeConfig, boolean autoCreateValidTimeIndex) {

  /**
   * Whether the valid-time interval index should be auto-created for a resource built from these
   * options: requires both a valid-time configuration and the auto-create flag.
   *
   * @return {@code true} if the interval index should be created automatically
   */
  public boolean shouldAutoCreateValidTimeIndex() {
    return autoCreateValidTimeIndex && validTimeConfig != null;
  }
}
