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
 * @param validTimeConfig optional valid-time (bitemporal) field configuration; {@code null} if the
 *        resource has no valid-time support
 * @param autoCreateValidTimeIndex whether the valid-time interval index is created automatically
 *        when {@code validTimeConfig} is set (default {@code true})
 */
public record Options(String commitMessage, Instant commitTimestamp, boolean useTextCompression,
    boolean buildPathSummary, boolean buildPathStatistics,
    StorageType storageType, boolean useDeweyIDs, HashType hashType,
    VersioningType versioningType, int numberOfNodesBeforeAutoCommit,
    ValidTimeConfig validTimeConfig, boolean autoCreateValidTimeIndex) {
}
