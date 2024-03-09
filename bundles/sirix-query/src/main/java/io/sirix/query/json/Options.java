package io.sirix.query.json;

import io.sirix.access.trx.node.HashType;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;

import java.time.Instant;

public record Options(String commitMessage, Instant commitTimestamp, boolean useTextCompression,
                      boolean buildPathSummary, StorageType storageType, boolean useDeweyIDs, HashType hashType, VersioningType versioningType, int numberOfNodesBeforeAutoCommit) {
}
