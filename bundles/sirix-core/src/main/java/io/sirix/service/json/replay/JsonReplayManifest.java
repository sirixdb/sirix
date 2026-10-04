package io.sirix.service.json.replay;

import io.sirix.access.trx.node.HashType;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/** Exact committed epochs and allocation contract for the internal identity replay protocol. */
public record JsonReplayManifest(int version, Path sourceResource, UUID sourceIdentity, int baseRevision, int targetRevision,
    int destinationRevision, long baseFrontier, long targetFrontier, boolean deweyIDs, HashType hashType) {
  public static final int VERSION = 1;

  public JsonReplayManifest {
    Objects.requireNonNull(sourceResource);
    Objects.requireNonNull(sourceIdentity);
    Objects.requireNonNull(hashType);
    sourceResource = sourceResource.toAbsolutePath().normalize();
    if (version != VERSION || baseRevision < 0 || targetRevision <= baseRevision || destinationRevision < 1
        || baseFrontier < 0 || targetFrontier < 0) {
      throw new IllegalArgumentException("Invalid identity replay manifest");
    }
  }

  /** A predecessor outside the copied suffix is unavailable, never an empty destination revision. */
  public int mapPreviousRevision(final int sourceRevision) {
    if (sourceRevision < 0) {
      return sourceRevision;
    }
    final int mapped = sourceRevision - (targetRevision - destinationRevision);
    return mapped > 0 ? mapped : -1;
  }

  /** Older live state first becomes available at the initial snapshot boundary of a copied suffix. */
  public int mapLastModifiedRevision(final int sourceRevision) {
    if (sourceRevision < 0) {
      return sourceRevision;
    }
    return Math.max(1, sourceRevision - (targetRevision - destinationRevision));
  }
}
