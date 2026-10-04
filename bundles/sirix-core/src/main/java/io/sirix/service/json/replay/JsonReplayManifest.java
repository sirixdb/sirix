package io.sirix.service.json.replay;

import io.sirix.access.trx.node.HashType;

import java.nio.file.Path;
import java.util.Objects;

/** Exact committed epochs and allocation contract for the internal identity replay protocol. */
public record JsonReplayManifest(int version, Path sourceResource, int baseRevision, int targetRevision,
    int destinationRevision, long baseFrontier, long targetFrontier, boolean deweyIDs, HashType hashType) {
  public static final int VERSION = 1;

  public JsonReplayManifest {
    Objects.requireNonNull(sourceResource);
    Objects.requireNonNull(hashType);
    sourceResource = sourceResource.toAbsolutePath().normalize();
    if (version != VERSION || baseRevision < 0 || targetRevision <= baseRevision || destinationRevision < 1
        || baseFrontier < 0 || targetFrontier < 0) {
      throw new IllegalArgumentException("Invalid identity replay manifest");
    }
  }

  /** Explicit revision mapping for a copied history suffix; unavailable predecessor metadata is -1. */
  public int mapRevision(final int sourceRevision) {
    if (sourceRevision < 0) {
      return sourceRevision;
    }
    final int mapped = sourceRevision - (targetRevision - destinationRevision);
    return mapped < 0 ? -1 : mapped;
  }
}
