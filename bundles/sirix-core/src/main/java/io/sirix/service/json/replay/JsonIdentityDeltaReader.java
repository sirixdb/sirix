package io.sirix.service.json.replay;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSets;

import java.util.Objects;

/** Reads committed identity state independently of presentation diff sidecars. */
public final class JsonIdentityDeltaReader {
  private JsonIdentityDeltaReader() {}

  /** Complete initial state, including sparse identities and the reserved allocation frontier. */
  public static JsonIdentityDelta snapshot(final JsonNodeReadOnlyTrx source, final int destinationRevision) {
    Objects.requireNonNull(source);
    requireCommitted(source);
    final var config = source.getResourceSession().getResourceConfig();
    final var manifest = new JsonReplayManifest(JsonReplayManifest.VERSION, config.getResource(), config.resourceUuid,
        0, source.getRevisionNumber(), destinationRevision, 0, source.getMaxNodeKey(), config.areDeweyIDsStored,
        config.hashType);
    final var records = new Long2ObjectOpenHashMap<JsonReplayRecord>();
    final long originalKey = source.getNodeKey();
    try {
      source.moveToDocumentRoot();
      final var axis = new DescendantAxis(source, IncludeSelf.YES);
      while (axis.hasNext()) {
        final long key = axis.nextLong();
        records.put(key, JsonReplayRecord.capture(source));
      }
      return new JsonIdentityDelta(manifest, records, LongSets.emptySet());
    } finally {
      source.moveTo(originalKey);
    }
  }

  /** Authoritative transition between two consecutive committed revisions of the same resource. */
  public static JsonIdentityDelta between(final JsonNodeReadOnlyTrx base, final JsonNodeReadOnlyTrx target,
      final int destinationRevision) {
    Objects.requireNonNull(base);
    Objects.requireNonNull(target);
    requireCommitted(base);
    requireCommitted(target);
    final var baseConfig = base.getResourceSession().getResourceConfig();
    final var config = target.getResourceSession().getResourceConfig();
    if (!baseConfig.resourceUuid.equals(config.resourceUuid)
        || !baseConfig.getResource()
                      .toAbsolutePath()
                      .normalize()
                      .equals(config.getResource().toAbsolutePath().normalize())
        || target.getRevisionNumber() != base.getRevisionNumber() + 1) {
      throw new IllegalArgumentException("Replay delta requires consecutive revisions of the same source resource");
    }
    final var manifest = new JsonReplayManifest(JsonReplayManifest.VERSION, config.getResource(), config.resourceUuid,
        base.getRevisionNumber(), target.getRevisionNumber(), destinationRevision, base.getMaxNodeKey(),
        target.getMaxNodeKey(), config.areDeweyIDsStored, config.hashType);
    final long oldPosition = base.getNodeKey();
    final long newPosition = target.getNodeKey();
    try {
      return new JsonDocumentDeltaWalk(base, target).read(manifest);
    } finally {
      base.moveTo(oldPosition);
      target.moveTo(newPosition);
    }
  }

  private static void requireCommitted(final JsonNodeReadOnlyTrx source) {
    if (source.getStorageEngineReader().hasTrxIntentLog() || source.getRevisionNumber() < 1
        || source.getRevisionNumber() > source.getResourceSession().getMostRecentRevisionNumber()) {
      throw new IllegalArgumentException("Replay reads only immutable committed source revisions");
    }
  }
}
