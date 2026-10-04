package io.sirix.service.json.replay;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSets;

import java.util.Objects;

/** Reads committed identity state independently of presentation diff sidecars. */
public final class JsonIdentityDeltaReader {
  private JsonIdentityDeltaReader() {
  }

  /** Complete initial state, including sparse identities and the reserved allocation frontier. */
  public static JsonIdentityDelta snapshot(final JsonNodeReadOnlyTrx source, final int destinationRevision) {
    Objects.requireNonNull(source);
    if (source.getStorageEngineReader().hasTrxIntentLog()
        || source.getRevisionNumber() > source.getResourceSession().getMostRecentRevisionNumber()) {
      throw new IllegalArgumentException("Replay reads only immutable committed source revisions");
    }
    final var config = source.getResourceSession().getResourceConfig();
    final var manifest = new JsonReplayManifest(JsonReplayManifest.VERSION, config.getResource(), config.resourceUuid,
        0, source.getRevisionNumber(), destinationRevision, 0, source.getMaxNodeKey(),
        config.areDeweyIDsStored, config.hashType);
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
}
