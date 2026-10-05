package io.sirix.service.json.replay;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/** Test-only reference transition: independently enumerate both complete physical trees. */
public final class JsonReplaySnapshotOracle {
  private JsonReplaySnapshotOracle() {}

  public static JsonIdentityDelta between(final JsonNodeReadOnlyTrx before, final JsonNodeReadOnlyTrx after,
      final int destinationRevision) {
    final var oldRecords = snapshot(before);
    final var newRecords = snapshot(after);
    final var puts = new Long2ObjectOpenHashMap<JsonReplayRecord>();
    final var deletes = new LongOpenHashSet();
    for (final var entry : newRecords.long2ObjectEntrySet()) {
      if (!entry.getValue().equals(oldRecords.remove(entry.getLongKey()))) {
        puts.put(entry.getLongKey(), entry.getValue());
      }
    }
    deletes.addAll(oldRecords.keySet());
    final var config = after.getResourceSession().getResourceConfig();
    final var manifest = new JsonReplayManifest(JsonReplayManifest.VERSION, config.getResource(), config.resourceUuid,
        before.getRevisionNumber(), after.getRevisionNumber(), destinationRevision, before.getMaxNodeKey(),
        after.getMaxNodeKey(), config.areDeweyIDsStored, config.hashType);
    return new JsonIdentityDelta(manifest, puts, deletes);
  }

  public static Long2ObjectOpenHashMap<JsonReplayRecord> snapshot(final JsonNodeReadOnlyTrx reader) {
    final long saved = reader.getNodeKey();
    final var records = new Long2ObjectOpenHashMap<JsonReplayRecord>();
    try {
      reader.moveToDocumentRoot();
      final var axis = new DescendantAxis(reader, IncludeSelf.YES);
      while (axis.hasNext()) {
        records.put(axis.nextLong(), JsonReplayRecord.capture(reader));
      }
      return records;
    } finally {
      reader.moveTo(saved);
    }
  }
}
