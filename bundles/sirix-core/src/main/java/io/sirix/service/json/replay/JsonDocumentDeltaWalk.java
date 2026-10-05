package io.sirix.service.json.replay;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/** Exact identity-sensitive document records from paired immutable document tries. */
final class JsonDocumentDeltaWalk {
  private final JsonNodeReadOnlyTrx before;
  private final JsonNodeReadOnlyTrx after;
  private final Long2ObjectOpenHashMap<JsonReplayRecord> puts = new Long2ObjectOpenHashMap<>();
  private final LongOpenHashSet deletes = new LongOpenHashSet();

  JsonDocumentDeltaWalk(final JsonNodeReadOnlyTrx before, final JsonNodeReadOnlyTrx after) {
    this.before = before;
    this.after = after;
  }

  JsonIdentityDelta read(final JsonReplayManifest manifest) {
    final var oldReader = before.getStorageEngineReader();
    final var newReader = after.getStorageEngineReader();
    final var oldRoot = oldReader.getActualRevisionRootPage();
    final var newRoot = newReader.getActualRevisionRootPage();
    new JsonReplayPageWalk(oldReader, newReader, IndexType.DOCUMENT, -1, this::compare).read(
        oldRoot.getIndirectDocumentIndexPageReference(), oldRoot.getCurrentMaxLevelOfDocumentIndexIndirectPages(),
        newRoot.getIndirectDocumentIndexPageReference(), newRoot.getCurrentMaxLevelOfDocumentIndexIndirectPages());
    return new JsonIdentityDelta(manifest, puts, deletes);
  }

  private void compare(final long key, final boolean oldExists, final boolean newExists) {
    final JsonReplayRecord oldRecord = oldExists && before.moveTo(key)
        ? JsonReplayRecord.capture(before)
        : null;
    final JsonReplayRecord newRecord = newExists && after.moveTo(key)
        ? JsonReplayRecord.capture(after)
        : null;
    if (newRecord == null) {
      if (oldRecord != null) {
        deletes.add(key);
      }
    } else if (!newRecord.equals(oldRecord)) {
      puts.put(key, newRecord);
    }
  }
}
