package io.sirix.service.json.replay;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import java.util.Objects;

/** Unordered final PUT/DELETE set. A node identity occurs at most once in an epoch. */
public final class JsonIdentityDelta {
  private final JsonReplayManifest manifest;
  private final Long2ObjectMap<JsonReplayRecord> puts;
  private final LongSet deletes;

  JsonIdentityDelta(final JsonReplayManifest manifest, final Long2ObjectMap<JsonReplayRecord> puts,
      final LongSet deletes) {
    this.manifest = Objects.requireNonNull(manifest);
    Objects.requireNonNull(puts);
    Objects.requireNonNull(deletes);
    final var putCopy = new Long2ObjectOpenHashMap<JsonReplayRecord>(puts.size());
    for (final var entry : puts.long2ObjectEntrySet()) {
      final var record = Objects.requireNonNull(entry.getValue());
      final long key = entry.getLongKey();
      if (key != record.key() || key > manifest.targetFrontier() || deletes.contains(key)) {
        throw new IllegalArgumentException("Invalid or duplicate replay identity " + key);
      }
      putCopy.put(key, record);
    }
    for (final long key : deletes) {
      if (key <= 0 || key > manifest.baseFrontier()) {
        throw new IllegalArgumentException("Invalid removed replay identity " + key);
      }
    }
    this.puts = Long2ObjectMaps.unmodifiable(putCopy);
    this.deletes = LongSets.unmodifiable(new LongOpenHashSet(deletes));
  }

  public JsonReplayManifest manifest() {
    return manifest;
  }

  public Long2ObjectMap<JsonReplayRecord> puts() {
    return puts;
  }

  public LongSet deletes() {
    return deletes;
  }
}
