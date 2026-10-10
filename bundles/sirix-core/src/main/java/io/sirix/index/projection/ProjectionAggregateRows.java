package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathSummaryReader;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/** Bounded row access for maintained aggregate indexes, sharing projection membership semantics. */
public final class ProjectionAggregateRows {
  @FunctionalInterface
  public interface Consumer {
    void accept(long record, @Nullable ProjectionIndexRowGroupPage page, int row);
  }

  private ProjectionAggregateRows() {}

  /** Recover row-local provenance only when a projection leaf carries a conservative poison flag. */
  public static ProjectionIndexRowGroupPage extract(final IndexDef definition, final PathSummaryReader paths,
      final JsonNodeReadOnlyTrx transaction, final long record) {
    final long saved = transaction.getNodeKey();
    try {
      final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(definition, paths);
      if (!extractor.extractInto(transaction, record)) {
        throw new IllegalStateException("Aggregate source record disappeared");
      }
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(extractor.columnKinds());
      if (!extractor.appendTo(page, record, false, new byte[] {1})) {
        throw new IllegalStateException("Cannot extract aggregate source row");
      }
      return page;
    } finally {
      transaction.moveTo(saved);
    }
  }

  public static void visit(final StorageEngineReader reader, final int projection,
      final ProjectionIndexMetadata metadata, final @Nullable LongSet dirty, final Consumer consumer) {
    Objects.requireNonNull(reader);
    Objects.requireNonNull(metadata);
    Objects.requireNonNull(consumer);
    if (dirty == null) {
      final int[] slots = ProjectionIndexFences.readPhysicalOrder(reader, projection, metadata.rowGroupCount());
      for (final int slot : slots) {
        final ProjectionIndexRowGroupPage page = readPage(reader, projection, metadata, slot);
        for (int row = 0; row < page.getRowCount(); row++) {
          consumer.accept(page.recordKeys()[row], page, row);
        }
      }
      return;
    }
    final ProjectionPersistedRecordLookup lookup = new ProjectionPersistedRecordLookup(reader, projection, metadata);
    final Int2ObjectOpenHashMap<ProjectionIndexRowGroupPage> pages = new Int2ObjectOpenHashMap<>();
    for (final LongIterator keys = dirty.iterator(); keys.hasNext();) {
      final long key = keys.nextLong();
      final long location = lookup.find(key);
      if (location == ProjectionPersistedRecordLookup.ABSENT) {
        consumer.accept(key, null, -1);
        continue;
      }
      final int slot = ProjectionPersistedRecordLookup.slot(location);
      final ProjectionIndexRowGroupPage page =
          pages.computeIfAbsent(slot, id -> readPage(reader, projection, metadata, id));
      consumer.accept(key, page, ProjectionPersistedRecordLookup.row(location));
    }
  }

  private static ProjectionIndexRowGroupPage readPage(final StorageEngineReader reader, final int projection,
      final ProjectionIndexMetadata metadata, final int slot) {
    return ProjectionIndexRowGroupPage.deserialize(Objects.requireNonNull(
        ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, projection, metadata.slotLayout(), slot),
        "Missing projection row group"));
  }
}
