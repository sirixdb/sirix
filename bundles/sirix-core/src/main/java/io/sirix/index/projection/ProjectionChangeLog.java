package io.sirix.index.projection;

import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogSlab.Entry;
import io.sirix.index.changelog.ChangeLogSlab.Row;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Captures rows while projection maintenance owns their decoded columns, before they are replaced. */
public final class ProjectionChangeLog implements AutoCloseable {
  private final StorageEngineWriter writer;
  private final ProjectionIndexMetadata metadata;
  private final int projection;
  private final ChangeLogStorage storage;
  private final LongSet dirty;
  private final Long2ObjectOpenHashMap<long[]> changed;
  private final Long2ObjectOpenHashMap<Entry> entries;
  private final byte[] kinds;
  private final List<Long2ObjectOpenHashMap<String>> dictionaries;
  private final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();

  ProjectionChangeLog(final StorageEngineWriter writer, final int projection, final ProjectionIndexMetadata metadata,
      final LongSet dirty, final Long2ObjectOpenHashMap<long[]> changed,
      final Long2ObjectOpenHashMap<Entry> entries) {
    this.writer = writer;
    this.projection = projection;
    this.metadata = metadata;
    this.dirty = dirty;
    this.changed = changed;
    this.entries = entries;
    storage = new ChangeLogStorage(writer, projection);
    kinds = kinds(metadata.columnKinds());
    dictionaries = new ArrayList<>(kinds.length);
    for (int column = 0; column < kinds.length; column++) {
      dictionaries.add(new Long2ObjectOpenHashMap<>());
    }
    if (metadata.segmentAnchors() != null) {
      for (final var anchor : metadata.segmentAnchors()) {
        anchors.seal(anchor.segment(), anchor.column(), anchor.headerKey(), anchor.sealedEntryCount());
      }
    }
    if (!storage.initialized()) {
      throw new IllegalStateException("Missing projection change-log baseline");
    }
  }

  /** Bootstrap only sparse row-local flags. Ordinary rows remain solely in the projection columns. */
  public static void initialize(final StorageEngineWriter writer, final IndexDef projection,
      final PathSummaryReader paths, final JsonNodeReadOnlyTrx transaction) {
    final ProjectionIndexMetadata metadata = Objects.requireNonNull(ProjectionIndexMetadata.parse(
        ProjectionIndexHOTStorage.readMetadataBlob(writer, projection.getID())));
    try (ChangeLogStorage storage = new ChangeLogStorage(writer, projection.getID())) {
      if (storage.initialized()) {
        return;
      }
      storage.initialize(writer.getRevisionNumber(), kinds(metadata.columnKinds()));
      final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(projection, paths);
      final boolean direct = projection.getProjectionRootPath().toString().equals("/[]")
          && projection.getProjectionFields().stream().allMatch(path -> path.toString().matches("/\\[\\]/[^/]+"));
      final long saved = transaction.getNodeKey();
      try {
        ProjectionAggregateRows.visit(writer, projection.getID(), metadata, null, (record, page, row) -> {
          boolean poisoned = false;
          boolean present = false;
          for (int column = 0; column < page.getColumnCount(); column++) {
            poisoned |= page.columnUnrepresentable(column) || page.columnNumericNonIntegral(column)
                || (page.columnKind(column) == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE && !page.columnPureDoubleSource(column));
            present |= (page.presenceColumnBits(column)[row >>> 6] & (1L << row)) != 0;
          }
          // A direct object field proves an object only for direct array-element declarations.
          // Other roots retain one initial kind check; maintenance never opens a historical reader.
          final boolean object = direct && present || writer.getRecord(record, IndexType.DOCUMENT, -1).getKind() == NodeKind.OBJECT;
          if (poisoned || !object) {
            if (!extractor.extractInto(transaction, record)) {
              throw new IllegalStateException("Missing change-log baseline record");
            }
            storage.provenance(record, extractor.captureChangeRow(object, null, null));
          }
        });
      } finally {
        transaction.moveTo(saved);
      }
    }
  }

  static byte[] kinds(final byte[] source) {
    final byte[] result = new byte[source.length];
    for (int column = 0; column < source.length; column++) {
      result[column] = switch (source[column]) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG -> ChangeLogSlab.LONG;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE -> ChangeLogSlab.DOUBLE;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> ChangeLogSlab.BOOLEAN;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT -> ChangeLogSlab.STRING;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> ChangeLogSlab.STRING_SET;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP -> ChangeLogSlab.TIMESTAMP;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_DATE -> ChangeLogSlab.DATE;
        default -> throw new IllegalArgumentException("Unsupported change-log column kind");
      };
    }
    return result;
  }

  boolean contains(final long record) { return dirty.contains(record); }

  void before(final ProjectionIndexRowGroupPage page) {
    for (int row = 0; row < page.getRowCount(); row++) {
      final long record = page.recordKeys()[row];
      if (!dirty.contains(record) || entries.containsKey(record)) {
        continue;
      }
      final byte[] provenance = storage.provenance(record);
      final Row captured = new Row(provenance == null || provenance[0] == 1, kinds.length);
      for (int column = 0; column < kinds.length; column++) {
        captured.flags[column] = (byte) ((page.presenceColumnBits(column)[row >>> 6] & (1L << row)) == 0 ? 0 : 1);
        if (provenance != null) {
          captured.flags[column] |= provenance[column + 1];
        }
        if (!captured.present(column) || (captured.flags[column] & ChangeLogSlab.UNREPRESENTABLE) != 0) {
          continue;
        }
        switch (kinds[column]) {
          case ChangeLogSlab.STRING -> captured.text[column] = string(page, column, row);
          case ChangeLogSlab.STRING_SET -> {
            int offset = 0;
            for (int prior = 0; prior < row; prior++) {
              offset += page.stringSetCountColumn(column)[prior];
            }
            final String[] values = new String[page.stringSetCountColumn(column)[row]];
            for (int i = 0; i < values.length; i++) {
              values[i] = localString(page, column, page.stringSetIdColumn(column)[offset + i]);
            }
            captured.text[column] = values;
          }
          case ChangeLogSlab.BOOLEAN -> captured.numbers[column] =
              (page.booleanColumnBits(column)[row >>> 6] & (1L << row)) == 0 ? 0 : 1;
          default -> captured.numbers[column] = page.numericColumn(column)[row];
        }
      }
      before(record, captured);
    }
  }

  void before(final long[] records, final ProjectionColumnStore.ColumnSlice[] slices) {
    for (int row = 0; row < records.length; row++) {
      final long record = records[row];
      if (!dirty.contains(record) || entries.containsKey(record)) {
        continue;
      }
      final byte[] provenance = storage.provenance(record);
      final Row captured = new Row(provenance == null || provenance[0] == 1, kinds.length);
      for (int column = 0; column < kinds.length; column++) {
        final ProjectionColumnStore.ColumnSlice slice = Objects.requireNonNull(slices[column]);
        captured.flags[column] = (byte) ((slice.presenceWords()[row >>> 6] & (1L << row)) == 0 ? 0 : 1);
        if (provenance != null) {
          captured.flags[column] |= provenance[column + 1];
        }
        if (!captured.present(column) || (captured.flags[column] & ChangeLogSlab.UNREPRESENTABLE) != 0) {
          continue;
        }
        switch (kinds[column]) {
          case ChangeLogSlab.STRING -> {
            if (slice.hasStringDictIds()) {
              captured.text[column] = slice.dictString(slice.stringDictIds()[row]);
            } else {
              captured.text[column] = globalString(metadata.columnKinds()[column], column, slice.numericValues()[row]);
            }
          }
          case ChangeLogSlab.STRING_SET -> {
            int offset = 0;
            for (int prior = 0; prior < row; prior++) {
              offset += slice.setCounts()[prior];
            }
            final String[] values = new String[slice.setCounts()[row]];
            for (int i = 0; i < values.length; i++) {
              values[i] = slice.dictString(slice.stringDictIds()[offset + i]);
            }
            captured.text[column] = values;
          }
          case ChangeLogSlab.BOOLEAN -> captured.numbers[column] =
              (slice.boolWords()[row >>> 6] & (1L << row)) == 0 ? 0 : 1;
          default -> captured.numbers[column] = slice.numericValues()[row];
        }
      }
      before(record, captured);
    }
  }

  private String string(final ProjectionIndexRowGroupPage page, final int column, final int row) {
    if (page.columnKind(column) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      return localString(page, column, page.stringDictIdColumn(column)[row]);
    }
    return globalString(page.columnKind(column), column, page.numericColumn(column)[row]);
  }

  private String globalString(final byte kind, final int column, final long cell) {
    return dictionaries.get(column).computeIfAbsent(cell, key -> {
      final boolean segment = kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT;
      final long header = segment ? anchors.headerKeyOf(ProjectionIndexRowGroupPage.segmentOfCell(key), column)
          : metadata.valueDictionaryHeaderKey(column);
      final int id = segment ? ProjectionIndexRowGroupPage.idOfCell(key) : Math.toIntExact(key);
      return Objects.requireNonNull(GlobalValueDictionary.value(header, id, writer));
    });
  }

  private static String localString(final ProjectionIndexRowGroupPage page, final int column, final int id) {
    return new String(page.stringDictionaryEntryBacking(column, id), page.stringDictionaryEntryOffset(column, id),
        page.stringDictionaryEntryLength(column, id), StandardCharsets.UTF_8);
  }

  private void before(final long record, final @Nullable Row row) {
    if (!entries.containsKey(record)) {
      long[] mask = changed.get(record);
      if (mask == null) {
        mask = new long[(kinds.length + 63) >>> 6];
        Arrays.fill(mask, -1L);
      }
      entries.put(record, new Entry(record, mask.clone(), row, row));
    }
  }

  void after(final long record, final ProjectionIndexRowExtractor extractor, final boolean object,
      final long @Nullable [] selected) {
    if (!dirty.contains(record)) {
      return;
    }
    before(record, null);
    final Entry prior = entries.get(record);
    final Row next = extractor.captureChangeRow(object, prior.after(), selected);
    final long[] mask = prior.changed();
    final long[] updates = changed.get(record);
    for (int word = 0; word < mask.length; word++) {
      mask[word] |= updates == null ? -1L : updates[word];
    }
    entries.put(record, new Entry(record, mask, prior.before(), next));
    if (next.exceptional() || prior.after() != null && prior.after().exceptional()) {
      storage.provenance(record, next);
    }
  }

  void removed(final long record) {
    final Entry prior = entries.get(record);
    if (prior == null) {
      before(record, null);
    } else {
      Arrays.fill(prior.changed(), -1L);
      entries.put(record, new Entry(record, prior.changed(), prior.before(), null));
      if (prior.after() != null && prior.after().exceptional()) {
        storage.provenance(record, null);
      }
    }
  }

  static ChangeLogSlab slab(final StorageEngineWriter writer, final int projection, final byte[] kinds,
      final Long2ObjectOpenHashMap<Entry> entries) {
    final long[] keys = entries.keySet().toLongArray();
    Arrays.sort(keys);
    final List<Entry> ordered = new ArrayList<>(keys.length);
    for (final long key : keys) {
      ordered.add(entries.get(key));
    }
    return new ChangeLogSlab(projection, writer.getRevisionNumber(), writer.getRevisionToRepresent(), kinds, ordered);
  }

  @Override
  public void close() { storage.close(); }
}
