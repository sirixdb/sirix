package io.sirix.index.aggregate;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.SegmentDictionaryAnchors;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Typed projection cells used identically by initial builds and commit maintenance. */
final class TemporalAggregateColumns {
  private final StorageEngineReader reader;
  private final ProjectionIndexMetadata metadata;
  private final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();

  TemporalAggregateColumns(final StorageEngineReader reader, final ProjectionIndexMetadata metadata) {
    this.reader = reader;
    this.metadata = metadata;
    final SegmentAnchor[] persisted = metadata.segmentAnchors();
    if (persisted != null) {
      for (final SegmentAnchor anchor : persisted) {
        anchors.seal(anchor.segment(), anchor.column(), anchor.headerKey(), anchor.sealedEntryCount());
      }
    }
  }

  static boolean present(final ProjectionIndexRowGroupPage page, final int column, final int row) {
    final long[] bits = page.presenceColumnBits(column);
    return (bits[row >>> 6] & (1L << row)) != 0;
  }

  static void requireRepresentable(final ProjectionIndexRowGroupPage page, final int column) {
    if (page.columnUnrepresentable(column) || page.columnNumericNonIntegral(column)) {
      throw new Unservable();
    }
  }

  static long number(final ProjectionIndexRowGroupPage page, final int column, final int row) {
    requireRepresentable(page, column);
    if (page.columnKind(column) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
      throw new Unservable();
    }
    return page.numericColumn(column)[row];
  }

  byte[] group(final ProjectionIndexRowGroupPage page, final int row, final List<Integer> columns) {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(1 + columns.size() * 9);
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeByte(1);
      for (final int column : columns) {
        requireRepresentable(page, column);
        if (!present(page, column, row)) {
          out.writeByte(0);
          continue;
        }
        switch (page.columnKind(column)) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG -> {
            out.writeByte(1);
            out.writeLong(number(page, column, row));
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
              ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
              ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT -> {
            final byte[] utf8 = Objects.requireNonNull(string(page, column, row)).getBytes(StandardCharsets.UTF_8);
            out.writeByte(2);
            out.writeInt(utf8.length);
            out.write(utf8);
          }
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> {
            out.writeByte(3);
            out.writeBoolean((page.booleanColumnBits(column)[row >>> 6] & (1L << row)) != 0);
          }
          default -> throw new Unservable();
        }
        if (bytes.size() > 2048) {
          throw new Unservable();
        }
      }
      return bytes.toByteArray();
    } catch (final IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Nullable String string(final ProjectionIndexRowGroupPage page, final int column, final int row) {
    requireRepresentable(page, column);
    if (!present(page, column, row)) {
      return null;
    }
    return switch (page.columnKind(column)) {
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
        final int id = page.stringDictIdColumn(column)[row];
        yield new String(page.stringDictionaryEntryBacking(column, id), page.stringDictionaryEntryOffset(column, id),
            page.stringDictionaryEntryLength(column, id), StandardCharsets.UTF_8);
      }
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> Objects.requireNonNull(
          GlobalValueDictionary.value(metadata.valueDictionaryHeaderKey(column),
              Math.toIntExact(page.numericColumn(column)[row]), reader), "Missing aggregate dictionary value");
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT -> {
        final long cell = page.numericColumn(column)[row];
        yield Objects.requireNonNull(GlobalValueDictionary.value(
            anchors.headerKeyOf(ProjectionIndexRowGroupPage.segmentOfCell(cell), column),
            ProjectionIndexRowGroupPage.idOfCell(cell), reader), "Missing aggregate segment dictionary value");
      }
      default -> throw new Unservable();
    };
  }

  /** Unsupported source data declines serving; corruption and missing persisted pages remain errors. */
  static final class Unservable extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private Unservable() {
      super(null, null, false, false);
    }
  }
}
