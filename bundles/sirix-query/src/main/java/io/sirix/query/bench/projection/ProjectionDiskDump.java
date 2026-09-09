/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.projection;

import io.sirix.access.Databases;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.RowGroupDescriptor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Full-accounting on-disk size report for a resource's projection index.
 *
 * <p>
 * A bench utility, not a serving mechanism: it opens any database read-only, finds the projection
 * covering a field (or the resource's first projection definition when none is named) and sums
 * every byte the index occupies from the row-group descriptors — the authority for each segment's
 * stored length. It exists because the storage plan's per-column B/row figures were previously read
 * off a dump that counted only KEYS, BODY, DICT and BLOOM, and therefore under-reported the index
 * by the DICT_HASHES segments, the descriptors themselves and the per-segment page framing.
 *
 * <p>
 * What is counted, per leaf and per column:
 * <ul>
 * <li>every segment the descriptor names — KEYS, and per column BODY, DICT, SET_COUNTS,
 * STRING_BLOOM and DICT_HASHES — at its recorded {@code byteLen};</li>
 * <li>the descriptor blob itself, at its serialized length, plus its container marker;</li>
 * <li>the per-segment storage framing: one slot discriminator byte for a segment stored inline in
 * its HOT slot, and for a referenced segment the discriminator plus the
 * {@link io.sirix.page.OverflowPage} envelope it costs on the wire.</li>
 * </ul>
 *
 * <p>
 * What is NOT counted, and why the total is therefore a floor: the HOT leaf pages that hold the
 * segment slots (their own headers, slot keys and directories), the fence chunks, and any further
 * compression the storage layer's byte-handler pipeline applies to a written page. The framing
 * figures are per-segment constants, printed in the report so every number can be re-derived.
 *
 * <p>
 * Usage: {@code ProjectionDiskDump <database dir> <resource> [<field>] [<rows>]}. With no field,
 * the first projection definition of the resource is used; with no row count, the exact row total
 * from the descriptors is used.
 */
public final class ProjectionDiskDump {

  /**
   * Payloads at or below this size are stored inline in their HOT slot rather than in an
   * {@link io.sirix.page.OverflowPage}. Mirrors
   * {@code ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES}, which is package-private; the value
   * is printed in the report so the split can be audited.
   */
  private static final int INLINE_SEGMENT_MAX_BYTES = 512;

  /**
   * A segment slot's leading discriminator byte ({@code 0x00} inline, {@code 0x01} referenced). Paid
   * once per segment, whichever way the segment is stored.
   */
  private static final int SEGMENT_SLOT_DISCRIMINATOR_BYTES = 1;

  /**
   * The blob container's marker in front of a descriptor: magic (4) + version (1) + length (4) +
   * XXH3-64 content hash (8). Paid once per descriptor.
   */
  private static final int BLOB_MARKER_BYTES = 4 + 1 + 4 + 8;

  /**
   * On-wire envelope of the one {@link io.sirix.page.OverflowPage} a referenced payload costs: the
   * writer's 4-byte page-length prefix, the 1-byte page-kind id, the 2-byte version + flags envelope
   * and the 4-byte data length. The payload bytes themselves are already counted as the segment's
   * {@code byteLen}.
   */
  private static final int OVERFLOW_PAGE_ENVELOPE_BYTES = 4 + 1 + 2 + 4;

  /** Segment kinds reported per column, in print order. */
  private static final int KIND_BODY = 0;
  private static final int KIND_DICT = 1;
  private static final int KIND_DICT_HASHES = 2;
  private static final int KIND_BLOOM = 3;
  private static final int KIND_SET_COUNTS = 4;
  private static final int KIND_COUNT = 5;

  private ProjectionDiskDump() {
    throw new AssertionError();
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: ProjectionDiskDump <database dir> <resource> [<field>] [<rows>]");
      System.exit(2);
      return;
    }
    final Path databasePath = Path.of(args[0]);
    final String resource = args[1];
    final String field = args.length > 2 && !args[2].isEmpty()
        ? args[2]
        : null;
    final long rowsOverride = args.length > 3 && !args[3].isEmpty()
        ? Long.parseLong(args[3])
        : -1L;

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(resource)) {
      final int revision = session.getMostRecentRevisionNumber();
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final List<IndexDef> definitions = projectionDefinitions(session, revision);
      if (definitions.isEmpty()) {
        System.out.println("no projection index defined on " + resourceKey + " at revision " + revision);
        return;
      }
      System.out.printf("resource=%s revision=%d projection definitions=%d%n", resourceKey, revision,
          definitions.size());
      for (final IndexDef definition : definitions) {
        System.out.printf("  def #%d root=%s fields=%d%n", definition.getID(), definition.getProjectionRootPath(),
            definition.getProjectionFields().size());
      }

      final ProjectionIndexRegistry.Handle handle = resolveHandle(session, resourceKey, revision, definitions, field);
      if (handle == null) {
        System.out.println("no projection handle covers " + (field == null
            ? "the resource's first definition"
            : "field '" + field + '\'') + " at revision " + revision);
        return;
      }
      final ProjectionColumnStore store = handle.columnStoreOrNull();
      if (store == null) {
        System.out.println("the covering handle carries no column store (payload-materialised handle)");
        return;
      }
      report(handle, store, rowsOverride);
    }
  }

  /** Every projection definition on the resource at {@code revision}, in definition-id order. */
  private static List<IndexDef> projectionDefinitions(final JsonResourceSession session, final int revision) {
    final IndexController<?, ?> controller = session.getRtxIndexController(revision);
    final List<IndexDef> definitions = new ArrayList<>();
    for (final IndexDef definition : controller.getIndexes().getIndexDefs()) {
      if (definition.isProjectionIndex()) {
        definitions.add(definition);
      }
    }
    definitions.sort((left, right) -> Integer.compare(left.getID(), right.getID()));
    return definitions;
  }

  /**
   * Resolve the covering handle through the ordinary catalog path — the same selection a query takes,
   * so the dump reports what a query would actually read. Each definition's declared root is tried in
   * turn; a named field additionally constrains coverage.
   */
  private static ProjectionIndexRegistry.Handle resolveHandle(final JsonResourceSession session,
      final String resourceKey, final int revision, final List<IndexDef> definitions, final String field) {
    final String[] requiredFields = field == null
        ? new String[0]
        : new String[] {field};
    for (final IndexDef definition : definitions) {
      final String[] sourcePath = sourcePathSegments(definition.getProjectionRootPath().toString());
      if (sourcePath.length == 0) {
        continue;
      }
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.lookupCovering(session, resourceKey, revision, sourcePath, requiredFields);
      if (handle != null && handle.columnStoreOrNull() != null) {
        return handle;
      }
    }
    return null;
  }

  /**
   * Split a declared root path ({@code "/[]"}, {@code "/b/[]"}) into the source-path segments the
   * catalog canonicalises back into that same string.
   */
  private static String[] sourcePathSegments(final String declaredRootPath) {
    final List<String> segments = new ArrayList<>();
    for (final String segment : declaredRootPath.split("/")) {
      if (!segment.isEmpty()) {
        segments.add(segment);
      }
    }
    return segments.toArray(new String[0]);
  }

  private static void report(final ProjectionIndexRegistry.Handle handle, final ProjectionColumnStore store,
      final long rowsOverride) {
    final int columns = store.columnCount();
    final int leaves = store.leafCount();
    final String[] fieldNames = handle.fieldNames();

    final long[][] bytesByColumnAndKind = new long[columns][KIND_COUNT];
    final long[] segmentsByColumn = new long[columns];
    long keysBytes = 0;
    long keysSegments = 0;
    long descriptorBytes = 0;
    long framingBytes = 0;
    long inlineSegments = 0;
    long referencedSegments = 0;
    long rowsFromDescriptors = 0;

    final int keysSegmentId = ProjectionIndexColumnSegmentCodec.keysColumnSegmentId();
    for (int leaf = 0; leaf < leaves; leaf++) {
      final byte[] descriptor = store.leafDescriptor(leaf);
      if (descriptor == null) {
        continue;
      }
      rowsFromDescriptors += RowGroupDescriptor.rowCount(descriptor);

      // The descriptor is a blob, not a column segment: a fixed marker plus the payload, inline in
      // the slot when small and in an OverflowPage otherwise.
      descriptorBytes += descriptor.length;
      framingBytes += BLOB_MARKER_BYTES;
      if (descriptor.length > INLINE_SEGMENT_MAX_BYTES) {
        framingBytes += OVERFLOW_PAGE_ENVELOPE_BYTES;
        referencedSegments++;
      } else {
        inlineSegments++;
      }

      final int keysLength = segmentByteLength(descriptor, keysSegmentId);
      if (keysLength >= 0) {
        keysBytes += keysLength;
        keysSegments++;
        framingBytes += segmentFramingBytes(keysLength);
        if (keysLength > INLINE_SEGMENT_MAX_BYTES) {
          referencedSegments++;
        } else {
          inlineSegments++;
        }
      }

      for (int column = 0; column < columns; column++) {
        final int[] segmentIds = {ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column),
            ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(column),
            ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(column),
            ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(column),
            ProjectionIndexColumnSegmentCodec.setCountsColumnSegmentId(column)};
        for (int kind = 0; kind < KIND_COUNT; kind++) {
          final int length = segmentByteLength(descriptor, segmentIds[kind]);
          if (length < 0) {
            continue;
          }
          bytesByColumnAndKind[column][kind] += length;
          segmentsByColumn[column]++;
          framingBytes += segmentFramingBytes(length);
          if (length > INLINE_SEGMENT_MAX_BYTES) {
            referencedSegments++;
          } else {
            inlineSegments++;
          }
        }
      }
    }

    final long rows = rowsOverride > 0
        ? rowsOverride
        : rowsFromDescriptors;
    final double perRowDivisor = rows > 0
        ? rows
        : 1.0;

    long columnBytesTotal = 0;
    final long[] kindTotals = new long[KIND_COUNT];
    for (int column = 0; column < columns; column++) {
      for (int kind = 0; kind < KIND_COUNT; kind++) {
        columnBytesTotal += bytesByColumnAndKind[column][kind];
        kindTotals[kind] += bytesByColumnAndKind[column][kind];
      }
    }
    final long total = columnBytesTotal + keysBytes + descriptorBytes + framingBytes;

    System.out.printf("%nprojection: def #%d root=%s columns=%d leaves=%,d rows=%,d%s%n", handle.defId(),
        handle.rootPath(), columns, leaves, rows, rowsOverride > 0
            ? " (given; descriptors say " + String.format("%,d", rowsFromDescriptors) + ')'
            : " (from the descriptors)");
    System.out.printf("%-28s %-14s %14s %14s %12s %12s %12s %12s %10s%n", "column", "kind", "body", "dict", "hashes",
        "bloom", "setCounts", "segments", "B/row");
    for (int column = 0; column < columns; column++) {
      long columnTotal = 0;
      for (int kind = 0; kind < KIND_COUNT; kind++) {
        columnTotal += bytesByColumnAndKind[column][kind];
      }
      System.out.printf("%-28s %-14s %,14d %,14d %,12d %,12d %,12d %,12d %10.2f%n", columnName(fieldNames, column),
          columnKindName(store.columnKind(column)), bytesByColumnAndKind[column][KIND_BODY],
          bytesByColumnAndKind[column][KIND_DICT], bytesByColumnAndKind[column][KIND_DICT_HASHES],
          bytesByColumnAndKind[column][KIND_BLOOM], bytesByColumnAndKind[column][KIND_SET_COUNTS],
          segmentsByColumn[column], columnTotal / perRowDivisor);
    }
    System.out.printf("%-28s %-14s %,14d %,14d %,12d %,12d %,12d %,12d %10.2f%n", "ALL COLUMNS", "",
        kindTotals[KIND_BODY], kindTotals[KIND_DICT], kindTotals[KIND_DICT_HASHES], kindTotals[KIND_BLOOM],
        kindTotals[KIND_SET_COUNTS], sum(segmentsByColumn), columnBytesTotal / perRowDivisor);

    System.out.println();
    printLine("column segments", columnBytesTotal, total, perRowDivisor);
    printLine("keys segments (record keys + order labels)", keysBytes, total, perRowDivisor);
    printLine("row-group descriptors", descriptorBytes, total, perRowDivisor);
    printLine("segment + descriptor framing", framingBytes, total, perRowDivisor);
    printLine("TOTAL", total, total, perRowDivisor);

    System.out.printf(
        "%nsegments: %,d keys, %,d column, %,d descriptors; %,d stored inline (<= %,d B), "
            + "%,d referenced through an OverflowPage%n",
        keysSegments, sum(segmentsByColumn), (long) leaves, inlineSegments, INLINE_SEGMENT_MAX_BYTES,
        referencedSegments);
    System.out.printf(
        "framing constants: %d B slot discriminator per segment, %d B blob marker per descriptor, "
            + "%d B OverflowPage envelope per referenced payload%n",
        SEGMENT_SLOT_DISCRIMINATOR_BYTES, BLOB_MARKER_BYTES, OVERFLOW_PAGE_ENVELOPE_BYTES);
    System.out.println("NOT counted (so the total is a floor): the HOT leaf pages carrying the segment slots, "
        + "the fence chunks, and any storage-layer page compression applied on top of these bytes.");
  }

  private static void printLine(final String label, final long bytes, final long total, final double perRowDivisor) {
    System.out.printf("%-44s %,16d B  %8.2f MiB  %8.2f B/row  %6.1f%%%n", label, bytes, bytes / (1024.0 * 1024.0),
        bytes / perRowDivisor, total == 0
            ? 0.0
            : 100.0 * bytes / total);
  }

  /**
   * The segment's stored length from the descriptor, or {@code -1} when the leaf has no such segment.
   */
  private static int segmentByteLength(final byte[] descriptor, final int columnSegmentId) {
    final int entry = RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId);
    return entry < 0
        ? -1
        : RowGroupDescriptor.entryByteLen(descriptor, entry);
  }

  /**
   * Slot discriminator, plus the OverflowPage envelope when the payload is too large to sit inline.
   */
  private static int segmentFramingBytes(final int payloadLength) {
    return payloadLength > INLINE_SEGMENT_MAX_BYTES
        ? SEGMENT_SLOT_DISCRIMINATOR_BYTES + OVERFLOW_PAGE_ENVELOPE_BYTES
        : SEGMENT_SLOT_DISCRIMINATOR_BYTES;
  }

  private static long sum(final long[] values) {
    long total = 0;
    for (final long value : values) {
      total += value;
    }
    return total;
  }

  private static String columnName(final String[] fieldNames, final int column) {
    return fieldNames != null && column < fieldNames.length
        ? fieldNames[column]
        : "#" + column;
  }

  private static String columnKindName(final byte columnKind) {
    return switch (columnKind) {
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG -> "numericLong";
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> "boolean";
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> "stringDict";
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE -> "numericDouble";
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> "stringSet";
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> "stringGlobal";
      default -> "kind#" + columnKind;
    };
  }
}
