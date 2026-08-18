package io.sirix.query.scan;

import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * MEASUREMENT PROBE, not a regression test (gated on a system property): dumps every projection
 * column segment's VERIFIED RAW BYTES (lightweight-encoded, pipeline-decompressed) from an existing
 * ClickBench corpus to a directory, so offline codecs (zstd/lz4 CLI) can be compared on exactly the
 * payloads the storage layer frames — the CedarDB encoding-vs-compression experiment for this
 * store's footprint tail.
 *
 * <p>
 * Run: {@code -Dsirix.probe.segmentDump=/path/to/db -Dsirix.probe.segmentDump.out=/tmp/out} against
 * a database whose resource carries a persisted projection catalog.
 */
public final class SegmentBytesDumpProbe {

  @Test
  @EnabledIfSystemProperty(named = "sirix.probe.segmentDump", matches = ".+")
  public void dumpSegments() throws Exception {
    final Path dbPath = Path.of(System.getProperty("sirix.probe.segmentDump")).resolve("clickbench");
    final Path out = Path.of(System.getProperty("sirix.probe.segmentDump.out", "/tmp/claude-1000/segdump"));
    Files.createDirectories(out);
    try (final var database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("hits.jn");
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey,
          rtx.getRevisionNumber(), new String[] {"[]"}, new String[] {"AdvEngineID"});
      long segments = 0;
      long bytes = 0;
      if (handle != null && handle.columnStoreOrNull() != null) {
        final ProjectionColumnStore store = handle.columnStoreOrNull();
        final var fetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        for (int col = 0; col < handle.fieldNames().length; col++) {
          if (!store.columnSliceable(col)) {
            continue;
          }
          final byte[][] segs = store.columnBytes(col, fetcher);
          for (int leaf = 0; leaf < segs.length; leaf++) {
            final byte[] s = segs[leaf];
            if (s == null || s.length == 0) {
              continue;
            }
            Files.write(out.resolve("c" + col + "_l" + leaf + ".seg"), s);
            segments++;
            bytes += s.length;
          }
        }
      }
      System.out.println("[segdump] segments=" + segments + " rawBytes=" + bytes + " -> " + out);
    }
  }
}
