package io.sirix.query.bench.clickbench;

import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.PrePassDictionaryBuilder;
import io.sirix.node.ValueDictionaryEntryNode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Pre-import runner for {@code -Dsirix.import.prepassRunner}: commits the resource-wide
 * rank-ordered dictionaries named by {@code -Dsirix.projection.globalDict.prepassValues} (format
 * {@code Column=valuesFile,...}) into the freshly created EMPTY resource, then publishes the
 * anchors via {@code sirix.projection.globalDict.prebuilt} so the load-time projection build binds
 * them at its first streaming epoch instead of running the election.
 *
 * <p>
 * This is the incremental fresh-build route: dictionaries first, then ONE load that shreds the
 * document and builds the projection against them. The alternative — deriving the projection by a
 * second full walk of the finished resource — re-decodes every record single-threaded against a
 * file the page cache cannot hold, and measured ~3x the whole load's wall time at 100M.
 * </p>
 */
public final class ClickBenchLoadPrepassHook {
  private ClickBenchLoadPrepassHook() {
    throw new AssertionError("no instances");
  }

  /**
   * Invoked reflectively by the store's pre-import seam.
   *
   * @param database the freshly created database
   * @param resourceName the freshly created, still-empty resource
   */
  public static void run(final Database<?> database, final String resourceName) throws Exception {
    final String spec = System.getProperty("sirix.projection.globalDict.prepassValues");
    if (spec == null || spec.isEmpty()) {
      return;
    }
    @SuppressWarnings("unchecked")
    final Database<JsonResourceSession> db = (Database<JsonResourceSession>) database;
    final List<String> columns = ClickBenchProjection.PROJECTED_COLUMNS;
    final StringBuilder prebuilt = new StringBuilder();
    final long started = System.nanoTime();
    try (JsonResourceSession session = db.beginResourceSession(resourceName)) {
      for (final String pair : spec.split(",")) {
        final int eq = pair.indexOf('=');
        final String name = pair.substring(0, eq).trim();
        final int column = columns.indexOf(name);
        if (column < 0) {
          throw new IllegalArgumentException("prepassValues names unknown projected column '" + name + "'");
        }
        final List<byte[]> values = loadAscending(pair.substring(eq + 1).trim());
        final long headerKey;
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          headerKey = PrePassDictionaryBuilder.build(wtx, column, values);
        }
        if (prebuilt.length() > 0) {
          prebuilt.append(',');
        }
        prebuilt.append(column).append(':').append(headerKey);
        System.out.printf("[prepass-hook] %-14s column=%d entries=%d headerKey=%d%n", name, column, values.size(),
            headerKey);
      }
    }
    System.setProperty("sirix.projection.globalDict.prebuilt", prebuilt.toString());
    System.out.printf("[prepass-hook] sirix.projection.globalDict.prebuilt=%s (%.1fs)%n", prebuilt,
        (System.nanoTime() - started) / 1e9);
  }

  /**
   * Distinct values plus the empty string the per-leaf dictionaries carry, in UTF-16 collation order.
   */
  private static List<byte[]> loadAscending(final String file) throws Exception {
    final List<byte[]> values = new ArrayList<>();
    values.add(new byte[0]);
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 22))) {
      in.readLong();
      in.readLong();
      final int count = in.readInt();
      for (int i = 0; i < count; i++) {
        final byte[] value = new byte[in.readInt()];
        in.readFully(value);
        values.add(value);
      }
    }
    values.sort((l, r) -> ValueDictionaryEntryNode.compareUtf16Range(l, 0, l.length, r, 0, r.length));
    return values;
  }
}
