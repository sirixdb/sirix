package io.sirix.query.bench;

import java.io.Reader;
import java.util.Random;

/**
 * Streams a JSON array {@code [{record0},{record1},…]} of {@code count} records with ids
 * {@code [startId, startId+count)} on the fly, so a benchmark can feed an arbitrarily large dataset
 * to a {@code JsonReader} without materializing the whole string.
 *
 * <p>Shared by {@link ScaleBenchMain} (one reader for the whole dataset, {@code startId=0}) and
 * {@link ParallelScaleBenchMain} (one reader per shard) so both benchmarks emit the <em>same</em>
 * record shape — the parallel-ingest speedup comparison is only valid if the per-record shred cost
 * matches the query-side benchmark, and a single generator makes that structural rather than a hand
 * maintained invariant across two copies.
 *
 * <p>The RNG is seeded explicitly so each reader is deterministic and independent (no shared
 * {@link Random} across partition writer threads).
 */
final class GeneratedRecordsReader extends Reader {

  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };

  private final long startId;
  private final long count;
  private final Random rng;
  private final StringBuilder line = new StringBuilder(96);
  private long produced = 0;
  private int pos = 0;
  private boolean opened = false;
  private boolean closed = false;

  GeneratedRecordsReader(final long startId, final long count, final long seed) {
    this.startId = startId;
    this.count = count;
    this.rng = new Random(seed);
  }

  private void refill() {
    line.setLength(0);
    pos = 0;
    if (!opened) {
      line.append('[');
      opened = true;
      return;
    }
    if (produced < count) {
      if (produced > 0) {
        line.append(',');
      }
      line.append("{\"id\":").append(startId + produced)
          .append(",\"age\":").append(18 + rng.nextInt(48))
          .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
          .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
          .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
          .append('}');
      produced++;
      return;
    }
    if (!closed) {
      line.append(']');
      closed = true;
    }
  }

  @Override
  public int read(final char[] cbuf, final int off, final int len) {
    if (pos >= line.length()) {
      if (closed) {
        return -1;
      }
      refill();
      if (pos >= line.length()) {
        return -1;
      }
    }
    final int n = Math.min(len, line.length() - pos);
    line.getChars(pos, pos + n, cbuf, off);
    pos += n;
    return n;
  }

  @Override
  public void close() {
    // streaming generator; nothing to release
  }
}
