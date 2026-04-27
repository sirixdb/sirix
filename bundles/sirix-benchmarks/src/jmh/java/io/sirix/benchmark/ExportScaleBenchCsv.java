/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Export the same synthesised records that {@link BrackitQueryOnSirixScaleMain}
 * feeds into its Sirix shred — but as a CSV file — so the identical workload
 * can be loaded into DuckDB / CedarDB / other columnar engines for a baseline
 * comparison.
 *
 * <p>Uses {@code Random(42)} and the exact {@code DEPTS}/{@code CITIES} arrays
 * plus {@code 18 + rng.nextInt(48)} age distribution so that aggregate sums
 * match bit-for-bit across engines.
 *
 * <p>Usage:
 * <pre>
 *   ./gradlew :sirix-benchmarks:exportScaleCsv \
 *       -PcsvArgs="10000000 /tmp/records-10m.csv"
 * </pre>
 *
 * <p>In DuckDB the generated file can be loaded with:
 * <pre>
 *   CREATE TABLE t AS SELECT * FROM read_csv_auto('/tmp/records-10m.csv');
 *   -- or for even tighter compression:
 *   COPY (SELECT * FROM read_csv_auto('/tmp/records-10m.csv'))
 *     TO '/tmp/records-10m.parquet' (FORMAT 'parquet', COMPRESSION 'zstd');
 * </pre>
 *
 * <p>SQL equivalents for the 9 Sirix benchmark queries live in
 * {@code src/jmh/resources/scale-bench-queries.sql}.
 */
public final class ExportScaleBenchCsv {

  private static final String[] DEPTS =
      {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};
  private static final String[] CITIES =
      {"NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL"};

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println(
          "Usage: ExportScaleBenchCsv <recordCount> <outPath>");
      System.exit(1);
    }
    final long recordCount = Long.parseLong(args[0]);
    final Path outPath = Path.of(args[1]);
    final Random rng = new Random(42);

    final long start = System.nanoTime();
    try (BufferedWriter w = Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
      w.write("id,age,dept,city,active\n");
      final StringBuilder line = new StringBuilder(96);
      for (long i = 0; i < recordCount; i++) {
        final int age = 18 + rng.nextInt(48);
        final String dept = DEPTS[rng.nextInt(DEPTS.length)];
        final String city = CITIES[rng.nextInt(CITIES.length)];
        final boolean active = rng.nextBoolean();
        line.setLength(0);
        line.append(i).append(',')
            .append(age).append(',')
            .append(dept).append(',')
            .append(city).append(',')
            .append(active ? "true" : "false")
            .append('\n');
        w.write(line.toString());
      }
    }
    final long ms = (System.nanoTime() - start) / 1_000_000L;
    System.out.printf("# Wrote %,d records to %s (%,d ms, %.0f records/sec)%n",
        recordCount, outPath, ms, recordCount * 1000.0 / Math.max(1, ms));
  }
}
