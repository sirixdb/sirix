/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Standalone microbench for {@link ProjectionIndexScan}. Generates a
 * synthetic stream of serialised {@link ProjectionIndexLeafPage}s and
 * times the conjunctive-count kernel across several predicate shapes.
 *
 * <p>The reference target is CedarDB's ~3 ns/record on equivalent
 * analytical filters. We want this kernel to be in that ballpark before
 * investing in query-plan integration, which is otherwise a lot of
 * plumbing for a non-structural gain.
 *
 * <p>Run with {@code ./gradlew :sirix-core:test ...} or directly via
 * {@code java -cp … io.sirix.index.projection.ProjectionIndexScanMicrobench}.
 */
public final class ProjectionIndexScanMicrobench {

  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  private static final String[] DEPTS = {
      "Eng", "Sales", "Ops", "Finance", "HR", "Marketing", "Legal", "Support",
      "Research", "Platform", "Data", "Security", "Design", "QA", "Partners",
      "Customer", "Mobile", "Cloud", "Compliance", "Analytics"
  };

  public static void main(final String[] args) {
    final int totalRows = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
    final int leaves = (totalRows + ProjectionIndexLeafPage.MAX_ROWS - 1) / ProjectionIndexLeafPage.MAX_ROWS;
    System.out.println("== ProjectionIndexScan microbench ==");
    System.out.printf("rows=%,d  leaves=%,d  (leaf capacity=%d)%n",
        totalRows, leaves, ProjectionIndexLeafPage.MAX_ROWS);

    final List<byte[]> payloads = generate(leaves, totalRows);
    final long payloadBytes = payloads.stream().mapToLong(b -> b.length).sum();
    System.out.printf("payload=%,d bytes (%.1f bytes/row)%n",
        payloadBytes, payloadBytes / (double) totalRows);

    // Predicate shapes
    final var numGt = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L)
    };
    final var boolTrue = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)
    };
    final var strEqHit = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    final var strEqMiss = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "NotInDict".getBytes(StandardCharsets.UTF_8))
    };
    final var threeWay = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };

    run("countRows (header-only)", payloads, totalRows, () -> ProjectionIndexScan.countRows(payloads));
    run("numeric GT single predicate", payloads, totalRows,
        () -> ProjectionIndexScan.conjunctiveCount(payloads, numGt));
    run("boolean EQ true", payloads, totalRows,
        () -> ProjectionIndexScan.conjunctiveCount(payloads, boolTrue));
    run("string EQ hit  ('Eng')", payloads, totalRows,
        () -> ProjectionIndexScan.conjunctiveCount(payloads, strEqHit));
    run("string EQ miss (absent literal)", payloads, totalRows,
        () -> ProjectionIndexScan.conjunctiveCount(payloads, strEqMiss));
    run("3-way AND (num & bool & str)", payloads, totalRows,
        () -> ProjectionIndexScan.conjunctiveCount(payloads, threeWay));
  }

  private static void run(final String label, final List<byte[]> payloads, final int totalRows,
      final CountOp op) {
    // Warmup — trigger JIT on the kernels. The black-hole side-effect reads
    // the result into a stable field so HotSpot can't DCE the call.
    long blackhole = 0L;
    for (int i = 0; i < 10; i++) blackhole ^= op.run();

    final int measureIters = 30;
    final long[] timingsNs = new long[measureIters];
    for (int i = 0; i < measureIters; i++) {
      final long t0 = System.nanoTime();
      blackhole ^= op.run();
      timingsNs[i] = System.nanoTime() - t0;
    }
    Arrays.sort(timingsNs);
    final long medianNs = timingsNs[measureIters >>> 1];
    final long bestNs = timingsNs[0];
    final double medianNsPerRow = medianNs / (double) totalRows;
    final double bestNsPerRow = bestNs / (double) totalRows;
    System.out.printf("  %-36s median=%6.2f ns/row  best=%6.2f ns/row  blackhole=%d%n",
        label, medianNsPerRow, bestNsPerRow, blackhole & 1L);
  }

  @FunctionalInterface
  private interface CountOp {
    long run();
  }

  private static List<byte[]> generate(final int leafCount, final int totalRows) {
    final Random rng = new Random(0xC0FFEEL);
    final List<byte[]> out = new ArrayList<>(leafCount);
    long recordKey = 0L;
    int remaining = totalRows;
    for (int l = 0; l < leafCount && remaining > 0; l++) {
      final int rows = Math.min(ProjectionIndexLeafPage.MAX_ROWS, remaining);
      final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
      final long[] nums = new long[3];
      final boolean[] bools = new boolean[3];
      final String[] strs = new String[3];
      for (int r = 0; r < rows; r++) {
        nums[0] = 20 + rng.nextInt(60);                 // age 20..79
        bools[1] = rng.nextInt(100) < 55;               // active ~55% true
        strs[2] = DEPTS[rng.nextInt(DEPTS.length)];     // dept (dense dict)
        page.appendRow(recordKey++, nums, bools, strs);
      }
      out.add(page.serialize());
      remaining -= rows;
    }
    return out;
  }

  private ProjectionIndexScanMicrobench() {
  }
}
