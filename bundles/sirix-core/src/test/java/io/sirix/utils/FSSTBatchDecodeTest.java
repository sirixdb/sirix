/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.utils;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link FSSTCompressor#batchDecode} stages each row's page bytes in one array local to the call.
 * The class used to keep a synchronized free list of 64 KiB arrays for it, two per CPU, outside
 * every budget; a batch now allocates what its rows need and keeps nothing.
 *
 * <p>
 * Nothing here measures time: the allocation assertions are bytes allocated by this thread.
 */
final class FSSTBatchDecodeTest {

  private static final int ROWS = 64;

  /** A batch of rows laid out the way record pages hold them: headerless payloads at offsets. */
  private static final class Batch {
    final List<byte[]> payloads = new ArrayList<>();
    final List<String> expected = new ArrayList<>();
    final List<Boolean> compressed = new ArrayList<>();

    void add(final String value, final byte[][] symbols) {
      final byte[] raw = value.getBytes(StandardCharsets.UTF_8);
      final byte[] headered = FSSTCompressor.encodeOrNull(raw, 0, raw.length, symbols);
      if (headered == null) {
        payloads.add(raw);
        compressed.add(Boolean.FALSE);
      } else {
        assertEquals(FSSTCompressor.HEADER_COMPRESSED, headered[0]);
        final byte[] payload = new byte[headered.length - 1];
        System.arraycopy(headered, 1, payload, 0, payload.length);
        payloads.add(payload);
        compressed.add(Boolean.TRUE);
      }
      expected.add(value);
    }

    /** Decodes every row of the selection across two pages; rows alternate between the pages. */
    String[] decode(final byte[][] symbols, final int[] selection, final String[] output) {
      final int rows = payloads.size();
      final MemorySegment[] pages = new MemorySegment[2];
      final int[] pageIndices = new int[rows];
      final int[] offsets = new int[rows];
      final int[] lengths = new int[rows];
      final boolean[] isCompressed = new boolean[rows];
      final int[] used = new int[2];
      for (int row = 0; row < rows; row++) {
        pageIndices[row] = row & 1;
        // A gap before every payload: an offset the decoder ignores would read the neighbour's bytes.
        offsets[row] = used[row & 1] + 3;
        lengths[row] = payloads.get(row).length;
        isCompressed[row] = compressed.get(row);
        used[row & 1] = offsets[row] + lengths[row];
      }
      for (int page = 0; page < 2; page++) {
        pages[page] = Arena.ofAuto().allocate(Math.max(1, used[page]));
        pages[page].fill((byte) 0x7E);
      }
      for (int row = 0; row < rows; row++) {
        MemorySegment.copy(payloads.get(row), 0, pages[row & 1], ValueLayout.JAVA_BYTE, offsets[row], lengths[row]);
      }
      FSSTCompressor.batchDecode(pages, pageIndices, offsets, lengths, isCompressed, selection, selection.length,
          new byte[][][] {symbols, symbols}, output);
      return output;
    }
  }

  private static byte[][] symbols() {
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      samples.add(sentence(i).getBytes(StandardCharsets.UTF_8));
    }
    final byte[][] symbols = FSSTCompressor.parseSymbolTable(FSSTCompressor.buildSymbolTable(samples));
    assertTrue(symbols.length > 0, "the samples must train a table, or no row is stored compressed");
    return symbols;
  }

  private static String sentence(final int i) {
    return "{\"name\":\"Person" + i + "\",\"age\":" + (20 + i % 50) + ",\"city\":\"City" + (i % 17) + "\"}";
  }

  private static int[] allRows(final int rows) {
    final int[] selection = new int[rows];
    for (int row = 0; row < rows; row++) {
      selection[row] = row;
    }
    return selection;
  }

  @Test
  void compressedAndRawRowsOfSeveralPagesDecodeAlsoBeyondTheInitialStaging() {
    final byte[][] symbols = symbols();
    final Batch batch = new Batch();
    for (int i = 0; i < ROWS; i++) {
      batch.add(sentence(1_000 + i), symbols);
    }
    batch.add("", symbols);
    batch.add("zq", symbols);
    // Values several times the initial staging array, one of each kind, then a short one again: the
    // array grows in the middle of a batch and the rows after the growth still decode. The compressed
    // one is long enough to exceed the array after compression, at eight bytes a symbol at best.
    final StringBuilder repeated = new StringBuilder();
    for (int i = 0; repeated.length() < 200_000; i++) {
      repeated.append(sentence(i));
    }
    batch.add(repeated.toString(), symbols);
    final StringBuilder unseen = new StringBuilder();
    for (int i = 0; unseen.length() < 30_000; i++) {
      unseen.append((char) ('À' + i % 300));
    }
    batch.add(unseen.toString(), symbols);
    batch.add(sentence(7), symbols);
    assertTrue(batch.compressed.contains(Boolean.TRUE), "the fixture stores no row compressed");
    assertTrue(batch.compressed.contains(Boolean.FALSE), "the fixture stores no row raw");
    final int rows = batch.payloads.size();
    assertTrue(batch.payloads.get(rows - 3).length > 8 * 1024 && batch.compressed.get(rows - 3),
        "the long repeated value must be stored compressed and exceed the initial staging");
    assertTrue(batch.payloads.get(rows - 2).length > 8 * 1024 && !batch.compressed.get(rows - 2),
        "the long unseen value must be stored raw and exceed the initial staging");

    final String[] output = batch.decode(symbols, allRows(rows), new String[rows]);
    for (int row = 0; row < rows; row++) {
      assertEquals(batch.expected.get(row), output[row], "row " + row);
    }
  }

  @Test
  void onlySelectedRowsAreDecodedAndMaterializedRowsAreLeftAlone() {
    final byte[][] symbols = symbols();
    final Batch batch = new Batch();
    for (int i = 0; i < 8; i++) {
      batch.add(sentence(2_000 + i), symbols);
    }
    final String[] output = new String[8];
    final String earlier = "materialized earlier";
    output[4] = earlier;
    batch.decode(symbols, new int[] {1, 4, 6}, output);
    assertEquals(batch.expected.get(1), output[1]);
    assertSame(earlier, output[4], "a row that already has its string is not decoded again");
    assertEquals(batch.expected.get(6), output[6]);
    for (final int unselected : new int[] {0, 2, 3, 5, 7}) {
      assertNull(output[unselected], "row " + unselected + " is not in the selection");
    }
  }

  @Test
  void aBatchOfShortRowsAllocatesItsStringsAndASmallStagingArray() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());

    // Raw rows only (an empty table), so the decoder's own output arrays do not enter the figure.
    final byte[][] noSymbols = new byte[0][];
    final Batch batch = new Batch();
    for (int i = 0; i < ROWS; i++) {
      batch.add("row-" + (100 + i), noSymbols);
    }
    final int rows = batch.payloads.size();
    final MemorySegment page = Arena.ofAuto().allocate(rows * 16L);
    final MemorySegment[] pages = {page};
    final int[] pageIndices = new int[rows];
    final int[] offsets = new int[rows];
    final int[] lengths = new int[rows];
    final boolean[] isCompressed = new boolean[rows];
    for (int row = 0; row < rows; row++) {
      offsets[row] = row * 16;
      lengths[row] = batch.payloads.get(row).length;
      MemorySegment.copy(batch.payloads.get(row), 0, page, ValueLayout.JAVA_BYTE, offsets[row], lengths[row]);
    }
    final int[] selection = allRows(rows);
    final byte[][][] symbolsByPage = {noSymbols};
    final String[] output = new String[rows];

    final long thread = Thread.currentThread().threadId();
    // The least of several batches: what a batch allocates it allocates every time, while the
    // virtual machine's own one-off events on this thread land in some batch at most.
    long leastAllocated = Long.MAX_VALUE;
    for (int round = 0; round < 8; round++) {
      Arrays.fill(output, null);
      final long start = bean.getThreadAllocatedBytes(thread);
      FSSTCompressor.batchDecode(pages, pageIndices, offsets, lengths, isCompressed, selection, rows, symbolsByPage,
          output);
      leastAllocated = Math.min(leastAllocated, bean.getThreadAllocatedBytes(thread) - start);
    }
    for (int row = 0; row < rows; row++) {
      assertEquals(batch.expected.get(row), output[row], "row " + row);
    }
    // 64 strings of seven characters are 64 x (24 + 24) bytes; the staging array is 4 KiB. The free
    // list this replaced handed a batch a 64 KiB array whenever the list was empty.
    assertTrue(leastAllocated >= 4 * 1024, "the batch staged its rows without an array: " + leastAllocated);
    assertTrue(leastAllocated <= 12 * 1024, "a batch of " + rows + " short rows allocated " + leastAllocated
        + " bytes: its staging array is no longer " + "sized for the batch");
  }
}
