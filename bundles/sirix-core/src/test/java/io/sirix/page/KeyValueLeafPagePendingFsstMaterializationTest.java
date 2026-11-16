/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.settings.Constants;
import io.sirix.utils.FSSTCompressor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Materializing pending records outside the commit makes the commit's inline-or-carrier decision:
 * string records are encoded against the page's bound table first, and only what still does not fit
 * becomes a canonical raw carrier. A page bound only by id refuses to judge raw strings.
 */
final class KeyValueLeafPagePendingFsstMaterializationTest {
  private static final long PAGE_KEY = 7L;
  private static final long TABLE_ID = 3L;
  private static final String TOKEN = "https://sirix.io/records/common-prefix/";
  private static final ResourceConfiguration CONFIG = new ResourceConfiguration.Builder("pending-fsst").build();

  @Test
  void aPendingStringIsEncodedAgainstTheBoundTableBeforeItsSizeIsJudged() {
    final byte[] table = table();
    final byte[] raw = (TOKEN.repeat(40) + "fits-once-encoded").getBytes(StandardCharsets.UTF_8);
    assertTrue(raw.length > PageConstants.MAX_RECORD_SIZE, "raw value must exceed the inline ceiling");
    final KeyValueLeafPage page = boundPage(table);
    try {
      page.setRecord(string(5, raw, false));
      assertEquals(1, page.materializePendingRecords(CONFIG));
      assertTrue(page.getReferencesMap().isEmpty(), "a value that fits once encoded stays inline");
      assertNotNull(page.getSlot(5));
      assertNull(page.getRecord(5), "materialization consumes every pending record");
    } finally {
      page.close();
    }
  }

  @Test
  void aPendingStringThatNeverFitsBecomesARawCarrier() {
    final byte[] raw = incompressible(PageConstants.MAX_RECORD_SIZE + 400);
    final KeyValueLeafPage page = boundPage(table());
    try {
      page.setRecord(string(9, raw, false));
      assertEquals(0, page.materializePendingRecords(CONFIG));
      assertTrue(page.getReferencesMap().containsKey(nodeKey(9)), "a value that never fits takes a carrier");
    } finally {
      page.close();
    }
  }

  @Test
  void aPageWithoutPendingRecordsMaterializesNothing() {
    final KeyValueLeafPage page = boundPage(table());
    try {
      assertEquals(0, page.materializePendingRecords(CONFIG));
    } finally {
      page.close();
    }
  }

  @Test
  void aTableBoundOnlyByIdRefusesToJudgeRawStrings() {
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, CONFIG, 2, null, null, false);
    try {
      page.setFsstSymbolTableId(TABLE_ID);
      page.setRecord(string(5, TOKEN.repeat(40).getBytes(StandardCharsets.UTF_8), false));
      final IllegalStateException refusal =
          assertThrows(IllegalStateException.class, () -> page.materializePendingRecords(CONFIG));
      assertTrue(refusal.getMessage().contains("symbols are unresolved"), refusal.getMessage());
    } finally {
      page.close();
    }
  }

  @Test
  void aTableBoundOnlyByIdRefusesToJudgeARawFusedString() {
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, CONFIG, 2, null, null, false);
    try {
      page.setFsstSymbolTableId(TABLE_ID);
      page.setRecord(new ObjectNamedStringNode(nodeKey(6), 0L, 0L, 0L, 1, 1L, 0, 0, 0L,
          TOKEN.repeat(40).getBytes(StandardCharsets.UTF_8), CONFIG.nodeHashFunction, (byte[]) null, false, null));
      assertThrows(IllegalStateException.class, () -> page.materializePendingRecords(CONFIG));
    } finally {
      page.close();
    }
  }

  @Test
  void aTableBoundOnlyByIdMaterializesAlreadyEncodedStrings() {
    final byte[] table = table();
    final byte[] encoded = FSSTCompressor.encode(TOKEN.repeat(4).getBytes(StandardCharsets.UTF_8), table);
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, CONFIG, 2, null, null, false);
    try {
      page.setFsstSymbolTableId(TABLE_ID);
      page.setRecord(string(5, encoded, true));
      assertEquals(0, page.materializePendingRecords(CONFIG));
      assertNotNull(page.getSlot(5));
      assertFalse(page.getReferencesMap().containsKey(nodeKey(5)));
    } finally {
      page.close();
    }
  }

  private static KeyValueLeafPage boundPage(final byte[] table) {
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, CONFIG, 2, null, null, false);
    page.setFsstSymbolTable(table);
    page.setFsstSymbolTableId(TABLE_ID);
    return page;
  }

  private static StringNode string(final int slot, final byte[] value, final boolean compressed) {
    return new StringNode(nodeKey(slot), 0L, 0, 0, 0L, 0L, 0L, value, CONFIG.nodeHashFunction, (byte[]) null,
        compressed, null);
  }

  private static long nodeKey(final int slot) {
    return (PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
  }

  private static byte[] table() {
    final List<byte[]> samples = new ArrayList<>(FSSTCompressor.MIN_SAMPLES_FOR_TABLE * 2);
    for (int i = 0; i < FSSTCompressor.MIN_SAMPLES_FOR_TABLE * 2; i++) {
      samples.add((TOKEN.repeat(8) + i).getBytes(StandardCharsets.UTF_8));
    }
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    assertNotNull(table);
    return table;
  }

  private static byte[] incompressible(final int length) {
    final Random random = new Random(42L);
    final byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) ('!' + random.nextInt(94));
    }
    return value;
  }
}
