/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Region payload compression is a per-payload, self-describing wire choice — and a per-resource
 * dial on the write side.
 *
 * <p>Two properties carry the feature. First, byte fidelity: whatever a payload's codec, the
 * reader must reconstruct the exact bytes, because scans and value-elision injection read them
 * verbatim from the cached arrays. Second, setting-independence on read: the codec byte lives on
 * each payload, so a database written with compression on stays readable by a resource configured
 * off and vice versa — the dial only chooses what future commits write. Losing that would turn a
 * speed/size preference into a format migration.
 */
@DisplayName("Region payload compression")
public final class RegionTableCompressionTest {

  /** Compressible text — long shared prefixes, the shape the string region actually holds. */
  private static byte[] compressibleText() {
    final StringBuilder sb = new StringBuilder(16 * 1024);
    for (int i = 0; i < 200; i++) {
      sb.append("https://example.org/catalog/products/item-").append(i)
          .append("/details?locale=en-US&currency=EUR&campaign=summer-sale\n");
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** Incompressible bytes — compression must lose the election, not distort them. */
  private static byte[] randomBytes(final int n) {
    final byte[] b = new byte[n];
    new Random(0xC0FFEE).nextBytes(b);
    return b;
  }

  private static RegionTable roundTrip(final RegionTable table, final boolean compress) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    table.write(sink, compress);
    return RegionTable.read(sink.bytesForRead());
  }

  @Test
  @DisplayName("compressed payloads reconstruct byte-for-byte")
  void compressedPayloadsRoundTrip() {
    final byte[] text = compressibleText();
    final byte[] noise = randomBytes(4096);
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_STRING, text);
    table.set(RegionTable.KIND_NUMBER, noise);

    final RegionTable read = roundTrip(table, true);

    assertArrayEquals(text, PaxTestSegments.bytes(read.payload(RegionTable.KIND_STRING)),
        "a compressed payload did not reconstruct exactly — scans and value injection would "
            + "read corrupt bytes");
    assertArrayEquals(noise, PaxTestSegments.bytes(read.payload(RegionTable.KIND_NUMBER)),
        "an incompressible payload was distorted; it should simply have stayed raw");
    assertNull(PaxTestSegments.bytes(read.payload(RegionTable.KIND_BOOLEAN)), "an absent region materialised");
  }

  @Test
  @DisplayName("the dial only affects what is written, never what can be read")
  void settingsReadEachOthersDatabases() {
    final byte[] text = compressibleText();
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_STRING, text);

    // Written compressed, read back — the reader has no setting to consult.
    assertArrayEquals(text, PaxTestSegments.bytes(roundTrip(table, true).payload(RegionTable.KIND_STRING)));
    // Written raw under NONE, read back identically.
    assertArrayEquals(text, PaxTestSegments.bytes(roundTrip(table, false).payload(RegionTable.KIND_STRING)));
  }

  @Test
  @DisplayName("compression actually pays on region-shaped text")
  void compressionPaysOnText() {
    final byte[] text = compressibleText();
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_STRING, text);

    final BytesOut<?> compressed = Bytes.elasticOffHeapByteBuffer();
    table.write(compressed, true);
    final BytesOut<?> raw = Bytes.elasticOffHeapByteBuffer();
    table.write(raw, false);

    assertTrue(compressed.writePosition() < raw.writePosition() / 2,
        "region-shaped text should compress at least 2x, got " + compressed.writePosition()
            + " vs raw " + raw.writePosition() + " — the election may be silently choosing raw");
  }




}
