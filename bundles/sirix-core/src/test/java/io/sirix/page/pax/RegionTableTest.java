package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RegionTable}. Covers round-trip of the empty scaffold
 * as well as the future-shape case where the table carries concrete region
 * payloads. The empty case is the only one exercised by Phase-1 writers; the
 * payload case protects the wire format against regressions from later tasks.
 */
@DisplayName("RegionTable")
final class RegionTableTest {

  @Test
  @DisplayName("empty table round-trips to 4 bytes")
  void emptyRoundTrip() {
    final RegionTable table = new RegionTable();
    assertTrue(table.isEmpty());
    assertEquals(0, table.size());
    assertNull(table.payload(RegionTable.KIND_NUMBER));

    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink);
    final byte[] wire = sink.bytesForRead().toByteArray();
    // 4 bytes: int regionCount = 0
    assertEquals(4, wire.length);

    final BytesIn<MemorySegment> source = Bytes.wrapForRead(wire);
    final RegionTable roundTripped = RegionTable.read(source);
    assertTrue(roundTripped.isEmpty());
    assertEquals(0, roundTripped.size());
    assertNull(roundTripped.payload(RegionTable.KIND_NUMBER));
    assertNull(roundTripped.payload(RegionTable.KIND_STRING));
  }

  @Test
  @DisplayName("populated table preserves payloads by kind")
  void populatedRoundTrip() {
    final byte[] numberPayload = new byte[] { 1, 2, 3, 4, 5 };
    final byte[] stringPayload = new byte[] { 9, 8, 7 };

    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER, numberPayload);
    table.set(RegionTable.KIND_STRING, stringPayload);

    assertFalse(table.isEmpty());
    assertEquals(2, table.size());
    assertArrayEquals(numberPayload, table.payload(RegionTable.KIND_NUMBER));
    assertArrayEquals(stringPayload, table.payload(RegionTable.KIND_STRING));
    assertNull(table.payload(RegionTable.KIND_STRUCT));

    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink);

    final BytesIn<MemorySegment> source = Bytes.wrapForRead(sink.bytesForRead().toByteArray());
    final RegionTable roundTripped = RegionTable.read(source);
    assertEquals(2, roundTripped.size());
    assertArrayEquals(numberPayload, roundTripped.payload(RegionTable.KIND_NUMBER));
    assertArrayEquals(stringPayload, roundTripped.payload(RegionTable.KIND_STRING));
    assertNull(roundTripped.payload(RegionTable.KIND_STRUCT));
  }

  @Test
  @DisplayName("clearing via set(kind, null) removes the region")
  void clearingByNull() {
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER, new byte[] { 1 });
    assertEquals(1, table.size());
    table.set(RegionTable.KIND_NUMBER, null);
    assertEquals(0, table.size());
    assertTrue(table.isEmpty());
    assertNull(table.payload(RegionTable.KIND_NUMBER));
  }

  @Test
  @DisplayName("zero-length payload round-trips without allocation surprises")
  void emptyPayloadRoundTrip() {
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_STRUCT, new byte[0]);
    assertEquals(1, table.size());
    assertEquals(0, table.payload(RegionTable.KIND_STRUCT).length);

    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink);

    final BytesIn<MemorySegment> source = Bytes.wrapForRead(sink.bytesForRead().toByteArray());
    final RegionTable roundTripped = RegionTable.read(source);
    assertEquals(1, roundTripped.size());
    assertEquals(0, roundTripped.payload(RegionTable.KIND_STRUCT).length);
  }
}
