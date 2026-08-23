package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RegionTable}. Covers round-trip of the empty scaffold as well as the
 * future-shape case where the table carries concrete region payloads. The empty case is the only
 * one exercised by Phase-1 writers; the payload case protects the wire format against regressions
 * from later tasks.
 */
@DisplayName("RegionTable")
final class RegionTableTest {

  /** A payload that compresses hard, so "materialized" and "still on the wire" differ in size. */
  private static byte[] compressiblePayload() {
    final byte[] p = new byte[8192];
    for (int i = 0; i < p.length; i++) {
      p[i] = (byte) (i % 7);
    }
    return p;
  }

  private static byte[] writeOne(final byte kind, final byte[] payload) {
    final RegionTable table = new RegionTable();
    table.set(kind, payload);
    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink, true);
    return sink.bytesForRead().toByteArray();
  }

  @Test
  @DisplayName("sizing a deferred table does not decompress it")
  void retainedBytesDoesNotMaterialize() {
    final byte[] payload = compressiblePayload();
    final byte[] wire = writeOne(RegionTable.KIND_STRING, payload);
    final int deferMask = RegionTable.maskOf(RegionTable.KIND_STRING);

    final RegionTable deferred = RegionTable.read(Bytes.wrapForRead(wire), RegionTable.ALL_KINDS, deferMask);

    // The region is present and counted...
    assertFalse(deferred.isEmpty());
    assertEquals(1, deferred.size(), "a deferred region must count towards size(), as it does for isEmpty()");

    // ...but only the COMPRESSED bytes are held. This is the actual regression guard: the previous
    // accounting summed payload(kind) over every kind, and payload() is the accessor that
    // decompresses — so merely asking a page how big it was materialized everything it had
    // deliberately deferred, at the cache-admission call site that most wanted to stay cheap.
    final int retained = deferred.retainedBytes();
    assertTrue(retained > 0, "deferred wire bytes must be accounted for, not reported as zero");
    assertTrue(retained < payload.length, "retainedBytes() reported " + retained + " for a " + payload.length
        + "-byte payload that was stored compressed — it materialized the region");

    // And the deferral is still honoured: the payload decompresses correctly on first demand.
    assertArrayEquals(payload, PaxTestSegments.bytes(deferred.payload(RegionTable.KIND_STRING)));
    // Once materialized, the accounting follows the real cost.
    assertEquals(payload.length, deferred.retainedBytes());
    assertEquals(1, deferred.size());
  }

  @Test
  @DisplayName("serializing a table with deferred regions fails loudly")
  void writingDeferredTableThrows() {
    final byte[] wire = writeOne(RegionTable.KIND_STRING, compressiblePayload());
    final RegionTable deferred =
        RegionTable.read(Bytes.wrapForRead(wire), RegionTable.ALL_KINDS, RegionTable.maskOf(RegionTable.KIND_STRING));

    // write() serializes payloads[], where a deferred region is absent — it would drop the region
    // and produce a page silently missing a column. Refuse instead.
    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    assertThrows(IllegalStateException.class, () -> deferred.write(sink, true));

    // After materializing, the same table serializes normally and round-trips intact.
    final byte[] payload = PaxTestSegments.bytes(deferred.payload(RegionTable.KIND_STRING));
    final BytesOut<MemorySegment> ok = Bytes.elasticHeapByteBuffer();
    deferred.write(ok, true);
    final RegionTable back = RegionTable.read(Bytes.wrapForRead(ok.bytesForRead().toByteArray()));
    assertArrayEquals(payload, PaxTestSegments.bytes(back.payload(RegionTable.KIND_STRING)));
  }

  @Test
  @DisplayName("empty table round-trips to 4 bytes")
  void emptyRoundTrip() {
    final RegionTable table = new RegionTable();
    assertTrue(table.isEmpty());
    assertEquals(0, table.size());
    assertNull(PaxTestSegments.bytes(table.payload(RegionTable.KIND_NUMBER)));

    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink, false);
    final byte[] wire = sink.bytesForRead().toByteArray();
    // 4 bytes: int regionCount = 0
    assertEquals(4, wire.length);

    final BytesIn<MemorySegment> source = Bytes.wrapForRead(wire);
    final RegionTable roundTripped = RegionTable.read(source);
    assertTrue(roundTripped.isEmpty());
    assertEquals(0, roundTripped.size());
    assertNull(PaxTestSegments.bytes(roundTripped.payload(RegionTable.KIND_NUMBER)));
    assertNull(PaxTestSegments.bytes(roundTripped.payload(RegionTable.KIND_STRING)));
  }

  @Test
  @DisplayName("populated table preserves payloads by kind")
  void populatedRoundTrip() {
    final byte[] numberPayload = new byte[] {1, 2, 3, 4, 5};
    final byte[] stringPayload = new byte[] {9, 8, 7};

    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER, numberPayload);
    table.set(RegionTable.KIND_STRING, stringPayload);

    assertFalse(table.isEmpty());
    assertEquals(2, table.size());
    assertArrayEquals(numberPayload, PaxTestSegments.bytes(table.payload(RegionTable.KIND_NUMBER)));
    assertArrayEquals(stringPayload, PaxTestSegments.bytes(table.payload(RegionTable.KIND_STRING)));
    assertNull(PaxTestSegments.bytes(table.payload(RegionTable.KIND_STRUCT)));

    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink, false);

    final BytesIn<MemorySegment> source = Bytes.wrapForRead(sink.bytesForRead().toByteArray());
    final RegionTable roundTripped = RegionTable.read(source);
    assertEquals(2, roundTripped.size());
    assertArrayEquals(numberPayload, PaxTestSegments.bytes(roundTripped.payload(RegionTable.KIND_NUMBER)));
    assertArrayEquals(stringPayload, PaxTestSegments.bytes(roundTripped.payload(RegionTable.KIND_STRING)));
    assertNull(PaxTestSegments.bytes(roundTripped.payload(RegionTable.KIND_STRUCT)));
  }

  @Test
  @DisplayName("prefix set copies caller storage before it can be reused")
  void prefixSetTakesOwnershipByCopy() {
    final byte[] reusable = {9, 8, 7, 6, 5, 4};
    final RegionTable table = new RegionTable();

    table.set(RegionTable.KIND_NUMBER_ZONEMAP, reusable, 4);
    Arrays.fill(reusable, (byte) 0);

    assertEquals(4, table.payload(RegionTable.KIND_NUMBER_ZONEMAP).byteSize());
    assertArrayEquals(new byte[] {9, 8, 7, 6}, PaxTestSegments.bytes(table.payload(RegionTable.KIND_NUMBER_ZONEMAP)));

    table.set(RegionTable.KIND_NUMBER_ZONEMAP, null, 0);
    assertNull(table.payload(RegionTable.KIND_NUMBER_ZONEMAP));
  }

  @Test
  @DisplayName("prefix set validates kind and length before publishing")
  void prefixSetValidatesBounds() {
    final RegionTable table = new RegionTable();
    final byte[] payload = {1, 2, 3};

    assertThrows(IllegalArgumentException.class, () -> table.set(RegionTable.KIND_NUMBER, payload, -1));
    assertThrows(IllegalArgumentException.class, () -> table.set(RegionTable.KIND_NUMBER, payload, payload.length + 1));
    assertThrows(IllegalArgumentException.class, () -> table.set(RegionTable.KIND_NUMBER, null, 1));
    assertThrows(IllegalArgumentException.class, () -> table.set((byte) -1, payload, payload.length));
    assertThrows(IllegalArgumentException.class,
        () -> table.set((byte) RegionTable.KIND_COUNT, payload, payload.length));
    assertTrue(table.isEmpty(), "a rejected prefix must not publish a region");
  }

  @Test
  @DisplayName("confined writer table closes its native payloads deterministically")
  void confinedWriterTableClosesNativePayloads() {
    final RegionTable table = RegionTable.newConfinedWriterTable();
    table.set(RegionTable.KIND_NUMBER, new byte[] {1, 2, 3, 4});
    final MemorySegment payload = table.payload(RegionTable.KIND_NUMBER);

    assertNotNull(payload);
    assertTrue(payload.isNative());
    assertTrue(payload.scope().isAlive());

    table.close();

    assertFalse(payload.scope().isAlive(), "close must reclaim the confined arena synchronously");
    assertNull(table.payload(RegionTable.KIND_NUMBER));
    assertTrue(table.isEmpty());
    table.close(); // Explicit writer close is idempotent.
  }

  @Test
  @DisplayName("ordinary automatic tables cannot be closed through the writer-only contract")
  void automaticTableRejectsExplicitClose() {
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER, new byte[] {5, 6, 7});
    final MemorySegment payload = table.payload(RegionTable.KIND_NUMBER);

    assertThrows(UnsupportedOperationException.class, table::close);
    assertTrue(payload.scope().isAlive(), "a rejected close must leave shared automatic payloads valid");
    assertArrayEquals(new byte[] {5, 6, 7}, PaxTestSegments.bytes(payload));
  }

  @Test
  @DisplayName("clearing via set(kind, null) removes the region")
  void clearingByNull() {
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER, new byte[] {1});
    assertEquals(1, table.size());
    table.set(RegionTable.KIND_NUMBER, null);
    assertEquals(0, table.size());
    assertTrue(table.isEmpty());
    assertNull(PaxTestSegments.bytes(table.payload(RegionTable.KIND_NUMBER)));
  }

  @Test
  @DisplayName("zero-length payload round-trips without allocation surprises")
  void emptyPayloadRoundTrip() {
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_STRUCT, new byte[0]);
    assertEquals(1, table.size());
    assertEquals(0, PaxTestSegments.bytes(table.payload(RegionTable.KIND_STRUCT)).length);

    final BytesOut<MemorySegment> sink = Bytes.elasticHeapByteBuffer();
    table.write(sink, false);

    final BytesIn<MemorySegment> source = Bytes.wrapForRead(sink.bytesForRead().toByteArray());
    final RegionTable roundTripped = RegionTable.read(source);
    assertEquals(1, roundTripped.size());
    assertEquals(0, PaxTestSegments.bytes(roundTripped.payload(RegionTable.KIND_STRUCT)).length);
  }


}
