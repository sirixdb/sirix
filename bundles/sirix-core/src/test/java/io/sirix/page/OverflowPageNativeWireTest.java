/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Wire-compatibility and lifetime checks for native staged {@link OverflowPage} views. */
final class OverflowPageNativeWireTest {

  private static final int NATIVE_OFFSET = 3;
  private static final byte SENTINEL = (byte) 0xA5;

  @Test
  void heapAndNativeViewsHaveIdenticalWireBytesAndRoundTrip() throws IOException {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("overflow-native-wire").build();
    final PagePersister persister = new PagePersister();

    for (final int length : new int[] {0, 1, 128 * 1024 + 17}) {
      final byte[] payload = new byte[length];
      for (int i = 0; i < payload.length; i++) {
        payload[i] = (byte) (i * 31 + 7);
      }

      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment reservoir = arena.allocate(NATIVE_OFFSET + length + 5L, Long.BYTES);
        reservoir.fill(SENTINEL);
        MemorySegment.copy(payload, 0, reservoir, ValueLayout.JAVA_BYTE, NATIVE_OFFSET, length);
        final OverflowPage nativePage = new OverflowPage(reservoir.asReadOnly(), NATIVE_OFFSET, length);
        final OverflowPage heapPage = new OverflowPage(payload);

        final byte[] heapWire = serialize(persister, config, heapPage);
        final byte[] nativeWire = serialize(persister, config, nativePage);
        assertArrayEquals(heapWire, nativeWire, "wire mismatch for payload length " + length);

        final OverflowPage roundTripped =
            (OverflowPage) persister.deserializePage(config, Bytes.wrapForRead(nativeWire), SerializationType.DATA);
        assertArrayEquals(payload, roundTripped.getDataBytes());
        assertEquals(SENTINEL, reservoir.get(ValueLayout.JAVA_BYTE, 0));
        assertEquals(SENTINEL, reservoir.get(ValueLayout.JAVA_BYTE, NATIVE_OFFSET + length));

        nativePage.close();
        assertThrows(IllegalStateException.class, nativePage::getDataBytes);
        try (BytesOut<?> closedSink = Bytes.elasticOffHeapByteBuffer(16)) {
          assertThrows(IllegalStateException.class, () -> nativePage.writeDataTo(closedSink));
        }
      }
    }
  }

  @Test
  void nativeConstructorRejectsAReadOnlyHeapSegment() {
    final MemorySegment heapSegment = MemorySegment.ofArray(new byte[8]).asReadOnly();
    assertThrows(IllegalArgumentException.class, () -> new OverflowPage(heapSegment, 0, 8));
  }

  private static byte[] serialize(final PagePersister persister, final ResourceConfiguration config,
      final OverflowPage page) throws IOException {
    try (BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      persister.serializePage(config, sink, page, SerializationType.DATA);
      return sink.toByteArray();
    }
  }
}
