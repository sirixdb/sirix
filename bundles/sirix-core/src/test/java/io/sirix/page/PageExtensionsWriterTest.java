package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.BytesIn;
import org.junit.Test;

import java.util.HexFormat;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public final class PageExtensionsWriterTest {
  @Test
  public void emptyAndKnownEntriesHavePinnedWireBytes() {
    assertSame(PageExtensions.EMPTY, PageExtensions.builder().build());
    final BytesOut<?> zeroTag = Bytes.elasticHeapByteBuffer();
    PageKind.writeVersionAndFlags(zeroTag, (byte) 0, PageExtensions.builder().add(0, new byte[0]).build());
    assertArrayEquals(HexFormat.of().parseHex("0080020000"), zeroTag.toByteArray());
    final BytesOut<?> empty = Bytes.elasticHeapByteBuffer();
    PageKind.writeVersionAndFlags(empty, (byte) 3, PageExtensions.EMPTY);
    assertArrayEquals(HexFormat.of().parseHex("0003"), empty.toByteArray());
    final byte[] payload = {(byte) 0xaa, (byte) 0xbb};
    final PageExtensions.Builder builder = PageExtensions.builder().add(1, payload);
    payload[0] = 0;
    final PageExtensions first = builder.build();
    builder.add(64, new byte[] {(byte) 0xcc});
    final BytesOut<?> out = Bytes.elasticHeapByteBuffer();
    PageKind.writeVersionAndFlags(out, (byte) 3, first);
    assertArrayEquals(HexFormat.of().parseHex("0083040102aabb"), out.toByteArray());
    final BytesOut<?> both = Bytes.elasticHeapByteBuffer();
    PageKind.writeVersionAndFlags(both, (byte) 0, builder.build());
    assertArrayEquals(HexFormat.of().parseHex("0080070102aabb4001cc"), both.toByteArray());
  }

  @Test
  public void payloadLengthBoundaryAndHighestTagArePreserved() {
    for (final int length : new int[] {0, 127, 128, 252}) {
      final byte[] payload = new byte[length];
      for (int index = 0; index < length; index++) {
        payload[index] = (byte) index;
      }
      final BytesOut<?> wire = Bytes.elasticHeapByteBuffer();
      PageKind.writeVersionAndFlags(wire, (byte) 0, PageExtensions.builder().add(255, payload).build());
      final BytesIn<?> source = Bytes.wrapForRead(wire.toByteArray());
      assertEquals(0, source.readByte());
      assertEquals((byte) 0x80, source.readByte());
      assertEquals(length + (length < 128
          ? 2
          : 3), source.readStopBit());
      assertEquals(255, source.readByte() & 0xFF);
      assertEquals(length, source.readStopBit());
      final byte[] copied = new byte[length];
      source.read(copied);
      assertArrayEquals(payload, copied);
      assertEquals(0, source.remaining());
    }
  }

  @Test
  public void writerEnforcesUnsignedAscendingTagsAndSizeBeforeChangingState() {
    final PageExtensions.Builder builder = PageExtensions.builder().add(1, new byte[0]);
    for (final int invalid : new int[] {-1, 0, 1, 256}) {
      assertThrows(IllegalArgumentException.class, () -> builder.add(invalid, new byte[0]));
    }
    assertThrows(NullPointerException.class, () -> builder.add(2, null));
    assertThrows(IllegalArgumentException.class, () -> builder.add(2, new byte[251]));
    builder.add(2, new byte[250]); // 2 bytes + (tag + two-byte length + 250) = 255
    final BytesOut<?> out = Bytes.elasticHeapByteBuffer();
    PageKind.writeVersionAndFlags(out, (byte) 0, builder.build());
    assertEquals(259, out.toByteArray().length);
    assertEquals((byte) 0xff, out.toByteArray()[2]);
    assertEquals(1, out.toByteArray()[3]);
    assertThrows(IllegalArgumentException.class, () -> builder.add(3, new byte[0]));
    assertThrows(IllegalArgumentException.class,
        () -> PageKind.writeVersionAndFlags(Bytes.elasticHeapByteBuffer(), (byte) 0x80, PageExtensions.EMPTY));
  }
}
