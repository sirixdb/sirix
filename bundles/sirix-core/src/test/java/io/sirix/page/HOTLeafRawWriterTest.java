package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.settings.VersioningType;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pins the mode-zero image selected for fixed-width projection keys. */
final class HOTLeafRawWriterTest {
  @Test
  void fixedWidthProjectionKeyKeepsTheModeZeroImage() {
    Allocators.getInstance().init(1L << 30);
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("raw-leaf").versioningApproach(VersioningType.FULL).build();
    try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      assertTrue(page.put(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, new byte[] {42}));
      PageKind.HOT_LEAF_PAGE.serializePage(config, sink, page, SerializationType.DATA);
      final byte[] expected =
          HexFormat.of().parseHex("0c000401010000000a0800010203040506070801000000050000000000000001002a");
      assertArrayEquals(expected, sink.toByteArray());
    }
  }
}
