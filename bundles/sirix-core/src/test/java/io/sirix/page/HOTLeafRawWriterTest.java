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

/** Also run in a fresh PIT JVM with the diagnostic compression property set to false. */
final class HOTLeafRawWriterTest {
  @Test
  void tinyLeafKeepsTheLegacyRawImage() {
    Allocators.getInstance().init(1L << 30);
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("raw-leaf").versioningApproach(VersioningType.FULL).build();
    try (HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION);
        MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      assertTrue(page.put(new byte[] {7}, new byte[] {42}));
      PageKind.HOT_LEAF_PAGE.serializePage(config, sink, page, SerializationType.DATA);
      final byte[] expected = HexFormat.of().parseHex("0c000001010000000a010007010000000500000000000000000001002a");
      assertArrayEquals(expected, sink.toByteArray());
    }
  }
}
