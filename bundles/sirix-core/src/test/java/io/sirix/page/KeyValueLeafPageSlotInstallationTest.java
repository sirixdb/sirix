package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.settings.Constants;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KeyValueLeafPageSlotInstallationTest {

  @ParameterizedTest
  @MethodSource("representations")
  void descriptorRetainsCompanionAndInlineReplacementRemovesIt(final NodeKind kind, final boolean nativeInput,
      final boolean deweyIDs) {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("slot-installation").useDeweyIDs(deweyIDs).build();
    try (final Arena arena = Arena.ofConfined();
        final KeyValueLeafPage page = new KeyValueLeafPage(1L, IndexType.DOCUMENT, config, 1, null, null, false)) {
      final int slot = 7;
      final long nodeKey = (1L << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
      final int fieldCount = NodeFieldLayout.fieldCountForKind(kind.getId());
      final byte[] data = new byte[2 + fieldCount];
      data[0] = kind.getId();
      data[1 + fieldCount] = kind == NodeKind.OBJECT_NAMED_STRING
          ? ObjectNamedStringNode.PAYLOAD_FLAG_OVERFLOW
          : ObjectNamedNumberNode.PAYLOAD_TYPE_OVERFLOW;
      final MemorySegment input = nativeInput
          ? arena.allocate(data.length)
          : MemorySegment.ofArray(data);
      if (nativeInput) {
        MemorySegment.copy(data, 0, input, ValueLayout.JAVA_BYTE, 0L, data.length);
      }
      final PageReference companion = new PageReference();
      page.setPageReference(nodeKey, companion);
      page.setSlotWithNodeKind(input, slot, kind.getId());
      assertSame(companion, page.getPageReference(nodeKey));
      assertTrue(page.isFusedOverflowDescriptor(slot));
      assertTrue(page.getSlot(slot).isNative());
      assertArrayEquals(data, page.getSlot(slot).toArray(ValueLayout.JAVA_BYTE));

      data[1 + fieldCount] = 0;
      if (nativeInput) {
        MemorySegment.copy(data, 0, input, ValueLayout.JAVA_BYTE, 0L, data.length);
      }
      page.setSlotWithNodeKind(input, slot, kind.getId());
      assertNull(page.getPageReference(nodeKey));
      assertArrayEquals(data, page.getSlot(slot).toArray(ValueLayout.JAVA_BYTE));
    }
  }

  private static Stream<Arguments> representations() {
    return Stream.of(NodeKind.OBJECT_NAMED_STRING, NodeKind.OBJECT_NAMED_NUMBER)
                 .flatMap(kind -> Stream.of(Arguments.of(kind, false, false), Arguments.of(kind, false, true),
                     Arguments.of(kind, true, false), Arguments.of(kind, true, true)));
  }
}
