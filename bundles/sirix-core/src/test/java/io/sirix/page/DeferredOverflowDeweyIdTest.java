package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.json.ObjectNamedStringNode;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** Deferred metadata reads must leave the writer frame unchanged and bypass disabled features. */
final class DeferredOverflowDeweyIdTest {
  private static final int SLOT = 7;
  private static final byte[] DEWEY_ID = new SirixDeweyID("1.3.5").toBytes();
  private static final byte[] NEW_DEWEY_ID = new SirixDeweyID("1.3.7").toBytes();

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(256L * 1024 * 1024);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void onlyDiffsWithDeweyIdsReadDeferredMetadata(final boolean storeDiffs, final boolean deweyIds) {
    final var config = configuration(storeDiffs, deweyIds);
    final var source = completePage(config);
    final var complete = spy(source);
    final var modified = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, 2, null, null, false);
    try {
      modified.setCompletePageRef(complete);
      modified.markSlotForPreservation(SLOT);
      modified.setPageReference(SLOT, source.getPageReference(SLOT));
      final byte[] frameBefore = modified.getSlottedPage().toArray(ValueLayout.JAVA_BYTE);
      clearInvocations(complete);

      final byte[] actual = modified.getDeweyIdAsByteArray(SLOT);
      if (storeDiffs && deweyIds) {
        assertArrayEquals(DEWEY_ID, actual);
        verify(complete, times(1)).getDeweyId(SLOT);
      } else {
        assertNull(actual);
        verifyNoInteractions(complete);
      }
      assertArrayEquals(frameBefore, modified.getSlottedPage().toArray(ValueLayout.JAVA_BYTE),
          "metadata reads must not materialize a descriptor or change the stored frame");
      assertNull(modified.getSlot(SLOT));
      assertEquals(0, modified.getSideSlotCount());
      assertTrue(modified.isSlotMarkedForPreservation(SLOT));
    } finally {
      modified.close();
      source.close();
    }
  }

  @Test
  void locallyPublishedIdWinsAndUnmarkedReferencesDoNotBorrow() {
    final var config = configuration(true, true);
    final var complete = completePage(config);
    final var modified = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, 2, null, null, false);
    try {
      modified.setCompletePageRef(complete);
      modified.setPageReference(SLOT, complete.getPageReference(SLOT));
      assertFalse(modified.isSlotMarkedForPreservation(SLOT));
      assertNull(modified.getDeweyIdAsByteArray(SLOT), "an unrelated reference must not inherit an old ID");

      modified.markSlotForPreservation(SLOT);
      modified.copySlotFromPage(complete, SLOT);
      modified.setDeweyId(NEW_DEWEY_ID, SLOT);
      assertArrayEquals(NEW_DEWEY_ID, modified.getDeweyIdAsByteArray(SLOT));
      assertArrayEquals(DEWEY_ID, complete.getDeweyIdAsByteArray(SLOT),
          "reading or replacing writer metadata must not mutate an older revision");
    } finally {
      modified.close();
      complete.close();
    }
  }

  private static ResourceConfiguration configuration(final boolean storeDiffs, final boolean deweyIds) {
    return ResourceConfiguration.newBuilder("deferred-metadata")
                                .storeDiffs(storeDiffs)
                                .useDeweyIDs(deweyIds)
                                .hashKind(HashType.NONE)
                                .build();
  }

  private static KeyValueLeafPage completePage(final ResourceConfiguration config) {
    final var page = new KeyValueLeafPage(0, IndexType.DOCUMENT, config, 1, null, null, false);
    page.setRecord(new ObjectNamedStringNode(SLOT, 1, -1, -1, 2, 3, 1, 1, 0,
        "overflow".repeat(PageConstants.MAX_RECORD_SIZE).getBytes(StandardCharsets.UTF_8), LongHashFunction.xx3(),
        config.areDeweyIDsStored
            ? DEWEY_ID
            : null,
        false, null));
    page.addReferences(config);
    assertTrue(page.isFusedOverflowDescriptor(SLOT));
    return page;
  }
}
