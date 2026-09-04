package io.sirix.query.json;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.sirix.access.Databases;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ArrayPageRangeSequenceOverflowTest {

  private static final String COLLECTION = "arrayRangeOverflowCollection";
  private static final String RESOURCE = "arrayRangeOverflowResource";
  private static final int ELEMENT_COUNT = 1_022;
  private static final String VALUE = "x".repeat(400);

  private Path databasePath;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    databasePath = Files.createTempDirectory("sirix-array-range-overflow-test");
    store = BasicJsonDBStore.newBuilder().location(databasePath).build();
  }

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.close();
    }
    if (databasePath != null) {
      Databases.removeDatabase(databasePath);
    }
  }

  @Test
  void referenceOnlyArrayMembersRemainVisibleToThePageRangeScan() {
    store.create(COLLECTION, RESOURCE, stringsInOneRecordPage());
    final JsonDBCollection collection = store.lookup(COLLECTION);
    final JsonDBArray array = assertInstanceOf(JsonDBArray.class, collection.getDocument(RESOURCE));
    final int revision = array.getTrx().getRevisionNumber();

    // Document root + array root + 1,022 children occupy exactly record page zero. Each string is
    // below the 512-byte per-record threshold, but together they exceed the bounded 256-KiB page;
    // the tail therefore proves the dense-page, reference-only fallback rather than large-value
    // overflow in isolation.
    try (final var reader = array.getResourceSession().createStorageEngineReader(revision)) {
      final var result = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision));
      assertNotNull(result);
      final KeyValueLeafPage page = assertInstanceOf(KeyValueLeafPage.class, result.page());
      assertEquals(ELEMENT_COUNT + 2, page.size());
      assertFalse(page.getReferencesMap().isEmpty(), "fixture must contain reference-only overflow carriers");
      assertEquals(0, page.getSideSlotCount(), "unnamed array values must use references, not fused side slots");
      assertTrue(page.populatedSlotCount() < page.size(),
          "the logical record count must exceed the physical inline bitmap count");
    }

    final var range = new ArrayPageRangeSequence(array.getResourceSession(), revision, array.getNodeKey(), collection,
        JsonItemFactory.INSTANCE, 0, 1);
    final Iter iterator = range.iterate();
    int count = 0;
    try {
      Item item;
      while ((item = iterator.next()) != null) {
        assertEquals(VALUE, item.atomize().stringValue());
        count++;
      }
    } finally {
      iterator.close();
    }
    assertEquals(ELEMENT_COUNT, count, "the dense-page overflow tail must not disappear from the array split");
  }

  private static String stringsInOneRecordPage() {
    final StringBuilder json = new StringBuilder(ELEMENT_COUNT * (VALUE.length() + 3) + 2);
    json.append('[');
    for (int i = 0; i < ELEMENT_COUNT; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append('"').append(VALUE).append('"');
    }
    return json.append(']').toString();
  }
}
