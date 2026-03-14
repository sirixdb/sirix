package io.sirix.query.compiler.vectorized;

import com.google.gson.stream.JsonReader;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.StorageEngineReader;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageScanIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the columnar scan pipeline:
 * {@link PageScanIterator} → {@link ColumnarPageExtractor} → {@link ColumnarScanAxis} → {@link ColumnBatch}.
 *
 * <p>Creates real Sirix databases with committed JSON data and verifies
 * end-to-end columnar extraction.</p>
 */
final class ColumnarScanIntegrationTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void scanSmallDocument() {
    // Create a document with a few string values
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      for (int i = 0; i < 10; i++) {
        wtx.insertStringValueAsFirstChild("value_" + i);
        wtx.moveToParent();
      }
      wtx.commit();
    }

    // Read back via columnar scan
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      try (final var axis = new ColumnarScanAxis(reader, false)) {
        final ColumnBatch batch = axis.nextBatch();
        assertNotNull(batch, "Should produce at least one batch");
        assertTrue(batch.rowCount() >= 10,
            "Batch should contain at least 10 string rows, got: " + batch.rowCount());
        assertTrue(batch.selectionCount() >= 10);

        // Verify node keys are valid
        for (int i = 0; i < batch.selectionCount(); i++) {
          final int row = batch.selectedRow(i);
          assertTrue(batch.getLong(ColumnarScanAxis.COL_NODE_KEY, row) >= 0,
              "Node key should be non-negative");
        }

        // Materialize strings and check they match
        batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);
        boolean foundValue0 = false;
        for (int i = 0; i < batch.selectionCount(); i++) {
          final int row = batch.selectedRow(i);
          final String value = batch.getString(ColumnarScanAxis.COL_STRING_VALUE, row);
          assertNotNull(value);
          if ("value_0".equals(value)) {
            foundValue0 = true;
          }
        }
        assertTrue(foundValue0, "Should find 'value_0' among materialized strings");
      }
    }
  }

  @Test
  void scanMultiPageDocument() {
    // Insert enough strings to span multiple pages (>1024 nodes)
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      // Each iteration creates 2 nodes (array child + string), so ~1500 strings
      for (int i = 0; i < 1500; i++) {
        wtx.insertStringValueAsFirstChild("multi_page_" + i);
        wtx.moveToParent();
      }
      wtx.commit();
    }

    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      int totalRows = 0;
      int batchCount = 0;
      try (final var axis = new ColumnarScanAxis(reader, false)) {
        ColumnBatch batch;
        while ((batch = axis.nextBatch()) != null) {
          assertTrue(batch.rowCount() > 0, "Non-null batch should have rows");
          totalRows += batch.rowCount();
          batchCount++;
        }
      }

      assertTrue(totalRows >= 1500,
          "Should extract at least 1500 string rows across pages, got: " + totalRows);
      assertTrue(batchCount >= 1,
          "Should produce at least 1 batch, got: " + batchCount);
    }
  }

  @Test
  void scanWithParentKeyFilter() {
    // Create document with two arrays containing strings
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    long itemsArrayKey;
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      // Use insertSubtreeAsFirstChild with JSON
      final String json = "{\"items\":[\"a\",\"b\",\"c\"],\"other\":[\"x\",\"y\"]}";
      wtx.insertSubtreeAsFirstChild(new JsonReader(new StringReader(json)));
      wtx.commit();

      // Navigate to find items array key
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // first record ("items" or "other")
      // Find the items array
      long foundKey = -1;
      do {
        if ("items".equals(wtx.getName().getLocalName())) {
          wtx.moveToFirstChild(); // array
          foundKey = wtx.getNodeKey();
          break;
        }
      } while (wtx.moveToRightSibling());
      itemsArrayKey = foundKey;
    }

    // Scan all strings, then apply parent key filter
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      try (final var axis = new ColumnarScanAxis(reader, false)) {
        final ColumnBatch batch = axis.nextBatch();
        assertNotNull(batch);

        // Apply parent key filter: keep only strings whose parent == itemsArrayKey
        ColumnBatchFilter.filterLong(batch, ColumnarScanAxis.COL_PARENT_KEY,
            ComparisonOperator.EQ, itemsArrayKey);

        // Materialize surviving strings
        batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);

        // Should have exactly 3 strings (a, b, c) — not x, y
        assertEquals(3, batch.selectionCount(),
            "Should have 3 strings under items array");

        boolean foundA = false;
        boolean foundB = false;
        boolean foundC = false;
        for (int i = 0; i < batch.selectionCount(); i++) {
          final int row = batch.selectedRow(i);
          final String val = batch.getString(ColumnarScanAxis.COL_STRING_VALUE, row);
          if ("a".equals(val)) foundA = true;
          if ("b".equals(val)) foundB = true;
          if ("c".equals(val)) foundC = true;
        }
        assertTrue(foundA && foundB && foundC,
            "Should find all three strings: a, b, c");
      }
    }
  }

  @Test
  void scanEmptyDocument() {
    // Create a document with no string nodes (just an empty object)
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertObjectAsFirstChild();
      wtx.commit();
    }

    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      try (final var axis = new ColumnarScanAxis(reader, false)) {
        final ColumnBatch batch = axis.nextBatch();
        // May return null (no strings) or a batch with 0 rows
        if (batch != null) {
          assertEquals(0, batch.rowCount(),
              "Empty document should have no string rows");
        }
      }
    }
  }

  @Test
  void scanMixedNodeTypeDocument() {
    // Create document with mixed types: strings, numbers, booleans, objects, arrays
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      final String json = "{\"name\":\"Alice\",\"age\":30,\"active\":true,\"tags\":[\"java\",\"db\"]}";
      wtx.insertSubtreeAsFirstChild(new JsonReader(new StringReader(json)));
      wtx.commit();
    }

    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      int totalStrings = 0;
      try (final var axis = new ColumnarScanAxis(reader, false)) {
        ColumnBatch batch;
        while ((batch = axis.nextBatch()) != null) {
          batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);
          for (int i = 0; i < batch.selectionCount(); i++) {
            final int row = batch.selectedRow(i);
            final String val = batch.getString(ColumnarScanAxis.COL_STRING_VALUE, row);
            assertNotNull(val, "Materialized string should not be null");
            totalStrings++;
          }
        }
      }

      // Should have 3 strings: "Alice", "java", "db"
      // (ObjectStringNode for "Alice", StringNodes for "java" and "db")
      assertTrue(totalStrings >= 3,
          "Should extract at least 3 strings (Alice, java, db), got: " + totalStrings);
    }
  }

  @Test
  void lateMaterializationVerification() {
    // Verify that strings are NOT decoded until materialize is called
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      for (int i = 0; i < 20; i++) {
        wtx.insertStringValueAsFirstChild("deferred_" + i);
        wtx.moveToParent();
      }
      wtx.commit();
    }

    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      try (final var axis = new ColumnarScanAxis(reader, false)) {
        final ColumnBatch batch = axis.nextBatch();
        assertNotNull(batch);
        assertTrue(batch.rowCount() >= 20);

        // Before materialization, string column should be null for all rows
        for (int i = 0; i < batch.rowCount(); i++) {
          assertNull(batch.getString(ColumnarScanAxis.COL_STRING_VALUE, i),
              "String should be null before materialization for row " + i);
        }

        // After materialization, strings should be available
        batch.materializeAllSelected(ColumnarScanAxis.COL_STRING_VALUE);
        for (int i = 0; i < batch.selectionCount(); i++) {
          final int row = batch.selectedRow(i);
          assertNotNull(batch.getString(ColumnarScanAxis.COL_STRING_VALUE, row),
              "String should be non-null after materialization for row " + row);
        }
      }
    }
  }

  @Test
  void pageScanIteratorBasic() {
    // Basic test of PageScanIterator alone
    final var db = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("test");
      wtx.commit();
    }

    try (final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();

      int pageCount = 0;
      try (final var iter = new PageScanIterator(reader)) {
        KeyValueLeafPage page;
        while ((page = iter.nextPage()) != null) {
          assertNotNull(page.getSlottedPage(), "Page should have a slotted page");
          pageCount++;
        }
      }

      assertTrue(pageCount >= 1, "Should have at least 1 page, got: " + pageCount);
    }
  }
}
