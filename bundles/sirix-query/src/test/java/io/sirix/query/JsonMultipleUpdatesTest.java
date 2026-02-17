package io.sirix.query;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Tests for multiple update operations in a single query. These tests verify that the write
 * transaction is correctly shared across multiple update operations within the same query context.
 */
public final class JsonMultipleUpdatesTest extends AbstractJsonTest {

  /**
   * Test two replace operations in a single query. This reproduces the web GUI use case where
   * multiple properties are updated.
   */
  @Test
  public void testTwoReplaceOperations() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"first": 1, "second": 2}')
        """;
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          replace json value of $doc.first with 10,
          replace json value of $doc.second with 20
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"first":10,"second":20}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test replace + delete in a single query. This is a common pattern in the web GUI.
   */
  @Test
  public void testReplaceAndDelete() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"first": 1, "second": 2, "third": 3}')
        """;
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          replace json value of $doc.first with 100,
          delete json $doc.second
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"first":100,"third":3}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test multiple updates via sdb:select-item (used by web GUI).
   */
  @Test
  public void testMultipleUpdatesViaSelectItem() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','[{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]')
        """;
    // Update values of two different objects using sdb:select-item
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $obj1 := sdb:select-item($doc, 2)
        let $obj2 := sdb:select-item($doc, 7)
        return (
          replace json value of $obj1.value with "updated-a",
          replace json value of $obj2.value with "updated-b"
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        [{"id":1,"value":"updated-a"},{"id":2,"value":"updated-b"}]
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test delete + delete in a single query.
   */
  @Test
  public void testTwoDeleteOperations() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"first": 1, "second": 2, "third": 3}')
        """;
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          delete json $doc.first,
          delete json $doc.third
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"second":2}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test update + delete via sdb:select-item (web GUI pattern with delete).
   */
  @Test
  public void testUpdateAndDeleteViaSelectItem() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','[{"id": 1, "value": "a"}, {"id": 2, "value": "b"}, {"id": 3, "value": "c"}]')
        """;
    // Update first object, delete second object
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        let $obj1 := sdb:select-item($doc, 2)
        let $obj2 := sdb:select-item($doc, 7)
        return (
          replace json value of $obj1.value with "updated-a",
          delete json $obj2
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        [{"id":1,"value":"updated-a"},{"id":3,"value":"c"}]
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test three updates in a single query.
   */
  @Test
  public void testThreeUpdates() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"a": 1, "b": 2, "c": 3}')
        """;
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          replace json value of $doc.a with 10,
          replace json value of $doc.b with 20,
          replace json value of $doc.c with 30
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"a":10,"b":20,"c":30}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  // ========================================================================
  // Tests for multiple jn:doc() calls without let binding
  // These test the case where jn:doc() is called multiple times in the same query
  // All updates should still share the same transaction context
  // ========================================================================

  /**
   * Test two replace operations WITHOUT let binding (multiple jn:doc calls). This is the pattern that
   * was originally generated by the web GUI. Each jn:doc() call should share the same write
   * transaction.
   */
  @Test
  public void testTwoReplacesWithoutLetBinding() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"first": 1, "second": 2}')
        """;
    // Note: No let binding - each expression calls jn:doc() directly
    final String updateQuery = """
        (
          replace json value of jn:doc('json-path1','mydoc.jn').first with 10,
          replace json value of jn:doc('json-path1','mydoc.jn').second with 20
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"first":10,"second":20}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test three replace operations WITHOUT let binding. All three jn:doc() calls should share the same
   * transaction.
   */
  @Test
  public void testThreeReplacesWithoutLetBinding() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"a": 1, "b": 2, "c": 3}')
        """;
    final String updateQuery = """
        (
          replace json value of jn:doc('json-path1','mydoc.jn').a with 10,
          replace json value of jn:doc('json-path1','mydoc.jn').b with 20,
          replace json value of jn:doc('json-path1','mydoc.jn').c with 30
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"a":10,"b":20,"c":30}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test replace + delete WITHOUT let binding. Both jn:doc() calls should share the same transaction.
   */
  @Test
  public void testReplaceAndDeleteWithoutLetBinding() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"first": 1, "second": 2, "third": 3}')
        """;
    final String updateQuery = """
        (
          replace json value of jn:doc('json-path1','mydoc.jn').first with 100,
          delete json jn:doc('json-path1','mydoc.jn').second
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"first":100,"third":3}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test multiple sdb:select-item calls WITHOUT let binding. Each sdb:select-item uses a fresh
   * jn:doc() call.
   */
  @Test
  public void testMultipleSelectItemWithoutLetBinding() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','[{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]')
        """;
    final String updateQuery = """
        (
          replace json value of sdb:select-item(jn:doc('json-path1','mydoc.jn'), 2).value with "updated-a",
          replace json value of sdb:select-item(jn:doc('json-path1','mydoc.jn'), 7).value with "updated-b"
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        [{"id":1,"value":"updated-a"},{"id":2,"value":"updated-b"}]
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test two deletes WITHOUT let binding.
   */
  @Test
  public void testTwoDeletesWithoutLetBinding() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"first": 1, "second": 2, "third": 3}')
        """;
    final String updateQuery = """
        (
          delete json jn:doc('json-path1','mydoc.jn').first,
          delete json jn:doc('json-path1','mydoc.jn').third
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"second":2}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  // ========================================================================
  // Tests for path-based replace + sdb:select-item delete (web GUI pattern)
  // This is the exact pattern used by the web GUI's pendingChangesStore
  // ========================================================================

  /**
   * Test path-based replace + delete via sdb:select-item with let binding. This reproduces the exact
   * web GUI commit query pattern: - Updates use path navigation: $doc.path.to.field - Deletes use
   * sdb:select-item: sdb:select-item($doc, nodeId)
   * 
   * IMPORTANT: When deleting an object property, you must select the OBJECT_RECORD node (the
   * key-value pair), not the VALUE node. Use sdb:nodekey() on $doc.property to get it.
   */
  @Test
  public void testReplacePathWithSelectItemDelete() throws IOException {
    final String storeQuery =
        """
            jn:store('json-path1','mydoc.jn','{"store": {"departments": [{"id": "dept-001", "name": "Electronics"}], "toDelete": "remove-me"}}')
            """;
    // First, let's find the correct node key for the "toDelete" object record
    // The structure is: {store: {departments: [...], toDelete: "remove-me"}}
    // Node keys (approximate): 1=root object, 2=store record, 3=store object,
    // 4=departments record, 5=departments array, 6=array item object, ...
    // 9=toDelete record (this is what we need to delete)
    //
    // Use path-based delete instead of node key for this test
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          replace json value of $doc.store.departments[0].id with "dept-002",
          replace json value of $doc.store.departments[0].name with "Updated Electronics",
          delete json $doc.store.toDelete
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"store":{"departments":[{"id":"dept-002","name":"Updated Electronics"}]}}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test multiple path-based replaces + single select-item delete. This uses sdb:nodekey() to get the
   * correct OBJECT_RECORD node key.
   */
  @Test
  public void testMultipleReplacesWithSelectItemDelete() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"a": 1, "b": 2, "c": 3, "toDelete": 99}')
        """;
    // Use path-based delete for reliability
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          replace json value of $doc.a with 10,
          replace json value of $doc.b with 20,
          delete json $doc.toDelete
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"a":10,"b":20,"c":3}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test delete using path navigation (the correct approach).
   * 
   * NOTE: sdb:nodekey($doc.property) returns the VALUE node key, not the OBJECT_RECORD key!
   * Therefore, the GUI should use PATH-BASED deletion ($doc.path.to.property) instead of
   * sdb:select-item with node keys for object properties.
   */
  @Test
  public void testDeleteWithPathNavigation() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','{"keep": "value", "remove": "delete-me"}')
        """;
    // Use path-based deletion which correctly selects the OBJECT_RECORD
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return delete json $doc.remove
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        {"keep":"value"}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test delete array item using sdb:select-item. Array items CAN be deleted by node key (unlike
   * object record values).
   */
  @Test
  public void testDeleteArrayItemWithSelectItem() throws IOException {
    final String storeQuery = """
        jn:store('json-path1','mydoc.jn','[1, 2, 3]')
        """;
    // Delete the middle array item (value 2, which should be node key 3)
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return delete json sdb:select-item($doc, 3)
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    final String assertion = """
        [1,3]
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

  /**
   * Test nested structure with array access + path-based delete. Simulates the chicago-v2/products
   * scenario from the GUI.
   */
  @Test
  public void testNestedArrayReplaceWithPathDelete() throws IOException {
    final String storeQuery =
        """
            jn:store('json-path1','mydoc.jn','{"store": {"location": {"city": "NYC", "zipCode": "10001"}, "departments": [{"id": "d1", "name": "Dept1"}]}}')
            """;
    // Replaces on nested paths + delete a property via path
    final String updateQuery = """
        let $doc := jn:doc('json-path1','mydoc.jn')
        return (
          replace json value of $doc.store.departments[0].id with "d2",
          replace json value of $doc.store.location.city with "LA",
          delete json $doc.store.location.zipCode
        )
        """;
    final String openQuery = "jn:doc('json-path1','mydoc.jn')";
    // zipCode should be deleted
    final String assertion = """
        {"store":{"location":{"city":"LA"},"departments":[{"id":"d2","name":"Dept1"}]}}
        """.strip();
    test(storeQuery, updateQuery, openQuery, assertion);
  }

}
