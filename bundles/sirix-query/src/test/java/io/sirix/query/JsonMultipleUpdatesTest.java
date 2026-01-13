package io.sirix.query;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Tests for multiple update operations in a single query.
 * These tests verify that the write transaction is correctly shared
 * across multiple update operations within the same query context.
 */
public final class JsonMultipleUpdatesTest extends AbstractJsonTest {

  /**
   * Test two replace operations in a single query.
   * This reproduces the web GUI use case where multiple properties are updated.
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
   * Test replace + delete in a single query.
   * This is a common pattern in the web GUI.
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
}
