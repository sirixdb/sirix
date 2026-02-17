package io.sirix.query.function.jn.index;

import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Integration tests that verify Path, CAS, and Name indexes correctly work across multiple
 * revisions. These tests verify that indexes improve query performance and return correct results
 * as the document evolves.
 * <p>
 * Note: Indexes in SirixDB are used as query optimizations. The query engine automatically uses
 * indexes when navigating paths that match indexed patterns.
 */
public final class IndexVersioningIntegrationTest extends AbstractJsonTest {

  @Nested
  @DisplayName("Path Index Tests")
  class PathIndexTests {

    @Test
    @DisplayName("Path index optimizes access to array elements")
    void testPathIndexArrayAccess() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','[{\"key\":0},{\"value\":[{\"key\":{\"boolean\":true}},{\"newkey\":\"yes\"}]},{\"key\":\"hey\",\"value\":false}]')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-path-index($doc, ('//*', '//[]')) return {\"revision\": sdb:commit($doc)}";
      final String openQuery =
          "for $i in jn:doc('json-path1','mydoc.jn')[].value[].key[?$$.boolean] return { $i, \"nodekey\": sdb:nodekey($i) }";
      test(storeQuery, indexQuery, openQuery, "{\"boolean\":true,\"nodekey\":10}");
    }

    @Test
    @DisplayName("Path index works after data modification")
    void testPathIndexAfterModification() throws IOException {
      // Store, create index, then append, then query
      query("jn:store('json-path1','mydoc.jn','{\"products\": [{\"name\": \"Widget\", \"price\": 10}]}')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-path-index($doc, ('/products', '/products/[]', '/products/[]/name')) return sdb:commit($doc)");
      query("append json {\"name\": \"Gadget\", \"price\": 20} into jn:doc('json-path1','mydoc.jn').products");
      test("for $p in jn:doc('json-path1','mydoc.jn').products[] order by $p.price return $p.name",
          "\"Widget\" \"Gadget\"");
    }

    @Test
    @DisplayName("Path index correctly filters with predicates")
    void testPathIndexWithPredicates() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','[{\"id\":1,\"active\":true},{\"id\":2,\"active\":false},{\"id\":3,\"active\":true}]')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-path-index($doc, ('//[]', '//*')) return {\"revision\": sdb:commit($doc)}";
      final String query =
          "for $i in jn:doc('json-path1','mydoc.jn')[] where $i.active eq true() order by $i.id return $i.id";
      test(storeQuery, indexQuery, query, "1 3");
    }
  }

  @Nested
  @DisplayName("CAS Index Tests")
  class CASIndexTests {

    @Test
    @DisplayName("CAS index optimizes integer comparisons")
    void testCASIndexIntegerComparison() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','[{\"key\":0},{\"value\":[{\"key\":{\"boolean\":5}},{\"newkey\":\"yes\"}]},{\"key\":\"hey\",\"value\":false}]')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/value/[]/key/boolean') return {\"revision\": sdb:commit($doc)}";
      final String openQuery =
          "for $i in jn:doc('json-path1','mydoc.jn')[1].value[].key[?$$.boolean gt 3] return { $i, \"nodekey\": sdb:nodekey($i) }";
      test(storeQuery, indexQuery, openQuery, "{\"boolean\":5,\"nodekey\":10}");
    }

    @Test
    @DisplayName("CAS index works with string values after updates")
    void testCASIndexStringAfterUpdate() throws IOException {
      query(
          "jn:store('json-path1','mydoc.jn','[{\"status\":\"pending\"},{\"status\":\"pending\"},{\"status\":\"done\"}]')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/[]/status') return sdb:commit($doc)");
      query("replace json value of jn:doc('json-path1','mydoc.jn')[0].status with \"done\"");
      test("count(for $i in jn:doc('json-path1','mydoc.jn')[] where $i.status eq 'done' return $i)", "2");
    }

    @Test
    @DisplayName("CAS index works with multiple numeric operations")
    void testCASIndexNumericOperations() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','[{\"price\":10},{\"price\":50},{\"price\":100},{\"price\":200}]')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/price') return {\"revision\": sdb:commit($doc)}";
      final String query = "count(for $i in jn:doc('json-path1','mydoc.jn')[] where $i.price ge 50 return $i)";
      test(storeQuery, indexQuery, query, "3");
    }
  }

  @Nested
  @DisplayName("Name Index Tests")
  class NameIndexTests {

    @Test
    @DisplayName("Name index optimizes field name lookups")
    void testNameIndexFieldLookup() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','{\"data\": {\"title\": \"First\"}, \"metadata\": {\"title\": \"Second\"}}')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-name-index($doc, ('title', 'data', 'metadata')) return {\"revision\": sdb:commit($doc)}";
      final String query = "let $d := jn:doc('json-path1','mydoc.jn') return ($d.data.title, $d.metadata.title)";
      test(storeQuery, indexQuery, query, "\"First\" \"Second\"");
    }

    @Test
    @DisplayName("Name index works after adding new fields")
    void testNameIndexAfterAddition() throws IOException {
      query("jn:store('json-path1','mydoc.jn','{\"items\": [{\"tag\": \"one\"}]}')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-name-index($doc, ('items', 'tag')) return sdb:commit($doc)");
      query("append json {\"tag\": \"two\"} into jn:doc('json-path1','mydoc.jn').items");
      test("for $t in jn:doc('json-path1','mydoc.jn').items[].tag order by $t return $t", "\"one\" \"two\"");
    }
  }

  @Nested
  @DisplayName("Combined Index Scenarios")
  class CombinedIndexTests {

    @Test
    @DisplayName("Multiple indexes work together")
    void testMultipleIndexes() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','{\"users\": [{\"name\": \"Alice\", \"age\": 30}, {\"name\": \"Bob\", \"age\": 25}]}')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $pathIdx := jn:create-path-index($doc, ('/users', '/users/[]')) let $casIdx := jn:create-cas-index($doc, 'xs:integer', '/users/[]/age') let $nameIdx := jn:create-name-index($doc, ('users', 'name', 'age')) return sdb:commit($doc)";
      final String query = "for $u in jn:doc('json-path1','mydoc.jn').users[] where $u.age gt 26 return $u.name";
      test(storeQuery, indexQuery, query, "\"Alice\"");
    }

    @Test
    @DisplayName("Index correctly handles delete and insert across revisions")
    void testIndexWithDeleteAndInsert() throws IOException {
      query("jn:store('json-path1','mydoc.jn','[{\"v\":1},{\"v\":2},{\"v\":3}]')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-path-index($doc, ('//[]', '//*')) return sdb:commit($doc)");
      query("delete json jn:doc('json-path1','mydoc.jn')[0]");
      query("append json {\"v\": 4} into jn:doc('json-path1','mydoc.jn')");
      test("for $i in jn:doc('json-path1','mydoc.jn')[] order by $i.v return $i.v", "2 3 4");
    }

    @Test
    @DisplayName("Index correctly handles replace value")
    void testIndexWithReplace() throws IOException {
      query("jn:store('json-path1','mydoc.jn','{\"item\": {\"name\": \"old\", \"count\": 5}}')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/item/name') return sdb:commit($doc)");
      query("replace json value of jn:doc('json-path1','mydoc.jn').item.name with \"new\"");
      test("jn:doc('json-path1','mydoc.jn').item.name", "\"new\"");
    }

    @Test
    @DisplayName("Index works with deeply nested structures")
    void testIndexDeepNesting() throws IOException {
      final String storeQuery =
          "jn:store('json-path1','mydoc.jn','{\"l1\": {\"l2\": {\"l3\": [{\"val\": 1}, {\"val\": 2}]}}}')";
      final String indexQuery =
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-path-index($doc, ('/l1', '/l1/l2', '/l1/l2/l3', '/l1/l2/l3/[]')) return sdb:commit($doc)";
      final String query = "for $v in jn:doc('json-path1','mydoc.jn').l1.l2.l3[].val order by $v return $v";
      test(storeQuery, indexQuery, query, "1 2");
    }
  }

  @Nested
  @DisplayName("Multi-Revision Index Tests")
  class MultiRevisionTests {

    @Test
    @DisplayName("Index maintains correctness across multiple separate commits")
    void testIndexAcrossMultipleCommits() throws IOException {
      query("jn:store('json-path1','mydoc.jn','[{\"id\":1}]')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-path-index($doc, ('//[]', '//*')) return sdb:commit($doc)");
      query("append json {\"id\":2} into jn:doc('json-path1','mydoc.jn')");
      query("append json {\"id\":3} into jn:doc('json-path1','mydoc.jn')");
      test("for $i in jn:doc('json-path1','mydoc.jn')[] order by $i.id return $i.id", "1 2 3");
    }

    @Test
    @DisplayName("CAS index correctly tracks value changes across revisions")
    void testCASIndexValueChangesAcrossRevisions() throws IOException {
      query("jn:store('json-path1','mydoc.jn','[{\"id\":1,\"status\":\"new\"},{\"id\":2,\"status\":\"new\"}]')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/[]/status') return sdb:commit($doc)");
      query("replace json value of jn:doc('json-path1','mydoc.jn')[0].status with \"processed\"");
      query("replace json value of jn:doc('json-path1','mydoc.jn')[1].status with \"processed\"");
      test("count(for $i in jn:doc('json-path1','mydoc.jn')[] where $i.status eq 'processed' return $i)", "2");
    }

    @Test
    @DisplayName("Name index handles field additions across multiple revisions")
    void testNameIndexFieldAdditionsAcrossRevisions() throws IOException {
      query("jn:store('json-path1','mydoc.jn','{\"root\": {\"items\": []}}')");
      query(
          "let $doc := jn:doc('json-path1','mydoc.jn') let $stats := jn:create-name-index($doc, ('root', 'items', 'tag')) return sdb:commit($doc)");
      query("append json {\"tag\": \"a\"} into jn:doc('json-path1','mydoc.jn').root.items");
      query("append json {\"tag\": \"b\"} into jn:doc('json-path1','mydoc.jn').root.items");
      query("append json {\"tag\": \"c\"} into jn:doc('json-path1','mydoc.jn').root.items");
      test("for $t in jn:doc('json-path1','mydoc.jn').root.items[].tag order by $t return $t", "\"a\" \"b\" \"c\"");
    }
  }
}
