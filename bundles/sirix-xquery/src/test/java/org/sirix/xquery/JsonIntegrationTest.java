package org.sirix.xquery;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public final class JsonIntegrationTest extends AbstractJsonTest {

  private static final Path JSON_RESOURCE_PATH = Path.of("src", "test", "resources", "json");

  @Test
  public void testSimpleQuery() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"test": "test string"}]')
        """;
    final String query = """
       for $i in jn:doc('mycol.jn','mydoc.jn')
       let $value := xs:string($i=>test)
       where contains($value, 'test')
       return $i
      """.stripIndent();
    final String assertion = """
       {"test":"test string"}
        """.strip();
    test(storeQuery, query, assertion);
  }

  @Test
  public void testDeleteOpWithVariable() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}}]')
        """;
    final String query = """
      let $doc := jn:doc('mycol.jn','mydoc.jn')
      let $objects := for $i in $doc where deep-equal($i=>"generic", 1)
                      return $i
      let $fields := jn:parse('["location"]')
      for $i in $fields
      return delete json $objects=>$i
      """.stripIndent();
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
       [{"generic":1}]
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testSimpleReplaceOp() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"first": 1, "second": 2}]')
        """;
    final String query = """
      let $doc := jn:doc('mycol.jn','mydoc.jn')
      let $rec := sdb:select-item($doc, 2)
      let $update := jn:parse('{"first": 2, "second": 3}')
      return for $key in bit:fields($update)
             return replace json value of $rec=>$key with $update=>$key
      """.stripIndent();
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
        [{"first":2,"second":3}]
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testSimpleDerefWithVariable() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','{"first": 1, "second": 2}')
        """;
    final String query = """
      let $doc := jn:doc('mycol.jn','mydoc.jn')
      let $rec := sdb:select-item($doc, 1)
      for $key in bit:fields($rec)
      return $rec=>$key
      """.stripIndent();
    final String assertion = """
          1 2
        """.strip();
    test(storeQuery, query, assertion);
  }

  @Test
  public void testSimpleRemove() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1}, {"location": {"state": "CA", "city": "Los Angeles"}}]')
        """;
    final String query = "let $doc := jn:doc('mycol.jn','mydoc.jn') for $i at $pos in $doc where deep-equal($i=>generic, 1) return delete json $doc[[$pos - 1]]";
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          [{"location":{"state":"CA","city":"Los Angeles"}}]
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testRemoveAll() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}},
                                           {"generic": 2, "location": {"state": "NY", "city": "New York"}},
                                           {"generic": 3, "location": {"state": "AL", "city": "Montgomery"}}]')
        """.strip();
    final String query = """
          let $doc := jn:doc('mycol.jn','mydoc.jn')
          let $m := for $i at $pos in $doc
                    return $pos - 1
          for $i in $m order by $i descending return delete json $doc[[$i]]
        """.strip();
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          []
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testArrayIndexBounds() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}},
                                           {"generic": 2, "location": {"state": "NY", "city": "New York"}},
                                           {"generic": 3, "location": {"state": "AL", "city": "Montgomery"}}]')
        """.strip();
    final String query = """
          let $doc := jn:doc('mycol.jn','mydoc.jn')
          return $doc[[0:1]]
        """.strip();
    final String assertion = """
          [{"generic":1,"location":{"state":"CA","city":"Los Angeles"}}]
        """.strip();
    test(storeQuery, query, assertion);
  }

  @Test
  public void testSimpleDeleteQuery() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}}]')
        """;
    final String query = "for $i in jn:doc('mycol.jn','mydoc.jn') where deep-equal($i=>generic, 1) return delete json $i=>location";
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          [{"generic":1}]
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testSimpleReplaceValueQuery() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}}, {"generic": 2, "location": {"state": "NY", "city": "New York"}}]')
        """;
    final String query = "for $i in jn:doc('mycol.jn','mydoc.jn') where deep-equal($i=>generic, 2) return replace json value of $i=>\"generic\" with 1";
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          [{"generic":1,"location":{"state":"CA","city":"Los Angeles"}},{"generic":1,"location":{"state":"NY","city":"New York"}}]
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testSubtreeReplaceValueQuery() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}}, {"generic": 2, "location": {"state": "NY", "city": "New York"}}]')
        """;
    final String query = "let $obj := sdb:select-item(jn:doc('mycol.jn','mydoc.jn'), 2) return replace json value of $obj=>location with {\"state\": \"NY\", \"city\": \"New York\"}";
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          [{"generic":1,"location":{"state":"NY","city":"New York"}},{"generic":2,"location":{"state":"NY","city":"New York"}}]
        """.strip();
    test(storeQuery, query, openQuery, assertion);
  }

  @Test
  public void testInsertionQuery1() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[]')
        """;
    final String updateQuery1 = """
          append json {"generic": 1, "foo": jn:null()} into jn:doc('mycol.jn','mydoc.jn')
        """.strip();
    final String query = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          [{"generic":1,"foo":null}]
        """.strip();
    test(storeQuery, updateQuery1, query, assertion);
  }

  @Test
  public void testInsertionQuery2() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[]')
        """;
    final String updateQuery1 = """
          append json {"generic": 1, "location": {"state": "NY", "ddd": {"sssss": []}, "city": "New York", "foobar": [[],{"bar": true()},[],{}]},"foo":jn:null()} into jn:doc('mycol.jn','mydoc.jn')
        """.strip();
    final String query = "jn:doc('mycol.jn','mydoc.jn')";
    final String assertion = """
          [{"generic":1,"location":{"state":"NY","ddd":{"sssss":[]},"city":"New York","foobar":[[],{"bar":true},[],{}]},"foo":null}]
        """.strip();
    test(storeQuery, updateQuery1, query, assertion);
  }

  @Ignore
  @Test
  public void testTimeTravelQuery() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[]')
        """;
    final String updateQuery1 = """
          append json {"generic": 1, "location": {"state": "NY", "city": "New York"}} into jn:doc('mycol.jn','mydoc.jn')
        """.strip();
    final String updateQuery2 = """
          insert json {'generic': 1, 'location': {'state': 'CA', 'city': 'Los Angeles'}} into jn:doc('mycol.jn','mydoc.jn') at position 0
        """.strip();
    final String query = "let $node := sdb:select-item(jn:doc('mycol.jn','mydoc.jn'), 1) let $result := for $rev in jn:all-times($node) return if (not(exists(jn:previous($rev)))) then sdb:revision($rev) else if (sdb:hash($rev) ne sdb:hash(jn:previous($rev))) then sdb:revision($rev) else () return for $i in $result order by $i descending return $i";
    final String assertion = """
          3 2 1
        """.strip();
    test(storeQuery, updateQuery1, updateQuery2, query, assertion);
  }

  @Test
  public void testArray() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','[{"generic": 1, "location": {"state": "CA", "city": "Los Angeles"}}, {"generic": 1, "location": {"state": "NY", "city": "New York"}}]')
        """;
    final String query = "for $i in jn:doc('mycol.jn','mydoc.jn') where deep-equal($i=>generic, 1) return {$i,'nodeKey': sdb:nodekey($i)}";
    final String assertion = """
          {"generic":1,"location":{"state":"CA","city":"Los Angeles"},"nodeKey":2} {"generic":1,"location":{"state":"NY","city":"New York"},"nodeKey":11}
        """.strip();
    test(storeQuery, query, assertion);
  }

  @Test
  public void testReplaceInArray() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",true,false,null]')";
    final String updateQuery = """
          replace json value of jn:doc('mycol.jn','mydoc.jn')[[1]] with "yes"
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "[\"foo\",\"yes\",false,null]");
  }

  @Test
  public void testRemoveFromArray() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",true,false,null]')";
    final String updateQuery = """
          delete json jn:doc('mycol.jn','mydoc.jn')[[1]]
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "[\"foo\",false,null]");
  }

  @Test
  public void testAppendToArray() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",true,false,null]')";
    final String updateQuery = """
          append json (1, 2, 3) into jn:doc('mycol.jn','mydoc.jn')
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "[\"foo\",true,false,null,[1,2,3]]");
  }

  @Test
  public void testInsertIntoArray1() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",true,false,null]')";
    final String updateQuery = """
          insert json (1, 2, 3) into jn:doc('mycol.jn','mydoc.jn') at position 3
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "[\"foo\",true,false,[1,2,3],null]");
  }

  @Test
  public void testInsertIntoArray2() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",true,false,null]')";
    final String updateQuery = """
          insert json { "name": "keyword" } into jn:doc('mycol.jn','mydoc.jn') at position 3
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "[\"foo\",true,false,{\"name\":\"keyword\"},null]");
  }

  @Test
  public void testInsertIntoArrayAsFirst() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",true,false,null]')";
    final String updateQuery = """
          insert json (1, 2, 3) into jn:doc('mycol.jn','mydoc.jn') at position 0
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "[[1,2,3],\"foo\",true,false,null]");
  }

  @Test
  public void testInsertIntoObject() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','{"foo": "bar"}')
        """;
    final String updateQuery = """
          insert json {"baz": true()} into jn:doc('mycol.jn','mydoc.jn')
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "{\"foo\":\"bar\",\"baz\":true}");
  }

  @Test
  public void testRemoveFromObject() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','{"foo": "bar", "baz": true}')
        """;
    final String updateQuery = """
          delete json jn:doc('mycol.jn','mydoc.jn')=>foo
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "{\"baz\":true}");
  }

  @Test
  public void testRenameFieldInObject() throws IOException {
    final String storeQuery = """
          jn:store('mycol.jn','mydoc.jn','{"foo": "bar", "baz": true}')
        """;
    final String updateQuery = """
          rename json jn:doc('mycol.jn','mydoc.jn')=>foo as "buzz"
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "{\"buzz\":\"bar\",\"baz\":true}");
  }

  @Test
  public void testReplaceInObject() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','{\"foo\": \"bar\", \"baz\": true}')";
    final String updateQuery = """
          replace json value of jn:doc('mycol.jn','mydoc.jn')=>baz with "yes"
        """;
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";
    test(storeQuery, updateQuery, openQuery, "{\"foo\":\"bar\",\"baz\":\"yes\"}");
  }

  @Test
  public void testArrayIteration() throws IOException {
    final String storeQuery =
        "jn:store('mycol.jn','mydoc.jn','[{\"key\":0,\"value\":true},{\"key\":\"hey\",\"value\":false}]')";
    final String openQuery =
        "for $i in jn:doc('mycol.jn','mydoc.jn') where deep-equal($i=>key, 0) return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, openQuery, "{\"key\":0,\"value\":true,\"nodekey\":2}");
  }

  @Test
  public void testNesting1() throws IOException {
    final String storeQuery =
        "jn:store('mycol.jn','mydoc.jn','[{\"key\":0},{\"value\":{\"key\":true}},{\"key\":\"hey\",\"value\":false}]')";
    final String openQuery =
        "for $i in jn:doc('mycol.jn','mydoc.jn')[]=>value where $i instance of object() and $i=>key eq true() return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, openQuery, "{\"key\":true,\"nodekey\":7}");
  }

  // Path index.
  @Test
  public void testNesting2() throws IOException {
    final String storeQuery =
        "jn:store('mycol.jn','mydoc.jn','[{\"key\":0},{\"value\":[{\"key\":{\"boolean\":true}},{\"newkey\":\"yes\"}]},{\"key\":\"hey\",\"value\":false}]')";
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('//*', '//[]')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "for $i in jn:doc('mycol.jn','mydoc.jn')[]=>value[]=>key[.=>boolean] return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, indexQuery, openQuery, "{\"boolean\":true,\"nodekey\":10}");
  }

  // CAS index.
  @Test
  public void testNesting3() throws IOException {
    final String storeQuery =
        "jn:store('mycol.jn','mydoc.jn','[{\"key\":0},{\"value\":[{\"key\":{\"boolean\":5}},{\"newkey\":\"yes\"}]},{\"key\":\"hey\",\"value\":false}]')";
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:integer', '/[]/value/[]/key/boolean') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "for $i in jn:doc('mycol.jn','mydoc.jn')[[1]]=>value[]=>key[.=>boolean gt 3] return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, indexQuery, openQuery, "{\"boolean\":5,\"nodekey\":10}");
  }

  @Test
  public void testNesting4() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("twitter.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/statuses/[]/user/entities/url/urls/[]/url') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "for $i in jn:doc('mycol.jn','mydoc.jn')=>statuses[]=>user=>entities=>url[.=>urls[]=>url eq 'https://t.co/TcEE6NS8nD'] order by sdb:nodekey($i) return {\"result\": $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting4").resolve("expectedOutput")));
  }

  @Test
  public void testNesting5() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting5").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('/paths/\\/consolidated_screening_list\\/search/get/tags/[]', '/paths/\\/consolidated_screening_list\\/search/get/tags')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>paths=>\"/consolidated_screening_list/search\"=>get=>parameters return { \"result\": $result, \"nodekey\": sdb:nodekey($result) }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting5").resolve("expectedOutput")));
  }

  @Test
  public void testNesting6() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting6").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('/paths/\\/consolidated_screening_list\\/search/get','/paths/\\/consolidated_screening_list\\/search/get/parameters/[]/name')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery = """
        for $i in jn:doc('mycol.jn','mydoc.jn')=>paths=>"/consolidated_screening_list/search"=>get
        let $j := $i=>parameters[]=>name
        return for $k in $j
               where $k eq 'keyword'
               return { "result": $i, "nodekey": sdb:nodekey($i) }
               """.stripIndent();
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting6").resolve("expectedOutput")));
  }

  @Test
  public void testNesting7() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting7").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/paths/\\/consolidated_screening_list\\/search/get/parameters/[]/name') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>paths=>\"/consolidated_screening_list/search\"=>get[.=>parameters[]=>name = 'keyword'] return { \"result\": $result, \"nodekey\": sdb:nodekey($result) }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting7").resolve("expectedOutput")));
  }

  @Test
  public void testNesting8() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting8").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/paths/\\/consolidated_screening_list\\/search/get/parameters/[]/name') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>paths=>\"/consolidated_screening_list/search\"=>get=>parameters[[3]]=>name return { \"result\": $result }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting8").resolve("expectedOutput")));
  }

  @Test
  public void testNesting9() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting9").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada/[]/foo') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[1]]=>revision=>tada[[0]]=>foo return { \"result\": $result }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting9").resolve("expectedOutput")));
  }

  @Test
  public void testNesting10() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting10").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[1]]=>revision=>tada[[0]] return { \"result\": $result }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting10").resolve("expectedOutput")));
  }

  @Test
  public void testNesting11() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting11").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada/[]') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[[4]] return { \"result\": $result }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting11").resolve("expectedOutput")));
  }

  @Test
  public void testNesting12() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting12").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('/sirix/[]/revision/tada/[]/foo','/sirix/[]/revision/tada/[]/[]/foo')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[][]=>foo return $result";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting12").resolve("expectedOutput")));
  }

  @Test
  public void testNesting13() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting13").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/sirix/[]/revision/tada//[]/foo/[]/baz') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[.[][]=>foo[]=>baz = 'bar'] return $result";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting13").resolve("expectedOutput")));
  }

  @Test
  public void testNesting14() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting14").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada//[]/foo/[]/baz') return {\"revision\": sdb:commit($doc)}";
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[[4]][]=>foo[[1]]=>baz";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting14").resolve("expectedOutput")));
  }

  @Test
  public void testNesting15() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting15").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada//[]/foo/[]/baz') return {\"revision\": sdb:commit($doc)}";
    final String openQuery = "jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[[4]][[0]]=>foo[[1]]=>baz";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting15").resolve("expectedOutput")));
  }

  @Test
  public void testNesting16() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting16").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada//[]/foo/[]/baz') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $baz := jn:doc('mycol.jn','mydoc.jn') let $return := $baz=>sirix[[2]]=>revision=>tada[[4]][[0]]=>foo[[1]]=>baz return $return";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting16").resolve("expectedOutput")));
  }

  // Same as 6 with one index access
  @Test
  public void testNesting17() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting17").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('/paths/\\/consolidated_screening_list\\/search/get')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery = """
        for $i in jn:doc('mycol.jn','mydoc.jn')=>paths=>"/consolidated_screening_list/search"=>get
        let $j := $i=>parameters[]=>name
        return for $k in $j
               where $k eq 'keyword'
               return { "result": $i, "nodekey": sdb:nodekey($i) }
               """.stripIndent();
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting17").resolve("expectedOutput")));
  }

  // Same as 6 without index access
  @Test
  public void testNesting18() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting18").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('/pathx')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery = """
        for $i in jn:doc('mycol.jn','mydoc.jn')=>paths=>"/consolidated_screening_list/search"=>get
        let $j := $i=>parameters[]=>name
        return for $k in $j
               where $k eq 'keyword'
               return { "result": $i, "nodekey": sdb:nodekey($i) }
               """.stripIndent();
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting18").resolve("expectedOutput")));
  }

  @Test
  public void testNesting19() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting19").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('foo')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery = """
        for $i in jn:doc('mycol.jn','mydoc.jn')=>paths=>"/consolidated_screening_list/search"
        let $j := $i=>get
        let $l := $j=>parameters[]=>name
        return for $k in $l
               where $k eq 'keyword'
               return { "result": $i, "nodekey": sdb:nodekey($i) }
               """.stripIndent();
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting19").resolve("expectedOutput")));
  }

  @Test
  public void testNesting20() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting20").resolve("trade-apis.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, ('/paths/\\/consolidated_screening_list\\/search','/paths/\\/consolidated_screening_list\\/search/get/parameters/[]/name')) return {\"revision\": sdb:commit($doc)}";
    final String openQuery = """
        for $i in jn:doc('mycol.jn','mydoc.jn')=>paths=>"/consolidated_screening_list/search"
        let $j := $i=>get
        let $l := $j=>parameters[]=>name
        return for $k in $l
               where $k eq 'keyword'
               return { "result": $i, "nodekey": sdb:nodekey($i) }
               """.stripIndent();
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting20").resolve("expectedOutput")));
  }

  @Test
  public void testNesting21() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting21").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/sirix/[]/revision/tada//[]/foo/[]/baz') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[.[][]=>foo[]=>baz >= 'baa' and .[][]=>foo[]=>baz <= 'brr'] return $result";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting21").resolve("expectedOutput")));
  }

  @Test
  public void testNesting22() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting22").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-name-index($doc, 'foo') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[]=>revision=>foo return $result";
    test(storeQuery,
        indexQuery,
        openQuery,
        Files.readString(JSON_RESOURCE_PATH.resolve("testNesting22").resolve("expectedOutput")));
  }

  @Test
  public void testNesting23() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting23").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-name-index($doc, 'revision') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[]=>revision=>foo return $result";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting22").resolve("expectedOutput")));
  }

  @Test
  public void testNesting24() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting24").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/sirix/[]/revision/tada//[]/foo/[]/baz') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[][]=>foo[]=>baz[starts-with(., 'ba')] return $result";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting24").resolve("expectedOutput")));
  }

  @Test
  public void testNesting25() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("testNesting25").resolve("multiple-revisions.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri);
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-path-index($doc, '/sirix/[]/revision/tada//[]/foo/[]') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "let $result := jn:doc('mycol.jn','mydoc.jn')=>sirix[[2]]=>revision=>tada[][]=>foo[] return $result";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting25").resolve("expectedOutput")));
  }
}
