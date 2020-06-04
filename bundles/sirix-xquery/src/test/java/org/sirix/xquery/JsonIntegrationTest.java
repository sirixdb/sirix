package org.sirix.xquery;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public final class JsonIntegrationTest extends AbstractJsonTest {

  private static final Path JSON_RESOURCE_PATH = Path.of("src", "test", "resources", "json");

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
        "for $i in jn:doc('mycol.jn','mydoc.jn')=>value where $i instance of record() and $i=>key eq true() return { $i, \"nodekey\": sdb:nodekey($i) }";
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
        "for $i in jn:doc('mycol.jn','mydoc.jn')=>value=>key[[]][.=>boolean] return { $i, \"nodekey\": sdb:nodekey($i) }";
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
        "for $i in bit:array-values(jn:doc('mycol.jn','mydoc.jn')=>value=>key)[.=>boolean gt 3] return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, indexQuery, openQuery, "{\"boolean\":5,\"nodekey\":10}");
  }

  @Test
  public void testNesting4() throws IOException {
    final URI docUri = JSON_RESOURCE_PATH.resolve("twitter.json").toUri();
    final String storeQuery = String.format("jn:load('mycol.jn','mydoc.jn','%s')", docUri.toString());
    final String indexQuery =
        "let $doc := jn:doc('mycol.jn','mydoc.jn') let $stats := jn:create-cas-index($doc, 'xs:string', '/statuses/[]/user/entities/url/urls/[]/url') return {\"revision\": sdb:commit($doc)}";
    final String openQuery =
        "for $i in bit:array-values(bit:array-values(jn:doc('mycol.jn','mydoc.jn')=>statuses=>user=>entities=>url)[.=>urls=>url eq 'https://t.co/TcEE6NS8nD']) return { \"result\": $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery,
         indexQuery,
         openQuery,
         Files.readString(JSON_RESOURCE_PATH.resolve("testNesting4").resolve("expectedOutput")));
  }
}
