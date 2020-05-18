package org.sirix.xquery;

import org.junit.Test;

import java.io.IOException;

public final class JsonIntegrationTest extends AbstractJsonTest {
  @Test
  public void testArrayIteration() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[{\"key\":0,\"value\":true},{\"key\":\"hey\",\"value\":false}]')";
    final String openQuery = "for $i in jn:doc('mycol.jn','mydoc.jn') where deep-equal($i=>key, 0) return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, openQuery,"{\"key\":0,\"value\":true,\"nodekey\":2}");
  }

  @Test
  public void testNesting1() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[{\"key\":0},{\"value\":{\"key\":true}},{\"key\":\"hey\",\"value\":false}]')";
    final String openQuery = "for $i in jn:doc('mycol.jn','mydoc.jn')=>value where $i instance of record() and $i=>key eq true() return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, openQuery,"{\"key\":true,\"nodekey\":7}");
  }

  @Test
  public void testNesting2() throws IOException {
    final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[{\"key\":0},{\"value\":[{\"key\":{\"boolean\":true}},{\"newkey\":\"yes\"}]},{\"key\":\"hey\",\"value\":false}]')";
    final String openQuery = "for $i in jn:doc('mycol.jn','mydoc.jn')=>value=>key return { $i, \"nodekey\": sdb:nodekey($i) }";
    test(storeQuery, openQuery,"{\"boolean\":true,\"nodekey\":10}");
  }
}
