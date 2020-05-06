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
}
