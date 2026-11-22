package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code xn:store($coll, $res, $nodes, $create-new)} stores a single node as resource {@code $res}
 * on every path, like {@code xn:load} and {@code jn:store}. Without {@code $res}, and for a
 * sequence of several nodes, resources keep their automatic names.
 */
final class StoreResourceNameTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"%s", "xn:sort(%s)", "(for $n in (%s, <unused/>) where name($n) != 'unused' return $n)"})
  void aSingleNodeIsStoredUnderTheGivenResourceNameOnEveryPath(final String representation) {
    final String a = representation.formatted("<a/>");
    final String b = representation.formatted("<b/>");
    final String c = representation.formatted("<c/>");
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      // A new database, by default and when the collection does not exist yet.
      run(chain, context, "xn:store('created','products'," + a + ")");
      assertEquals(List.of("products"), resources(store, "created"));
      assertEquals("<a/>", run(chain, context, "xn:doc('created','products')"));
      run(chain, context, "xn:store('missing','products'," + a + ",false())");
      assertEquals(List.of("products"), resources(store, "missing"));
      assertEquals("<a/>", run(chain, context, "xn:doc('missing','products')"));
      // Recreating replaces the database, again under the given name.
      run(chain, context, "xn:store('created','orders'," + b + ")");
      assertEquals(List.of("orders"), resources(store, "created"));
      assertEquals("<b/>", run(chain, context, "xn:doc('created','orders')"));
      // Appending to an existing collection.
      run(chain, context, "xn:store('created','customers'," + c + ",false())");
      assertEquals(List.of("customers", "orders"), resources(store, "created"));
      assertEquals("<c/>", run(chain, context, "xn:doc('created','customers')"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%s", "xn:sort(%s)", "(for $n in (%s, <unused/>) where name($n) != 'unused' return $n)"})
  void withoutANameOrForSeveralNodesResourcesKeepTheirAutomaticNames(final String representation) {
    final String one = representation.formatted("<a/>");
    final String several = representation.formatted("(<a/>,<b/>)");
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      run(chain, context, "xn:store('unnamed',()," + one + ")");
      assertEquals(List.of("resource1"), resources(store, "unnamed"));
      run(chain, context, "xn:store('unnamed',()," + one + ",false())");
      assertEquals(List.of("resource", "resource1"), resources(store, "unnamed"));
      run(chain, context, "xn:store('unnamed',()," + one + ")");
      assertEquals(List.of("resource1"), resources(store, "unnamed"));
      run(chain, context, "xn:store('unnamed-missing',()," + one + ",false())");
      assertEquals(List.of("resource1"), resources(store, "unnamed-missing"));
      for (final String name : List.of("'ignored'", "()")) {
        run(chain, context, "xn:store('several'," + name + "," + several + ")");
        assertEquals(List.of("resource1", "resource2"), resources(store, "several"));
        run(chain, context, "xn:store('several'," + name + "," + several + ",false())");
        assertEquals(List.of("resource1", "resource2", "resource3", "resource4"), resources(store, "several"));
        assertEquals("<a/>", run(chain, context, "xn:doc('several','resource3')"));
        assertEquals("<b/>", run(chain, context, "xn:doc('several','resource4')"));
      }
      run(chain, context, "xn:store('several-missing','ignored'," + several + ",false())");
      assertEquals(List.of("resource1", "resource2"), resources(store, "several-missing"));
      final String three = representation.formatted("(<a/>,<b/>,<c/>)");
      run(chain, context, "xn:store('three','ignored'," + three + ")");
      assertEquals(List.of("resource1", "resource2", "resource3"), resources(store, "three"));
      assertEquals("<c/>", run(chain, context, "xn:doc('three','resource3')"));
      run(chain, context, "xn:store('three','ignored'," + three + ",false())");
      assertEquals(List.of("resource1", "resource2", "resource3", "resource4", "resource5", "resource6"),
          resources(store, "three"));
      assertEquals("<c/>", run(chain, context, "xn:doc('three','resource6')"));
    }
  }

  private static List<String> resources(final BasicXmlDBStore store, final String collection) {
    return store.lookup(collection)
                .getDatabase()
                .listResources()
                .stream()
                .map(path -> path.getFileName().toString())
                .sorted()
                .toList();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(context, writer);
    }
    return out.toString();
  }
}
