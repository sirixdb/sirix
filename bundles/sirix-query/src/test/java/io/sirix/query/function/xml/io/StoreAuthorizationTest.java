package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.MutationAuthorizer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * {@code xn:store} asks the query context's authorizer for CREATE on the target database and
 * resource before it touches the store, as {@code xn:load} and {@code jn:store} do; a refusal
 * leaves the store unchanged.
 */
final class StoreAuthorizationTest {
  private static final List<String> EVERY_PATH =
      List.of("xn:store('fresh','r',<a/>)", "xn:store('existing','added',<a/>,false())",
          "xn:store('existing','resource1',<a/>,false())", "xn:store('sequence',(),(<a/>,<b/>))");

  @TempDir
  Path directory;

  @Test
  void aRefusedCreateLeavesTheStoreUnchangedOnEveryPath() throws IOException {
    final SecurityException refusal = new SecurityException("create refused");
    final JsonDBStore authorizer = authorizer();
    doThrow(refusal).when((MutationAuthorizer) authorizer).authorizeCreate(any(), any());
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      store.create("existing", new DocumentParser("<root/>"));
      final List<String> databases = list(directory);
      final List<String> resources = list(directory.resolve("existing").resolve("resources"));
      for (final String query : EVERY_PATH) {
        final QueryException refused = assertThrows(QueryException.class, () -> run(store, authorizer, query), query);
        assertSame(refusal, refused.getCause(), query);
        assertEquals(databases, list(directory), query);
        assertEquals(resources, list(directory.resolve("existing").resolve("resources")), query);
      }
    }
  }

  @Test
  void aGrantedCreateStoresAndNamesTheTarget() throws IOException {
    final JsonDBStore authorizer = authorizer();
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      store.create("existing", new DocumentParser("<root/>"));
      run(store, authorizer, "xn:store('fresh','r',<a/>)");
      run(store, authorizer, "xn:store('existing','added',<a/>,false())");
      run(store, authorizer, "xn:store('sequence',(),(<a/>,<b/>))");
      verify((MutationAuthorizer) authorizer).authorizeCreate("fresh", "r");
      verify((MutationAuthorizer) authorizer).authorizeCreate("existing", "added");
      verify((MutationAuthorizer) authorizer).authorizeCreate("sequence", null);
      assertEquals(List.of("existing", "fresh", "sequence"), list(directory));
      assertEquals(List.of("added", "resource1"), list(directory.resolve("existing").resolve("resources")));
    }
  }

  /** A context store that authorizes mutations, as the REST stores are. */
  private static JsonDBStore authorizer() {
    return mock(JsonDBStore.class, withSettings().extraInterfaces(MutationAuthorizer.class));
  }

  private static void run(final BasicXmlDBStore store, final JsonDBStore authorizer, final String query) {
    try (final SirixCompileChain chain = SirixCompileChain.createWithNodeAndJsonStore(store, authorizer);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(store,
            authorizer, CommitStrategy.AUTO)) {
      new Query(chain, query).execute(context);
    }
  }

  private static List<String> list(final Path directory) throws IOException {
    try (final Stream<Path> entries = Files.list(directory)) {
      return entries.map(path -> path.getFileName().toString()).sorted().toList();
    }
  }
}
