package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.QueryException;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.MutationGate;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.io.StorageType;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.BasicXmlDBStore;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code xn:store} and {@code xn:load} into an existing database (#1384), with the same rule as
 * {@code jn:store} and {@code jn:load}: a free resource name adds the resource; a name that is
 * taken is refused with {@code SIRIXDBF0006} and nothing is written, whatever {@code $create-new}
 * says. No call drops the database or replaces a resource in place, and every other resource keeps
 * its content, revisions and indexes.
 */
final class StoreIntoExistingDatabaseTest {
  private static final String OTHER = "xn:doc('db','other')";

  @TempDir
  Path directory;

  private Path content;

  @BeforeEach
  void writeInputFiles() throws IOException {
    content = Files.createDirectories(directory.resolve("input"));
    Files.writeString(content.resolve("new.xml"), "<v>new</v>");
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void aNewResourceNameAddsTheResourceAndKeepsTheRest(final String function) {
    for (final String flag : new String[] {"", ",true()", ",false()"}) {
      withDatabase(function + flag, (chain, context, store) -> {
        final Witness before = witness(chain, context, store);
        run(chain, context, call(function, "added", flag));
        assertEquals(List.of("added", "first", "other"), resources(store));
        assertEquals("<v>new</v>", run(chain, context, "xn:doc('db','added')"));
        assertEquals(before, witness(chain, context, store), function + flag);
      });
    }
  }

  /**
   * A name that is already taken is refused with {@code SIRIXDBF0006} and nothing is written, for
   * every spelling of {@code $create-new}: these functions never replace a resource in place.
   */
  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void anExistingNameIsRefusedUnchangedWhateverCreateNewSays(final String function) {
    for (final String flag : new String[] {"", ",true()", ",false()"}) {
      withDatabase(function + flag, (chain, context, store) -> {
        final Witness before = witness(chain, context, store);
        final QueryException refused = assertThrows(QueryException.class,
            () -> run(chain, context, call(function, "first", flag)), function + flag);
        assertEquals(XMLFun.ERR_RESOURCE_EXISTS, refused.getCode(), refused.getMessage());
        assertEquals("SIRIXDBF0006", refused.getCode().getLocalName());
        assertEquals(List.of("first", "other"), resources(store), function + flag);
        assertEquals("<v>first-2</v>", run(chain, context, "xn:doc('db','first')"), function + flag);
        assertEquals(2, revisions(store, "first"), function + flag);
        assertEquals(before, witness(chain, context, store), function + flag);
      });
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void severalItemsAreAddedUnderNewNamesWithoutReplacingAnything(final String function) {
    withDatabase(function, (chain, context, store) -> {
      final Witness before = witness(chain, context, store);
      final String file = "'" + content.resolve("new.xml").toUri() + "'";
      final String items = "store".equals(function)
          ? "(<a/>,<b/>)"
          : "(" + file + "," + file + ")";
      run(chain, context, "xn:" + function + "('db',()," + items + ")");
      assertEquals(4, resources(store).size(), resources(store).toString());
      assertEquals(2, revisions(store, "first"));
      assertEquals("<v>first-2</v>", run(chain, context, "xn:doc('db','first')"));
      assertEquals(before, witness(chain, context, store));
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"", ",true()", ",false()"})
  void aNamedMultiInputLoadUsesNewAutomaticNames(final String flag) throws IOException {
    Files.writeString(content.resolve("second.xml"), "<w>second</w>");
    final String inputs =
        "('" + content.resolve("new.xml").toUri() + "','" + content.resolve("second.xml").toUri() + "')";
    withDatabase("named-multi" + flag, (chain, context, store) -> {
      final Witness before = witness(chain, context, store);
      run(chain, context, "xn:load('db','first'," + inputs + flag + ")");
      assertEquals(2, revisions(store, "first"));
      assertEquals("<v>first-2</v>", run(chain, context, "xn:doc('db','first')"));
      assertEquals(List.of("first", "other", "resource3", "resource4"), resources(store));
      assertEquals("<v>new</v>", run(chain, context, "xn:doc('db','resource3')"));
      assertEquals("<w>second</w>", run(chain, context, "xn:doc('db','resource4')"));
      assertEquals(before, witness(chain, context, store));
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "/*"})
  void aSelfReferentialInputIsRefusedUnchanged(final String axis) {
    withDatabase("self" + axis.length(), (chain, context, store) -> {
      final QueryException refused = assertThrows(QueryException.class,
          () -> run(chain, context, "xn:store('db','first',xn:doc('db','first')" + axis + ")"), axis);
      assertEquals(XMLFun.ERR_RESOURCE_EXISTS, refused.getCode(), refused.getMessage());
      assertEquals("<v>first-2</v>", run(chain, context, "xn:doc('db','first')"), axis);
      assertEquals(2, revisions(store, "first"), axis);
      assertEquals(List.of("first", "other"), resources(store), axis);
    });
  }

  @Test
  void aLazySingletonLoadKeepsItsNamedTargetInANewDatabase() {
    withDatabase("lazy", (chain, context, store) -> {
      final String uri = content.resolve("new.xml").toUri().toString();
      run(chain, context, "xn:store('db','uris',<uris><uri>" + uri + "</uri></uris>)");
      run(chain, context, "xn:load('created','chosen',(for $u in xn:doc('db','uris')/uris/uri return string($u)))");
      assertEquals("<v>new</v>", run(chain, context, "xn:doc('created','chosen')"));
      assertEquals(1, store.lookup("created").getDatabase().listResources().size());
    });
  }

  /** xn:load returns the collection it loaded into, whether it created it or added to it. */
  @Test
  void loadReturnsTheCollectionItLoadedInto() {
    withDatabase("load-result", (chain, context, store) -> {
      assertEquals("true", run(chain, context, "exists(" + call("load", "added", "") + ")"));
      assertEquals("true",
          run(chain, context, "exists(" + call("load", "first", "").replace("'db'", "'created'") + ")"));
    });
  }

  /**
   * A caller that finds the database missing creates it under the exclusive database hold and looks
   * again once it holds it: a database another caller created meanwhile is written into, not dropped
   * and recreated.
   */
  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void aDatabaseCreatedByARacingCallerIsKept(final String function) throws Exception {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve(function)).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final Thread caller;
      try (var gate = MutationGate.acquireDatabase(store.databaseDirectory("db"))) {
        caller = new Thread(() -> run(chain, context, call(function, "added", "")));
        caller.start();
        // Without the hold the caller would create the database now; with it, it parks.
        while (caller.getState() != Thread.State.WAITING && caller.isAlive()) {
          Thread.onSpinWait();
        }
        store.create("db", "racer", new DocumentParser("<v>racer</v>"));
      }
      caller.join();
      assertEquals(List.of("added", "racer"), resources(store));
      assertEquals("<v>racer</v>", run(chain, context, "xn:doc('db','racer')"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"store", "load"})
  void importsHonorTheConfiguredCommitBound(final String function) throws IOException {
    final String input = "<root>" + "<item/>".repeat(128) + "</root>";
    final Path source = Files.writeString(directory.resolve("bounded.xml"), input);
    try (
        final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                     .location(directory.resolve("bounded-" + function))
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .numberOfNodesBeforeAutoCommit(16)
                                                     .build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      store.create("db", "kept", new DocumentParser("<kept/>"));
      final String argument = "store".equals(function)
          ? input
          : "'" + source.toUri() + "'";
      if ("load".equals(function)) {
        final Str uri = new Str(source.toUri().toString());
        new Load(true).execute(null, context, new Sequence[] {new Str("db"), new Str("named"), uri});
        new Load(true).execute(null, context, new Sequence[] {new Str("db"), null, new ItemSequence(uri, uri)});
      } else {
        new Query(chain, "xn:store('db','named'," + argument + ")").evaluate(context);
        new Query(chain, "xn:store('db',(),(" + argument + "," + argument + "))").evaluate(context);
      }
      for (final String resource : resources(store)) {
        if (resource.equals("kept")) {
          continue;
        }
        final XmlResourceSession session = store.lookup("db").getDatabase().beginResourceSession(resource);
        assertEquals(0, session.activeTrxCount());
        assertTrue(session.getMostRecentRevisionNumber() > 1,
            "imports must checkpoint beyond the configured node bound");
        assertEquals(input, run(chain, context, "xn:doc('db','" + resource + "')"));
      }
      final var added = store.lookup("db").add("ordinary", new DocumentParser(input));
      try (final var transaction = added.getTrx()) {
        assertEquals(1, transaction.getResourceSession().activeTrxCount());
        assertEquals(1, transaction.getResourceSession().getMostRecentRevisionNumber());
      }
    }
  }

  private record Witness(String content, int revisions, String index) {
  }

  private Witness witness(final SirixCompileChain chain, final SirixQueryContext context, final BasicXmlDBStore store) {
    return new Witness(run(chain, context, OTHER), revisions(store, "other"),
        run(chain, context, "xn:find-path-index(" + OTHER + ",'/v')"));
  }

  private String call(final String function, final String resource, final String flag) {
    final String input = "store".equals(function)
        ? "<v>new</v>"
        : "'" + content.resolve("new.xml").toUri() + "'";
    return "xn:" + function + "('db','" + resource + "'," + input + flag + ")";
  }

  private interface Body {
    void run(SirixCompileChain chain, SirixQueryContext context, BasicXmlDBStore store);
  }

  /**
   * Database {@code db}: {@code first} with two revisions, {@code other} with two and a path index.
   */
  private void withDatabase(final String name, final Body body) {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve(name)).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      run(chain, context, "xn:store('db','first',<v>first-1</v>)");
      run(chain, context, "xn:store('db','other',<v>other-1</v>,false())");
      run(chain, context, "replace value of node xn:doc('db','first')/v with 'first-2'");
      run(chain, context, "replace value of node " + OTHER + "/v with 'other-2'");
      run(chain, context,
          "let $doc := " + OTHER + " let $i := xn:create-path-index($doc,'/v') return sdb:commit($doc)");
      body.run(chain, context, store);
    }
  }

  private static List<String> resources(final BasicXmlDBStore store) {
    return store.lookup("db")
                .getDatabase()
                .listResources()
                .stream()
                .map(path -> path.getFileName().toString())
                .sorted()
                .toList();
  }

  private static int revisions(final BasicXmlDBStore store, final String resource) {
    final XmlResourceSession session = store.lookup("db").getDatabase().beginResourceSession(resource);
    return session.getMostRecentRevisionNumber();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(context, writer);
    }
    return out.toString();
  }
}
