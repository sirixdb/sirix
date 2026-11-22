package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.MutationGate;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Several XML documents added to an existing collection get automatic names that are always new
 * (#1478): an existing resource is never written into, whatever names the collection already holds
 * and whatever a concurrent caller creates meanwhile.
 */
final class XmlDBCollectionAutomaticNamesTest {
  @TempDir
  Path directory;

  /** {@code resource1} and {@code resource3} exist, so the former naming (size + 1) hit resource3. */
  @ParameterizedTest
  @ValueSource(strings = {"xn:store", "xn:load", "addAll"})
  void automaticNamesSkipEveryExistingName(final String entryPoint) throws IOException {
    final Path first = Files.writeString(directory.resolve("first.xml"), "<a/>");
    final Path second = Files.writeString(directory.resolve("second.xml"), "<b/>");
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("db", "resource1", new DocumentParser("<kept/>"));
      run(chain, context, "xn:store('db','resource3',<kept/>,false())");
      switch (entryPoint) {
        case "xn:store" -> run(chain, context, "xn:store('db',(),(<a/>,<b/>),false())");
        case "xn:load" ->
          run(chain, context, "xn:load('db',(),('" + first.toUri() + "','" + second.toUri() + "'),false())");
        case "addAll" -> collection.addAll(List.of(new DocumentParser("<a/>"), new DocumentParser("<b/>")));
        default -> throw new IllegalArgumentException(entryPoint);
      }
      assertEquals(List.of("resource1", "resource3", "resource4", "resource5"), resources(store));
      for (final String kept : List.of("resource1", "resource3")) {
        assertEquals(1, revisions(store, kept), kept);
        assertEquals("<kept/>", run(chain, context, "xn:doc('db','" + kept + "')"));
      }
      assertEquals("<a/>", run(chain, context, "xn:doc('db','resource4')"));
      assertEquals("<b/>", run(chain, context, "xn:doc('db','resource5')"));
    }
  }

  /** A single unnamed document also takes a free name: {@code resource3} (size + 1) exists here. */
  @Test
  void aSingleUnnamedDocumentTakesAFreeName() {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("db", "resource1", new DocumentParser("<kept/>"));
      run(chain, context, "xn:store('db','resource3',<kept/>,false())");
      collection.add(new DocumentParser("<added/>")).getTrx().close();
      assertEquals(List.of("resource1", "resource3", "resource4"), resources(store));
      assertEquals(1, revisions(store, "resource3"));
      assertEquals("<kept/>", run(chain, context, "xn:doc('db','resource3')"));
      assertEquals("<added/>", run(chain, context, "xn:doc('db','resource4')"));
    }
  }

  /** A name another caller creates while this one waits for it is skipped, not written into. */
  @Test
  void aNameCreatedConcurrentlyIsSkipped() throws Exception {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("db", "resource1", new DocumentParser("<first/>"));
      final Path databaseFile = collection.getDatabase().getDatabaseConfig().getDatabaseFile();
      final Thread caller;
      // One resource exists, so the caller's first candidate is resource2.
      try (var gate = MutationGate.acquireResource(databaseFile, "resource2")) {
        caller = new Thread(() -> collection.addAll(List.of(new DocumentParser("<added/>"))));
        caller.start();
        while (caller.getState() != Thread.State.WAITING && caller.isAlive()) {
          Thread.onSpinWait();
        }
        collection.add("resource2", new DocumentParser("<concurrent/>")).getTrx().close();
      }
      caller.join();
      assertEquals(List.of("resource1", "resource2", "resource3"), resources(store));
      assertEquals(1, revisions(store, "resource2"));
      assertEquals("<concurrent/>", run(chain, context, "xn:doc('db','resource2')"));
      assertEquals("<added/>", run(chain, context, "xn:doc('db','resource3')"));
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
