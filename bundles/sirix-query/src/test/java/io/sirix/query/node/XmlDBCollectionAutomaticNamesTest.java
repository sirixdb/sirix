package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.MutationGate;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.MutationAuthorizer;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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
      final XmlDBNode added = collection.add(new DocumentParser("<added/>"));
      assertEquals("resource4", added.getTrx().getResourceSession().getResourceConfig().getName(),
          "add returns the document it stored");
      added.getTrx().close();
      assertEquals(List.of("resource1", "resource3", "resource4"), resources(store));
      assertEquals(1, revisions(store, "resource3"));
      assertEquals("<kept/>", run(chain, context, "xn:doc('db','resource3')"));
      assertEquals("<added/>", run(chain, context, "xn:doc('db','resource4')"));
    }
  }

  /** An automatic name creates a resource, so it needs CREATE; a refusal creates nothing. */
  @Test
  void anAutomaticNameNeedsCreate() {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve("db")).build()) {
      final XmlDBCollection collection = store.create("db", "resource1", new DocumentParser("<kept/>"));
      final MutationAuthorizer refusing = mock(MutationAuthorizer.class);
      final SecurityException refusal = new SecurityException("create refused");
      doThrow(refusal).when(refusing).authorizeCreate(any(), any());
      final SecurityException thrown = assertThrows(SecurityException.class,
          () -> MutationAuthorization.where(refusing)
                                     .run(() -> collection.addAll(List.of(new DocumentParser("<added/>")))));
      assertSame(refusal, thrown);
      assertEquals(List.of("resource1"), resources(store));
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
