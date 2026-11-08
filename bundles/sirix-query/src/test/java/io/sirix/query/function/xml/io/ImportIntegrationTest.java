package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.ResourceConfiguration;
import io.sirix.io.StorageType;
import io.sirix.service.xml.serialize.XmlSerializer;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImportIntegrationTest {
  @TempDir
  Path directory;

  @Test
  void inlineAndLocalFileImportsPreserveIdentityAndHistory() throws Exception {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      store.create("versions", "doc", new DocumentParser("<root><stable>same</stable><changed>1</changed></root>"));
      final XmlDBNode stable = node(new Query(chain, "xn:doc('versions','doc')/root/stable").evaluate(context));
      final long key = stable.getNodeKey();
      final XmlDBNode imported = (XmlDBNode) new Query(chain,
          "xn:import('versions','doc','<root><stable>same</stable><changed>2</changed></root>')").evaluate(context);
      final var session = imported.getTrx().getResourceSession();
      assertEquals(2, session.getMostRecentRevisionNumber());
      final XmlDBNode unchanged = node(new Query(chain, "xn:doc('versions','doc')/root/stable").evaluate(context));
      assertEquals(key, unchanged.getNodeKey());
      assertEquals("1", ((Str) new Query(chain, "string(xn:doc('versions','doc',1)/root/changed)").evaluate(context)).stringValue());
      assertEquals("2", ((Str) new Query(chain, "string(xn:doc('versions','doc')/root/changed)").evaluate(context)).stringValue());
      final Path source = Files.writeString(directory.resolve("next.xml"), "<root><stable>same</stable><changed>3</changed></root>");
      new Query(chain, "xn:import('versions','doc','" + source + "')").evaluate(context);
      assertEquals(3, session.getMostRecentRevisionNumber());
      assertEquals("3", ((Str) new Query(chain, "string(xn:doc('versions','doc')/root/changed)").evaluate(context)).stringValue());
    }
  }

  @Test
  void rejectsMissingResourcesWithoutMutation() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final var collection = store.create("versions", "doc", new DocumentParser("<root/>"));
      final QueryException missing = assertThrows(QueryException.class,
          () -> new Query(chain, "xn:import('versions','missing','<root/>')").evaluate(context));
      assertTrue(missing.getMessage().contains("missing"));
      assertFalse(collection.getDatabase().existsResource("missing"));
      assertEquals(1, collection.getDocument("doc").getTrx().getResourceSession().getMostRecentRevisionNumber());
    }
  }

  @Test
  void omittedStoreResourceNamesPreserveCreateAndAppendBehavior() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      new Query(chain, "xn:store('created',(),<root/>)").evaluate(context);
      final var created = store.lookup("created");
      assertTrue(created.getDatabase().existsResource("resource1"));
      assertEquals(1, created.getDocumentCount());
      store.create("appended", "existing", new DocumentParser("<root/>"));
      new Query(chain, "xn:store('appended',(),<added/>,false())").evaluate(context);
      final var appended = store.lookup("appended");
      assertTrue(appended.getDatabase().existsResource("resource"));
      assertTrue(appended.getDatabase().existsResource("existing"));
      assertEquals(2, appended.getDocumentCount());
      assertFalse(appended.getDatabase().beginResourceSession("resource").getNodeTrx().isPresent());
      new Query(chain, "xn:store('appended',(),<again/>,false())").evaluate(context);
      assertEquals(2, appended.getDatabase().beginResourceSession("resource").getMostRecentRevisionNumber());
      assertEquals(2, appended.getDocumentCount());
    }
  }

  @Test
  void importsIntoAnExistingEmptyResourceWithoutClosingBorrowedReaders() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final var collection = store.create("versions");
      assertTrue(collection.getDatabase().createResource(ResourceConfiguration.newBuilder("doc")
          .storageType(StorageType.FILE_CHANNEL).build()));
      final var session = collection.getDatabase().beginResourceSession("doc");
      final int initialRevision = session.getMostRecentRevisionNumber();
      try (final var borrowedReader = session.beginNodeReadOnlyTrx()) {
        assertFalse(borrowedReader.hasFirstChild());
        final XmlDBNode imported = (XmlDBNode) new Query(chain,
            "xn:import('versions','doc','<root>imported</root>')").evaluate(context);
        assertSame(session, imported.getTrx().getResourceSession());
        assertFalse(session.isClosed());
        assertFalse(borrowedReader.isClosed());
        assertFalse(borrowedReader.hasFirstChild());
        assertFalse(session.getNodeTrx().isPresent());
        assertEquals(initialRevision + 1, session.getMostRecentRevisionNumber());
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        XmlSerializer.newBuilder(session, output, initialRevision + 1).build().call();
        assertEquals("<root>imported</root>", output.toString(StandardCharsets.UTF_8));
        try (final var historic = session.beginNodeReadOnlyTrx(initialRevision)) {
          assertFalse(historic.hasFirstChild());
        }
      }
    }
  }

  private static XmlDBNode node(final Sequence result) {
    if (result instanceof XmlDBNode node) {
      return node;
    }
    try (final Iter items = result.iterate()) {
      return (XmlDBNode) items.next();
    }
  }

}
