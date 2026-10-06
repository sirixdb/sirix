package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlCollectionQueryRegressionTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void batchLoadPreservesExistingDocuments(final VersioningType versioning) throws Exception {
    final Path first = Files.writeString(directory.resolve("first.xml"), "<value>first</value>");
    final Path second = Files.writeString(directory.resolve("second.xml"), "<value>second</value>");
    try (final BasicXmlDBStore store = openStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<value>seed</value>"));
      new Query(chain, "xn:load('collection',(),('" + first.toUri() + "','" + second.toUri() + "'),false())").evaluate(
          context);
      assertEquals(3, collection.getDocumentCount());
      assertEquals("seed", value(chain, context, "resource1", ""));
      assertEquals("first", value(chain, context, "resource2", ""));
      assertEquals("second", value(chain, context, "resource3", ""));
      for (final String name : new String[] {"resource2", "resource3"}) {
        assertEquals(versioning,
            collection.getDatabase().beginResourceSession(name).getResourceConfig().versioningType);
      }
    }
    try (final BasicXmlDBStore store = openStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      assertEquals(3, store.lookup("collection").getDocumentCount());
      assertEquals("seed", value(chain, context, "resource1", ""));
      assertEquals("first", value(chain, context, "resource2", ""));
      assertEquals("second", value(chain, context, "resource3", ""));
    }
  }

  @Test
  void batchLoadCreatesMissingCollection() throws Exception {
    final Path first = Files.writeString(directory.resolve("first.xml"), "<value>first</value>");
    final Path second = Files.writeString(directory.resolve("second.xml"), "<value>second</value>");
    try (final BasicXmlDBStore store = openStore(VersioningType.SLIDING_SNAPSHOT);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      new Query(chain, "xn:load('collection',(),('" + first.toUri() + "','" + second.toUri() + "'),false())").evaluate(
          context);
      assertEquals(2, store.lookup("collection").getDocumentCount());
      assertEquals("first", value(chain, context, "resource1", ""));
      assertEquals("second", value(chain, context, "resource2", ""));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedBatchLoadPreservesSeedAfterReopen(final boolean malformed) throws Exception {
    final Path invalid = malformed
        ? Files.writeString(directory.resolve("malformed.xml"), "<value>")
        : directory.resolve("missing.xml");
    final Path valid = Files.writeString(directory.resolve("valid.xml"), "<value>valid</value>");
    try (final BasicXmlDBStore store = openStore(VersioningType.SLIDING_SNAPSHOT);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      store.create("collection", new DocumentParser("<value>seed</value>"));
      assertThrows(QueryException.class,
          () -> new Query(chain,
              "xn:load('collection',(),('" + invalid.toUri() + "','" + valid.toUri() + "'),false())").evaluate(
                  context));
    }
    try (final BasicXmlDBStore store = openStore(VersioningType.SLIDING_SNAPSHOT);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      assertTrue(store.lookup("collection").getDocumentCount() >= 1);
      assertEquals("seed", value(chain, context, "resource1", ""));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void addedResourceHistorySurvivesXqueryUpdate(final VersioningType versioning) {
    try (final BasicXmlDBStore store = openStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      assertNotNull(collection.add("tree", new DocumentParser("<value>one</value>")));
      final Instant timestamp;
      try (final var trx = collection.getDatabase().beginResourceSession("tree").beginNodeReadOnlyTrx(1)) {
        timestamp = trx.getRevisionTimestamp();
      }
      new Query(chain, "replace value of node xn:doc('collection','tree')/value/text() with 'two'").evaluate(context);
      assertEquals("two", value(chain, context, "tree", ""));
      assertEquals("one", value(chain, context, "tree", ",1"));
      assertEquals("one", collection.getDocument("tree", timestamp).getValue().stringValue());
      new Query(chain, "replace value of node xn:doc('collection','tree')/value/text() with 'three'").evaluate(context);
      assertEquals("three", value(chain, context, "tree", ""));
      assertEquals("two", value(chain, context, "tree", ",2"));
      assertEquals("one", value(chain, context, "tree", ",1"));
      assertEquals("one", collection.getDocument("tree", timestamp).getValue().stringValue());
    }
  }

  private BasicXmlDBStore openStore(final VersioningType versioning) {
    return BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }

  private static String value(final SirixCompileChain chain, final SirixQueryContext context, final String resource,
      final String revision) {
    return ((Atomic) new Query(chain, "string(xn:doc('collection','" + resource + "'" + revision + "))").evaluate(
        context)).stringValue();
  }
}
