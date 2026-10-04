package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.node.stream.ArrayStream;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlCollectionResourceConfigurationTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void everyAddPathUsesStoreSettingsAndPreservesHistory(final VersioningType versioning) throws Exception {
    for (final boolean pathSummary : new boolean[] {false, true}) {
      final Path location = directory.resolve(Boolean.toString(pathSummary));
      try (final BasicXmlDBStore store = configuredStore(location, versioning, pathSummary);
          final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
        final XmlDBCollection collection = store.create("collection", new DocumentParser("<seed/>"));
        assertConfiguration(collection, "resource1", versioning, pathSummary, false);
        collection.add("parser", new DocumentParser("<value>one</value>"));
        collection.add(new DocumentParser("<value>one</value>"));
        collection.add("timestamp", new DocumentParser("<value>one</value>"), "initial",
            Instant.parse("2020-01-01T00:00:00Z"));
        new Query(chain, "xml:store('collection','element',<value>one</value>,false())").evaluate(context);
        new Query(chain, "xml:store('collection','document',document {<value>one</value>},false())").evaluate(context);
        new Query(chain,
            "xml:store('collection',(),(<value>one</value>,document {<value>one</value>}),false())").evaluate(context);
        final Path first = Files.writeString(directory.resolve("first.xml"), "<value>one</value>");
        new Query(chain, "xml:load('collection','loaded','" + first.toUri() + "',false())").evaluate(context);
        assertEquals(9, collection.getDocumentCount());
        for (final Path resource : collection.getDatabase().listResources()) {
          final String name = resource.getFileName().toString();
          if (name.equals("resource1")) {
            continue;
          }
          assertConfiguration(collection, name, versioning, pathSummary, name.equals("timestamp"));
          writeHistory(collection, name);
        }
      }
      try (final BasicXmlDBStore store = configuredStore(location, versioning, pathSummary)) {
        final XmlDBCollection collection = store.lookup("collection");
        assertNotNull(collection);
        for (final Path resource : collection.getDatabase().listResources()) {
          final String name = resource.getFileName().toString();
          assertConfiguration(collection, name, versioning, pathSummary, name.equals("timestamp"));
          if (!name.equals("resource1")) {
            assertHistory(collection, name);
          }
        }
        collection.add("reopened", new DocumentParser("<value>one</value>"));
        assertConfiguration(collection, "reopened", versioning, pathSummary, false);
        writeHistory(collection, "reopened");
        assertHistory(collection, "reopened");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void parserStreamCollectionsCarryStoreSettings(final VersioningType versioning) {
    final Path location = directory.resolve("stream");
    try (final BasicXmlDBStore store = configuredStore(location, versioning, true)) {
      final XmlDBCollection collection = store.create("collection", new ArrayStream<>(new NodeSubtreeParser[0]));
      collection.add("added", new DocumentParser("<value>one</value>"));
      assertConfiguration(collection, "added", versioning, true, false);
      writeHistory(collection, "added");
      assertHistory(collection, "added");
    }
  }

  private static BasicXmlDBStore configuredStore(final Path location, final VersioningType versioning,
      final boolean pathSummary) {
    return BasicXmlDBStore.newBuilder()
                          .location(location)
                          .versioningType(versioning)
                          .storageType(pathSummary
                              ? StorageType.MEMORY_MAPPED
                              : StorageType.FILE_CHANNEL)
                          .hashType(HashType.NONE)
                          .storeDeweyIds(false)
                          .storeNodeHistory(false)
                          .buildPathSummary(pathSummary)
                          .buildPathStatistics(pathSummary)
                          .build();
  }

  private static void assertConfiguration(final XmlDBCollection collection, final String name,
      final VersioningType versioning, final boolean pathSummary, final boolean customTimestamp) {
    try (final XmlResourceSession session = collection.getDatabase().beginResourceSession(name)) {
      final ResourceConfiguration configuration = session.getResourceConfig();
      assertEquals(versioning, configuration.versioningType, name);
      assertEquals(pathSummary
          ? StorageType.MEMORY_MAPPED
          : StorageType.FILE_CHANNEL, configuration.storageType, name);
      assertEquals(HashType.NONE, configuration.hashType, name);
      assertEquals(pathSummary, configuration.withPathSummary, name);
      assertEquals(pathSummary, configuration.withPathStatistics, name);
      assertFalse(configuration.areDeweyIDsStored, name);
      assertFalse(configuration.storeNodeHistory(), name);
      assertFalse(configuration.useTextCompression, name);
      assertEquals(customTimestamp, configuration.customCommitTimestamps(), name);
    }
  }

  private static void writeHistory(final XmlDBCollection collection, final String name) {
    try (final XmlResourceSession session = collection.getDatabase().beginResourceSession(name)) {
      session.getNodeTrx().ifPresent(trx -> trx.close());
      try (final var trx = session.beginNodeTrx()) {
        for (int revision = 2; revision <= 5; revision++) {
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          trx.setName(new QNm("value" + revision));
          assertTrue(trx.moveToFirstChild());
          trx.setValue("revision" + revision);
          trx.commit();
        }
      }
    }
  }

  private static void assertHistory(final XmlDBCollection collection, final String name) {
    try (final XmlResourceSession session = collection.getDatabase().beginResourceSession(name)) {
      assertEquals(5, session.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 5; revision++) {
        try (final var trx = session.beginNodeReadOnlyTrx(revision)) {
          assertTrue(trx.moveToFirstChild());
          assertEquals(revision == 1
              ? "value"
              : "value" + revision, trx.getName().getLocalName(), name);
          assertTrue(trx.moveToFirstChild());
          assertEquals(revision == 1
              ? "one"
              : "revision" + revision, trx.getValue(), name);
        }
      }
    }
  }
}
