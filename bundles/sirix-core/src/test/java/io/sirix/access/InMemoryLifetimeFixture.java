/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.brackit.query.atomic.QNm;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.io.StorageType;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Public-API operations shared by the TXN-615 acceptance tests
 * (spec/TXN-transactions-durability.md): one value node per commit in a JSON array or an XML
 * element, addressed by its node key.
 */
final class InMemoryLifetimeFixture {

  /** The two node models every TXN-615 criterion is checked for. */
  enum Model {
    JSON, XML
  }

  private InMemoryLifetimeFixture() {}

  static void createDatabase(final Model model, final Path path) {
    final DatabaseConfiguration configuration = new DatabaseConfiguration(path);
    if (model == Model.JSON) {
      Databases.createJsonDatabase(configuration);
    } else {
      Databases.createXmlDatabase(configuration);
    }
  }

  static Database<? extends ResourceSession<?, ?>> open(final Model model, final Path path) {
    return model == Model.JSON
        ? Databases.openJsonDatabase(path)
        : Databases.openXmlDatabase(path);
  }

  static void createResource(final Database<? extends ResourceSession<?, ?>> database, final String name,
      final StorageType storageType) {
    assertTrue(database.createResource(ResourceConfiguration.newBuilder(name)
                                                            .storageType(storageType)
                                                            .hashKind(HashType.NONE)
                                                            .buildPathSummary(false)
                                                            .build()),
        "resource " + name + " must be creatable");
  }

  /**
   * Insert the first value of a resource (inside a container node), commit, return the value's key.
   */
  static long commitFirstValue(final ResourceSession<?, ?> session, final Model model, final String value) {
    try (NodeTrx writer = session.beginNodeTrx()) {
      if (model == Model.JSON) {
        final JsonNodeTrx json = (JsonNodeTrx) writer;
        json.insertArrayAsFirstChild();
        json.insertStringValueAsFirstChild(value);
      } else {
        final XmlNodeTrx xml = (XmlNodeTrx) writer;
        xml.insertElementAsFirstChild(new QNm("root"));
        xml.insertElementAsFirstChild(new QNm("item"));
        xml.insertTextAsFirstChild(value);
      }
      final long key = writer.getNodeKey();
      writer.commit();
      return key;
    }
  }

  /** Insert a value after the value with {@code previousKey}, commit, return the new value's key. */
  static long commitNextValue(final ResourceSession<?, ?> session, final Model model, final long previousKey,
      final String value) {
    try (NodeTrx writer = session.beginNodeTrx()) {
      assertTrue(writer.moveTo(previousKey), "the previous value must be visible to the next writer");
      final long key = insertValueAfter(writer, model, value);
      writer.commit();
      return key;
    }
  }

  /**
   * Insert a value after the current value node of {@code writer} without committing; return its key.
   */
  static long insertValueAfter(final NodeTrx writer, final Model model, final String value) {
    if (model == Model.JSON) {
      ((JsonNodeTrx) writer).insertStringValueAsRightSibling(value);
    } else {
      final XmlNodeTrx xml = (XmlNodeTrx) writer;
      xml.moveToParent();
      xml.insertElementAsRightSibling(new QNm("item"));
      xml.insertTextAsFirstChild(value);
    }
    return writer.getNodeKey();
  }

  /**
   * The value of node {@code key} in {@code revision}, or {@code null} when that revision lacks it.
   */
  static @Nullable String valueAt(final ResourceSession<?, ?> session, final Model model, final int revision,
      final long key) {
    try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      return valueOf(reader, model, key);
    }
  }

  static @Nullable String valueOf(final NodeReadOnlyTrx reader, final Model model, final long key) {
    if (!reader.moveTo(key)) {
      return null;
    }
    return model == Model.JSON
        ? ((JsonNodeReadOnlyTrx) reader).getValue()
        : ((XmlNodeReadOnlyTrx) reader).getValue();
  }

  /** Whether the newest revision of the resource holds any content below its document root. */
  static boolean hasContent(final ResourceSession<?, ?> session) {
    try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      // JSON and XML readers are both cursors.
      final NodeCursor cursor = (NodeCursor) reader;
      cursor.moveToDocumentRoot();
      return cursor.hasFirstChild();
    }
  }

  static Set<String> resourceNames(final Database<? extends ResourceSession<?, ?>> database) {
    final List<Path> resources = database.listResources();
    return resources.stream().map(path -> path.getFileName().toString()).collect(Collectors.toSet());
  }
}
