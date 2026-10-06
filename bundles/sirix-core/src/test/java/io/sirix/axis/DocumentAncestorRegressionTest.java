package io.sirix.axis;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.NodeCursor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DocumentAncestorRegressionTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlAxesIncludeDocumentAcrossHistory(final VersioningType versioning) {
    final Path path = directory.resolve("xml");
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    final long root;
    final long child;
    final long text;
    final long attribute;
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data").versioningApproach(versioning).build());
      try (final XmlResourceSession session = database.beginResourceSession("data");
          final XmlNodeTrx trx = session.beginNodeTrx()) {
        root = trx.insertElementAsFirstChild(new QNm("r")).getNodeKey();
        attribute = trx.insertAttribute(new QNm("a"), "v").getNodeKey();
        trx.moveTo(root);
        child = trx.insertElementAsFirstChild(new QNm("child")).getNodeKey();
        text = trx.insertTextAsFirstChild("old").getNodeKey();
        trx.commit();
        trx.setValue("new");
        trx.commit();
      }
    }
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path);
        final XmlResourceSession session = database.beginResourceSession("data")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (final XmlNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          checkAxes(trx, root, child, text);
          trx.moveTo(attribute);
          assertAxis(new ParentAxis(trx), root);
          assertAxis(new AncestorAxis(trx), root, 0);
          assertAxis(new AncestorAxis(trx, IncludeSelf.YES), attribute, root, 0);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void jsonAxesIncludeDocumentAcrossHistory(final VersioningType versioning) {
    final Path path = directory.resolve("json");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final long root;
    final long child;
    final long value;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data").versioningApproach(versioning).build());
      try (final JsonResourceSession session = database.beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        root = trx.insertArrayAsFirstChild().getNodeKey();
        child = trx.insertArrayAsFirstChild().getNodeKey();
        value = trx.insertStringValueAsFirstChild("old").getNodeKey();
        trx.commit();
        trx.setStringValue("new");
        trx.commit();
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        final JsonResourceSession session = database.beginResourceSession("data")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (final JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          checkAxes(trx, root, child, value);
        }
      }
    }
  }

  private static void checkAxes(final NodeCursor trx, final long root, final long child, final long value) {
    assertTrue(trx.moveTo(root));
    assertAxis(new ParentAxis(trx), 0);
    assertAxis(new AncestorAxis(trx), 0);
    assertAxis(new AncestorAxis(trx, IncludeSelf.YES), root, 0);
    assertTrue(trx.moveTo(child));
    assertAxis(new ParentAxis(trx), root);
    assertAxis(new AncestorAxis(trx), root, 0);
    assertAxis(new AncestorAxis(trx, IncludeSelf.YES), child, root, 0);
    assertTrue(trx.moveTo(value));
    assertAxis(new ParentAxis(trx), child);
    assertAxis(new AncestorAxis(trx), child, root, 0);
    assertAxis(new AncestorAxis(trx, IncludeSelf.YES), value, child, root, 0);
    trx.moveToDocumentRoot();
    assertAxis(new ParentAxis(trx));
    assertAxis(new AncestorAxis(trx));
    assertAxis(new AncestorAxis(trx, IncludeSelf.YES), 0);
    final ParentAxis reused = new ParentAxis(trx);
    assertAxis(reused);
    reused.reset(root);
    assertAxis(reused, 0);
  }

  private static void assertAxis(final Axis axis, final long... expected) {
    final long start = axis.getStartKey();
    for (final long key : expected) {
      assertTrue(axis.hasNext());
      assertTrue(axis.hasNext());
      assertEquals(key, axis.nextLong());
      axis.getCursor().moveToDocumentRoot();
    }
    assertFalse(axis.hasNext());
    assertFalse(axis.hasNext());
    assertEquals(start, axis.getCursor().getNodeKey());
  }
}
