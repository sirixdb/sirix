package io.sirix.service.xml.serialize;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.w3c.dom.Element;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static io.sirix.service.xml.serialize.XmlSerializationAssertions.assertElement;
import static io.sirix.service.xml.serialize.XmlSerializationAssertions.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class XmlSerializerEscapingTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void declarationAttributeAndTextValuesRoundTrip(final VersioningType versioning) throws Exception {
    final Path databasePath = directory.resolve("xml");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
        for (int i = 0; i < XmlSerializationAssertions.ENCODED_VALUES.size(); i++) {
          database.createResource(ResourceConfiguration.newBuilder("data" + i)
                                                       .storageType(StorageType.FILE_CHANNEL)
                                                       .versioningApproach(versioning)
                                                       .build());
          try (final var session = database.beginResourceSession("data" + i);
              final var writer = session.beginNodeTrx()) {
            writer.insertSubtreeAsFirstChild(XmlShredder.createStringReader(
                XmlSerializationAssertions.document(XmlSerializationAssertions.ENCODED_VALUES.get(i))));
            writer.commit();
          }
        }
      }
      // Read from disk as well as checking the decoded transaction values after each output.
      try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
        for (int i = 0; i < XmlSerializationAssertions.ENCODED_VALUES.size(); i++) {
          final String input = XmlSerializationAssertions.document(XmlSerializationAssertions.ENCODED_VALUES.get(i));
          final String value = XmlSerializationAssertions.DECODED_VALUES.get(i);
          final Element expected = parse(input).getDocumentElement();
          try (final var session = database.beginResourceSession("data" + i);
              final var reader = session.beginNodeReadOnlyTrx()) {
            assertDecoded(reader, value);
            final long rootKey = reader.getNodeKey();
            assertTrue(reader.moveToFirstChild());
            final long childKey = reader.getNodeKey();
            for (final Mode mode : Mode.values()) {
              final String output = serialize(session, 0, mode);
              final Element actual =
                  (Element) parse(output).getElementsByTagNameNS(expected.getNamespaceURI(), "root").item(0);
              assertElement(expected, actual, mode.metadata);
              if (i == 0 && mode == Mode.PLAIN) {
                // Baseline serialization keeps the persistent attribute order chosen by the shredder.
                assertEquals(input.replace("value=\"plain\" p:value=\"plain\"", "p:value=\"plain\" value=\"plain\""),
                    output, "plain output must remain byte-identical");
              }
              final String detached = serialize(session, childKey, mode);
              final Element expectedChild = (Element) expected.getFirstChild();
              final Element actualChild =
                  (Element) parse(detached).getElementsByTagNameNS(expectedChild.getNamespaceURI(), "child").item(0);
              assertElement(expectedChild, actualChild, mode.metadata);
              assertDecoded(reader, value);
              assertEquals(rootKey, reader.getNodeKey());
            }
          }
        }
      }
    } finally {
      Databases.removeDatabase(databasePath);
    }
  }

  private static String serialize(final XmlResourceSession session, final long key, final Mode mode) {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final var builder = XmlSerializer.newBuilder(session, output).startNodeKey(key);
    if (mode.rest) {
      builder.emitRESTful().emitRESTSequence();
    }
    if (mode.metadata) {
      builder.emitMetaData();
    }
    builder.build().call();
    return output.toString(StandardCharsets.UTF_8);
  }

  private static void assertDecoded(final XmlNodeReadOnlyTrx reader, final String value) {
    reader.moveToDocumentRoot();
    assertTrue(reader.moveToFirstChild());
    final long rootKey = reader.getNodeKey();
    final String defaultUri = "https://example.test/default?" + value;
    final String prefixUri = "https://example.test/prefix?" + value;
    assertEquals(new QNm(defaultUri, "", "root"), reader.getName());
    assertEquals(2, reader.getNamespaceCount());
    for (int i = 0; i < 2; i++) {
      assertTrue(reader.moveToNamespace(i));
      assertEquals(reader.getPrefixKey() == -1
          ? defaultUri
          : prefixUri, reader.nameForKey(reader.getURIKey()));
      reader.moveTo(rootKey);
    }
    for (int i = 0; i < 2; i++) {
      assertTrue(reader.moveToAttribute(i));
      assertEquals(value, reader.getValue());
      reader.moveTo(rootKey);
    }
    assertTrue(reader.moveToFirstChild());
    assertEquals(new QNm(prefixUri, "p", "child"), reader.getName());
    assertTrue(reader.moveToFirstChild());
    assertEquals(value, reader.getValue());
    reader.moveTo(rootKey);
  }

  private enum Mode {
    PLAIN(false, false), REST(true, false), METADATA(false, true), REST_METADATA(true, true);

    private final boolean rest;
    private final boolean metadata;

    Mode(final boolean rest, final boolean metadata) {
      this.rest = rest;
      this.metadata = metadata;
    }
  }
}
