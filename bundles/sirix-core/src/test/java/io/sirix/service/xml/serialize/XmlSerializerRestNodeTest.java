package io.sirix.service.xml.serialize;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class XmlSerializerRestNodeTest {
  @TempDir
  Path directory;

  // REST-203: expanded names and decoded values survive both serializer branches.
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void standaloneAttributesAndNamespacesKeepTheirIdentity(final boolean prefixed) throws Exception {
    final Path path = directory.resolve("xml");
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openXmlDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("res").build());
      try (final var session = database.beginResourceSession("res")) {
        final String uri = "urn:example?a=1&b=2";
        final String value = "\"<&水";
        final String prefix = prefixed
            ? "p"
            : "";
        final long attribute;
        final long namespace;
        try (final var writer = session.beginNodeTrx()) {
          writer.insertElementAsFirstChild(new QNm("root"));
          final long root = writer.getNodeKey();
          writer.insertNamespace(new QNm(uri, prefix, ""));
          namespace = writer.getNodeKey();
          assertTrue(writer.moveTo(root));
          writer.insertAttribute(prefixed
              ? new QNm(uri, prefix, "name")
              : new QNm("name"), value);
          attribute = writer.getNodeKey();
          writer.commit();
        }
        for (final long key : new long[] {attribute, namespace}) {
          final var output = new ByteArrayOutputStream();
          XmlSerializer.newBuilder(session, output).emitRESTful().emitRESTSequence().startNodeKey(key).build().call();
          final var factory = DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          final Element envelope =
              factory.newDocumentBuilder().parse(new ByteArrayInputStream(output.toByteArray())).getDocumentElement();
          final String local = key == attribute
              ? "attribute"
              : "namespace";
          final Element node = (Element) envelope.getElementsByTagNameNS("https://sirix.io/rest", local).item(0);
          assertEquals(1, envelope.getElementsByTagNameNS("https://sirix.io/rest", local).getLength());
          if (key == attribute) {
            assertEquals(prefixed
                ? "p:name"
                : "name", node.getAttribute("name"));
            assertEquals(prefixed
                ? uri
                : "", node.getAttribute("namespace"));
            assertEquals(value, node.getAttribute("value"));
          } else {
            assertEquals(prefix, node.getAttribute("prefix"));
            assertEquals(uri, node.getAttribute("uri"));
          }
          assertThrows(IllegalStateException.class,
              () -> XmlSerializer.newBuilder(session, new ByteArrayOutputStream()).startNodeKey(key).build().call());
        }
      }
    }
  }
}
