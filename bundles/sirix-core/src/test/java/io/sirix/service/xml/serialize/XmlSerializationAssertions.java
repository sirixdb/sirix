package io.sirix.service.xml.serialize;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Independent XML parser oracle shared by core and query serialization regressions. */
public final class XmlSerializationAssertions {
  public static final List<String> ENCODED_VALUES =
      List.of("plain", "&amp;", "&lt;", "&gt;", "&quot;", "&apos;", "a=1&amp;b=2&lt;tag&gt;&quot;&apos;é😀&amp;amp;");
  public static final List<String> DECODED_VALUES =
      List.of("plain", "&", "<", ">", "\"", "'", "a=1&b=2<tag>\"'é😀&amp;");

  private XmlSerializationAssertions() {}

  public static String document(final String encodedValue) {
    return "<root xmlns=\"https://example.test/default?" + encodedValue + "\" xmlns:p=\"https://example.test/prefix?"
        + encodedValue + "\" value=\"" + encodedValue + "\" p:value=\"" + encodedValue + "\"><p:child value=\""
        + encodedValue + "\">" + encodedValue + "</p:child><control xmlns=\"\" value=\"plain\">plain</control></root>";
  }

  public static Document parse(final String xml) throws Exception {
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    return factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
  }

  /** Compare names, effective namespace bindings, attributes and every text node. */
  public static void assertElement(final Element expected, final Element actual, final boolean metadata) {
    assertNotNull(actual);
    assertEquals(expected.getTagName(), actual.getTagName());
    assertEquals(expected.getNamespaceURI(), actual.getNamespaceURI());
    assertEquals(expected.getLocalName(), actual.getLocalName());
    assertEquals(expected.getPrefix(), actual.getPrefix());
    assertEquals(expected.lookupNamespaceURI(null), actual.lookupNamespaceURI(null));
    assertEquals(expected.lookupNamespaceURI("p"), actual.lookupNamespaceURI("p"));
    final NamedNodeMap attributes = expected.getAttributes();
    int persistentAttributes = 0;
    for (int i = 0; i < attributes.getLength(); i++) {
      final Node attribute = attributes.item(i);
      // A detached subtree may repeat inherited declarations on its root.
      if (XMLConstants.XMLNS_ATTRIBUTE_NS_URI.equals(attribute.getNamespaceURI())) {
        continue;
      }
      persistentAttributes++;
      final Node parsed = actual.getAttributeNodeNS(attribute.getNamespaceURI(), attribute.getLocalName());
      assertNotNull(parsed);
      assertEquals(attribute.getNodeName(), parsed.getNodeName());
      assertEquals(attribute.getNodeValue(), parsed.getNodeValue());
    }
    int actualAttributes = 0;
    final NamedNodeMap parsedAttributes = actual.getAttributes();
    for (int i = 0; i < parsedAttributes.getLength(); i++) {
      if (!XMLConstants.XMLNS_ATTRIBUTE_NS_URI.equals(parsedAttributes.item(i).getNamespaceURI())) {
        actualAttributes++;
      }
    }
    assertEquals(persistentAttributes + (metadata
        ? 1
        : 0), actualAttributes);
    Node expectedChild = expected.getFirstChild();
    Node actualChild = actual.getFirstChild();
    while (expectedChild != null) {
      assertNotNull(actualChild);
      assertEquals(expectedChild.getNodeType(), actualChild.getNodeType());
      if (expectedChild instanceof Element expectedElement) {
        assertElement(expectedElement, (Element) actualChild, metadata);
      } else {
        assertEquals(expectedChild.getNodeValue(), actualChild.getNodeValue());
      }
      expectedChild = expectedChild.getNextSibling();
      actualChild = actualChild.getNextSibling();
    }
    assertNull(actualChild);
  }
}
