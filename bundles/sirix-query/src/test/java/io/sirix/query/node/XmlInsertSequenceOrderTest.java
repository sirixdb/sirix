package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Kind;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import org.custommonkey.xmlunit.Diff;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks sequence order independently of node keys, before commit and after a cold reopen. */
final class XmlInsertSequenceOrderTest {
  private static final String DOCUMENT = "<root><left/><anchor><old/></anchor><right/></root>";
  private static final String SOURCE = "xml:doc('order','resource1')";

  @TempDir
  Path directory;

  enum Position {
    FIRST("as first into"), LAST("as last into"), INTO("into"), BEFORE("before"), AFTER("after");

    final String expression;

    Position(final String expression) {
      this.expression = expression;
    }

    boolean allowsAttributes() {
      return this != BEFORE && this != AFTER;
    }

    String expected(final Content content) {
      final String anchor = "<anchor" + content.attributes + ">";
      return switch (this) {
        case FIRST -> "<root><left/>" + anchor + content.children + "<old/></anchor><right/></root>";
        case LAST, INTO -> "<root><left/>" + anchor + "<old/>" + content.children + "</anchor><right/></root>";
        case BEFORE -> "<root><left/>" + content.children + "<anchor><old/></anchor><right/></root>";
        case AFTER -> "<root><left/><anchor><old/></anchor>" + content.children + "<right/></root>";
      };
    }
  }

  enum Content {
    ELEMENTS("(<a><nested/></a>, <b/>, <c/>, <d/>)", "<a><nested/></a><b/><c/><d/>", ""), TEXT(
        "(text {'one'}, text {'two'}, text {'three'})", "onetwothree",
        ""), MIXED("(text {'one'}, <b/>, text {'two'}, <d/>, text {'three'})", "one<b/>two<d/>three", ""), NAMESPACES(
            "(<item xmlns='urn:a'>one</item>, <item xmlns='urn:b'>two</item>, "
                + "<p:item xmlns:p='urn:a'>three</p:item>, <item>four</item>)",
            "<item xmlns=\"urn:a\">one</item><item xmlns=\"urn:b\">two</item>"
                + "<p:item xmlns:p=\"urn:a\">three</p:item><item>four</item>",
            ""), NAMESPACE_REBINDING(
                "<p:parent xmlns:p='urn:a'><p:child xmlns:p='urn:b'/><leaf xmlns='urn:c'><plain xmlns=''/></leaf></p:parent>",
                "<p:parent xmlns:p=\"urn:a\"><p:child xmlns:p=\"urn:b\"/><leaf xmlns=\"urn:c\"><plain xmlns=\"\"/></leaf></p:parent>",
                ""), COMMENT_PI_TEXT(
                    "(comment {'first'}, processing-instruction marker {'second'}, text {'third'}, <last/>)",
                    "<!-- first --><?marker second?>third<last/>",
                    ""), PI("processing-instruction marker {'second with spaces'}", "<?marker second with spaces?>",
                        ""), ATTRIBUTES("(attribute a {'one'}, attribute b {'two'}, <c/>, text {'three'}, <d/>)",
                            "<c/>three<d/>", " a=\"one\" b=\"two\"");

    final String expression;
    final String children;
    final String attributes;

    Content(final String expression, final String children, final String attributes) {
      this.expression = expression;
      this.children = children;
      this.attributes = attributes;
    }
  }

  static Stream<Arguments> cases() {
    return Stream.of(VersioningType.values())
                 .flatMap(
                     versioning -> Stream.of(Position.values())
                                         .flatMap(position -> Stream.of(Content.values())
                                                                    .filter(content -> content != Content.ATTRIBUTES
                                                                        || position.allowsAttributes())
                                                                    .map(content -> Arguments.of(versioning, position,
                                                                        content))));
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("cases")
  void xqueryUpdatePreservesOrder(final VersioningType versioning, final Position position, final Content content) {
    final String actual;
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithNodeStore(store);
        final var context = SirixQueryContext.createWithNodeStore(store)) {
      store.create("order", new DocumentParser(DOCUMENT));
      new Query(chain,
          "insert nodes " + content.expression + " " + position.expression + " " + SOURCE + "/root/anchor").evaluate(
              context);
      actual = serialize(chain, context, SOURCE, content);
    }
    assertAll(() -> assertXmlEquals(position.expected(content), actual),
        () -> assertReopened(versioning, position.expected(content), content));
  }

  /**
   * Uses the transaction-backed single-node APIs called by the update primitives. Advancing the
   * insertion cursor after each child preserves the sequence without changing those APIs' semantics.
   */
  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("cases")
  void directTransactionBindingPreservesOrder(final VersioningType versioning, final Position position,
      final Content content) {
    final String actual;
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithNodeStore(store);
        final var context = SirixQueryContext.createWithNodeStore(store)) {
      final var collection = store.create("order", new DocumentParser(DOCUMENT));
      final var session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final XmlNodeTrx trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        final XmlDBNode document = new XmlDBNode(trx, collection);
        final XmlDBNode anchor = document.getFirstChild().getFirstChild().getNextSibling();
        XmlDBNode previous = null;
        try (final Iter nodes = new Query(chain, content.expression).evaluate(context).iterate()) {
          Node<?> child;
          while ((child = (Node<?>) nodes.next()) != null) {
            if (child.getKind() == Kind.ATTRIBUTE) {
              anchor.setAttribute(child);
            } else if (previous != null) {
              previous = previous.insertAfter(child);
            } else {
              previous = switch (position) {
                case FIRST -> anchor.prepend(child);
                case LAST, INTO -> anchor.append(child);
                case BEFORE -> anchor.insertBefore(child);
                case AFTER -> anchor.insertAfter(child);
              };
            }
          }
        }
        assertStoredNonElements(document, content);
        actual = serialize(document);
        trx.commit();
      }
    }
    assertAll(() -> assertXmlEquals(position.expected(content), actual),
        () -> assertReopened(versioning, position.expected(content), content));
  }

  private BasicXmlDBStore openStore(final VersioningType versioning) {
    return BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }

  private void assertReopened(final VersioningType versioning, final String expected, final Content content) {
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithNodeStore(store);
        final var context = SirixQueryContext.createWithNodeStore(store)) {
      assertAll(() -> assertXmlEquals(expected, serialize(chain, context, SOURCE, content)),
          () -> assertXmlEquals(expected, serialize(chain, context, "xml:doc('order','resource1',2)", content)),
          () -> assertXmlEquals(DOCUMENT,
              serialize(chain, context, "xml:doc('order','resource1',1)", Content.ELEMENTS)));
    }
  }

  /** Namespace declarations may be redundant; node names, content and order must remain identical. */
  private static void assertXmlEquals(final String expected, final String actual) {
    try {
      final Diff diff = new Diff(expected, actual);
      assertTrue(diff.identical(), diff::toString);
    } catch (final IOException | SAXException e) {
      throw new AssertionError("Serialized result must be well-formed XML", e);
    }
  }

  private static String serialize(final SirixCompileChain chain, final SirixQueryContext context,
      final String expression, final Content content) {
    final XmlDBNode document = (XmlDBNode) new Query(chain, expression).evaluate(context);
    assertStoredNonElements(document, content);
    return serialize(document);
  }

  private static void assertStoredNonElements(final XmlDBNode document, final Content content) {
    if (content != Content.COMMENT_PI_TEXT && content != Content.PI) {
      return;
    }
    int comments = 0;
    int processingInstructions = 0;
    try (final var subtree = document.getSubtree()) {
      XmlDBNode node;
      while ((node = subtree.next()) != null) {
        if (node.getKind() == Kind.COMMENT) {
          comments++;
          assertEquals("first", node.getValue().stringValue());
        } else if (node.getKind() == Kind.PROCESSING_INSTRUCTION) {
          processingInstructions++;
          assertEquals("marker", node.getName().getLocalName());
          assertEquals(content == Content.PI
              ? "second with spaces"
              : "second", node.getValue().stringValue());
        }
      }
    }
    assertEquals(content == Content.COMMENT_PI_TEXT
        ? 1
        : 0, comments);
    assertEquals(1, processingInstructions);
  }

  private static String serialize(final Sequence sequence) {
    final StringWriter output = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(sequence);
    }
    return output.toString();
  }
}
