package io.sirix.query.function.xml.diff;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class DiffNamespaceReplayTest {
  @TempDir
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(VersioningType.values())
                 .flatMap(versioning -> Stream.of(false, true)
                                              .flatMap(replace -> Stream.of(false, true)
                                                                        .map(localBindings -> Arguments.of(versioning,
                                                                            replace, localBindings))));
  }

  @ParameterizedTest(name = "{0} replace={1} localBindings={2}")
  @MethodSource("cases")
  void replayPreservesInheritedElementAndAttributeNamespaces(final VersioningType versioning, final boolean replace,
      final boolean localBindings) {
    final String defaultNamespace = localBindings
        ? ""
        : "urn:default";
    final String prefixNamespace = localBindings
        ? "urn:rebound"
        : "urn:prefix";
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
        final var chain = SirixCompileChain.createWithNodeStore(store);
        final var context = SirixQueryContext.createWithNodeStore(store)) {
      final var collection = store.create("replay",
          new DocumentParser("<root xmlns='urn:default' xmlns:p='urn:prefix'><anchor/><tail/></root>"));
      final var session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final XmlNodeTrx writer = session.beginNodeTrx()) {
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        final long rootKey = writer.getNodeKey();
        if (replace) {
          writer.moveToFirstChild();
          writer.remove();
          writer.moveTo(rootKey);
        }
        writer.insertElementAsFirstChild(new QNm(defaultNamespace, "", "item"));
        assertEquals(0, writer.getNamespaceCount());
        if (localBindings) {
          writer.insertNamespace(new QNm("", "", "")).moveToParent();
          writer.insertNamespace(new QNm(prefixNamespace, "p", "")).moveToParent();
        }
        writer.insertAttribute(new QNm(prefixNamespace, "p", "flag"), "é").moveToParent();
        writer.insertElementAsFirstChild(new QNm(prefixNamespace, "p", "nested"));
        writer.insertTextAsFirstChild("value");
        final long textKey = writer.getNodeKey();
        writer.commit();
        writer.moveTo(textKey);
        writer.setValue("newest");
        writer.commit();
      }

      final String update =
          ((Str) new Query(chain, "xn:diff('replay','resource1',1,2)").evaluate(context)).stringValue();
      new Query(chain, update).execute(context);

      for (final int revision : new int[] {2, 4}) {
        final XmlDBNode document =
            (XmlDBNode) new Query(chain, "xn:doc('replay','resource1'," + revision + ")").evaluate(context);
        final XmlDBNode item = document.getFirstChild().getFirstChild();
        assertEquals(new QNm(defaultNamespace, "", "item"), item.getName());
        assertEquals("é", item.getAttribute(new QNm(prefixNamespace, "p", "flag")).getValue().stringValue());
        final XmlDBNode nested = item.getFirstChild();
        assertEquals(new QNm(prefixNamespace, "p", "nested"), nested.getName());
        assertEquals("value", nested.getValue().stringValue());
        assertEquals(replace
            ? "tail"
            : "anchor", item.getNextSibling().getName().getLocalName());
      }
    }
  }
}
