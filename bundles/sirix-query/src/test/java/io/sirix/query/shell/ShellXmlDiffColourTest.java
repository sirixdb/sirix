package io.sirix.query.shell;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;
import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ShellXmlDiffColourTest {
  @TempDir
  Path directory;

  @Test
  void serializedAttributeOperationsKeepNamesNeutral() throws Exception {
    int resource = 0;
    for (final String name : List.of("delete", "insert", "append", "replace", "rename", "delete-insert",
        "insert-delete", "p:delete", "delete:insert", "商店-delete", "delete1", "delete-1", "delete.1")) {
      try (final var store = BasicXmlDBStore.newBuilder().location(directory.resolve("case" + resource++)).build();
          final var ctx = SirixQueryContext.createWithNodeStore(store);
          final var chain = SirixCompileChain.createWithNodeStore(store)) {
        final var collection = store.create("shop", new DocumentParser("<root/>"));
        final long root;
        final long attribute;
        try (final var writer = collection.getDocument("resource1").getTrx().getResourceSession().beginNodeTrx()) {
          writer.moveToFirstChild();
          root = writer.getNodeKey();
          final int colon = name.indexOf(':');
          final QNm qualified = colon < 0
              ? new QNm(name)
              : new QNm("urn:test", name.substring(0, colon), name.substring(colon + 1));
          writer.insertAttribute(qualified, "v");
          attribute = writer.getNodeKey();
          writer.commit();
          writer.setValue("changed");
          writer.setName(new QNm("renamed"));
          writer.commit();
          writer.remove();
          writer.commit();
        }
        final String nl = System.lineSeparator();
        final List<String> operations =
            List.of("  insert node attribute " + name + " { \"v\" } into sdb:select-item($doc," + root + ")",
                "  rename node sdb:select-item($doc, " + attribute + ") as \"renamed\"," + nl
                    + "  replace value of node sdb:select-item($doc, " + attribute + ") with \"changed\"",
                "  delete nodes sdb:select-item($doc, " + attribute + ")");
        for (int revision = 1; revision <= 3; revision++) {
          final String source = "xn:diff('shop','resource1'," + revision + "," + (revision + 1) + ")";
          final String expected = "let $doc := xn:doc('shop','resource1', " + revision + ")" + nl + "return (" + nl
              + operations.get(revision - 1) + nl + ")" + nl;
          final String serialized = ((Str) new Query(chain, source).evaluate(ctx)).stringValue();
          assertEquals(expected, serialized);
          for (final ShellPalette palette : ShellPalette.values()) {
            final StringWriter out = new StringWriter();
            new ShellOutput(palette, new PrintWriter(out), new PrintWriter(Writer.nullWriter()), () -> 0L).result(
                new Query(chain, source), ctx, source);
            final String rendered = out.toString();
            assertEquals((palette.enabled()
                ? ""
                : nl) + "Query result" + nl + expected + nl
                + (palette.enabled()
                    ? "  0 ms" + nl
                    : "")
                + nl, ShellResultWriterTest.strip(rendered));
            if (palette.enabled()) {
              final var styled = AttributedString.fromAnsi(rendered);
              final String payload = styled.toString();
              if (revision == 1) {
                assertStyle(styled, payload.indexOf("insert node"), palette.green());
                final int offset = payload.indexOf("attribute ") + "attribute ".length();
                for (int i = 0; i < name.length(); i++)
                  assertStyle(styled, offset + i, palette.reset());
              } else if (revision == 2) {
                assertStyle(styled, payload.indexOf("rename node"), palette.orange());
                assertStyle(styled, payload.indexOf("replace value"), palette.orange());
              } else
                assertStyle(styled, payload.indexOf("delete nodes"), palette.red());
            }
          }
        }
      }
    }
  }

  private static void assertStyle(final AttributedString styled, final int offset, final String colour) {
    assertEquals(AttributedString.fromAnsi(colour + "X").styleAt(0), styled.styleAt(offset));
  }
}
