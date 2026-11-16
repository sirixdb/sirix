package io.sirix.query.shell;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.shell.ShellResultWriter.DiffMode;
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

  @Test
  void operationPositionsSurviveEverySplitOverloadAndFinish() throws Exception {
    final String input = "let $doc := xn:doc('delete','insert',1) return (\n"
        + "insert node attribute delete-insert { \"v\" } into sdb:select-item($doc,1),\n"
        + "insert node attribute delete:insert { \"v\" } into sdb:select-item($doc,1),\n"
        + "replace value of node $doc/delete with \"insert\", rename node $doc/insert as 'delete',\n"
        + "delete nodes $doc/replace); append nodes <delete insert='replace'>rename</delete>";
    final String expected =
        "let $doc := xn:doc(\033[32m'delete'\033[0m,\033[32m'insert'\033[0m,\033[33m1\033[0m) return (\n"
            + "\033[32minsert\033[0m node attribute delete-insert { \033[32m\"v\"\033[0m } into sdb:select-item($doc,\033[33m1\033[0m),\n"
            + "\033[32minsert\033[0m node attribute delete:insert { \033[32m\"v\"\033[0m } into sdb:select-item($doc,\033[33m1\033[0m),\n"
            + "\033[33mreplace\033[0m value of node $doc/delete with \033[32m\"insert\"\033[0m, \033[33mrename\033[0m node $doc/insert as \033[32m'delete'\033[0m,\n"
            + "\033[31mdelete\033[0m nodes $doc/replace); \033[32mappend\033[0m nodes \033[36m<delete insert=\033[32m'replace'\033[0m\033[36m>\033[0mrename\033[36m</delete>\033[0m";
    final char[] chars = input.toCharArray();
    for (int overload = 0; overload < 3; overload++) {
      for (int split = 0; split <= input.length(); split++) {
        final StringWriter out = new StringWriter();
        try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
          for (int i = 0; i < input.length(); i++) {
            if (i == split)
              writer.flush();
            if (overload == 0)
              writer.write(input, i, 1);
            else if (overload == 1)
              writer.write(chars, i, 1);
            else
              writer.write(chars[i]);
          }
          writer.finish();
          assertEquals(expected, out.toString());
          writer.write("delete");
          writer.finish();
        }
        assertEquals(expected + "\033[31mdelete\033[0m", out.toString());
      }
    }
    for (final DiffMode mode : List.of(DiffMode.NONE, DiffMode.JSON)) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, mode)) {
        writer.write("insert delete append replace rename");
      }
      assertEquals("insert delete append replace rename", out.toString());
    }
  }

  @Test
  void markupAndValueConstructorsCannotIntroduceOperations() throws Exception {
    final String input = "<a/>delete; insert node attribute a { (delete), rename }; delete";
    final StringWriter out = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
      writer.write(input);
    }
    assertEquals(
        "\033[36m<a/>\033[0mdelete; \033[32minsert\033[0m node attribute a { (delete), rename }; \033[31mdelete\033[0m",
        out.toString());
    final StringWriter start = new StringWriter();
    try (final ShellResultWriter writer = new ShellResultWriter(start, ShellPalette.ANSI_16, DiffMode.XML)) {
      writer.write("delete");
    }
    assertEquals("\033[31mdelete\033[0m", start.toString());
  }

  @Test
  void longWordsPreservePayloadAndFollowingOperationStyle() throws Exception {
    final String word = "deleteNamedOperand".repeat(100);
    for (int chunk = 1; chunk <= 20; chunk++) {
      final StringWriter out = new StringWriter();
      try (final ShellResultWriter writer = new ShellResultWriter(out, ShellPalette.ANSI_16, DiffMode.XML)) {
        for (int offset = 0; offset < word.length(); offset += chunk) {
          writer.write(word, offset, Math.min(chunk, word.length() - offset));
          writer.flush();
        }
        writer.write("; delete nodes $doc/a");
      }
      assertEquals(word + "; \033[31mdelete\033[0m nodes $doc/a", out.toString());
    }
  }

  private static void assertStyle(final AttributedString styled, final int offset, final String colour) {
    assertEquals(AttributedString.fromAnsi(colour + "X").styleAt(0), styled.styleAt(offset));
  }
}
