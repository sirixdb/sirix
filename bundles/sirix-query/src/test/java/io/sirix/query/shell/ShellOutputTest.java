package io.sirix.query.shell;

import io.brackit.query.Query;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.sirix.query.SirixStringSerializer;
import io.sirix.query.shell.ShellResultWriter.DiffMode;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.json.BasicJsonDBStore;
import java.nio.file.Path;
import java.util.List;
import org.jline.utils.AttributedString;
import java.io.Writer;
import java.io.BufferedWriter;
import java.util.concurrent.atomic.AtomicInteger;
import io.sirix.query.node.BasicXmlDBStore;
import io.brackit.query.node.parser.DocumentParser;
import org.junit.jupiter.api.io.TempDir;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ShellOutputTest {
  @TempDir
  Path directory;

  @Test
  void plainOutputMatchesTheExistingSerializerByteContract() throws Exception {
    for (final String source : new String[] {"{\"name\":\"商店😀\",\"n\":12}", "(1,2,3)", "()",
        "<a x='1'><b>商店</b></a>"}) {
      for (final boolean pretty : new boolean[] {false, true}) {
        final var ctx = new BrackitQueryContext();
        final Query query = new Query(source);
        if (pretty)
          query.prettyPrint();
        final StringWriter expected = new StringWriter();
        final PrintWriter previous = new PrintWriter(expected);
        previous.println();
        previous.println("Query result");
        final SirixStringSerializer serializer = new SirixStringSerializer(previous);
        serializer.setFormat(pretty);
        query.serialize(ctx, serializer);
        previous.println();
        previous.println();
        final StringWriter actual = new StringWriter();
        new ShellOutput(ShellPalette.NONE, new PrintWriter(actual), new PrintWriter(new StringWriter())).result(query,
            ctx, source);
        assertEquals(expected.toString(), actual.toString(), source);
      }
    }
  }

  @Test
  void terminalFormattingPreservesLegacyUpdateSideEffects() throws Exception {
    String legacy = null;
    for (final boolean coloured : new boolean[] {false, true}) {
      try (final var store = BasicJsonDBStore.newBuilder()
                                             .location(directory.resolve(coloured
                                                 ? "colour"
                                                 : "legacy"))
                                             .build();
          final var ctx = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {
        final String[] queries = {
            "jn:store('shop','products','[{\"name\":\"Laptop\",\"price\":999},{\"name\":\"Phone\",\"price\":699}]')",
            "let $doc := jn:doc('shop','products') return replace json value of $doc[0].price with 899",
            "let $doc := jn:doc('shop','products') return append json {\"name\":\"Tablet\",\"price\":449} into $doc",
            "let $doc := jn:doc('shop','products') return delete json $doc[1]"};
        final ShellOutput output = new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(Writer.nullWriter()),
            new PrintWriter(Writer.nullWriter()));
        for (final String source : queries) {
          final Query query = new Query(chain, source);
          if (coloured)
            output.result(query, ctx, source);
          else
            query.serialize(ctx, new SirixStringSerializer(new PrintWriter(Writer.nullWriter())));
        }
        final StringWriter snapshot = new StringWriter();
        new Query(chain, "jn:doc('shop','products')").serialize(ctx, new PrintWriter(snapshot));
        if (coloured)
          assertEquals(legacy, snapshot.toString());
        else
          legacy = snapshot.toString();
      }
    }
  }

  @Test
  void deterministicTimingAndBufferedOutputAreFlushed() throws Exception {
    final StringWriter out = new StringWriter();
    final AtomicInteger calls = new AtomicInteger();
    final ShellOutput output = new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(new BufferedWriter(out)),
        new PrintWriter(Writer.nullWriter()), () -> calls.getAndIncrement() == 0
            ? 10_000_000L
            : 15_000_000L);
    output.result(new Query("1"), new BrackitQueryContext(), "1");
    final String nl = System.lineSeparator();
    assertEquals("\033[2mQuery result\033[0m" + nl + "\033[33m1\033[0m" + nl + "\033[2m  5 ms\033[0m" + nl + nl,
        out.toString());
    assertEquals(2, calls.get());
    calls.set(0);
    new ShellOutput(ShellPalette.NONE, new PrintWriter(Writer.nullWriter()), new PrintWriter(Writer.nullWriter()),
        () -> calls.incrementAndGet()).result(new Query("1"), new BrackitQueryContext(), "1");
    assertEquals(0, calls.get(), "Plain output must not read a timing clock");
  }

  @Test
  void conciseErrorCodesAndPlainLegacyErrorsAreFlushed() {
    final QueryException failure = new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE, "bad input");
    for (final ShellPalette palette : new ShellPalette[] {ShellPalette.NONE, ShellPalette.ANSI_16}) {
      final StringWriter err = new StringWriter();
      new ShellOutput(palette, new PrintWriter(Writer.nullWriter()), new PrintWriter(new BufferedWriter(err))).error(
          failure);
      assertEquals(palette.enabled()
          ? "\033[31mError [err:XPTY0004]: bad input\033[0m" + System.lineSeparator()
          : "Error: " + failure.getMessage() + System.lineSeparator(), err.toString());
    }
  }

  @Test
  void actualJsonAndXmlDiffFunctionsSelectOperationColours() throws Exception {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory.resolve("jdiff")).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('shop','products','{\"price\":999}')").evaluate(ctx);
      new Query(chain, "replace json value of jn:doc('shop','products').price with 899").evaluate(ctx);
      final String source = "jn:diff('shop','products',1,2)";
      final StringWriter out = new StringWriter();
      new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(Writer.nullWriter())).result(
          new Query(chain, source), ctx, source);
      assertTrue(out.toString().contains("\033[33m\"update\"\033[0m"), out.toString());
    }
    try (final var store = BasicXmlDBStore.newBuilder().location(directory.resolve("xdiff")).build();
        final var ctx = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final var collection = store.create("shop", new DocumentParser("<a>1</a>"));
      try (final var writer = collection.getDocument("resource1").getTrx().getResourceSession().beginNodeTrx()) {
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.setValue("2");
        writer.commit();
      }
      for (final String source : List.of("xn:diff('shop','resource1',1,2)",
          "(: leading :) xn:diff (: argument list :) ('shop','resource1',1,2) (: trailing :)")) {
        final String rendered = render(chain, ctx, source);
        assertTrue(rendered.contains("\033[33mreplace\033[0m"), rendered);
        assertSerializedPayload(chain, ctx, source, rendered);
      }
    }
    final String source = "(: jn:diff('a','b',1,2) :) {\"insert\":1}";
    final StringWriter out = new StringWriter();
    new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(Writer.nullWriter())).result(
        new Query(source), new BrackitQueryContext(), source);
    assertTrue(out.toString().contains("\033[36m\"insert\"\033[0m"), out.toString());
  }

  @Test
  void actualXmlDiffBackslashReplacementRestoresFollowingDeleteColour() throws Exception {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory.resolve("xml-quoting")).build();
        final var ctx = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final var collection = store.create("shop", new DocumentParser("<a>old<b/></a>"));
      final long textKey;
      final long deletedKey;
      try (final var writer = collection.getDocument("resource1").getTrx().getResourceSession().beginNodeTrx()) {
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        textKey = writer.getNodeKey();
        writer.setValue("C:\\");
        writer.moveToRightSibling();
        deletedKey = writer.getNodeKey();
        writer.remove();
        writer.commit();
      }
      final String source = "xn:diff('shop','resource1',1,2)";
      final String nl = System.lineSeparator();
      final String patch = "let $doc := xn:doc('shop','resource1', 1)" + nl + "return (" + nl
          + "  replace value of node sdb:select-item($doc, " + textKey + ") with \"C:\\\"," + nl
          + "  delete nodes sdb:select-item($doc, " + deletedKey + ")" + nl + ")" + nl;
      for (final ShellPalette palette : ShellPalette.values()) {
        final StringWriter out = new StringWriter();
        new ShellOutput(palette, new PrintWriter(out), new PrintWriter(Writer.nullWriter()), () -> 0L).result(
            new Query(chain, source), ctx, source);
        final String expected = (palette.enabled()
            ? ""
            : nl) + "Query result" + nl + patch + nl
            + (palette.enabled()
                ? "  0 ms" + nl
                : "")
            + nl;
        assertEquals(expected, ShellResultWriterTest.strip(out.toString()));
        assertTrue(out.toString().contains(palette.orange() + "replace" + palette.reset()), out.toString());
        assertTrue(out.toString().contains(palette.red() + "delete" + palette.reset()), out.toString());
        if (palette.enabled()) {
          final var styled = AttributedString.fromAnsi(out.toString());
          final int value = styled.toString().indexOf("\"C:\\\"");
          for (int i = value; i < value + 5; i++)
            assertEquals(AttributedString.fromAnsi(palette.green() + "X").styleAt(0), styled.styleAt(i));
        }
      }
    }
  }

  @Test
  void skippedDeferredAndMixedDiffCallsKeepOrdinaryResultStyles() throws Exception {
    try (final var json = BasicJsonDBStore.newBuilder().location(directory.resolve("json-context")).build();
        final var xml = BasicXmlDBStore.newBuilder().location(directory.resolve("xml-context")).build();
        final var ctx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(xml, json,
            SirixQueryContext.CommitStrategy.AUTO);
        final var chain = SirixCompileChain.createWithNodeAndJsonStore(xml, json)) {
      json.create("shop", "products", "{\"price\":999}");
      new Query(chain, "replace json value of jn:doc('shop','products').price with 899").execute(ctx);
      final var collection = xml.create("shop", new DocumentParser("<a>1</a>"));
      try (final var writer = collection.getDocument("resource1").getTrx().getResourceSession().beginNodeTrx()) {
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.setValue("2");
        writer.commit();
      }
      final String ordinary = "{\"insert\":1,\"update\":2,\"replace\":3,\"delete\":4}";
      for (final String call : List.of("jn:diff('shop','products',1,2)", "xn:diff('shop','resource1',1,2)")) {
        for (final String source : List.of("if (false()) then " + call + " else " + ordinary,
            "(if (false()) then " + call + " else (), " + ordinary + ")",
            "((for $unused in () return " + call + "), " + ordinary + ")",
            "declare function local:unused() { " + call + " }; " + ordinary, "(" + call + ", " + ordinary + ")",
            "(" + ordinary + ", " + call + ")")) {
          final String rendered = render(chain, ctx, source);
          final var styled = AttributedString.fromAnsi(rendered);
          for (final String operation : List.of("insert", "update", "replace", "delete")) {
            final int offset = styled.toString().lastIndexOf("\"" + operation + "\"");
            assertTrue(offset >= 0, source);
            assertEquals(AttributedString.fromAnsi("\033[36mX").styleAt(0), styled.styleAt(offset), source);
          }
          assertSerializedPayload(chain, ctx, source, rendered);
        }
        for (final String word : List.of("insert", "append", "replace", "rename", "delete")) {
          for (final String source : List.of("if (false()) then " + call + " else '" + word + "'",
              "(" + call + ", '" + word + "')")) {
            final String rendered = render(chain, ctx, source);
            final var styled = AttributedString.fromAnsi(rendered);
            assertEquals(AttributedString.fromAnsi("\033[0mX").styleAt(0),
                styled.styleAt(styled.toString().lastIndexOf(word)), source);
            assertSerializedPayload(chain, ctx, source, rendered);
          }
        }
      }
    }
  }

  @Test
  void onlySupportedDirectDiffFormsSelectOperationStyles() throws Exception {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory.resolve("direct-context")).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("shop", "products", "{\"price\":999}");
      new Query(chain, "replace json value of jn:doc('shop','products').price with 899").execute(ctx);
      for (final String source : List.of("jn:diff('shop','products',1,2)", "jn:diff('shop','products',1,2,0)",
          "jn:diff('shop','products',1,2,0,0)",
          "(: leading :) jn:diff ('shop',(: argument :) 'products',1,2) (: trailing :)")) {
        final String rendered = render(chain, ctx, source);
        assertTrue(rendered.contains("\033[33m\"update\"\033[0m"), source + rendered);
        assertSerializedPayload(chain, ctx, source, rendered);
      }
      for (final String source : List.of("(jn:diff('shop','products',1,2))", "jn:diff(('shop'),'products',1,2)",
          "jn:diff('shop','products',1,1+1)", "jn:diff('shop','products',1,2).diffs")) {
        final String rendered = render(chain, ctx, source);
        assertTrue(rendered.contains("\033[36m\"update\"\033[0m"), source + rendered);
        assertSerializedPayload(chain, ctx, source, rendered);
      }
    }
  }

  @Test
  void directDiffClassificationUsesLiteralBoundariesWithoutNativeNames() {
    for (final String prefix : List.of("jn", "xn")) {
      final DiffMode expected = prefix.equals("jn")
          ? DiffMode.JSON
          : DiffMode.XML;
      for (final String literal : List.of("'C:\\'", "'商店😀\\''path\\'", "\"C:\\\\\"", "\"a\\\"b\"")) {
        for (final String arguments : List.of(literal + ",'resource'", "'shop'," + literal)) {
          final String call = prefix + ":diff(" + arguments + ",1,2)";
          assertEquals(expected, ShellOutput.diffMode(call), call);
          assertEquals(expected, ShellOutput.diffMode("(: before :)\n" + call + " (: after :)"), call);
          for (final String source : List.of("(" + call + ")", call + ", 42", "if (false()) then " + call + " else ()",
              "declare function local:unused() { " + call + " }; ()"))
            assertEquals(DiffMode.NONE, ShellOutput.diffMode(source), source);
        }
      }
      for (final String source : List.of(prefix + ":diff('C:\\", prefix + ":diff('shop','resource\\",
          prefix + ":diff(\"shop\\\",'resource',1,2)"))
        assertEquals(DiffMode.NONE, ShellOutput.diffMode(source), source);
    }
  }

  private static String render(final SirixCompileChain chain, final SirixQueryContext ctx, final String source)
      throws Exception {
    final StringWriter out = new StringWriter();
    new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(Writer.nullWriter()), () -> 0L).result(
        new Query(chain, source), ctx, source);
    return out.toString();
  }

  private static void assertSerializedPayload(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String source, final String rendered) {
    final StringWriter serialized = new StringWriter();
    final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(serialized));
    serializer.setFormat(true);
    new Query(chain, source).serialize(ctx, serializer);
    final String plain = ShellResultWriterTest.strip(rendered);
    final String nl = System.lineSeparator();
    assertEquals("Query result" + nl + serialized + nl + "  0 ms" + nl + nl, plain, source);
  }

  @Test
  void structuralJsonReplacementDiffUsesModificationColour() throws Exception {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory.resolve("replace-diff")).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("shop", "products", "{\"price\":{\"amount\":999}}");
      new Query(chain, "replace json value of jn:doc('shop','products').price with 899").execute(ctx);
      final String source = "jn:diff('shop','products',1,2)";
      final StringWriter out = new StringWriter();
      new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(Writer.nullWriter())).result(
          new Query(chain, source), ctx, source);
      assertTrue(out.toString().contains("\033[33m\"replace\"\033[0m"), out.toString());
    }
  }

  @Test
  void terminalResultsArePrettyAndTimedAndErrorHasCode() throws Exception {
    final StringWriter out = new StringWriter();
    final StringWriter err = new StringWriter();
    final ShellOutput shell = new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(err));
    shell.result(new Query("{\"x\":{\"n\":1}}"), new BrackitQueryContext(), "{\"x\":{\"n\":1}}");
    final String plain = ShellResultWriterTest.strip(out.toString());
    assertTrue(plain.contains("\n  "), plain);
    assertTrue(plain.matches("(?s).*\\d+ ms.*"), plain);
    shell.error(new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE, "bad input"));
    assertTrue(err.toString().startsWith("\033[31mError ["), err.toString());
    assertTrue(err.toString().contains("XPTY0004"), err.toString());
    assertTrue(err.toString().contains("bad input"), err.toString());
  }
}
