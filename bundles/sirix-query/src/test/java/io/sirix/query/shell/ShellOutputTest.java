package io.sirix.query.shell;

import io.brackit.query.Query;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.sirix.query.SirixStringSerializer;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.json.BasicJsonDBStore;
import java.nio.file.Path;
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
      final String source = "xn:diff('shop','resource1',1,2)";
      final StringWriter out = new StringWriter();
      new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(Writer.nullWriter())).result(
          new Query(chain, source), ctx, source);
      assertTrue(out.toString().contains("\033[33mreplace\033[0m"), out.toString());
    }
    final String source = "(: jn:diff('a','b',1,2) :) {\"insert\":1}";
    final StringWriter out = new StringWriter();
    new ShellOutput(ShellPalette.ANSI_16, new PrintWriter(out), new PrintWriter(Writer.nullWriter())).result(
        new Query(source), new BrackitQueryContext(), source);
    assertTrue(out.toString().contains("\033[36m\"insert\"\033[0m"), out.toString());
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
