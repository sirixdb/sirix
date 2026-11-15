package io.sirix.query.shell;

import io.brackit.query.Query;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.brackit.query.node.parser.DocumentParser;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.io.PrintWriter;
import java.io.Writer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;

final class ShellPromptTest {
  @TempDir
  Path directory;

  @Test
  void literalReferencesIgnoreCommentsAndStringsAndRejectAmbiguity() {
    assertEquals(new ShellPrompt.Reference("shop", "products", 1, false),
        ShellPrompt.reference("(: jn:doc('bad','bad') :) jn:doc('shop', 'products', 1)"));
    assertNull(ShellPrompt.reference("\"jn:doc('bad','bad')\""));
    assertNull(ShellPrompt.reference("jn:doc($db, 'products')"));
    assertNull(ShellPrompt.reference("(jn:doc('a','b'), jn:doc('c','d'))"));
    assertNull(ShellPrompt.reference("jn:doc('a','b', xs:dateTime('2020-01-01T00:00:00Z'))"));
    assertNull(ShellPrompt.reference("jn:doc('a','b', 999999999999999999999)"));
    assertEquals(new ShellPrompt.Reference("it's", "商品", -1, true), ShellPrompt.reference("xn:doc('it''s','商品')"));
  }

  @Test
  void truncatedAndEscapedReferencesStayNeutral() {
    for (final String input : List.of("jn:doc(", "jn:doc('a','b'", "jn:doc('a','b',1", "jn:doc('a\\b','r')",
        "jn:doc('a','b',0)", "jn:doc('a','b',-1)", "other:doc('a','b')", "xn:store($a,'b')")) {
      assertNull(ShellPrompt.reference(input), input);
    }
    assertEquals(new ShellPrompt.Reference("", "r", -1, false), ShellPrompt.reference("jn:doc('','r')"));
    assertEquals(new ShellPrompt.Reference("a", "b", -1, true), ShellPrompt.reference("xn:store('a','b','<x/>')"));
  }

  @Test
  void executedNestedAndSkippedReferencesStayNeutralForBothStores() {
    final ShellPrompt prompt = new ShellPrompt();
    try (final var json = BasicJsonDBStore.newBuilder().location(directory.resolve("json")).build();
        final var xml = BasicXmlDBStore.newBuilder().location(directory.resolve("xml")).build();
        final var ctx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(xml, json,
            SirixQueryContext.CommitStrategy.AUTO);
        final var chain = SirixCompileChain.createWithNodeAndJsonStore(xml, json)) {
      for (final String prefix : List.of("jn", "xn")) {
        final String resource = prefix.equals("jn") ? "products" : "resource1";
        if (prefix.equals("jn")) {
          executeAndObserve(chain, ctx, prompt, "jn:store('shop','products','[1]')");
        } else {
          xml.create("shop", new DocumentParser("<a/>"));
          executeAndObserve(chain, ctx, prompt, "xn:doc('shop','resource1')");
        }
        assertEquals("sirix shop/" + resource + "@1> ", prompt.text());
        for (int revision = 2; revision <= 4; revision++) {
          new Query(chain, "sdb:commit(" + prefix + ":doc('shop','" + resource + "'))")
              .serialize(ctx, new PrintWriter(Writer.nullWriter()));
        }
        final String doc = prefix + ":doc('shop','" + resource + "',1)";
        for (final String query : List.of("sdb:commit(" + doc + ")", "if (false()) then " + doc + " else 42",
            "for $x in () return " + doc, "let $unused := " + doc + " return 42",
            "(" + doc + ", 42)", "declare function local:unused() { " + doc + " }; 42")) {
          executeAndObserve(chain, ctx, prompt, query);
          assertEquals("sirix> ", prompt.text(), query);
        }
        executeAndObserve(chain, ctx, prompt, doc);
        assertEquals("sirix shop/" + resource + "@1> ", prompt.text());
        executeAndObserve(chain, ctx, prompt, prefix + ":doc('shop','" + resource + "')");
        assertEquals("sirix shop/" + resource + "@5> ", prompt.text());
      }
    }
  }

  private static void executeAndObserve(final SirixCompileChain chain, final SirixQueryContext ctx,
      final ShellPrompt prompt, final String input) {
    final Query query = new Query(chain, input);
    query.serialize(ctx, new PrintWriter(Writer.nullWriter()));
    prompt.observe(input, ctx);
  }

  @Test
  void unsupportedAndIncompleteUpdateShapesStayNeutral() {
    for (final String input : List.of("jn:doc('a','b') + 1", "jn:store('a','b',$data)",
        "jn:store('a','b','[]',true())", "jn:doc('a','b',1,2)", "let $d", "let $d = jn:doc('a','b')",
        "let $d := jn:doc('a','b') return 42", "let $d := jn:doc('a','b') return delete json $d",
        "let $d := jn:doc('a','b') return replace json value",
        "let $d := jn:doc('a','b') return replace json value of $d", "let $d := jn:doc('a','b',1) return delete json $d[0]",
        "let $d := jn:doc('a','b') return if (false()) then delete json $d[0] else ()",
        "let $d := jn:doc('a','b') return replace json value of $other[0] with 1",
        "let $d := jn:doc('a','b') return append json true() into $d",
        "let $d := jn:doc('a','b') return append json 1 into $other")) {
      assertNull(ShellPrompt.reference(input), input);
    }
    for (final String input : List.of("jn:doc('a','b',1)", "jn:store('a','b','[]')",
        "let $d := jn:doc('a','b') return replace json value of $d[0] with 1")) {
      for (int end = 0; end <= input.length(); end++) {
        final String partial = input.substring(0, end);
        assertDoesNotThrow(() -> ShellPrompt.reference(partial), partial);
      }
    }
  }

  @Test
  void xmlMetadataAndLongPromptNamesAreHandledConservatively() {
    final String database = "s".repeat(81);
    final ShellPrompt prompt = new ShellPrompt();
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithNodeStore(store)) {
      store.create(database, new DocumentParser("<a/>"));
      prompt.observe("xn:doc('" + database + "','resource1')", ctx);
      assertEquals("sirix " + "s".repeat(80) + "/resource1@1> ", prompt.text());
      prompt.observe("xn:doc('" + database + "','missing')", ctx);
      assertEquals("sirix> ", prompt.text());
    }
  }

  @Test
  void actualCommitsAdvanceAndHistoricalReadsReturnToRevisionOne() {
    final ShellPrompt prompt = new ShellPrompt();
    assertEquals("sirix> ", prompt.text());
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String[] queries =
          {"jn:store('shop','products','[{\"name\":\"Laptop\",\"price\":999},{\"name\":\"Phone\",\"price\":699}]')",
              "let $doc := jn:doc('shop','products') return replace json value of $doc[0].price with 899",
              "let $doc := jn:doc('shop','products') return append json {\"name\":\"Tablet\",\"price\":449} into $doc",
              "let $doc := jn:doc('shop','products') return delete json $doc[1]", "jn:doc('shop','products',1)"};
      final int[] revisions = {1, 2, 3, 4, 1};
      for (int i = 0; i < queries.length; i++) {
        final Query query = new Query(chain, queries[i]);
        query.serialize(ctx, new PrintWriter(Writer.nullWriter()));
        prompt.observe(queries[i], ctx);
        assertEquals("sirix shop/products@" + revisions[i] + "> ", prompt.text());
      }
      prompt.observe("1+1", ctx);
      assertEquals("sirix> ", prompt.text());
    }
  }
}
