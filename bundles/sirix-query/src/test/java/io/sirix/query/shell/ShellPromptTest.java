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
  void xmlMetadataAndLongPromptNamesAreHandledConservatively() {
    final String database = "s".repeat(81);
    final ShellPrompt prompt = new ShellPrompt();
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithNodeStore(store)) {
      store.create(database, new DocumentParser("<a/>"));
      prompt.observe("xn:doc('" + database + "','resource1')", ctx, false);
      assertEquals("sirix " + "s".repeat(80) + "/resource1@1> ", prompt.text());
      prompt.observe("xn:doc('" + database + "','missing')", ctx, false);
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
        prompt.observe(queries[i], ctx, query.getModule().getBody().isUpdating());
        assertEquals("sirix shop/products@" + revisions[i] + "> ", prompt.text());
      }
      prompt.observe("1+1", ctx, false);
      assertEquals("sirix> ", prompt.text());
    }
  }
}
