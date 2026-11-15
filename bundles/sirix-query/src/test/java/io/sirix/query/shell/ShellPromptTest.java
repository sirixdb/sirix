package io.sirix.query.shell;

import io.brackit.query.Query;
import io.sirix.api.Database;
import io.sirix.service.json.shredder.JsonShredder;
import java.io.StringWriter;
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
  void backslashNamesStayNeutralAfterSingleQuotedTokenBoundariesRecover() {
    for (final String prefix : List.of("jn", "xn")) {
      for (final String function : List.of("doc", "store")) {
        for (final String literal : List.of("'C:\\'", "'商店😀\\''path\\'", "\"C:\\\\\"")) {
          for (final String names : List.of(literal + ",'resource'", "'shop'," + literal)) {
            final String call = prefix + ":" + function + "(" + names + (function.equals("store")
                ? ",'[]')"
                : ")");
            assertNull(ShellPrompt.reference(call), call);
            if (prefix.equals("jn") && function.equals("doc")) {
              for (final String update : List.of("replace json value of $d[0] with 1", "append json 1 into $d",
                  "delete json $d[0]")) {
                final String source = "let $d := " + call + " return " + update;
                assertNull(ShellPrompt.reference(source), source);
              }
            }
          }
        }
      }
    }
  }

  @Test
  void encodedNamesStayNeutralAcrossLiteralFieldsAndForms() {
    for (final String prefix : List.of("jn", "xn")) {
      for (final String function : List.of("doc", "store")) {
        for (final String quote : List.of("'", "\"")) {
          for (final String[] names : new String[][] {{"a&amp;b", "resource"}, {"shop", "a&#38;b"},
              {"shop", "a&#x26;b"}}) {
            final String query = prefix + ":" + function + "(" + quote + names[0] + quote + "," + quote + names[1]
                + quote + (function.equals("store")
                    ? ",'[]'"
                    : "")
                + ")";
            assertNull(ShellPrompt.reference(query), query);
          }
        }
      }
    }
  }

  @Test
  void executedEntityAndCharacterReferencesCannotSelectOtherRevisionMetadata() throws Exception {
    final ShellPrompt prompt = new ShellPrompt();
    try (final var json = BasicJsonDBStore.newBuilder().location(directory.resolve("entity-json")).build();
        final var xml = BasicXmlDBStore.newBuilder().location(directory.resolve("entity-xml")).build();
        final var ctx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(xml, json,
            SirixQueryContext.CommitStrategy.AUTO);
        final var chain = SirixCompileChain.createWithNodeAndJsonStore(xml, json)) {
      final var jsonResources = json.create("shop", "a&b", "[1]");
      final var xmlResources = xml.create("shop", "a&b", new DocumentParser("<v>1</v>"));
      json.create("a&b", "products", "[1]");
      xml.create("a&b", "products", new DocumentParser("<v>1</v>"));
      for (final String encoded : List.of("a&amp;b", "a&#38;b", "a&#x26;b")) {
        try (final var reader = JsonShredder.createStringReader("[3]")) {
          jsonResources.add(encoded, reader).getTrx().close();
        }
        xmlResources.add(encoded, new DocumentParser("<v>3</v>")).getTrx().close();
        advanceToRevisionThree(jsonResources.getDatabase(), encoded);
        advanceToRevisionThree(xmlResources.getDatabase(), encoded);
        advanceToRevisionThree(json.create(encoded, "products", "[3]").getDatabase(), "products");
        advanceToRevisionThree(xml.create(encoded, "products", new DocumentParser("<v>3</v>")).getDatabase(),
            "products");
        for (final String prefix : List.of("jn", "xn")) {
          final String actual = prefix.equals("jn")
              ? "[1]"
              : "<v>1</v>";
          final String other = prefix.equals("jn")
              ? "[3]"
              : "<v>3</v>";
          for (final String[] names : new String[][] {{"shop", encoded}, {encoded, "products"}}) {
            final String arguments = "'" + names[0] + "','" + names[1] + "'";
            for (final String revision : List.of("", ",1")) {
              final String query = prefix + ":doc(" + arguments + revision + ")";
              assertEquals(actual, serialize(chain, ctx, query), query);
              prompt.observe(query, ctx);
              assertEquals("sirix> ", prompt.text(), query);
            }
            final String escapedArguments = arguments.replace("&", "&amp;");
            final String otherQuery = prefix + ":doc(" + escapedArguments + ")";
            assertEquals(other, serialize(chain, ctx, otherQuery), otherQuery);
            prompt.observe(otherQuery, ctx);
            assertEquals("sirix> ", prompt.text(), otherQuery);
          }
        }
      }
      for (final String arguments : List.of("'shop','a&amp;b'", "'a&amp;b','products'")) {
        final String update = "let $d := jn:doc(" + arguments + ") return replace json value of $d[0] with 2";
        executeAndObserve(chain, ctx, prompt, update);
        assertEquals("sirix> ", prompt.text(), update);
        assertEquals("[2]", serialize(chain, ctx, "jn:doc(" + arguments + ")"));
        assertEquals("2", serialize(chain, ctx, "sdb:revision(jn:doc(" + arguments + "))"));
      }
      executeAndObserve(chain, ctx, prompt, "jn:store('a&amp;b','products','[9]')");
      assertEquals("sirix> ", prompt.text());
      assertEquals("[9]", serialize(chain, ctx, "jn:doc('a&amp;b','products')"));
      assertEquals("1", serialize(chain, ctx, "sdb:revision(jn:doc('a&amp;b','products'))"));
      executeAndObserve(chain, ctx, prompt, "jn:store('plain','products','[7]')");
      assertEquals("sirix plain/products@1> ", prompt.text());
    }
  }

  private static void advanceToRevisionThree(final Database<?> database, final String resource) throws Exception {
    try (final var session = database.beginResourceSession(resource); final var writer = session.beginNodeTrx()) {
      writer.commit();
      writer.commit();
      assertEquals(3, session.getMostRecentRevisionNumber());
    }
  }

  private static String serialize(final SirixCompileChain chain, final SirixQueryContext ctx, final String source) {
    final StringWriter out = new StringWriter();
    new Query(chain, source).serialize(ctx, new PrintWriter(out));
    return out.toString();
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
        final String resource = prefix.equals("jn")
            ? "products"
            : "resource1";
        if (prefix.equals("jn")) {
          executeAndObserve(chain, ctx, prompt, "jn:store('shop','products','[1]')");
        } else {
          xml.create("shop", new DocumentParser("<a/>"));
          executeAndObserve(chain, ctx, prompt, "xn:doc('shop','resource1')");
        }
        assertEquals("sirix shop/" + resource + "@1> ", prompt.text());
        for (int revision = 2; revision <= 4; revision++) {
          new Query(chain, "sdb:commit(" + prefix + ":doc('shop','" + resource + "'))").serialize(ctx,
              new PrintWriter(Writer.nullWriter()));
        }
        final String doc = prefix + ":doc('shop','" + resource + "',1)";
        for (final String query : List.of("sdb:commit(" + doc + ")", "if (false()) then " + doc + " else 42",
            "for $x in () return " + doc, "let $unused := " + doc + " return 42", "(" + doc + ", 42)",
            "declare function local:unused() { " + doc + " }; 42")) {
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
    for (final String input : List.of("jn:doc('a','b') + 1", "jn:store('a','b',$data)", "jn:store('a','b','[]',true())",
        "jn:doc('a','b',1,2)", "let $d", "let $d = jn:doc('a','b')", "let $d := jn:doc('a','b') return 42",
        "let $d := jn:doc('a','b') return delete json $d", "let $d := jn:doc('a','b') return replace json value",
        "let $d := jn:doc('a','b') return replace json value of $d",
        "let $d := jn:doc('a','b',1) return delete json $d[0]",
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
        assertDoesNotThrow(() -> {
          ShellPrompt.reference(partial);
        }, partial);
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
