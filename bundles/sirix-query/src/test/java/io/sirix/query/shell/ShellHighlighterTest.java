package io.sirix.query.shell;

import org.junit.jupiter.api.Test;
import org.jline.utils.AttributedString;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ShellHighlighterTest {
  @Test
  void lexicalCategoriesFollowJsoniqRatherThanSubstringMatching() {
    final var tokens = ShellHighlighter.tokens(
        "let $doc := jn:doc('商店','products', 12.5e-2) (: a (: nested :) :) return {\"x\": true()}");
    assertEquals(ShellHighlighter.Kind.KEYWORD, tokens.getFirst().kind());
    assertToken(tokens, "$doc", ShellHighlighter.Kind.VARIABLE);
    assertToken(tokens, "jn:doc", ShellHighlighter.Kind.FUNCTION);
    assertToken(tokens, "'商店'", ShellHighlighter.Kind.STRING);
    assertToken(tokens, "12.5e-2", ShellHighlighter.Kind.NUMBER);
    assertToken(tokens, "(: a (: nested :) :)", ShellHighlighter.Kind.COMMENT);
    assertToken(tokens, "{", ShellHighlighter.Kind.BRACE);
    assertToken(tokens, "true", ShellHighlighter.Kind.FUNCTION);
    assertToken(ShellHighlighter.tokens("returning"), "returning", ShellHighlighter.Kind.TEXT);
  }

  @Test
  void expressionKeywordsBeforeParenthesesKeepKeywordStyle() {
    final ShellHighlighter highlighter = new ShellHighlighter(ShellPalette.ANSI_16);
    final var keywordStyle = AttributedString.fromAnsi("\033[33mX").styleAt(0);
    final var functionStyle = AttributedString.fromAnsi("\033[36mX").styleAt(0);
    for (final String input : List.of("if (true()) then 1 else 2", "for $x in (1,2) return ($x+1)",
        "typeswitch (1)", "switch (1)", "if(", "return\n(")) {
      final String keyword = input.startsWith("for") ? "return" : input.split("[ (\\n]", 2)[0];
      assertEquals(keywordStyle, highlighter.highlight(null, input).styleAt(input.indexOf(keyword)), input);
    }
    for (final String name : List.of("true", "false", "not", "empty", "fn:true", "local:if")) {
      assertEquals(functionStyle, highlighter.highlight(null, name + " ()").styleAt(0), name);
    }
    for (final String name : List.of("true", "false", "not", "empty")) {
      assertEquals(keywordStyle, highlighter.highlight(null, name).styleAt(0), name);
    }
    highlighter.prefix("for $x in (1,2)\n");
    assertEquals(keywordStyle, highlighter.highlight(null, "return ($x+1)").styleAt(0));
  }

  @Test
  void incompleteEscapedAndUnicodeInputRoundTripsWhileTyping() {
    for (final String input : List.of("'unterminated 😀", "\"a\\\"b\"", "'it''s'", "(: (:", "(: x :) 1", "[]{}()",
        "-3 .5 2e+4", "$变量", "")) {
      final var reconstructed = new StringBuilder();
      for (final var token : ShellHighlighter.tokens(input))
        reconstructed.append(token.text());
      assertEquals(input, reconstructed.toString());
      assertEquals(input, new ShellHighlighter(ShellPalette.ANSI_16).highlight(null, input).toString());
    }
    assertToken(ShellHighlighter.tokens("(: (:"), "(: (:", ShellHighlighter.Kind.COMMENT);
    assertToken(ShellHighlighter.tokens("'unfinished"), "'unfinished", ShellHighlighter.Kind.STRING);
  }

  @Test
  void goldenInputAndDisabledMode() {
    assertEquals("let 1", new ShellHighlighter(ShellPalette.NONE).highlight(null, "let 1").toAnsi());
    final var highlighted = new ShellHighlighter(ShellPalette.ANSI_16).highlight(null, "let 1");
    assertTrue(highlighted.styleAt(0).getStyle() != highlighted.styleAt(3).getStyle());
  }

  @Test
  void multilineCommentContinuesAndLiteralEscapeCharactersAreNotInterpreted() {
    final ShellHighlighter highlighter = new ShellHighlighter(ShellPalette.ANSI_16);
    highlighter.prefix("(: open comment\n");
    final var highlighted = highlighter.highlight(null, "still comment :) let");
    assertEquals("still comment :) let", highlighted.toString());
    assertTrue(highlighted.styleAt(0).getStyle() != highlighted.styleAt(18).getStyle());
    highlighter.prefix("");
    assertEquals("'\033[31m'", highlighter.highlight(null, "'\033[31m'").toString());
  }

  @Test
  void eachLexicalCategoryUsesTheSpecifiedTerminalStyle() {
    final ShellHighlighter highlighter = new ShellHighlighter(ShellPalette.ANSI_16);
    for (final String[] sample : new String[][] {{"let", "33"}, {"42", "33"}, {"$x", "36"}, {"sdb:revision()", "36"},
        {"{", "36"}, {"'abc'", "32"}, {"(: comment :)", "2"}, {"name", "0"}}) {
      assertEquals(AttributedString.fromAnsi("\033[" + sample[1] + "mX").styleAt(0),
          highlighter.highlight(null, sample[0]).styleAt(0), sample[0]);
    }
    highlighter.setErrorIndex(0);
    highlighter.setErrorPattern(null);
    assertEquals("let", highlighter.highlight(null, "let").toString());
  }

  @Test
  void everyTruncatedTokenKeepsItsExactLexicalBoundary() {
    for (final String input : List.of("(", "(:x(", "(:x:", "(:x(:", "'abc\\", ".", ".5", "1.25", "1e", "1e+", "1e-4",
        "1E4")) {
      final var tokens = ShellHighlighter.tokens(input);
      assertEquals(1, tokens.size(), input);
      assertEquals(input, tokens.getFirst().text());
    }
    assertEquals(ShellHighlighter.Kind.NUMBER, ShellHighlighter.tokens(".5").getFirst().kind());
    assertEquals(ShellHighlighter.Kind.NUMBER, ShellHighlighter.tokens("1e-4").getFirst().kind());
    assertEquals(List.of(new ShellHighlighter.Token("\"a\\\"b\"", ShellHighlighter.Kind.STRING),
        new ShellHighlighter.Token(" ", ShellHighlighter.Kind.TEXT),
        new ShellHighlighter.Token("x", ShellHighlighter.Kind.TEXT)), ShellHighlighter.tokens("\"a\\\"b\" x"));
    assertToken(ShellHighlighter.tokens("(: x :) return"), "return", ShellHighlighter.Kind.KEYWORD);
    assertToken(ShellHighlighter.tokens("1e+4 x"), "1e+4", ShellHighlighter.Kind.NUMBER);
    assertToken(ShellHighlighter.tokens("1e4 x"), "1e4", ShellHighlighter.Kind.NUMBER);
  }

  private static void assertToken(final List<ShellHighlighter.Token> tokens, final String text,
      final ShellHighlighter.Kind kind) {
    assertTrue(tokens.stream().anyMatch(t -> t.text().equals(text) && t.kind() == kind), text);
  }
}
