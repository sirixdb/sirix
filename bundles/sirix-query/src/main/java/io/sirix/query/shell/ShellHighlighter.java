package io.sirix.query.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jspecify.annotations.Nullable;

/** Tolerant lexical highlighting, including incomplete input and nested XQuery comments. */
public final class ShellHighlighter implements Highlighter {
  public enum Kind {
    TEXT, KEYWORD, FUNCTION, VARIABLE, STRING, NUMBER, COMMENT, BRACE
  }
  public record Token(String text, Kind kind) {
  }

  private static final Set<String> KEYWORDS = Set.of("let", "for", "in", "return", "where", "order", "by", "ascending",
      "descending", "group", "count", "at", "if", "then", "else", "some", "every", "satisfies", "and", "or", "not",
      "eq", "ne", "lt", "le", "gt", "ge", "div", "idiv", "mod", "to", "union", "intersect", "except", "instance", "of",
      "cast", "as", "castable", "treat", "typeswitch", "switch", "case", "default", "try", "catch", "declare",
      "namespace", "function", "variable", "external", "import", "module", "version", "jsoniq", "xquery", "replace",
      "json", "value", "with", "insert", "delete", "append", "into", "rename", "node", "nodes", "first", "last",
      "before", "after", "copy", "modify", "updating", "true", "false", "null", "empty", "allowing", "stable");
  private final ShellPalette palette;
  private String prefix = "";

  public void prefix(final String prefix) {
    this.prefix = Objects.requireNonNull(prefix);
  }

  public ShellHighlighter(final ShellPalette palette) {
    this.palette = Objects.requireNonNull(palette);
  }

  @Override
  public AttributedString highlight(final @Nullable LineReader reader, final String buffer) {
    if (!palette.enabled())
      return new AttributedString(buffer);
    final AttributedStringBuilder highlighted = new AttributedStringBuilder(buffer.length());
    int position = 0;
    for (final Token token : tokens(prefix + buffer)) {
      final String colour = switch (token.kind()) {
        case KEYWORD, NUMBER -> palette.orange();
        case FUNCTION, VARIABLE, BRACE -> palette.blue();
        case STRING -> palette.green();
        case COMMENT -> palette.subtle();
        case TEXT -> "";
      };
      final int skip = Math.max(0, prefix.length() - position);
      if (skip < token.text().length()) {
        final AttributedStyle style = colour.isEmpty()
            ? AttributedStyle.DEFAULT
            : AttributedString.fromAnsi(colour + "x").styleAt(0);
        highlighted.style(style).append(token.text(), skip, token.text().length());
      }
      position += token.text().length();
    }
    return highlighted.toAttributedString();
  }

  /** Tokens cover the exact input once, including whitespace and malformed partial tokens. */
  public static List<Token> tokens(final String input) {
    final List<Token> tokens = new ArrayList<>();
    int offset = 0;
    while (offset < input.length()) {
      final int start = offset;
      final char c = input.charAt(offset++);
      final Kind kind;
      if (c == '(' && offset < input.length() && input.charAt(offset) == ':') {
        offset++;
        int depth = 1;
        while (offset < input.length() && depth != 0) {
          if (offset + 1 < input.length() && input.charAt(offset) == '(' && input.charAt(offset + 1) == ':') {
            depth++;
            offset += 2;
          } else if (offset + 1 < input.length() && input.charAt(offset) == ':' && input.charAt(offset + 1) == ')') {
            depth--;
            offset += 2;
          } else
            offset++;
        }
        kind = Kind.COMMENT;
      } else if (c == '\'' || c == '"') {
        while (offset < input.length()) {
          final char next = input.charAt(offset++);
          if (next == '\\' && offset < input.length())
            offset++;
          else if (next == c) {
            if (offset < input.length() && input.charAt(offset) == c)
              offset++;
            else
              break;
          }
        }
        kind = Kind.STRING;
      } else if (Character.isDigit(c)
          || (c == '.' && offset < input.length() && Character.isDigit(input.charAt(offset)))) {
        while (offset < input.length() && Character.isDigit(input.charAt(offset)))
          offset++;
        if (offset < input.length() && input.charAt(offset) == '.') {
          offset++;
          while (offset < input.length() && Character.isDigit(input.charAt(offset)))
            offset++;
        }
        if (offset < input.length() && (input.charAt(offset) == 'e' || input.charAt(offset) == 'E')) {
          offset++;
          if (offset < input.length() && (input.charAt(offset) == '+' || input.charAt(offset) == '-'))
            offset++;
          while (offset < input.length() && Character.isDigit(input.charAt(offset)))
            offset++;
        }
        kind = Kind.NUMBER;
      } else if (c == '$' || Character.isUnicodeIdentifierStart(c)) {
        while (offset < input.length() && namePart(input.charAt(offset)))
          offset++;
        int next = offset;
        while (next < input.length() && Character.isWhitespace(input.charAt(next)))
          next++;
        kind = c == '$'
            ? Kind.VARIABLE
            : next < input.length() && input.charAt(next) == '('
                ? Kind.FUNCTION
                : KEYWORDS.contains(input.substring(start, offset))
                    ? Kind.KEYWORD
                    : Kind.TEXT;
      } else
        kind = "{}[]()".indexOf(c) >= 0
            ? Kind.BRACE
            : Kind.TEXT;
      tokens.add(new Token(input.substring(start, offset), kind));
    }
    return tokens;
  }

  private static boolean namePart(final char c) {
    return Character.isUnicodeIdentifierPart(c) || c == ':' || c == '-' || c == '.';
  }

  @Override
  public void setErrorPattern(final @Nullable Pattern errorPattern) {}

  @Override
  public void setErrorIndex(final int errorIndex) {}
}
