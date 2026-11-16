package io.sirix.query.shell;

import io.brackit.query.QueryContext;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/** Conservative context indicator: never guesses dynamic query arguments or executes a query. */
public final class ShellPrompt {
  public record Reference(String database, String resource, int revision, boolean xml) {
  }

  private static final Set<String> UPDATE_KEYWORDS =
      Set.of("replace", "json", "value", "of", "with", "append", "into", "delete", "true", "false", "null");

  private String text = "sirix> ";

  public String text() {
    return text;
  }

  public void observe(final String query, final QueryContext context) {
    text = "sirix> ";
    final Reference ref = reference(query);
    if (ref == null)
      return;
    try {
      final int latest;
      if (ref.xml()) {
        final var collection = context.getNodeStore().lookup(ref.database());
        if (!(collection instanceof XmlDBCollection xml))
          return;
        if (!xml.getDatabase().existsResource(ref.resource()))
          return;
        latest = xml.getDatabase().beginResourceSession(ref.resource()).getMostRecentRevisionNumber();
      } else {
        final var collection = context.getJsonItemStore().lookup(ref.database());
        if (!(collection instanceof JsonDBCollection json))
          return;
        if (!json.getDatabase().existsResource(ref.resource()))
          return;
        latest = json.getDatabase().beginResourceSession(ref.resource()).getMostRecentRevisionNumber();
      }
      final int revision = ref.revision() < 0
          ? latest
          : ref.revision();
      text = "sirix " + safe(ref.database()) + "/" + safe(ref.resource()) + "@" + revision + "> ";
    } catch (final RuntimeException exception) {
      // A dropped/closed resource must never turn successful query output into an error.
    }
  }

  private static String safe(final String name) {
    final StringBuilder safe = new StringBuilder(Math.min(name.length(), 80));
    for (int i = 0; i < name.length() && i < 80; i++) {
      final char c = name.charAt(i);
      safe.append(Character.isISOControl(c)
          ? '?'
          : c);
    }
    return safe.toString();
  }

  public static @Nullable Reference reference(final String query) {
    final List<ShellHighlighter.Token> tokens = significantTokens(query);
    if (tokens.isEmpty())
      return null;
    final boolean bound = tokens.getFirst().text().equals("let");
    final int start = bound
        ? 4
        : 0;
    if (tokens.size() - start < 6 || (bound && !boundPrefix(tokens)))
      return null;
    final String name = tokens.get(start).text();
    if (!literalCall(tokens, start, name))
      return null;
    final String database = unquote(tokens.get(start + 2).text());
    final String resource = unquote(tokens.get(start + 4).text());
    if (database == null || resource == null)
      return null;
    int end = start + 5;
    int revision = -1;
    if (name.endsWith(":store")) {
      if (end + 2 >= tokens.size() || !tokens.get(end).text().equals(",")
          || tokens.get(end + 1).kind() != ShellHighlighter.Kind.STRING)
        return null;
      end += 2;
    } else if (tokens.get(end).text().equals(",")) {
      revision = literalRevision(tokens, end);
      if (revision < 1)
        return null;
      end += 2;
    }
    if (!tokens.get(end).text().equals(")") || !supportedSuffix(tokens, end, name, revision, bound))
      return null;
    return new Reference(database, resource, revision, name.startsWith("xn:"));
  }

  private static int literalRevision(final List<ShellHighlighter.Token> tokens, final int end) {
    if (end + 2 >= tokens.size())
      return 0;
    try {
      return Integer.parseInt(tokens.get(end + 1).text());
    } catch (final NumberFormatException _) {
      return 0;
    }
  }

  private static boolean supportedSuffix(final List<ShellHighlighter.Token> tokens, final int end, final String name,
      final int revision, final boolean bound) {
    if (bound)
      return name.equals("jn:doc") && revision == -1 && simpleUpdate(tokens, end + 1, tokens.get(1).text());
    return end == tokens.size() - 1;
  }

  private static List<ShellHighlighter.Token> significantTokens(final String query) {
    final List<ShellHighlighter.Token> tokens = new ArrayList<>();
    for (final var token : ShellHighlighter.tokens(query)) {
      if (token.kind() != ShellHighlighter.Kind.COMMENT && !token.text().isBlank())
        tokens.add(token);
    }
    return tokens;
  }

  private static boolean boundPrefix(final List<ShellHighlighter.Token> tokens) {
    return tokens.get(1).kind() == ShellHighlighter.Kind.VARIABLE && tokens.get(2).text().equals(":")
        && tokens.get(3).text().equals("=");
  }

  private static boolean literalCall(final List<ShellHighlighter.Token> tokens, final int start, final String name) {
    return (name.equals("jn:doc") || name.equals("jn:store") || name.equals("xn:doc") || name.equals("xn:store"))
        && tokens.get(start + 1).text().equals("(") && tokens.get(start + 2).kind() == ShellHighlighter.Kind.STRING
        && tokens.get(start + 3).text().equals(",") && tokens.get(start + 4).kind() == ShellHighlighter.Kind.STRING;
  }

  private static boolean simpleUpdate(final List<ShellHighlighter.Token> tokens, final int start,
      final String variable) {
    if (start + 4 >= tokens.size() || !tokens.get(start).text().equals("return"))
      return false;
    final String operation = tokens.get(start + 1).text();
    final boolean target = switch (operation) {
      case "delete" -> tokens.get(start + 2).text().equals("json") && tokens.get(start + 3).text().equals(variable);
      case "replace" -> start + 6 < tokens.size() && tokens.get(start + 2).text().equals("json")
          && tokens.get(start + 3).text().equals("value") && tokens.get(start + 4).text().equals("of")
          && tokens.get(start + 5).text().equals(variable);
      case "append" -> tokens.get(start + 2).text().equals("json")
          && tokens.get(tokens.size() - 2).text().equals("into") && tokens.getLast().text().equals(variable);
      default -> false;
    };
    if (!target)
      return false;
    for (int i = start + 1; i < tokens.size(); i++) {
      final var token = tokens.get(i);
      if (token.kind() == ShellHighlighter.Kind.FUNCTION
          || (token.kind() == ShellHighlighter.Kind.VARIABLE && !token.text().equals(variable))
          || (token.kind() == ShellHighlighter.Kind.KEYWORD && !UPDATE_KEYWORDS.contains(token.text())))
        return false;
    }
    return true;
  }

  private static @Nullable String unquote(final String text) {
    if (text.length() < 2 || text.charAt(text.length() - 1) != text.charAt(0) || text.indexOf('\\') >= 0
        || text.indexOf('&') >= 0)
      return null;
    final String quote = text.substring(0, 1);
    return text.substring(1, text.length() - 1).replace(quote + quote, quote);
  }
}
