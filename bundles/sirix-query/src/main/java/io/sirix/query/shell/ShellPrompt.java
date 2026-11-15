package io.sirix.query.shell;

import io.brackit.query.QueryContext;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBCollection;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/** Conservative context indicator: never guesses dynamic query arguments or executes a query. */
public final class ShellPrompt {
  public record Reference(String database, String resource, int revision, boolean xml) {
  }

  private String text = "sirix> ";

  public String text() {
    return text;
  }

  public void observe(final String query, final QueryContext context, final boolean updating) {
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
      final int revision = updating || ref.revision() < 0
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
    final List<ShellHighlighter.Token> tokens = new ArrayList<>();
    for (final var token : ShellHighlighter.tokens(query)) {
      if (token.kind() != ShellHighlighter.Kind.COMMENT && !token.text().isBlank())
        tokens.add(token);
    }
    Reference found = null;
    for (int i = 0; i < tokens.size(); i++) {
      final String name = tokens.get(i).text();
      if (tokens.get(i).kind() != ShellHighlighter.Kind.FUNCTION
          || !(name.equals("jn:doc") || name.equals("jn:store") || name.equals("xn:doc") || name.equals("xn:store")))
        continue;
      if (i + 5 >= tokens.size() || !tokens.get(i + 1).text().equals("(")
          || tokens.get(i + 2).kind() != ShellHighlighter.Kind.STRING || !tokens.get(i + 3).text().equals(",")
          || tokens.get(i + 4).kind() != ShellHighlighter.Kind.STRING)
        return null;
      final String database = unquote(tokens.get(i + 2).text());
      final String resource = unquote(tokens.get(i + 4).text());
      if (database == null || resource == null)
        return null;
      int revision = -1;
      if (name.endsWith(":doc") && tokens.get(i + 5).text().equals(",")) {
        if (i + 7 >= tokens.size() || !tokens.get(i + 7).text().equals(")"))
          return null;
        try {
          revision = Integer.parseInt(tokens.get(i + 6).text());
        } catch (final NumberFormatException exception) {
          return null;
        }
        if (revision < 1)
          return null;
      }
      final Reference ref = new Reference(database, resource, revision, name.startsWith("xn:"));
      if (found != null && !found.equals(ref))
        return null;
      found = ref;
    }
    return found;
  }

  private static @Nullable String unquote(final String text) {
    if (text.length() < 2 || text.charAt(text.length() - 1) != text.charAt(0) || text.indexOf('\\') >= 0)
      return null;
    final String quote = text.substring(0, 1);
    return text.substring(1, text.length() - 1).replace(quote + quote, quote);
  }
}
