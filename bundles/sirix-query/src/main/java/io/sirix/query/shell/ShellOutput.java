package io.sirix.query.shell;

import io.brackit.query.Query;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.sirix.query.SirixStringSerializer;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/** Presentation of an already serialized result; the plain branch retains the CLI byte contract. */
public final class ShellOutput {
  private final ShellPalette palette;
  private final PrintWriter out;
  private final PrintWriter err;
  private final LongSupplier clock;

  public ShellOutput(final ShellPalette palette, final PrintWriter out, final PrintWriter err) {
    this(palette, out, err, System::nanoTime);
  }

  ShellOutput(final ShellPalette palette, final PrintWriter out, final PrintWriter err, final LongSupplier clock) {
    this.palette = Objects.requireNonNull(palette);
    this.out = Objects.requireNonNull(out);
    this.err = Objects.requireNonNull(err);
    this.clock = Objects.requireNonNull(clock);
  }

  public void result(final Query query, final QueryContext context, final String source) throws IOException {
    final long started = palette.enabled()
        ? clock.getAsLong()
        : 0;
    if (!palette.enabled())
      out.println();
    out.println(palette.subtle() + "Query result" + palette.reset());
    if (palette.enabled()) {
      try (final ShellResultWriter writer = new ShellResultWriter(out, palette, isDiff(source))) {
        final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(writer));
        serializer.setFormat(true);
        query.serialize(context, serializer);
      }
    } else {
      final SirixStringSerializer serializer = new SirixStringSerializer(out);
      serializer.setFormat(query.isPrettyPrint());
      query.serialize(context, serializer);
    }
    out.println();
    if (palette.enabled())
      out.println(palette.subtle() + "  " + ((clock.getAsLong() - started) / 1_000_000) + " ms" + palette.reset());
    out.println();
    out.flush();
  }

  private static boolean isDiff(final String source) {
    final List<ShellHighlighter.Token> tokens = new ArrayList<>(14);
    for (final var token : ShellHighlighter.tokens(source)) {
      if (token.kind() != ShellHighlighter.Kind.COMMENT && !token.text().isBlank())
        tokens.add(token);
    }
    final int size = tokens.size();
    if (size < 10)
      return false;
    final String name = tokens.getFirst().text();
    final boolean json = name.equals("jn:diff");
    if (!json && !name.equals("xn:diff"))
      return false;
    if (size > (json
        ? 14
        : 10) || size % 2 != 0 || !tokens.get(1).text().equals("(") || !tokens.getLast().text().equals(")"))
      return false;
    for (int i = 2; i < size - 1; i += 2) {
      final ShellHighlighter.Kind expected = i < 6
          ? ShellHighlighter.Kind.STRING
          : ShellHighlighter.Kind.NUMBER;
      if (tokens.get(i).kind() != expected || (i > 2 && !tokens.get(i - 1).text().equals(",")))
        return false;
    }
    return true;
  }

  public void error(final QueryException exception) {
    if (palette.enabled()) {
      final String prefix = exception.getCode() + ": ";
      final String description = exception.getDescription();
      final String message = description.startsWith(prefix)
          ? description.substring(prefix.length())
          : description;
      err.println(palette.red() + "Error [" + exception.getCode() + "]: " + message + palette.reset());
    } else
      err.println("Error: " + exception.getMessage());
    err.flush();
  }
}
