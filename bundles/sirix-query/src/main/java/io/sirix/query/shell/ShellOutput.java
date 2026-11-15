package io.sirix.query.shell;

import io.brackit.query.Query;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.sirix.query.SirixStringSerializer;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Objects;
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
    for (final var token : ShellHighlighter.tokens(source)) {
      if (token.kind() == ShellHighlighter.Kind.FUNCTION
          && (token.text().equals("jn:diff") || token.text().equals("xn:diff")))
        return true;
    }
    return false;
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
