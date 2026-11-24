package io.sirix.pit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import org.pitest.mutationtest.ClassMutationResults;
import org.pitest.mutationtest.MutationResult;
import org.pitest.mutationtest.MutationResultListener;
import org.pitest.mutationtest.engine.Location;
import org.pitest.mutationtest.engine.MutationDetails;
import org.pitest.mutationtest.engine.MutationIdentifier;

/**
 * Writes one {@code <mutation>} element per line, flushed as each analysed class finishes, so a run
 * killed by its deadline still reports the statuses it had already recorded (CONV-202).
 *
 * <p>
 * The lane's {@link StreamingCompletionAgent} delivers completed analysis units without waiting for
 * earlier submitted units. PIT's XML listener serialises at the end of the run, which is why a
 * killed partition left a zero-byte {@code mutations.xml} and discarded every finished mutant. The
 * elements here carry the fields the reporter reads, in PIT's own shape, so the reporter decodes
 * and parses each record independently, retaining the intact prefix when a kill tears the final
 * UTF-8 or XML record.
 */
public final class StreamingResultListener implements MutationResultListener {

  /** The file this listener writes inside PIT's report directory. */
  public static final String FILE = "mutations-stream.xml";

  private final Writer writer;

  StreamingResultListener(final Writer writer) {
    this.writer = writer;
  }

  @Override
  public void runStart() {
    // Nothing: the first analysed class writes the first line.
  }

  @Override
  public void handleMutationResult(final ClassMutationResults results) {
    try {
      for (final MutationResult result : results.getMutations()) {
        writer.write(render(result));
        writer.write('\n');
      }
      // One class per flush: the finest granularity PIT's listener contract offers.
      writer.flush();
    } catch (final IOException error) {
      throw new UncheckedIOException("cannot append to " + FILE, error);
    }
  }

  @Override
  public void runEnd() {
    try {
      writer.close();
    } catch (final IOException error) {
      throw new UncheckedIOException("cannot close " + FILE, error);
    }
  }

  private static String render(final MutationResult result) {
    final MutationDetails details = result.getDetails();
    final MutationIdentifier id = details.getId();
    final Location location = id.getLocation();
    final StringBuilder element = new StringBuilder(512);
    element.append("<mutation detected='")
           .append(result.getStatus().isDetected())
           .append("' status='")
           .append(result.getStatus().name())
           .append("' numberOfTestsRun='")
           .append(result.getNumberOfTestsRun())
           .append("'>");
    tag(element, "sourceFile", details.getFilename());
    tag(element, "mutatedClass", location.getClassName().asJavaName());
    tag(element, "mutatedMethod", location.getMethodName());
    tag(element, "methodDescription", location.getMethodDesc());
    tag(element, "lineNumber", Integer.toString(details.getLineNumber()));
    tag(element, "mutator", id.getMutator());
    element.append("<indexes>");
    final List<Integer> indexes = id.getIndexes();
    for (final Integer index : indexes) {
      tag(element, "index", Integer.toString(index.intValue()));
    }
    element.append("</indexes>");
    tag(element, "killingTest", result.getKillingTest().orElse(""));
    tag(element, "description", details.getDescription());
    return element.append("</mutation>").toString();
  }

  private static void tag(final StringBuilder element, final String name, final String value) {
    element.append('<').append(name).append('>');
    escape(element, value);
    element.append("</").append(name).append('>');
  }

  /** Escapes text and strips the line breaks that would split a record across lines. */
  private static void escape(final StringBuilder element, final String value) {
    for (int index = 0; index < value.length(); index++) {
      final char character = value.charAt(index);
      switch (character) {
        case '&' -> element.append("&amp;");
        case '<' -> element.append("&lt;");
        case '>' -> element.append("&gt;");
        case '"' -> element.append("&quot;");
        case '\'' -> element.append("&apos;");
        case '\n', '\r' -> element.append(' ');
        default -> element.append(character);
      }
    }
  }
}
