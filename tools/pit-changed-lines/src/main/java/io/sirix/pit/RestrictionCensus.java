package io.sirix.pit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.pitest.mutationtest.engine.Location;
import org.pitest.mutationtest.engine.MutationDetails;
import org.pitest.mutationtest.engine.MutationIdentifier;

/**
 * The identities of the mutants the changed-line restriction removed (CONV-205).
 *
 * <p>
 * One JSON object per line: the six identity fields of CONV-201, plus the mutated class, its source
 * file and its line, so the reporter can re-derive each entry's attribution from its own
 * changed-line index and fail the run if the restriction removed a mutant the verdict judges.
 * Nothing else is recorded, and no status is: these mutants were generated and never run.
 *
 * <p>
 * Records are deduplicated by identity, because an interceptor that participates in PIT's pre-scan
 * sees a class more than once, and written on {@link #flush()} at every class boundary, so a run
 * killed by its deadline leaves a usable census of the classes it reached.
 *
 * <p>
 * Writes append and never truncate. PIT may build its interceptor pipeline more than once in a run
 * - a pre-scan pass and the analysis pass - and a census that truncated on construction would drop
 * whatever the earlier pass recorded. Making the file fresh belongs to the reporter, which deletes
 * it before the run; the reporter also deduplicates by identity when it reads it, so a record two
 * passes both wrote is counted once.
 */
public final class RestrictionCensus {

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private final Path path;

  private final Set<MutationIdentifier> written = new HashSet<>();

  private final List<String> pending = new ArrayList<>();

  private RestrictionCensus(final Path path) {
    this.path = path;
  }

  /** A census that appends to {@code path}, creating its directory if it does not exist yet. */
  public static RestrictionCensus at(final Path path) {
    final Path parent = path.getParent();
    if (parent != null) {
      try {
        Files.createDirectories(parent);
      } catch (final IOException error) {
        throw new UncheckedIOException("cannot create the census directory " + parent, error);
      }
    }
    return new RestrictionCensus(path);
  }

  /** Records one removed mutant, unless its identity was recorded before. */
  public synchronized void record(final MutationDetails mutation) {
    if (!written.add(mutation.getId())) {
      return;
    }
    pending.add(render(mutation));
  }

  /** Appends every record taken since the last flush. */
  public synchronized void flush() {
    if (pending.isEmpty()) {
      return;
    }
    try (Writer writer =
        Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      for (final String record : pending) {
        writer.write(record);
        writer.write('\n');
      }
    } catch (final IOException error) {
      throw new UncheckedIOException("cannot append to the restriction census " + path, error);
    }
    pending.clear();
  }

  private static String render(final MutationDetails mutation) {
    final MutationIdentifier id = mutation.getId();
    final Location location = id.getLocation();
    final StringBuilder json = new StringBuilder(256);
    json.append("{\"class\":")
        .append(quote(location.getClassName().asJavaName()))
        .append(",\"method\":")
        .append(quote(location.getMethodName()))
        .append(",\"descriptor\":")
        .append(quote(location.getMethodDesc()))
        .append(",\"mutator\":")
        .append(quote(id.getMutator()))
        .append(",\"indexes\":[");
    final List<Integer> indexes = id.getIndexes();
    for (int position = 0; position < indexes.size(); position++) {
      if (position > 0) {
        json.append(',');
      }
      json.append(indexes.get(position).intValue());
    }
    json.append("],\"description\":")
        .append(quote(mutation.getDescription()))
        .append(",\"sourceFile\":")
        .append(quote(mutation.getFilename()))
        .append(",\"lineNumber\":")
        .append(mutation.getLineNumber())
        .append('}');
    return json.toString();
  }

  private static String quote(final String value) {
    final StringBuilder quoted = new StringBuilder(value.length()).append('"');
    for (int index = 0; index < value.length(); index++) {
      final char character = value.charAt(index);
      switch (character) {
        case '"' -> quoted.append("\\\"");
        case '\\' -> quoted.append("\\\\");
        case '\b' -> quoted.append("\\b");
        case '\f' -> quoted.append("\\f");
        case '\n' -> quoted.append("\\n");
        case '\r' -> quoted.append("\\r");
        case '\t' -> quoted.append("\\t");
        default -> {
          if (character < 0x20) {
            // A control character JSON cannot carry literally; no locale is involved.
            quoted.append("\\u00").append(HEX[character >> 4]).append(HEX[character & 0xF]);
          } else {
            quoted.append(character);
          }
        }
      }
    }
    return quoted.append('"').toString();
  }
}
