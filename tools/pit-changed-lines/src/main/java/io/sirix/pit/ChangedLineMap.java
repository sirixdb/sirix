package io.sirix.pit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The lines a pull request changed, per source file, as the lane's reporter wrote them.
 *
 * <p>
 * Format, one record per selected source, written by {@code pr_mutation.py}:
 *
 * <pre>
 * # sirix-changed-lines 1
 * &lt;package&gt;\t&lt;file name&gt;\t&lt;line&gt; &lt;line&gt; ...
 * </pre>
 *
 * <p>
 * The package is empty for the default package. Lines are positive and sorted on read, so a lookup
 * is a binary search over a primitive array rather than a boxed set membership test.
 *
 * <p>
 * Every deviation from the format is an error (CONV-205): a map this class cannot read must fail
 * the analysis, because filtering on a half-understood map would either spend the budget on mutants
 * no verdict reads or silently remove mutants the verdict decides.
 */
public final class ChangedLineMap {

  /** Marks the format this class understands; a different header is an error. */
  public static final String HEADER = "# sirix-changed-lines 1";

  private static final int[] NONE = new int[0];

  private final Map<String, int[]> lines;

  private ChangedLineMap(final Map<String, int[]> lines) {
    this.lines = lines;
  }

  /**
   * Reads the map at {@code path}.
   *
   * @throws ChangedLineMapException when the file is absent, unreadable, empty of records, or
   *         malformed in any line
   */
  public static ChangedLineMap read(final Path path) {
    final List<String> records;
    try {
      records = Files.readAllLines(path, StandardCharsets.UTF_8);
    } catch (final IOException error) {
      throw new ChangedLineMapException("cannot read the changed-line map " + path, error);
    }
    if (records.isEmpty() || !HEADER.equals(records.get(0))) {
      throw new ChangedLineMapException("the changed-line map " + path + " must start with: " + HEADER);
    }
    final Map<String, int[]> parsed = new HashMap<>();
    for (int index = 1; index < records.size(); index++) {
      final String record = records.get(index);
      if (record.isEmpty()) {
        continue;
      }
      parse(path, index + 1, record, parsed);
    }
    if (parsed.isEmpty()) {
      throw new ChangedLineMapException("the changed-line map " + path + " lists no source; nothing could be judged");
    }
    return new ChangedLineMap(parsed);
  }

  private static void parse(final Path path, final int number, final String record, final Map<String, int[]> parsed) {
    final String[] fields = record.split("\t", -1);
    if (fields.length != 3) {
      throw new ChangedLineMapException(
          malformed(path, number) + "expected <package>\\t<file>\\t<lines>, found " + fields.length + " field(s)");
    }
    if (fields[1].isEmpty()) {
      throw new ChangedLineMapException(malformed(path, number) + "the file name is empty");
    }
    final String key = key(fields[0], fields[1]);
    if (parsed.containsKey(key)) {
      throw new ChangedLineMapException(malformed(path, number) + "duplicate source " + key);
    }
    final List<Integer> numbers = new ArrayList<>();
    for (final String field : fields[2].split(" ", -1)) {
      if (field.isEmpty()) {
        continue;
      }
      final int line;
      try {
        line = Integer.parseInt(field);
      } catch (final NumberFormatException error) {
        throw new ChangedLineMapException(malformed(path, number) + "line number is not an integer: " + field, error);
      }
      if (line <= 0) {
        throw new ChangedLineMapException(malformed(path, number) + "line number is not positive: " + line);
      }
      numbers.add(line);
    }
    final int[] sorted = new int[numbers.size()];
    for (int position = 0; position < sorted.length; position++) {
      sorted[position] = numbers.get(position);
    }
    Arrays.sort(sorted);
    parsed.put(key, sorted);
  }

  private static String malformed(final Path path, final int number) {
    return "malformed changed-line map " + path + " at line " + number + ": ";
  }

  private static String key(final String packageName, final String fileName) {
    return packageName + '/' + fileName;
  }

  /**
   * Whether the pull request changed {@code line} of the named source.
   *
   * <p>
   * A source the map does not list has no changed line at all: its mutants are out of scope.
   */
  public boolean contains(final String packageName, final String fileName, final int line) {
    final int[] changed = lines.getOrDefault(key(packageName, fileName), NONE);
    return Arrays.binarySearch(changed, line) >= 0;
  }

  /** The number of sources the map lists, for the log line the factory writes. */
  public int sources() {
    return lines.size();
  }

  /** Signals a map the restriction must not act on. */
  public static final class ChangedLineMapException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    ChangedLineMapException(final String message) {
      super(message);
    }

    ChangedLineMapException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
