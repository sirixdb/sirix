/*
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import io.sirix.exception.SirixIOException;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Splits a JSON file into <em>ordered, record-aligned byte ranges</em> that can be ingested
 * concurrently by {@link ParallelJsonShredder} — the piece that turns "one writer thread" into "one
 * writer thread per core".
 *
 * <h2>Why this exists</h2>
 * {@link ParallelJsonShredder} already shreds N partitions into N resources in parallel, but it
 * requires the <em>caller</em> to hand it N ready-made {@link JsonReader}s. Splitting a multi-GB JSON
 * file at record boundaries by hand is neither obvious nor cheap to get right: containers nest,
 * separators hide inside string literals, and escapes hide quotes. This class does that split once,
 * in a single byte-level pass, and hands back partitions the shredder consumes directly.
 *
 * <p>The design follows DuckDB's buffered JSON reader: <em>detect the layout from the data, cut the
 * input into morsels at record boundaries, and let independent workers own a morsel each.</em>
 *
 * <h2>Layout detection</h2>
 * With {@link Format#AUTO} (the default) the shape of the data decides the layout:
 * <ul>
 *   <li>{@code [ {...}, {...} ]} — a root array that is the file's only top-level value →
 *       {@link Format#ARRAY}; its elements are the records.</li>
 *   <li>Two or more concatenated top-level values → {@link Format#NEWLINE_DELIMITED} (LDJSON / JSON
 *       Lines); each top-level value is a record. Detection is structural rather than line-based, so
 *       pretty-printed concatenated documents split correctly and a newline inside a string literal
 *       is never mistaken for a separator.</li>
 *   <li>{@code {"laureates": [ {...}, {...} ], "meta": {...}}} — a document wrapping the records in
 *       an array → {@link Format#NESTED_ARRAY}. This is what most real-world JSON exports look like,
 *       so it is detected rather than rejected: the largest array wins, or the caller names it
 *       explicitly (DuckDB exposes the same choice as its {@code json_path} option).</li>
 *   <li>Anything else — a lone object with no records array, a lone scalar →
 *       {@link Format#SINGLE_DOCUMENT}; one partition covering the whole file is returned, because
 *       there is nothing to parallelise.</li>
 * </ul>
 *
 * <h2>What {@code NESTED_ARRAY} leaves behind</h2>
 * Splitting on a wrapped array ingests <em>that array's elements and nothing else</em> — the wrapper's
 * other members do not appear in any partition. That is the same trade DuckDB makes when a
 * {@code json_path} selects records out of a wrapper, but it means detection alone must never make it
 * on a document where the array is incidental. {@link Format#AUTO} therefore requires the array to be
 * essentially the whole document before choosing this layout; every real export measured puts 99% or
 * more of its bytes there, while {@code {"important": <4 KB>, "tiny": [1,2,3]}} stays a single
 * document and is ingested whole. Naming the path explicitly bypasses the check — the caller has
 * asserted the intent.
 *
 * <h2>Partition shape</h2>
 * Except for {@code SINGLE_DOCUMENT}, every partition is presented to the shredder as a <em>JSON
 * array of the records it owns</em>, so all shards have the same shape and the global record order is
 * the concatenation of the shards in index order. A {@code SINGLE_DOCUMENT} partition is presented
 * verbatim.
 *
 * <h2>Cost</h2>
 * The boundary scan materialises nothing — no tokens, no strings, no boxing, only a depth counter and
 * two flags (see {@link JsonStructureScanner}) — and skips the runs between structural bytes in bulk,
 * so it reads at a few hundred MB/s: more than an order of magnitude faster than SirixDB ingests, and
 * therefore a small fraction of the shred it parallelises. Root arrays and concatenated records are
 * planned in a single pass; a nested records array costs a second one, because which array holds the
 * records is only known once the first pass has seen them all.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * final var plan = JsonPartitioner.plan(path, Runtime.getRuntime().availableProcessors());
 * final var resources = ParallelJsonShredder.shredPartitioned(database, plan.readers(), "shard",
 *     name -> ResourceConfiguration.newBuilder(name).build(), 1_000_000, 0);
 * }</pre>
 *
 * <p>This class is stateless and thread-safe; all state is method-local.
 *
 * @author Johannes Lichtenberger
 * @see ParallelJsonShredder
 * @see JsonStructureScanner
 */
public final class JsonPartitioner {

  /** Size of the reusable scan buffer. Large enough to amortise syscalls, small enough to stay cached. */
  private static final int SCAN_BUFFER_SIZE = 1 << 20;

  /** Longest member name the scanner captures; a longer one is dropped rather than truncated. */
  private static final int MAX_FIELD_NAME_BYTES = 256;

  /**
   * Deepest nesting level at which an array is still considered a candidate records array. Real
   * exports wrap their records one or two levels down; bounding the depth bounds the probe's state to
   * a fixed number of slots regardless of how deeply the data nests.
   */
  private static final int MAX_RECORDS_ARRAY_DEPTH = 6;

  /**
   * Fraction of the file a wrapped array must cover before {@link Format#AUTO} will treat it as the
   * records array.
   *
   * <p>Splitting on a wrapped array ingests that array's elements and <em>nothing else</em>, so this
   * bound is what stops detection from silently discarding a document: {@code {"important": <4 KB>,
   * "tiny": [1,2,3]}} would otherwise be "split" into three records and lose 99.9% of the file. Every
   * real-world export measured — Nobel laureates, blockchain transactions, a Reddit listing — puts
   * 99% or more of its bytes in the records array, so the bound never costs the case this exists for.
   * A caller who names the path explicitly bypasses it, having asserted the intent.
   */
  private static final double MIN_RECORDS_ARRAY_COVERAGE = 0.9D;

  /**
   * Default lower bound on a partition's byte span. Shredding a shard carries fixed per-resource cost
   * (resource bootstrap, page transaction, commit), so slicing a small input into many tiny shards
   * loses more to overhead than it wins in parallelism.
   */
  public static final long DEFAULT_MIN_PARTITION_BYTES = 4L << 20;

  private JsonPartitioner() {
    throw new AssertionError("no instances");
  }

  /**
   * The layout of the records in the input.
   */
  public enum Format {
    /** Detect the layout from the data (the default). Never the resolved format of a {@link Plan}. */
    AUTO,
    /** A root array whose elements are the records: {@code [ {..}, {..} ]}. */
    ARRAY,
    /** Concatenated top-level values, one record each (LDJSON / JSON Lines). */
    NEWLINE_DELIMITED,
    /** A root object with a member whose array holds the records: {@code {"data": [ {..}, {..} ]}}. */
    NESTED_ARRAY,
    /** One top-level value that is the whole document — not partitionable. */
    SINGLE_DOCUMENT
  }

  /**
   * One record-aligned byte range of the input.
   *
   * @param index              the partition's position in the input's record order, {@code 0}-based
   * @param startOffset        first byte of the partition's content, inclusive
   * @param endOffsetExclusive one past the last byte of the partition's content
   * @param recordCount        the number of top-level records in this partition
   * @param wrapInArray        whether the content is a bare record sequence that must be presented as
   *                           a JSON array (true for every format except {@code SINGLE_DOCUMENT})
   * @param spliceSeparators   whether record separators must be synthesised because the content holds
   *                           concatenated values rather than comma-separated ones (true for
   *                           {@code NEWLINE_DELIMITED})
   */
  public record Partition(int index, long startOffset, long endOffsetExclusive, long recordCount,
                          boolean wrapInArray, boolean spliceSeparators) {

    public Partition {
      if (index < 0) {
        throw new IllegalArgumentException("index must not be negative: " + index);
      }
      if (startOffset < 0) {
        throw new IllegalArgumentException("startOffset must not be negative: " + startOffset);
      }
      if (endOffsetExclusive < startOffset) {
        throw new IllegalArgumentException(
            "endOffsetExclusive (" + endOffsetExclusive + ") must not precede startOffset (" + startOffset + ")");
      }
      if (recordCount < 0) {
        throw new IllegalArgumentException("recordCount must not be negative: " + recordCount);
      }
      if (spliceSeparators && !wrapInArray) {
        throw new IllegalArgumentException("spliceSeparators requires wrapInArray");
      }
    }

    /** The partition's span in bytes. */
    public long byteLength() {
      return endOffsetExclusive - startOffset;
    }
  }

  /**
   * The result of planning a split: the resolved layout plus the ordered partitions.
   *
   * @param file         the planned input file
   * @param format       the resolved layout (never {@link Format#AUTO})
   * @param recordsField for {@link Format#NESTED_ARRAY}, the root-object member holding the records;
   *                     {@code null} for every other format
   * @param recordCount  the total number of records the scan found
   * @param partitions   the ordered partitions; never empty
   */
  public record Plan(Path file, Format format, @Nullable String recordsField, long recordCount,
                     List<Partition> partitions) {

    public Plan {
      Objects.requireNonNull(file, "file");
      Objects.requireNonNull(format, "format");
      if (format == Format.AUTO) {
        throw new IllegalArgumentException("a plan's format must be resolved, not AUTO");
      }
      if ((recordsField != null) != (format == Format.NESTED_ARRAY)) {
        throw new IllegalArgumentException(
            "recordsField must be set exactly for NESTED_ARRAY plans, but format is " + format);
      }
      partitions = List.copyOf(Objects.requireNonNull(partitions, "partitions"));
      if (partitions.isEmpty()) {
        throw new IllegalArgumentException("a plan must hold at least one partition");
      }
    }

    /**
     * The partitions as lazily-opening {@link JsonReader} factories, ready to hand to
     * {@link ParallelJsonShredder#shredPartitioned}. Each factory opens its own file channel on the
     * worker thread that runs it; the shredder closes the reader.
     *
     * @return one factory per partition, in partition order
     */
    public List<Callable<JsonReader>> readers() {
      final List<Callable<JsonReader>> factories = new ArrayList<>(partitions.size());
      for (final Partition partition : partitions) {
        factories.add(() -> reader(file, partition));
      }
      return factories;
    }

    /** Whether this plan actually splits the input, i.e. whether shredding it in parallel can pay off. */
    public boolean isPartitioned() {
      return partitions.size() > 1;
    }
  }

  /**
   * Plan a split of {@code file} into at most {@code targetPartitions} record-aligned partitions,
   * detecting the layout from the data.
   *
   * @param file             the JSON file to split (must exist and be readable)
   * @param targetPartitions the desired number of partitions; the plan may hold fewer when the input
   *                         has too few records or too few bytes to fill them
   * @return the plan
   * @throws IllegalArgumentException if {@code targetPartitions < 1}
   * @throws SirixIOException         if the file cannot be read or is not well-formed enough to find
   *                                  record boundaries
   * @throws NullPointerException     if {@code file} is {@code null}
   */
  public static Plan plan(final Path file, final int targetPartitions) {
    return plan(file, targetPartitions, Format.AUTO, DEFAULT_MIN_PARTITION_BYTES, null);
  }

  /**
   * Plan a split of {@code file} into at most {@code targetPartitions} record-aligned partitions.
   *
   * @param file              the JSON file to split (must exist and be readable)
   * @param targetPartitions  the desired number of partitions; the plan may hold fewer when the input
   *                          has too few records or too few bytes to fill them
   * @param requestedFormat   the layout to assume, or {@link Format#AUTO} to detect it
   * @param minPartitionBytes never emit a partition shorter than this, except for the trailing one and
   *                          when the whole input is shorter; {@code <= 0} disables the bound
   * @param recordsField      for a root object wrapping the records, the member holding them; {@code
   *                          null} lets the scan pick the member whose array spans the most bytes
   * @return the plan
   * @throws IllegalArgumentException if {@code targetPartitions < 1}, or if {@code recordsField} names
   *                                  a member that is not an array of the root object
   * @throws SirixIOException         if the file cannot be read or is not well-formed enough to find
   *                                  record boundaries
   * @throws NullPointerException     if {@code file} or {@code requestedFormat} is {@code null}
   */
  public static Plan plan(final Path file, final int targetPartitions, final Format requestedFormat,
      final long minPartitionBytes, final @Nullable String recordsField) {
    Objects.requireNonNull(file, "file");
    Objects.requireNonNull(requestedFormat, "requestedFormat");
    if (targetPartitions < 1) {
      throw new IllegalArgumentException("targetPartitions must be at least 1: " + targetPartitions);
    }

    final long size;
    try {
      size = Files.size(file);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    if (recordsField != null && requestedFormat != Format.AUTO && requestedFormat != Format.NESTED_ARRAY) {
      // Naming a records path only means something for a wrapped-array layout. Ignoring it silently —
      // which is what happens once no probe is built — hides the caller's mistake behind a split on a
      // different axis, which is the failure the path validation below exists to prevent.
      throw new IllegalArgumentException("recordsField '" + recordsField + "' is only meaningful with "
          + Format.AUTO + " or " + Format.NESTED_ARRAY + ", not " + requestedFormat);
    }

    if (size == 0L) {
      // wrapInArray, so the one partition reads back as `[]`. Presented verbatim it would be an empty
      // reader, and the shredder rejects that — failing every other shard in the same batch with it.
      return new Plan(file, Format.SINGLE_DOCUMENT, null, 0L,
          List.of(new Partition(0, 0L, 0L, 0L, true, false)));
    }
    if (requestedFormat == Format.SINGLE_DOCUMENT) {
      // The caller asserted the layout; take them at their word and skip the scan.
      return wholeFile(file, size, 1L);
    }
    // Note there is deliberately no shortcut for targetPartitions == 1. Skipping the scan would
    // hand back the raw file as one "single document" partition, and reading that back yields only
    // the *first* top-level value — a concatenated-record input would silently lose every record
    // after it. One partition still has to be a correctly-typed partition.

    // Pass one does everything the root-array and concatenated layouts need: the probes resolve the
    // layout while the collectors accumulate the boundaries each candidate layout implies. Detecting
    // first and cutting second would mean two passes over a file that may be tens of gigabytes.
    final long minBytes = Math.max(0L, minPartitionBytes);
    final boolean auto = requestedFormat == Format.AUTO;
    final FormatProbe probe = auto ? new FormatProbe() : null;
    final RecordsArrayProbe recordsProbe =
        auto || requestedFormat == Format.NESTED_ARRAY ? new RecordsArrayProbe(recordsField) : null;
    final BoundaryCollector arrayCollector = auto || requestedFormat == Format.ARRAY
        ? BoundaryCollector.forRootArray(size, targetPartitions, minBytes)
        : null;
    final BoundaryCollector concatenatedCollector = auto || requestedFormat == Format.NEWLINE_DELIMITED
        ? BoundaryCollector.forConcatenatedValues(size, targetPartitions, minBytes)
        : null;

    final ScanOutcome outcome =
        walk(file, size, new CompositeSink(probe, recordsProbe, arrayCollector, concatenatedCollector));

    if (outcome.sawCommentMarker()) {
      // Comments are outside what the scanner models, and Gson's reader accepts them, so a commented
      // input would otherwise be cut wherever a comma happened to sit — including inside a comment.
      //
      // Degrading to a whole-file SINGLE_DOCUMENT partition is NOT a safe fallback, however tempting:
      // such a partition is handed to the shredder verbatim, and reading a concatenated-record file
      // as one JSON value stops after the first record. That would trade a loud parse failure for
      // silent data loss. Once a comment may exist, no structural observation from this pass can be
      // trusted, so refuse to plan and say what to do instead.
      throw new SirixIOException("'" + file
          + "' contains a comment (a '/' or '#' outside a string literal). Record boundaries cannot be"
          + " found safely in commented JSON — ingest it with the single-threaded shredder, which"
          + " accepts comments, or strip them before partitioning.");
    }
    outcome.requireBalanced(file);

    if (recordsField != null && recordsProbe != null && recordsProbe.choose(recordsField) == null) {
      // The caller named a records array explicitly; silently falling back to a single unsplit
      // partition would hide the typo behind a merely slow ingest.
      throw new IllegalArgumentException("'" + file + "' has no array at path '" + recordsField + "'");
    }

    final Format format = auto ? probe.resolve(recordsProbe, recordsField, size) : requestedFormat;
    return switch (format) {
      case ARRAY -> arrayCollector.finish(file, Format.ARRAY, null);
      case NEWLINE_DELIMITED -> concatenatedCollector.finish(file, Format.NEWLINE_DELIMITED, null);
      case NESTED_ARRAY -> planNestedArray(file, size, targetPartitions, minBytes, recordsProbe, recordsField);
      case SINGLE_DOCUMENT -> wholeFile(file, size, probe != null ? probe.topLevelValues : 1L);
      case AUTO -> throw new AssertionError("format resolution must not yield AUTO");
    };
  }

  /**
   * Open a {@link JsonReader} over one partition of {@code file}. The reader presents the partition as
   * a single well-formed JSON value; the caller — {@link ParallelJsonShredder}, normally — closes it,
   * which also closes the underlying channel.
   *
   * @param file      the file the partition was planned against
   * @param partition the partition to read
   * @return a lenient {@link JsonReader} over the partition
   * @throws SirixIOException     if the file cannot be opened
   * @throws NullPointerException if an argument is {@code null}
   */
  public static JsonReader reader(final Path file, final Partition partition) {
    Objects.requireNonNull(file, "file");
    Objects.requireNonNull(partition, "partition");

    FileChannel channel = null;
    try {
      channel = FileChannel.open(file, StandardOpenOption.READ);
      final InputStream in = new JsonPartitionInputStream(channel, partition);
      // Decoding through an InputStreamReader keeps the reader streaming — the partition is never
      // materialised — while staying correct for multi-byte code points.
      final JsonReader jsonReader = new JsonReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      jsonReader.setStrictness(Strictness.LENIENT);
      return jsonReader;
    } catch (final IOException e) {
      closeQuietly(channel, e);
      throw new SirixIOException(e);
    } catch (final RuntimeException e) {
      closeQuietly(channel, e);
      throw e;
    }
  }

  /**
   * Convenience: plan a split and hand back the readers in one step.
   *
   * @param file             the JSON file to split
   * @param targetPartitions the desired number of partitions
   * @return one {@link JsonReader} factory per partition, in record order
   * @throws SirixIOException if the file cannot be read
   */
  public static List<Callable<JsonReader>> partitionedReaders(final Path file, final int targetPartitions) {
    return plan(file, targetPartitions).readers();
  }

  // ==================== Planning helpers ====================

  private static Plan wholeFile(final Path file, final long size, final long recordCount) {
    return new Plan(file, Format.SINGLE_DOCUMENT, null, recordCount,
        List.of(new Partition(0, 0L, size, recordCount, false, false)));
  }

  /**
   * Plan the split of a records array wrapped by the document. Which array holds the records is only
   * known once the first pass has seen them all, so the boundaries inside the chosen array cost a
   * second pass — paid only for this layout.
   */
  private static Plan planNestedArray(final Path file, final long size, final int targetPartitions,
      final long minPartitionBytes, final RecordsArrayProbe recordsProbe, final @Nullable String recordsPath) {
    final RecordsArrayProbe.RecordsArray chosen = recordsProbe.choose(recordsPath);
    if (chosen != null && recordsPath == null && chosen.span() < (long) (size * MIN_RECORDS_ARRAY_COVERAGE)) {
      // Only naming the path asserts intent. Without one, picking the largest array here would bypass
      // the coverage bound that stops detection from discarding the document around it.
      throw new IllegalArgumentException("the largest array in '" + file + "' covers only "
          + (100L * chosen.span() / Math.max(1L, size)) + "% of it; name the records path explicitly if"
          + " the rest of the document really should not be ingested");
    }
    if (chosen == null) {
      throw new IllegalArgumentException(recordsPath == null
          ? "no records array found in '" + file + "'"
          : "'" + file + "' has no array at path '" + recordsPath + "'");
    }

    final BoundaryCollector collector = BoundaryCollector.forNestedArray(chosen.depth(), chosen.contentStart(),
        chosen.contentEndExclusive(), targetPartitions, minPartitionBytes);
    walk(file, size, collector).requireBalanced(file);
    return collector.finish(file, Format.NESTED_ARRAY, chosen.path());
  }

  // ==================== Scan sinks ====================

  /**
   * Receives the bytes that can change JSON structure, plus a bulk summary of the runs between them.
   *
   * <p>Calling a sink per byte is what makes a naive scanner slow: the interesting bytes are a few
   * percent of a JSON file, and the rest — string bodies, number digits, whitespace inside containers
   * — cannot move the parser's structure. {@link #walk} therefore skips those runs in a tight loop
   * with no virtual calls and reports each as a single {@link #run} instead. Implementations stay
   * allocation-free.
   */
  private interface ScanSink {

    /**
     * A byte that can change structure, already applied to {@code scanner}: a bracket, a brace, a
     * comma, a quote, an escape (with the byte it escapes), or any byte at the top level.
     */
    void structural(byte b, long offset, JsonStructureScanner scanner);

    /**
     * A skipped run of bytes that cannot change structure.
     *
     * @param buffer      the scan buffer the run lives in
     * @param from        index of the run's first byte in {@code buffer}, inclusive
     * @param to          index one past the run's last byte in {@code buffer}
     * @param offset      absolute offset of the run's first byte
     * @param inString    whether the run is the body of a string literal
     * @param depth       the container nesting depth the run sits at
     * @param sawContent  whether the run held anything other than whitespace
     */
    default void run(byte[] buffer, int from, int to, long offset, boolean inString, int depth,
        boolean sawContent) {
      // Most sinks only care about structure.
    }
  }

  /**
   * Fans one pass out to the probes and the candidate collectors. Fields are final so the JIT can
   * inline the callees into the scan loop rather than dispatching through the interface each time.
   */
  private record CompositeSink(@Nullable FormatProbe probe, @Nullable RecordsArrayProbe recordsProbe,
                               @Nullable BoundaryCollector arrayCollector,
                               @Nullable BoundaryCollector concatenatedCollector) implements ScanSink {
    @Override
    public void structural(final byte b, final long offset, final JsonStructureScanner scanner) {
      if (probe != null) {
        probe.structural(b, offset, scanner);
      }
      if (recordsProbe != null) {
        recordsProbe.structural(b, offset, scanner);
      }
      if (arrayCollector != null) {
        arrayCollector.structural(b, offset, scanner);
      }
      if (concatenatedCollector != null) {
        concatenatedCollector.structural(b, offset, scanner);
      }
    }

    @Override
    public void run(final byte[] buffer, final int from, final int to, final long offset, final boolean inString,
        final int depth, final boolean sawContent) {
      if (recordsProbe != null) {
        recordsProbe.run(buffer, from, to, offset, inString, depth, sawContent);
      }
      if (arrayCollector != null) {
        arrayCollector.run(buffer, from, to, offset, inString, depth, sawContent);
      }
      if (concatenatedCollector != null) {
        concatenatedCollector.run(buffer, from, to, offset, inString, depth, sawContent);
      }
    }
  }

  /**
   * Counts top-level values and remembers what the first content byte opened — enough to tell a root
   * array of records from a stream of concatenated records from a single document.
   */
  private static final class FormatProbe implements ScanSink {
    private long topLevelValues;
    private boolean firstByteOpensArray;
    private boolean firstByteOpensObject;
    private boolean sawFirstContentByte;

    @Override
    public void structural(final byte b, final long offset, final JsonStructureScanner scanner) {
      if (!sawFirstContentByte && scanner.isContent()) {
        sawFirstContentByte = true;
        firstByteOpensArray = scanner.isStructural() && b == '[';
        firstByteOpensObject = scanner.isStructural() && b == '{';
      }
      if (scanner.startedTopLevelValue()) {
        topLevelValues++;
      }
    }

    Format resolve(final @Nullable RecordsArrayProbe recordsProbe, final @Nullable String recordsPath,
        final long size) {
      if (topLevelValues > 1) {
        return Format.NEWLINE_DELIMITED;
      }
      if (topLevelValues != 1) {
        return Format.SINGLE_DOCUMENT;
      }
      if (firstByteOpensArray) {
        return Format.ARRAY;
      }
      if (!firstByteOpensObject || recordsProbe == null) {
        return Format.SINGLE_DOCUMENT;
      }
      final RecordsArrayProbe.RecordsArray candidate = recordsProbe.choose(recordsPath);
      if (candidate == null) {
        return Format.SINGLE_DOCUMENT;
      }
      if (recordsPath != null) {
        // The caller named the array, so they have accepted that the wrapper is not ingested.
        return Format.NESTED_ARRAY;
      }
      // Detection alone must not decide to throw the wrapper away. Splitting on a wrapped array
      // ingests that array's elements and nothing else, so choosing it for a document where the
      // array is incidental would silently drop the bulk of the file. Requiring the array to BE
      // essentially the whole document keeps the case this exists for -- every real export measured
      // puts 99% or more of its bytes in the records array -- while a document that merely happens
      // to contain an array stays a single document, ingested whole.
      return candidate.span() >= (long) (size * MIN_RECORDS_ARRAY_COVERAGE)
          ? Format.NESTED_ARRAY
          : Format.SINGLE_DOCUMENT;
    }
  }

  /**
   * Finds the array that holds the records when the document wraps them — {@code {"data": [ ... ]}},
   * {@code {"data": {"children": [ ... ]}}} and so on — which is what most real-world JSON exports
   * look like.
   *
   * <p>Selection is by byte span: an array nested inside another is strictly smaller than its
   * container, so "the largest array in the document" picks the outermost record-bearing one. A
   * caller who knows better names the path explicitly, the way DuckDB's {@code json_path} option
   * does.
   *
   * <p>The probe holds a fixed amount of state regardless of input size: per-depth slots up to
   * {@value #MAX_RECORDS_ARRAY_DEPTH}, plus the single best candidate seen so far. Member names are
   * captured as raw bytes into per-depth buffers and only turned into a {@link String} when a
   * candidate becomes the new best, so scanning a file with millions of nested arrays allocates
   * nothing per array.
   */
  private static final class RecordsArrayProbe implements ScanSink {

    /** The records array and the byte range of its elements. */
    record RecordsArray(String path, int depth, long contentStart, long contentEndExclusive) {
      long span() {
        return contentEndExclusive - contentStart;
      }
    }

    /**
     * Per-depth slots are indexed by depth and guarded with {@code depth < SLOTS}, so this is exactly
     * {@link #MAX_RECORDS_ARRAY_DEPTH}{@code  + 1} — an array at that depth is the deepest tracked.
     */
    private static final int SLOTS = MAX_RECORDS_ARRAY_DEPTH + 1;

    /** Whether the container open at each depth is an object (as opposed to an array). */
    private final boolean[] containerIsObject = new boolean[SLOTS];

    /** Whether the next string at each depth is a key rather than a value. */
    private final boolean[] expectingKey = new boolean[SLOTS];

    /** The most recent key seen at each depth, as raw UTF-8 bytes. */
    private final byte[][] keyBytes = new byte[SLOTS][MAX_FIELD_NAME_BYTES];
    private final int[] keyLength = new int[SLOTS];

    /** Content start offset of the array open at each depth, or {@code -1} when the container is not an array. */
    private final long[] arrayContentStart = new long[SLOTS];

    private int capturingKeyAtDepth = -1;

    /** Whether the key being captured overflowed the buffer or held an escape, and must be dropped. */
    private boolean keyRejected;

    private @Nullable RecordsArray best;
    private @Nullable RecordsArray requestedMatch;

    /** The dotted path the caller asked for, or {@code null} to take the largest array. */
    private final @Nullable String requestedPath;

    RecordsArrayProbe(final @Nullable String requestedPath) {
      this.requestedPath = requestedPath;
      Arrays.fill(arrayContentStart, -1L);
    }

    @Override
    public void structural(final byte b, final long offset, final JsonStructureScanner scanner) {
      captureKey(b, scanner);

      if (!scanner.isStructural()) {
        return;
      }
      final int depth = scanner.depth();
      final int depthBefore = scanner.depthBefore();
      if (depth < 0 || depthBefore < 0) {
        // More closers than openers. The input is unbalanced and walk() will say so; until then just
        // stop tracking, because these are array indices and a negative one is a crash, not a bug
        // report.
        return;
      }

      if (depth == depthBefore + 1) {
        // A container opened at `depth`.
        if (depth < SLOTS) {
          containerIsObject[depth] = b == '{';
          expectingKey[depth] = b == '{';
          keyLength[depth] = 0;
          arrayContentStart[depth] = b == '[' ? offset + 1 : -1L;
        }
        return;
      }
      if (depth == depthBefore - 1) {
        // A container closed at `depthBefore`.
        if (depthBefore < SLOTS && b == ']' && arrayContentStart[depthBefore] >= 0L) {
          consider(depthBefore, arrayContentStart[depthBefore], offset);
          arrayContentStart[depthBefore] = -1L;
        }
        return;
      }
      if (b == ',' && depth < SLOTS && containerIsObject[depth]) {
        expectingKey[depth] = true;
      }
    }

    /**
     * The body of a key is delivered here in bulk — it holds no structural bytes — so a key costs one
     * array copy rather than one call per character.
     */
    @Override
    public void run(final byte[] buffer, final int from, final int to, final long offset, final boolean inString,
        final int depth, final boolean sawContent) {
      if (capturingKeyAtDepth < 0 || !inString || keyRejected) {
        return;
      }
      final int length = to - from;
      final int used = keyLength[capturingKeyAtDepth];
      if (used + length > MAX_FIELD_NAME_BYTES) {
        keyRejected = true;
        return;
      }
      System.arraycopy(buffer, from, keyBytes[capturingKeyAtDepth], used, length);
      keyLength[capturingKeyAtDepth] = used + length;
    }

    /**
     * Track the opening and closing quotes of an object key. Keys and values sit at the same depth, so
     * the {@code expectingKey} flag — armed when the object opens and by each comma inside it — tells
     * them apart. A key containing an escape is dropped rather than decoded: it can only weaken a
     * candidate's <em>name</em>, never its byte range, and decoding would need a buffer this probe
     * deliberately does not keep.
     */
    private void captureKey(final byte b, final JsonStructureScanner scanner) {
      if (capturingKeyAtDepth >= 0) {
        if (!scanner.inString()) {
          // The closing quote was just consumed.
          if (keyRejected) {
            keyLength[capturingKeyAtDepth] = 0;
          }
          expectingKey[capturingKeyAtDepth] = false;
          capturingKeyAtDepth = -1;
        } else {
          // Still inside the key: the only bytes reaching us here are escapes and what they escape.
          keyRejected = true;
        }
        return;
      }
      final int depth = scanner.depth();
      if ((b == '"' || b == '\'') && scanner.inString() && depth >= 0 && depth < SLOTS
          && expectingKey[depth]) {
        capturingKeyAtDepth = depth;
        keyLength[depth] = 0;
        keyRejected = false;
      }
    }

    /** Offer a closed array as the records array, keeping the largest (or the explicitly requested) one. */
    private void consider(final int depth, final long contentStart, final long contentEndExclusive) {
      final long span = contentEndExclusive - contentStart;
      if (span <= 0L) {
        return;
      }
      final boolean isBest = best == null || span > best.span();
      if (!isBest && requestedPath == null) {
        return;
      }
      // Building the path allocates, so only do it for a candidate that can still win.
      final String path = pathTo(depth);
      if (requestedPath != null && requestedPath.equals(path)) {
        final RecordsArray match = new RecordsArray(path, depth, contentStart, contentEndExclusive);
        if (requestedMatch == null || match.span() > requestedMatch.span()) {
          requestedMatch = match;
        }
      }
      if (isBest) {
        best = new RecordsArray(path, depth, contentStart, contentEndExclusive);
      }
    }

    /**
     * The dotted member path of the array open at {@code depth}, e.g. {@code data.children}. Each
     * enclosing object contributes the key that was current when this array opened; enclosing arrays
     * contribute nothing, since their elements have no names.
     */
    private String pathTo(final int depth) {
      final StringBuilder path = new StringBuilder(32);
      for (int level = 1; level < depth; level++) {
        if (!containerIsObject[level] || keyLength[level] == 0) {
          continue;
        }
        if (!path.isEmpty()) {
          path.append('.');
        }
        path.append(new String(keyBytes[level], 0, keyLength[level], StandardCharsets.UTF_8));
      }
      return path.toString();
    }

    /**
     * @param requested the dotted member path the caller named, or {@code null} to take the largest array
     * @return the chosen records array, or {@code null} if there is none
     */
    @Nullable
    RecordsArray choose(final @Nullable String requested) {
      return requested != null ? requestedMatch : best;
    }
  }

  // ==================== Boundary collection ====================

  /**
   * Emits a cut once the partition being accumulated has grown past its target span and the scanner
   * reaches the next legal record boundary. For array layouts a boundary is a comma at the records
   * array's depth; for concatenated layouts it is the end of a top-level value.
   */
  private static final class BoundaryCollector implements ScanSink {

    /** Whether records are array elements (cut at commas) or concatenated values (cut at value ends). */
    private final boolean arrayElements;

    /** Depth at which the records array's element separators sit: 1 for a root array, 2 for a nested one. */
    private final int recordsArrayDepth;

    /** For a nested array, the offset its content starts at; {@code -1} to activate at the first open. */
    private final long activateAtContentStart;

    /** Absolute offset past which no content lies — the records array's end, or the file's. */
    private final long contentLimit;

    private final long targetSpan;
    private final long minPartitionBytes;
    private final List<Partition> partitions;

    /** Start offset of the partition currently accumulating; {@code -1} until content is reached. */
    private long currentStart = -1L;
    private long recordsInCurrent;
    private long recordCount;

    /** One past the last byte that belongs to record content; {@code -1} until known. */
    private long contentEnd = -1L;

    /** Whether the records array has closed — trailing bytes are not content. */
    private boolean recordsArrayClosed;

    /** Whether content has been seen since the last separator: a trailing element awaiting its count. */
    private boolean pendingRecord;

    /** Whether the input held any content at all — distinguishes an empty file from a wrong format. */
    private boolean sawAnyContent;

    private BoundaryCollector(final boolean arrayElements, final int recordsArrayDepth,
        final long activateAtContentStart, final long contentLimit, final long spanToDivide,
        final int targetPartitions, final long minPartitionBytes) {
      this.partitions = new ArrayList<>(targetPartitions);
      this.arrayElements = arrayElements;
      this.recordsArrayDepth = recordsArrayDepth;
      this.activateAtContentStart = activateAtContentStart;
      this.contentLimit = contentLimit;
      this.minPartitionBytes = minPartitionBytes;
      // Ceiling division so targetPartitions spans cover the content rather than leaving a tail that
      // forces one more shard than asked for.
      this.targetSpan = Math.max(1L, (spanToDivide + targetPartitions - 1) / targetPartitions);
    }

    static BoundaryCollector forRootArray(final long size, final int targetPartitions,
        final long minPartitionBytes) {
      return new BoundaryCollector(true, 1, -1L, size, size, targetPartitions, minPartitionBytes);
    }

    static BoundaryCollector forNestedArray(final int arrayDepth, final long contentStart,
        final long contentEndExclusive, final int targetPartitions, final long minPartitionBytes) {
      return new BoundaryCollector(true, arrayDepth, contentStart, contentEndExclusive,
          contentEndExclusive - contentStart, targetPartitions, minPartitionBytes);
    }

    static BoundaryCollector forConcatenatedValues(final long size, final int targetPartitions,
        final long minPartitionBytes) {
      return new BoundaryCollector(false, 0, -1L, size, size, targetPartitions, minPartitionBytes);
    }

    @Override
    public void structural(final byte b, final long offset, final JsonStructureScanner scanner) {
      if (scanner.isContent()) {
        sawAnyContent = true;
      }
      if (arrayElements) {
        acceptArrayElement(b, offset, scanner);
      } else {
        acceptConcatenatedValue(offset, scanner);
      }
    }

    /**
     * A skipped run inside the records array still tells us that the element in progress has content,
     * which is how the trailing element — the one with no comma after it — gets counted.
     */
    @Override
    public void run(final byte[] buffer, final int from, final int to, final long offset, final boolean inString,
        final int depth, final boolean sawContent) {
      if (arrayElements && !recordsArrayClosed && currentStart >= 0L && (inString || sawContent)) {
        pendingRecord = true;
      }
    }

    /** Records are the elements of the records array; separators are commas at its depth. */
    private void acceptArrayElement(final byte b, final long offset, final JsonStructureScanner scanner) {
      if (recordsArrayClosed) {
        return;
      }
      if (currentStart < 0L) {
        // The records array's '[' takes depth d-1 -> d; its elements start right after it.
        final boolean opensRecordsArray = scanner.isStructural() && b == '['
            && scanner.depthBefore() == recordsArrayDepth - 1 && scanner.depth() == recordsArrayDepth
            && (activateAtContentStart < 0L || offset + 1 == activateAtContentStart);
        if (opensRecordsArray) {
          currentStart = offset + 1;
        }
        return;
      }
      if (scanner.isStructural() && b == ']' && scanner.depthBefore() == recordsArrayDepth
          && scanner.depth() == recordsArrayDepth - 1) {
        contentEnd = offset;
        recordsArrayClosed = true;
        return;
      }
      if (scanner.isStructural() && b == ',' && scanner.depth() == recordsArrayDepth) {
        recordsInCurrent++;
        recordCount++;
        pendingRecord = false;
        if (shouldCut(offset)) {
          // Cut *at* the comma so it separates the shards instead of belonging to either.
          emit(currentStart, offset);
          currentStart = offset + 1;
        }
        return;
      }
      if (scanner.isContent()) {
        pendingRecord = true;
      }
    }

    /** Records are the concatenated top-level values; a boundary is the end of one of them. */
    private void acceptConcatenatedValue(final long offset, final JsonStructureScanner scanner) {
      if (scanner.startedTopLevelValue()) {
        final long previousEnd = scanner.previousTopLevelValueEnd();
        if (currentStart < 0L) {
          currentStart = offset;
        } else if (previousEnd > currentStart && shouldCut(previousEnd)) {
          emit(currentStart, previousEnd);
          currentStart = offset;
        }
        recordsInCurrent++;
        recordCount++;
      }
      if (scanner.isContent() && scanner.depth() == 0) {
        contentEnd = offset + 1;
      }
    }

    /**
     * A cut is legal once the accumulated span reaches the target and clears the minimum, and only
     * while enough content remains for the tail to form its own minimum-sized shard — otherwise the
     * cut would strand a sliver at the end.
     */
    private boolean shouldCut(final long boundary) {
      final long span = boundary - currentStart;
      if (span < targetSpan || span < minPartitionBytes) {
        return false;
      }
      return contentLimit - boundary >= minPartitionBytes;
    }

    private void emit(final long start, final long end) {
      partitions.add(new Partition(partitions.size(), start, end, recordsInCurrent, true, !arrayElements));
      recordsInCurrent = 0L;
    }

    Plan finish(final Path file, final Format format, final @Nullable String recordsField) {
      if (currentStart < 0L) {
        // The collector never activated. For an empty or whitespace-only input that is simply "no
        // records", and the one partition must still present as an array — handed to the shredder
        // verbatim it would be an empty reader, which fails the whole batch.
        //
        // For a caller-asserted format the data does not have, it means the assertion was wrong, and
        // there is no safe plan to return: a verbatim whole-file partition reads back as only the
        // first top-level value, silently dropping every record after it.
        if (sawAnyContent) {
          throw new SirixIOException("'" + file + "' does not have the requested " + format
              + " layout — no records were found where that layout puts them. Use " + Format.AUTO
              + " to detect the layout from the data.");
        }
        return new Plan(file, format, recordsField, 0L,
            List.of(new Partition(0, 0L, Math.max(0L, contentLimit), 0L, true, false)));
      }
      if (arrayElements && pendingRecord) {
        // The records array's final element has no trailing comma to have counted it.
        recordsInCurrent++;
        recordCount++;
      }
      final long end = Math.max(currentStart, contentEnd >= 0L ? contentEnd : contentLimit);
      // A tail holding no record — everything after a trailing comma, or trailing whitespace — would
      // become a shard whose whole content is an empty array, i.e. a resource created for nothing.
      // Content always implies a counted record by here (the array path counts its trailing element
      // just above; the concatenated path counts at each value start), so dropping a record-less tail
      // can never drop data. The sole exception is a plan that would otherwise be empty, which the
      // Plan invariant forbids: an empty input still gets its one empty partition.
      if (recordsInCurrent > 0L || partitions.isEmpty()) {
        emit(currentStart, end);
      }
      return new Plan(file, format, recordsField, recordCount, partitions);
    }
  }

  // ==================== The pass ====================

  /**
   * Bytes that can change structure when encountered outside a string literal. Indexed by unsigned
   * byte value, so the skip loop's test is one array load and one branch.
   */
  private static final boolean[] STRUCTURAL_OUTSIDE_STRING = structuralOutsideString();

  private static boolean[] structuralOutsideString() {
    final boolean[] table = new boolean[256];
    // '/' and '#' are not JSON structure, but outside a string they can only begin a comment — which
    // the scanner deliberately does not model. Breaking the skip loop on them is how the pass notices.
    for (final char c : new char[] {'"', '\'', '{', '}', '[', ']', ',', '/', '#'}) {
      table[c] = true;
    }
    return table;
  }

  /**
   * Length of the UTF-8 byte-order mark at the start of {@code buffer}, or {@code 0} if there is none.
   *
   * <p>A BOM is not JSON content, but it is not whitespace either: fed to the scanner it would read as
   * a bare top-level scalar, which turns a BOM-prefixed array into "two top-level values" — detected
   * as a concatenated-record stream, and then partitioned so that the mark itself surfaces as a
   * spurious leading record. BOM-prefixed exports are common enough (Windows tooling emits them) that
   * skipping the mark is the only safe reading. Gson's own reader skips it on the value side, so the
   * bytes still parse if a partition happens to include them.
   */
  private static int byteOrderMarkLength(final byte[] buffer, final int read) {
    return read >= 3 && buffer[0] == (byte) 0xEF && buffer[1] == (byte) 0xBB && buffer[2] == (byte) 0xBF ? 3 : 0;
  }

  /**
   * One sequential pass over the file, driving {@link JsonStructureScanner} with the bytes that can
   * change structure and handing {@code sink} the runs in between in bulk.
   *
   * <p>The inner skip loops are where the throughput comes from: inside a string literal only a quote
   * or a backslash matters, and inside a container only the six structural characters do, so the
   * scanner walks those runs with an array load and a compare per byte and no virtual dispatch at all.
   * Every byte at the top level is treated as structural — there are only a handful of them (the
   * separators between records) and value-boundary tracking needs them.
   */
  private static ScanOutcome walk(final Path file, final long size, final ScanSink sink) {
    final byte[] buffer = new byte[SCAN_BUFFER_SIZE];
    final JsonStructureScanner scanner = new JsonStructureScanner();
    long offset = 0L;
    // The byte following a backslash is consumed literally; it must reach the scanner so the escape
    // is cleared, even though it is not structural in its own right.
    boolean pendingEscapedByte = false;
    boolean sawCommentMarker = false;

    try (final InputStream in = Files.newInputStream(file)) {
      // readNBytes rather than read: a short read must not split the byte-order-mark check below,
      // and a full buffer keeps the skip loops running at their intended length.
      int read = in.readNBytes(buffer, 0, buffer.length);
      boolean atFileStart = true;
      while (read > 0) {
        int i = 0;
        if (atFileStart) {
          atFileStart = false;
          i = byteOrderMarkLength(buffer, read);
        }
        while (i < read) {
          if (!pendingEscapedByte) {
            final boolean inString = scanner.inString();
            final int depth = scanner.depth();
            final int runStart = i;
            boolean sawContent = false;

            if (inString) {
              // The active quote, not always '"': a lenient single-quoted string ends at '\''.
              final byte quote = scanner.quoteChar();
              while (i < read) {
                final byte b = buffer[i];
                if (b == quote || b == '\\') {
                  break;
                }
                i++;
              }
              sawContent = i > runStart;
            } else if (depth > 0) {
              while (i < read) {
                final byte b = buffer[i];
                if (STRUCTURAL_OUTSIDE_STRING[b & 0xFF]) {
                  break;
                }
                if ((b & 0xFF) > ' ') {
                  sawContent = true;
                }
                i++;
              }
            }

            if (i > runStart) {
              // The scanner did not see these bytes; tell it what the run ended with so its
              // value-position state does not go stale.
              scanner.noteSkippedRun(buffer[i - 1]);
              sink.run(buffer, runStart, i, offset + runStart, inString, depth, sawContent);
              continue;
            }
            if (i == read) {
              break;
            }
          }

          final byte b = buffer[i];
          final boolean wasInString = scanner.inString();
          if ((b == '/' || b == '#') && !wasInString && !pendingEscapedByte) {
            sawCommentMarker = true;
          }
          scanner.step(b, offset + i);
          sink.structural(b, offset + i, scanner);
          pendingEscapedByte = !pendingEscapedByte && wasInString && b == '\\';
          i++;
        }
        offset += read;
        read = in.readNBytes(buffer, 0, buffer.length);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    if (offset != size) {
      throw new SirixIOException(
          "short read on '" + file + "': expected " + size + " bytes but scanned " + offset);
    }
    return new ScanOutcome(scanner.depth() == 0 && !scanner.inString(), sawCommentMarker, scanner.depth(),
        scanner.inString());
  }

  /**
   * What the pass observed beyond what the sinks collected.
   *
   * @param balanced         whether the input ended at depth zero outside any string literal
   * @param sawCommentMarker whether a {@code /} appeared outside a string literal — in JSON that can
   *                         only be a comment, which the scanner does not model
   * @param endDepth         nesting depth at end of input, for the diagnostic
   * @param endedInString    whether the input ended inside a string literal, for the diagnostic
   */
  private record ScanOutcome(boolean balanced, boolean sawCommentMarker, int endDepth, boolean endedInString) {

    /**
     * Throw unless the input is well-formed enough to have found real boundaries. Comments are
     * checked by the caller first: an unbalanced depth is usually a <em>consequence</em> of brackets
     * inside a comment, so reporting imbalance there would name the wrong cause.
     */
    void requireBalanced(final Path file) {
      if (!balanced) {
        throw new SirixIOException("unbalanced JSON in '" + file + "': the input ends at nesting depth "
            + endDepth + (endedInString ? " inside a string literal" : "")
            + " — cannot determine record boundaries");
      }
    }
  }

  private static void closeQuietly(final @Nullable FileChannel channel, final Throwable primary) {
    if (channel == null) {
      return;
    }
    try {
      channel.close();
    } catch (final IOException closeError) {
      primary.addSuppressed(closeError);
    }
  }
}
