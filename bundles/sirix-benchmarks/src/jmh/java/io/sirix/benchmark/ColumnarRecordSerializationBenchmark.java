package io.sirix.benchmark;

import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.query.ColumnarRecordSequence;
import io.sirix.query.SirixStringSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Serialization only; fixture construction and ordering are outside the measured operation. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"--enable-preview", "--add-modules=jdk.incubator.vector"})
public class ColumnarRecordSerializationBenchmark {
  @Param({"5412", "8000"})
  public int rows;

  @Param({"false", "true"})
  public boolean stringColumn;

  // JMH invokes @Setup before any benchmark; NullAway does not recognize that lifecycle.
  @SuppressWarnings("NullAway.Init")
  private ItemSequence records;
  @SuppressWarnings("NullAway.Init")
  private ColumnarRecordSequence columns;
  @SuppressWarnings("NullAway.Init")
  private StringWriter output;
  @SuppressWarnings("NullAway.Init")
  private StringSerializer brackit;
  @SuppressWarnings("NullAway.Init")
  private SirixStringSerializer sirix;

  @Setup
  public void setup() {
    final QNm[] names =
        {new QNm("id"), new QNm("old_cost"), new QNm("new_cost"), new QNm("old_qty"), new QNm("new_qty")};
    final ColumnarRecordSequence.Column[] fields = new ColumnarRecordSequence.Column[names.length];
    final long[][] values = new long[names.length][rows];
    final boolean[] present = new boolean[rows];
    Arrays.fill(present, true);
    final String[] strings = new String[rows];
    final int[] order = new int[rows];
    final Item[] items = new Item[rows];
    for (int row = 0; row < rows; row++) {
      order[row] = row;
      strings[row] = row % 2 == 0
          ? "plain region"
          : "region\"\\\n";
      final Sequence[] record = new Sequence[names.length];
      for (int field = 0; field < names.length; field++) {
        values[field][row] = (long) row * (field + 1);
        record[field] = field == 0 && stringColumn
            ? new Str(strings[row])
            : new Int64(values[field][row]);
      }
      items[row] = new ArrayObject(names, record);
    }
    for (int field = 0; field < names.length; field++) {
      fields[field] = field == 0 && stringColumn
          ? ColumnarRecordSequence.strings(strings)
          : ColumnarRecordSequence.longs(values[field], present);
    }
    columns = new ColumnarRecordSequence(names, fields, order);
    records = new ItemSequence(items);
    output = new StringWriter(rows * 128);
    final PrintWriter writer = new PrintWriter(output);
    brackit = new StringSerializer(writer);
    sirix = new SirixStringSerializer(writer);
  }

  @Benchmark
  public int brackitRecords() {
    output.getBuffer().setLength(0);
    brackit.serialize(records);
    return output.getBuffer().length();
  }

  @Benchmark
  public int columnarRecords() {
    output.getBuffer().setLength(0);
    sirix.serialize(columns);
    return output.getBuffer().length();
  }

  @Benchmark
  public int computedRecords() {
    output.getBuffer().setLength(0);
    sirix.serialize(records);
    return output.getBuffer().length();
  }
}
