package io.sirix.query.budget;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.module.Module;
import io.brackit.query.util.serialize.StringSerializer;
import io.brackit.query.util.serialize.SubtreePrinter;
import io.sirix.query.ColumnarRecordSequence;
import io.sirix.query.SirixStringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.MockedConstruction;

import java.io.PrintWriter;
import java.io.Writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/** Counts output writes, not elapsed time; the reference is the positive control. */
@Isolated
final class ColumnarSerializationWorkBudgetTest {
  @Test
  void executionScopedRecordsAreWrittenInBatchesRatherThanPerField() {
    final int size = 8000;
    final long[] numbers = new long[size];
    final boolean[] present = new boolean[size];
    final String[] strings = new String[size];
    final int[] rows = new int[size];
    for (int i = 0; i < size; i++) {
      numbers[i] = i;
      present[i] = true;
      strings[i] = "north\"\\\n";
      rows[i] = size - i - 1;
    }
    final ColumnarRecordSequence columns =
        new ColumnarRecordSequence(new QNm[] {new QNm("id"), new QNm("region")}, new ColumnarRecordSequence.Column[] {
            ColumnarRecordSequence.longs(numbers, present), ColumnarRecordSequence.strings(strings)}, rows);
    final Module module = mock(Module.class);
    when(module.getBody()).thenReturn(columns);
    final Sequence wrapped = new Query(module).execute(new BrackitQueryContext());
    final CountingWriter actual = new CountingWriter();
    try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(actual))) {
      serializer.serialize(wrapped);
    }
    final CountingWriter reference = new CountingWriter();
    try (final StringSerializer serializer = new StringSerializer(new PrintWriter(reference))) {
      serializer.serialize(wrapped);
    }
    assertEquals(reference.output.toString(), actual.output.toString());
    assertTrue(actual.writes > 0, "nonempty output must go through the counting writer");
    assertTrue(actual.writes <= 16, "32 KiB batches must bound output calls: " + actual.writes);
    assertTrue(reference.writes > size * 8, "the unbatched positive control must do per-field writes");
    assertTrue(reference.writes > actual.writes * 1000, "the writer-call reduction is observable");
  }

  @Test
  void groupedPrimitiveRecordsUseTheSameBatchWriter() {
    final int size = 8000;
    final QNm[] names = {new QNm("key"), new QNm("bucket"), new QNm("count"), new QNm("minimum"), new QNm("maximum")};
    final Item[] rows = new Item[size];
    for (int row = 0; row < size; row++) {
      rows[row] = new ArrayObject(names, new Sequence[] {new Int64(row), new Int64(row % 4), new Int64(3),
          new Int64(12000 + row), new Int64(14000 + row)});
    }
    final ItemSequence records = new ItemSequence(rows);
    final CountingWriter actual = new CountingWriter();
    try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(actual))) {
      serializer.serialize(records);
    }
    final CountingWriter reference = new CountingWriter();
    try (final StringSerializer serializer = new StringSerializer(new PrintWriter(reference))) {
      serializer.serialize(records);
    }
    assertEquals(reference.output.toString(), actual.output.toString());
    assertTrue(actual.writes > 0, "nonempty output must go through the counting writer");
    assertTrue(actual.writes <= 24, "five-field grouped records must still batch their output: " + actual.writes);
    assertTrue(reference.writes > size * 12, "the Brackit positive control writes individual fields");
    assertTrue(reference.writes > actual.writes * 1000, "batching applies to existing grouped answers too");
  }

  @Test
  void mixedRecordsDelegateTheSuffixWithConstantPrintersAndFlushes() {
    final int size = 8000;
    final Sequence records =
        new Query("({'v':1}, for $i in 1 to " + size + " return {'v':[$i]})").execute(new BrackitQueryContext());
    final CountingWriter actual = new CountingWriter();
    try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(actual))) {
      serializer.serialize(records);
    }
    final CountingWriter reference = new CountingWriter();
    try (final StringSerializer serializer = new StringSerializer(new PrintWriter(reference))) {
      serializer.serialize(records);
    }
    final StringBuilder expected = new StringBuilder("{\"v\":1}");
    for (int row = 1; row <= size; row++) {
      expected.append(" {\"v\":[").append(row).append("]}");
    }
    assertEquals(expected.toString(), reference.output.toString());
    assertEquals(reference.output.toString(), actual.output.toString());
    assertTrue(actual.flushes > 0, "the mixed result must exercise the flush counter");
    assertTrue(actual.flushes <= 2, "the entire fallback suffix must use one flush pair: " + actual.flushes);

    final CountingWriter perItem = new CountingWriter();
    try (final StringSerializer serializer = new StringSerializer(new PrintWriter(perItem));
        final Iter iterator = records.iterate()) {
      boolean first = true;
      for (Item item = iterator.next(); item != null; item = iterator.next()) {
        if (!first) {
          perItem.write(' ');
        }
        serializer.serialize(item);
        first = false;
      }
    }
    assertEquals(expected.toString(), perItem.output.toString());
    assertTrue(perItem.flushes >= size * 2, "per-item fallback must violate the constant flush budget");

    final CountingWriter allocationOutput = new CountingWriter();
    try (final MockedConstruction<SubtreePrinter> printers = mockConstruction(SubtreePrinter.class)) {
      try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(allocationOutput))) {
        serializer.serialize(records);
      }
      assertEquals(reference.output.toString(), allocationOutput.output.toString());
      final int suffixPrinters = printers.constructed().size();
      assertEquals(1, suffixPrinters, "all unsupported records must share one fallback printer");
      try (final StringSerializer serializer = new StringSerializer(new PrintWriter(new CountingWriter()));
          final Iter iterator = records.iterate()) {
        for (Item item = iterator.next(); item != null; item = iterator.next()) {
          serializer.serialize(item);
        }
      }
      assertEquals(size + 1, printers.constructed().size() - suffixPrinters,
          "per-item serialization must expose growing printer allocations");
    }
  }

  private static final class CountingWriter extends Writer {
    private final StringBuilder output = new StringBuilder(512 * 1024);
    private int writes;
    private int flushes;

    @Override
    public void write(final char[] chars, final int offset, final int length) {
      writes++;
      output.append(chars, offset, length);
    }

    @Override
    public void write(final String value, final int offset, final int length) {
      writes++;
      output.append(value, offset, offset + length);
    }

    @Override
    public void write(final int value) {
      writes++;
      output.append((char) value);
    }

    @Override
    public void flush() {
      flushes++;
    }

    @Override
    public void close() {}
  }
}
