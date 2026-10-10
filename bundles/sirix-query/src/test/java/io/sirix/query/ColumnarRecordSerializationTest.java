package io.sirix.query;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.module.Module;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.serialize.StringSerializer;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class ColumnarRecordSerializationTest {
  private static final QNm[] NAMES = {new QNm("n\"\\\n"), new QNm("s")};

  @Test
  void primitiveColumnsMatchBrackitForEscapingOrderMissingAndIntegerExtremes() {
    final StringBuilder controls = new StringBuilder();
    for (char c = 0; c < 32; c++) {
      controls.append(c);
    }
    final long[] longs = {Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 123456789};
    final boolean[] present = {true, true, false, true, true};
    final String[] strings =
        {controls.toString(), "quote\" slash\\ /", null, "\u00e4\u4e2d\ud83d\ude00", "\ud800x\udfff"};
    final int[] order = {4, 1, 0, 3, 2, 0};
    assertParity(longs, present, strings, order);
  }

  @Test
  void arbitraryUtf16AndLongColumnsMatchTheReference() {
    final Random random = new Random(729413L);
    final int count = 400;
    final long[] longs = new long[count];
    final boolean[] present = new boolean[count];
    final String[] strings = new String[count];
    final int[] order = new int[count];
    for (int i = 0; i < count; i++) {
      longs[i] = random.nextLong();
      present[i] = random.nextBoolean();
      final char[] chars = new char[random.nextInt(180)];
      for (int j = 0; j < chars.length; j++) {
        chars[j] = (char) random.nextInt(65536);
      }
      strings[i] = random.nextInt(8) == 0
          ? null
          : new String(chars);
      order[i] = random.nextInt(count);
    }
    assertParity(longs, present, strings, order);
  }

  private static void assertParity(final long[] longs, final boolean[] present, final String[] strings,
      final int[] order) {
    final ColumnarRecordSequence columns = new ColumnarRecordSequence(NAMES, new ColumnarRecordSequence.Column[] {
        ColumnarRecordSequence.longs(longs, present), ColumnarRecordSequence.strings(strings)}, order);
    final Item[] records = new Item[order.length];
    for (int i = 0; i < order.length; i++) {
      final int row = order[i];
      records[i] = new ArrayObject(NAMES, new Sequence[] {present[row]
          ? new Int64(longs[row])
          : null,
          strings[row] == null
              ? null
              : new Str(strings[row])});
    }
    final ItemSequence reference = new ItemSequence(records);
    assertEquals(serialize(reference, false, false), serialize(columns, true, false));
    assertArrayEquals(restReference(reference).getBytes(StandardCharsets.UTF_8),
        rest(columns).getBytes(StandardCharsets.UTF_8));
    assertEquals(serialize(reference, false, true), serialize(columns, true, true));
    assertEquals(serialize(reference, false, false), serialize(columns, false, false));
  }

  @Test
  void everyOtherJdmValueFallsBackWithoutChangingItsSpelling() {
    final String[] expressions = {"()", "null", "true()", "false()", "1", "-1", "(1,2)",
        "xs:integer('123456789012345678901234567890')", "xs:decimal('123.4500')", "xs:double('-0')", "xs:double('NaN')",
        "xs:double('INF')", "xs:float('-INF')", "'quote\" slash\\'", "xs:dateTime('2024-01-02T03:04:05Z')",
        "xs:date('2024-01-02')", "xs:time('03:04:05')", "xs:dayTimeDuration('P1DT2H')", "xs:yearMonthDuration('P1Y2M')",
        "[1,null,{'x':'y'}]", "{'a':(1,2),'b':[],'c':{}}", "<x a='v'>text</x>"};
    final QNm[] names = {new QNm("value")};
    final BrackitQueryContext context = new BrackitQueryContext();
    for (final String expression : expressions) {
      final Sequence value = new Query(expression).execute(context);
      final ColumnarRecordSequence columns = new ColumnarRecordSequence(names,
          new ColumnarRecordSequence.Column[] {ColumnarRecordSequence.sequences(new Sequence[] {value})},
          new int[] {0});
      final ArrayObject reference = new ArrayObject(names, new Sequence[] {value});
      assertEquals(serialize(reference, false, false), serialize(columns, true, false), expression);
      assertEquals(serialize(reference, false, true), serialize(columns, true, true), expression);
      assertEquals(restReference(reference), rest(columns), expression);
      final ItemSequence ordinaryRows = new ItemSequence(reference, reference);
      assertEquals(serialize(ordinaryRows, false, false), serialize(ordinaryRows, true, false), expression);
    }
  }

  @Test
  void executionWrapperRetainsTheBatchPathAndOrdinaryItemAccess() {
    final ColumnarRecordSequence columns = fixture();
    final Module module = mock(Module.class);
    when(module.getBody()).thenReturn(columns);
    final Sequence wrapped = new Query(module).execute(new BrackitQueryContext());
    assertFalse(wrapped instanceof ColumnarRecordSequence, "exercise Brackit's real execution wrapper");
    assertEquals("{\"v\":2} {\"v\":1}", serialize(wrapped, true, false));
    assertEquals("{\"v\":2}", serialize(columns.get(Int32.ONE), true, false));
    try (final Iter iterator = wrapped.iterate()) {
      final Object object = assertInstanceOf(Object.class, iterator.next());
      assertEquals(new Int64(2), object.get(new QNm("v")));
      assertEquals(1, object.len());
      assertEquals(new Int64(1), ((Object) iterator.next()).value(0));
      assertNull(iterator.next());
    }
  }

  @Test
  void mutationAndRepeatedIterationPreserveNormalObjectSemantics() {
    final ColumnarRecordSequence columns = fixture();
    final Object first = (Object) columns.get(Int32.ONE);
    assertSame(first, columns.get(Int32.ONE));
    first.replace(new QNm("v"), new Str("new"));
    first.insert(new QNm("z"), null);
    first.rename(new QNm("v"), new QNm("renamed"));
    assertEquals("{\"renamed\":\"new\",\"z\":null} {\"v\":1}", serialize(columns, true, false));
    first.remove(new QNm("absent"));
    first.remove(1);
    assertEquals("{\"renamed\":\"new\"} {\"v\":1}", serialize(columns, true, false));
    assertThrows(QueryException.class, columns::booleanValue);
    assertNull(columns.get(Int32.ZERO));
    assertNull(columns.get(new Int64(Long.MAX_VALUE)));
    assertEquals(new Int32(2), columns.size());
    assertEquals(new Int32(2), columns.knownSize());
    assertTrue(columns.isRepeatable());
  }

  @Test
  void buildersAndConstructorsValidateAndOwnInputArrays() {
    final QNm[] names = {new QNm("v")};
    final long[] longs = {2, 1};
    final boolean[] present = {true, true};
    final int[] order = {0, 1};
    final ColumnarRecordSequence columns = new ColumnarRecordSequence(names,
        new ColumnarRecordSequence.Column[] {ColumnarRecordSequence.longs(longs, present)}, order);
    names[0] = new QNm("changed");
    longs[0] = 9;
    present[0] = false;
    order[0] = 1;
    assertEquals("{\"v\":2} {\"v\":1}", serialize(columns, true, false));
    final ColumnarRecordSequence.Builder builder =
        new ColumnarRecordSequence.Builder(NAMES, new boolean[] {false, true}, 0);
    for (int i = 0; i < 40; i++) {
      final int row = builder.addRow();
      builder.setLong(0, row, i);
      builder.setString(1, row, "s");
    }
    assertEquals("{\"n\\\"\\\\\\n\":39,\"s\":\"s\"}", serialize(builder.build(new int[] {39}), true, false));
    assertThrows(IllegalArgumentException.class, () -> ColumnarRecordSequence.longs(new long[1], new boolean[0]));
    assertThrows(IllegalArgumentException.class, () -> new ColumnarRecordSequence(new QNm[0],
        new ColumnarRecordSequence.Column[] {ColumnarRecordSequence.strings(new String[0])}, new int[0]));
    assertThrows(IllegalArgumentException.class, () -> new ColumnarRecordSequence(new QNm[] {new QNm("v")},
        new ColumnarRecordSequence.Column[] {ColumnarRecordSequence.strings(new String[1])}, new int[] {-1}));
    assertThrows(IllegalArgumentException.class, () -> new ColumnarRecordSequence(new QNm[] {new QNm("v")},
        new ColumnarRecordSequence.Column[] {ColumnarRecordSequence.strings(new String[1])}, new int[] {1}));
  }

  @Test
  void mixedSequencesAndRepeatedSerializeCallsHaveTheSameSeparators() {
    final ColumnarRecordSequence columns = fixture();
    final ItemSequence mixed = new ItemSequence(columns.get(Int32.ONE), new Str("plain"), columns.get(new Int32(2)),
        new Query("[1,2]").execute(new BrackitQueryContext()).get(Int32.ONE));
    assertEquals(serialize(mixed, false, false), serialize(mixed, true, false));
    final StringWriter actual = new StringWriter();
    final StringWriter expected = new StringWriter();
    try (final SirixStringSerializer fast = new SirixStringSerializer(new PrintWriter(actual));
        final StringSerializer generic = new StringSerializer(new PrintWriter(expected))) {
      fast.serialize(columns);
      fast.serialize(columns);
      generic.serialize(fixture());
      generic.serialize(fixture());
    }
    assertEquals(expected.toString(), actual.toString());
    final ColumnarRecordSequence empty =
        new ColumnarRecordSequence(new QNm[0], new ColumnarRecordSequence.Column[0], new int[0]);
    assertEquals("", serialize(empty, true, false));
    assertEquals("{\"rest\":[]}", rest(empty));
    assertEquals("{} {}", serialize(
        new ColumnarRecordSequence(new QNm[0], new ColumnarRecordSequence.Column[0], new int[] {0, 1}), true, false));
  }

  @Test
  void lazyNonColumnarSequenceIsReadAndClosedOnce() {
    final int[] events = new int[3];
    final Sequence lazy = new AbstractSequence() {
      public Iter iterate() {
        events[0]++;
        return new BaseIter() {
          private boolean emitted;

          public Item next() {
            events[1]++;
            if (emitted) {
              return null;
            }
            emitted = true;
            return new Str("x");
          }

          public void close() {
            events[2]++;
          }
        };
      }
    };
    assertEquals("x", serialize(lazy, true, false));
    assertEquals(1, events[0]);
    assertEquals(2, events[1]);
    assertEquals(1, events[2]);
  }

  @Test
  void oversizedStringsAndSurrogatesAcrossBufferEdgesRemainByteIdentical() {
    final String text = "x".repeat(32758) + "\ud83d\ude00" + "\u0000" + "y".repeat(70000);
    assertParity(new long[] {Long.MIN_VALUE}, new boolean[] {true}, new String[] {text}, new int[] {0, 0});
  }

  @Test
  void genericRestRecordsPreserveTheReferenceCharactersWithoutAnEncodingRoundTrip() {
    final ArrayObject record = new ArrayObject(NAMES,
        new Sequence[] {new Query("xs:decimal('1.2')").execute(new BrackitQueryContext()), new Str("x\ud800y\udfff")});
    assertEquals(restReference(record), rest(record));
  }

  @Test
  void iteratorFailureFlushesCompletedRecordsAndClosesOnce() {
    final int[] closed = new int[1];
    final ColumnarRecordSequence columns = fixture();
    final Sequence failing = new AbstractSequence() {
      public Iter iterate() {
        return new BaseIter() {
          private boolean emitted;

          public Item next() {
            if (emitted) {
              throw new IllegalStateException("after first record");
            }
            emitted = true;
            return columns.get(Int32.ONE);
          }

          public void close() {
            closed[0]++;
          }
        };
      }
    };
    final StringWriter output = new StringWriter();
    try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(output))) {
      assertThrows(IllegalStateException.class, () -> serializer.serialize(failing));
    }
    assertEquals("{\"v\":2}", output.toString());
    assertEquals(1, closed[0]);
  }

  private static ColumnarRecordSequence fixture() {
    return new ColumnarRecordSequence(new QNm[] {new QNm("v")}, new ColumnarRecordSequence.Column[] {
        ColumnarRecordSequence.longs(new long[] {1, 2}, new boolean[] {true, true})}, new int[] {1, 0});
  }

  private static String serialize(final Sequence sequence, final boolean fast, final boolean format) {
    final StringWriter output = new StringWriter();
    try (final StringSerializer serializer = fast
        ? new SirixStringSerializer(new PrintWriter(output))
        : new StringSerializer(new PrintWriter(output))) {
      serializer.setFormat(format);
      serializer.setIndent("\t");
      serializer.serialize(sequence);
    }
    return output.toString();
  }

  private static String restReference(final Sequence sequence) {
    final StringBuilder output = new StringBuilder("{\"rest\":[");
    try (final Iter iterator = sequence.iterate()) {
      boolean first = true;
      for (Item item = iterator.next(); item != null; item = iterator.next()) {
        if (!first) {
          output.append(',');
        }
        output.append(serialize(item, false, false));
        first = false;
      }
    }
    return output.append("]}").toString();
  }

  private static String rest(final Sequence sequence) {
    final StringBuilder output = new StringBuilder();
    try (final JsonDBSerializer serializer = new JsonDBSerializer(output, false)) {
      serializer.serialize(sequence);
    }
    return output.toString();
  }
}
