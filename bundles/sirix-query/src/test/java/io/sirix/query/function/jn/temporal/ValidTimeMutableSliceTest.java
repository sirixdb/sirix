package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBArraySlice;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

final class ValidTimeMutableSliceTest {
  private static final String POINT = "2024-01-01T00:00:00Z";
  private static final String FROM = "2023-01-01T00:00:00Z";
  private static final String TO = "2025-01-01T00:00:00Z";
  private static final String EXPIRED = "2023-06-01T00:00:00Z";
  private static final Instant INSTANT = Instant.parse(POINT);

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"0,false", "0,true", "1,false", "1,true", "2,false", "2,true"})
  void pendingEndpointEditsRemoveOldMatchesAndAddPreviouslyAbsentMatches(final int representation,
      final boolean addMatch) {
    try (final Fixture fixture = fixture("[" + row(1, FROM, addMatch
        ? EXPIRED
        : TO) + "]")) {
      final JsonDBArray document = array(fixture, representation);
      final JsonDBItem object = (JsonDBItem) document.at(0);
      ((Object) object).get(new QNm("vt"));
      document.values();
      edit(fixture.writer(), object.getNodeKey(), "vt", addMatch
          ? TO
          : EXPIRED);
      final List<Long> expected = addMatch
          ? List.of(1L)
          : List.of();
      assertResults(fixture, scan(fixture, document), expected);
      assertResults(fixture, scan(fixture, object), expected);
      assertKeys(document, expected, false);
      assertNull(ValidTimeIntervalIndex.sequence(document, INSTANT, config(fixture), false, false));
      final AtomicInteger evaluations = new AtomicInteger();
      assertNull(ValidTimeIntervalIndex.comparisonSequence(document, () -> {
        evaluations.incrementAndGet();
        return new DateTime(POINT);
      }, config(fixture), false, true));
      assertEquals(0, evaluations.get(), "mutable admission must decline before evaluating the point");
      assertResults(fixture, scan(fixture, fixture.historical()), addMatch
          ? List.of()
          : List.of(1L));
      assertNotNull(ValidTimeIntervalIndex.sequence(fixture.historical(), INSTANT, config(fixture), false, false));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void currentEndpointBoundariesPreserveEveryStrictInclusiveMode(final int representation) {
    try (final Fixture fixture =
        fixture("[" + row(1, FROM, TO) + "," + row(2, FROM, TO) + "," + row(3, FROM, TO) + "]")) {
      final JsonDBArray document = array(fixture, representation);
      final long first = ((JsonDBItem) document.at(0)).getNodeKey();
      final long second = ((JsonDBItem) document.at(1)).getNodeKey();
      edit(fixture.writer(), first, "vt", POINT);
      edit(fixture.writer(), second, "vf", POINT);
      final List<List<Long>> expected = List.of(List.of(1L, 2L, 3L), List.of(1L, 3L), List.of(2L, 3L), List.of(3L));
      for (int mode = 0; mode < 4; mode++) {
        assertResults(fixture, comparisons(fixture, document, mode), expected.get(mode));
      }
      assertKeys(document, List.of(1L, 2L, 3L), false);
      assertKeys(document, List.of(2L, 3L), true);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void heldArrayViewsReadInsertDeleteAndContainerMoves(final int representation) {
    try (final Fixture fixture = fixture("[" + row(1, FROM, TO) + "," + row(2, FROM, TO) + "]")) {
      final JsonDBArray document = array(fixture, representation);
      document.values();
      fixture.writer().moveTo(document.getNodeKey());
      fixture.writer()
             .insertSubtreeAsFirstChild(JsonShredder.createStringReader(row(3, FROM, TO)), JsonNodeTrx.Commit.NO);
      assertResults(fixture, scan(fixture, document), List.of(3L, 1L, 2L));
      assertResults(fixture, comparisons(fixture, document, 2), List.of(3L, 1L, 2L));
      fixture.writer().moveTo(document.getNodeKey());
      fixture.writer().moveToLastChild();
      final long last = fixture.writer().getNodeKey();
      fixture.writer().moveTo(document.getNodeKey());
      fixture.writer().moveSubtreeToFirstChild(last);
      assertResults(fixture, scan(fixture, document), List.of(2L, 3L, 1L));
      fixture.writer().moveTo(document.getNodeKey());
      fixture.writer().moveToFirstChild();
      fixture.writer().remove();
      assertResults(fixture, scan(fixture, document), List.of(3L, 1L));
      assertResults(fixture, comparisons(fixture, document, 2), List.of(3L, 1L));
      assertKeys(document, List.of(1L, 3L), true);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void warmedSliceMembersAndAnchorsKeepTheirWindowAcrossEdits(final int representation) {
    try (final Fixture fixture = fixture(
        "[" + row(1, FROM, TO) + "," + row(2, FROM, TO) + "," + row(3, FROM, TO) + "," + row(4, FROM, TO) + "]")) {
      final JsonDBArray whole = array(fixture, representation);
      final JsonDBArraySlice slice = (JsonDBArraySlice) whole.range(Int32.ONE, new Int32(3));
      final JsonDBItem second = (JsonDBItem) slice.at(0);
      ((Object) second).get(new QNm("vt"));
      slice.values();
      edit(fixture.writer(), second.getNodeKey(), "vt", EXPIRED);
      assertResults(fixture, scan(fixture, slice), List.of(3L));
      assertResults(fixture, comparisons(fixture, slice, 2), List.of(3L));
      fixture.writer().moveTo(whole.getNodeKey());
      fixture.writer()
             .insertSubtreeAsFirstChild(JsonShredder.createStringReader(row(5, FROM, TO)), JsonNodeTrx.Commit.NO);
      assertResults(fixture, scan(fixture, slice), List.of(1L));
      assertEquals(1L, id(slice.at(0)));
      fixture.writer().moveTo(whole.getNodeKey());
      fixture.writer().moveToFirstChild();
      fixture.writer().remove();
      assertEquals(3L, id(slice.at(1)), "the previous cursor anchor must not shift the positional window");
      assertResults(fixture, scan(fixture, slice), List.of(3L));
      final JsonDBArraySlice anchored = (JsonDBArraySlice) whole.range(Int32.ONE, new Int32(3));
      assertEquals(2L, id(anchored.at(0)));
      fixture.writer().moveTo(whole.getNodeKey());
      fixture.writer()
             .insertSubtreeAsFirstChild(JsonShredder.createStringReader(row(5, FROM, TO)), JsonNodeTrx.Commit.NO);
      assertEquals(2L, id(anchored.at(Int32.ONE)), "a surviving node anchor can move to a different slice position");
      final JsonDBArraySlice empty = (JsonDBArraySlice) whole.range(Int32.ONE, Int32.ONE);
      assertResults(fixture, ScanValidTimeIndex.comparisonScan(null, fixture.context(), empty, () -> {
        throw new AssertionError("empty slice evaluated its deferred point");
      }, "vf", "vt", 2), List.of());
    }
  }

  @Test
  void immutableSlicesUseTheirWindowInsteadOfWholeArrayPostings() {
    try (final Fixture fixture = fixture(
        "[" + row(1, FROM, TO) + "," + row(2, FROM, TO) + "," + row(3, FROM, TO) + "," + row(4, FROM, TO) + "]")) {
      final JsonDBItem slice = (JsonDBItem) ((Array) fixture.historical()).range(Int32.ONE, new Int32(3));
      assertResults(fixture, scan(fixture, slice), List.of(2L, 3L));
      assertResults(fixture, comparisons(fixture, slice, 2), List.of(2L, 3L));
      assertKeys(slice, List.of(2L, 3L), true);
      assertNull(ValidTimeIntervalIndex.sequence(slice, INSTANT, config(fixture), false, true));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void objectValueViewsUseCurrentFieldsAndPreserveHistoricalValues(final int representation) {
    try (final Fixture fixture = fixture("{\"left\":" + row(1, FROM, TO) + ",\"right\":" + row(2, FROM, TO) + "}")) {
      final Object historical = (Object) fixture.historical();
      final JsonDBItem historicalValues = (JsonDBItem) historical.values();
      assertResults(fixture, scan(fixture, historicalValues), List.of(1L, 2L));
      assertNull(ValidTimeIntervalIndex.sequence(historicalValues, INSTANT, config(fixture), false, false));
      final JsonDBItem historicalNames = (JsonDBItem) historical.names();
      assertResults(fixture, scan(fixture, historicalNames), List.of());
      final JsonDBObject owner = new JsonDBObject(cursor(fixture, representation), fixture.collection());
      final JsonDBItem values = (JsonDBItem) owner.values();
      final JsonDBItem first = (JsonDBItem) ((Array) values).at(0);
      ((Object) first).get(new QNm("vt"));
      ((Array) values).values();
      edit(fixture.writer(), first.getNodeKey(), "vt", EXPIRED);
      assertResults(fixture, scan(fixture, values), List.of(2L));
      assertResults(fixture, comparisons(fixture, values, 2), List.of(2L));
      assertKeys(values, List.of(2L), true);
      assertResults(fixture, scan(fixture, historicalValues), List.of(1L, 2L));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void heldEmptyWrapperSeesNewChildrenAndMalformedComparisonsStillRaise(final int representation) {
    try (final Fixture fixture = fixture("[]")) {
      final JsonDBArray document = array(fixture, representation);
      assertEquals(0, document.len());
      document.values();
      fixture.writer().moveTo(document.getNodeKey());
      fixture.writer()
             .insertSubtreeAsFirstChild(JsonShredder.createStringReader(row(1, FROM, TO)), JsonNodeTrx.Commit.NO);
      assertResults(fixture, comparisons(fixture, document, 2), List.of(1L));
      fixture.writer().moveTo(document.getNodeKey());
      fixture.writer().moveToFirstChild();
      edit(fixture.writer(), fixture.writer().getNodeKey(), "vt", "bad-date");
      assertThrows(QueryException.class, () -> comparisons(fixture, document, 2).size());
      assertResults(fixture, scan(fixture, document), List.of(1L));
    }
  }

  private Fixture fixture(final String rows) {
    final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
    final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
    final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
    new Query(chain,
        "jn:store('mutable','rows','" + rows + "',true(),"
            + "{\"validFromPath\":\"vf\",\"validToPath\":\"vt\",\"autoCreateValidTimeIndex\":true()})").evaluate(
                context);
    final JsonDBCollection collection = store.lookup("mutable");
    final JsonDBItem historical = collection.getDocument("rows");
    final JsonNodeTrx writer =
        historical.getResourceSession().getNodeTrx().orElseGet(historical.getResourceSession()::beginNodeTrx);
    return new Fixture(store, context, chain, collection, historical, writer);
  }

  private static JsonDBArray array(final Fixture fixture, final int representation) {
    return new JsonDBArray(cursor(fixture, representation), fixture.collection());
  }

  private static JsonNodeReadOnlyTrx cursor(final Fixture fixture, final int representation) {
    final JsonNodeReadOnlyTrx cursor;
    if (representation == 0) {
      cursor = fixture.writer();
    } else {
      cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(fixture.writer()));
      if (representation == 2) {
        final StorageEngineReader reader =
            mock(StorageEngineReader.class, delegatesTo(fixture.writer().getStorageEngineReader()));
        assertTrue(reader.hasTrxIntentLog());
        doReturn(reader).when(cursor).getStorageEngineReader();
      }
    }
    cursor.moveTo(fixture.historical().getNodeKey());
    return cursor;
  }

  private static void edit(final JsonNodeTrx writer, final long object, final String field, final String value) {
    assertTrue(writer.moveTo(object));
    assertTrue(writer.moveToFirstChild());
    while (!field.equals(writer.getName().getLocalName())) {
      assertTrue(writer.moveToRightSibling());
    }
    writer.setStringValue(value);
  }

  private static String row(final int id, final String from, final String to) {
    return "{\"id\":" + id + ",\"vf\":\"" + from + "\",\"vt\":\"" + to + "\"}";
  }

  private static Sequence scan(final Fixture fixture, final JsonDBItem document) {
    return new ScanValidTimeIndex().execute(null, fixture.context(), new Sequence[] {document, new DateTime(POINT)});
  }

  private static Sequence comparisons(final Fixture fixture, final JsonDBItem document, final int mode) {
    return ScanValidTimeIndex.comparisonScan(null, fixture.context(), document, () -> new DateTime(POINT), "vf", "vt",
        mode);
  }

  private static ValidTimeConfig config(final Fixture fixture) {
    return fixture.historical().getResourceSession().getResourceConfig().getValidTimeConfig();
  }

  private static void assertResults(final Fixture fixture, final Sequence result, final List<Long> expected) {
    assertEquals(expected.size(), result.size().intValue());
    final List<Long> actual = new ArrayList<>();
    try (var iterator = result.iterate()) {
      Item item;
      while ((item = iterator.next()) != null) {
        actual.add(id(item));
      }
    }
    assertEquals(expected, actual);
    if (expected.isEmpty()) {
      assertNull(result.get(Int32.ONE));
    } else {
      assertEquals(expected.getFirst().longValue(), id(result.get(Int32.ONE)));
    }
    fixture.context().bind(new QNm("matches"), result);
    assertEquals(expected.isEmpty()
        ? Bool.FALSE
        : Bool.TRUE,
        new Query(fixture.chain(), "declare variable $matches external; exists($matches)").evaluate(fixture.context()));
  }

  private static long id(final Sequence item) {
    return ((Numeric) ((Object) item).get(new QNm("id"))).longValue();
  }

  private static void assertKeys(final JsonDBItem document, final List<Long> expected, final boolean strictEnd) {
    final long[] keys = ValidTimeIntervalIndex.keys(document, INSTANT, strictEnd);
    final long[] sorted = keys.clone();
    Arrays.sort(sorted);
    assertArrayEquals(sorted, keys);
    final List<Long> actual = new ArrayList<>();
    for (final long key : keys) {
      document.getTrx().moveTo(key);
      actual.add(id(new JsonDBObject(document.getTrx(), document.getCollection())));
    }
    assertEquals(expected, actual);
  }

  private record Fixture(BasicJsonDBStore store, SirixQueryContext context, SirixCompileChain chain,
      JsonDBCollection collection, JsonDBItem historical, JsonNodeTrx writer) implements AutoCloseable {
    @Override
    public void close() {
      writer.rollback();
      writer.close();
      chain.close();
      context.close();
      store.close();
    }
  }
}
