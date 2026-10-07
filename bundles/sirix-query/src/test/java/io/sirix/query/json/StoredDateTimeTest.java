package io.sirix.query.json;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.ExprUtil;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;

final class StoredDateTimeTest {
  @TempDir
  Path directory;

  @Test
  void storedCastsMatchTheGeneralParserIncludingErrors() {
    final String[] forms = {"2024-02-29T03:04:05Z", "1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z",
        "9999-12-31T23:59:59Z", "0001-01-01T00:00:00Z", "2024-02-29T03:04:05", "2024-02-29T03:04:05+02:30",
        "2024-02-29T03:04:05-14:00", "2024-02-29T03:04:05.123456789Z", "2024-02-29T03:04:05.1", "2024-12-31T24:00:00Z",
        "-0001-01-01T00:00:00Z", "10000-01-01T00:00:00Z", " 2024-02-29T03:04:05Z ", "0000-01-01T00:00:00Z",
        "2023-02-29T03:04:05Z", "2024-13-01T00:00:00Z", "2024-01-01T00:60:00Z", "2024-01-01T00:00:60Z",
        "2024-01-01T00:00:00z", "2024-01-01T00:00:00+14:01", ""};
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (int index = 0; index < forms.length; index++) {
        final String text = forms[index];
        final String resource = "r" + index;
        store.create("dates", resource, "{\"ts\":\"" + text + "\"}");
        for (final String cast : new String[] {"xs:dateTime($d.ts)", "$d.ts cast as xs:dateTime"}) {
          final Query query = new Query(chain, "let $d := jn:doc('dates','" + resource + "') return " + cast);
          final DateTime expected;
          try {
            expected = new DateTime(text);
          } catch (final QueryException invalid) {
            final QueryException actual =
                assertThrows(QueryException.class, () -> ExprUtil.asItem(query.execute(context)));
            assertEquals(invalid.getCode(), actual.getCode(), text);
            assertEquals(invalid.getMessage(), actual.getMessage(), text);
            continue;
          }
          final Item result = ExprUtil.asItem(query.execute(context));
          assertEquals(expected.stringValue(), result.atomize().stringValue(), text);
          assertEquals(0, expected.cmp(result.atomize()), text);
        }
      }
    }
  }

  @Test
  void constructorAndCastShareTheStoredFieldMemo() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("dates", "r", "{\"ts\":\"2024-02-29T03:04:05Z\"}");
      final Sequence result = new Query(chain, "let $d := jn:doc('dates','r') "
          + "return (xs:dateTime($d.ts), $d.ts cast as xs:dateTime, xs:dateTime($d.ts))").execute(context);
      try (final Iter iterator = result.iterate()) {
        final Item first = iterator.next();
        assertSame(first, iterator.next());
        assertSame(first, iterator.next());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void fieldMemoNeverSurvivesWritesThroughOtherWrappersOrTheCursor(final VersioningType versioning) {
    final QNm field = new QNm("ts");
    final String before = "2024-01-01T00:00:00Z";
    final String after = "2025-01-01T00:00:00Z";
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).versioningType(versioning).build()) {
      final var collection = store.create("dates", "r", "{\"ts\":\"" + before + "\"}");
      try (final var session = collection.getDatabase().beginResourceSession("r");
          final var writer = session.beginNodeTrx();
          final var oldReader = session.beginNodeReadOnlyTrx()) {
        writer.moveToFirstChild();
        final var object = new JsonDBObject(writer, collection);
        final long objectKey = object.getNodeKey();
        assertDate(before, object, field);
        writer.moveTo(objectKey);
        new JsonDBObject(writer, collection).replace(field, new Str(after));
        assertDate(after, object, field);
        writer.moveTo(objectKey);
        writer.moveToFirstChild();
        writer.setStringValue(before);
        assertDate(before, object, field);
        writer.commit();
        oldReader.moveToFirstChild();
        final var snapshot = new JsonDBObject(oldReader, collection);
        assertDate(before, snapshot, field);
        object.replace(field, new Str(after));
        writer.commit();
        assertDate(before, snapshot, field);
        assertDate(after, object, field);
        final QNm renamed = new QNm("renamed");
        object.rename(field, renamed);
        assertNull(object.get(field));
        assertDate(after, object, renamed);
        object.rename(renamed, field);
        object.remove(field);
        assertNull(object.get(field));
        object.insert(field, new Str(before));
        assertDate(before, object, field);
        object.replace(field, new Str(after));
        writer.commit();
        try (final var latest = session.beginNodeReadOnlyTrx()) {
          latest.moveToFirstChild();
          assertDate(after, new JsonDBObject(latest, collection), field);
        }
      }
    }
  }

  private static void assertDate(final String expected, final JsonDBObject object, final QNm field) {
    final var value = (AtomicStrJsonDBItem) object.get(field);
    assertEquals(expected, value.stringValue());
    assertEquals(expected, value.dateTime().stringValue());
    assertEquals(0, new DateTime(expected).cmp(value.dateTime()));
    assertSame(value.dateTime(), value.dateTime());
  }

}
