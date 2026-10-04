package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DTD;
import io.brackit.query.atomic.Date;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.Time;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SirixQueryContextTimeTest {

  private static final DateTimeToInstant CONVERTER = new DateTimeToInstant();

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"-04:00, true, 4, 0", "-03:30, true, 3, 30", "+05:30, false, 5, 30", "+02:00, false, 2, 0"})
  void timezoneEncodingPreservesSignAndMagnitudes(final String offset, final boolean negative, final int hours,
      final int minutes) {
    final DTD timezone = SirixQueryContext.timezoneFromOffset(ZoneOffset.of(offset));

    assertEquals(negative, timezone.isNegative());
    assertEquals(0, timezone.getDays());
    assertEquals(hours, timezone.getHours());
    assertEquals(minutes, timezone.getMinutes());
    assertEquals(0, timezone.getMicros());
  }

  @ParameterizedTest
  @ValueSource(strings = {"current-dateTime()", "current-date()", "current-time()", "implicit-timezone()"})
  void currentFunctionsShareOneStableInstantAndJvmOffset(final String firstFunction) {
    try (final var jsonStore = BasicJsonDBStore.newBuilder().location(directory.resolve("json")).build();
        final var xmlStore = BasicXmlDBStore.newBuilder().location(directory.resolve("xml")).build();
        final var ctx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(xmlStore, jsonStore,
            CommitStrategy.AUTO);
        final var chain = SirixCompileChain.createWithNodeAndJsonStore(xmlStore, jsonStore)) {
      final Instant before = Instant.now().truncatedTo(ChronoUnit.MICROS);
      final Atomic firstValue = (Atomic) new Query(chain, firstFunction).evaluate(ctx);
      final Instant after = Instant.now();
      assertCurrentInstant(ctx, before, after);

      final DateTime dateTime = ctx.getDateTime();
      assertSame(dateTime, ctx.getDateTime());
      assertSame(ctx.getDate(), ctx.getDate());
      assertSame(ctx.getTime(), ctx.getTime());
      assertSame(dateTime.getTimezone(), ctx.getImplicitTimezone());
      assertEquals(new Date(dateTime).stringValue(),
          ((Date) new Query(chain, "current-date()").evaluate(ctx)).stringValue());
      assertEquals(new Time(dateTime).stringValue(),
          ((Time) new Query(chain, "current-time()").evaluate(ctx)).stringValue());
      assertEquals(firstValue.stringValue(), ((Atomic) new Query(chain, firstFunction).evaluate(ctx)).stringValue());
    }
  }

  @Test
  void currentDateTimeOpensTheCorrectJsonRevision() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Instant reference = Instant.now().truncatedTo(ChronoUnit.MILLIS);
      final Instant present = reference.minusSeconds(60);
      final var options = new ArrayObject(new QNm[] {new QNm("commitTimestamp")},
          new Sequence[] {new Str(reference.minusSeconds(120).toString())});
      final var collection = store.create("products", "data", "[\"past\"]", options);
      try (final var session = collection.getDatabase().beginResourceSession("data");
          final var trx = session.beginNodeTrx()) {
        assertTrue(trx.moveToFirstChild());
        assertTrue(trx.moveToFirstChild());
        trx.setStringValue("present");
        trx.commit(null, present);
      }

      final Instant before = Instant.now().truncatedTo(ChronoUnit.MICROS);
      final JsonDBItem document =
          (JsonDBItem) new Query(chain, "jn:open('products','data',current-dateTime())").evaluate(ctx);
      final Instant after = Instant.now();
      assertNotNull(document);
      assertEquals(2, document.getTrx().getRevisionNumber());
      assertEquals(present, document.getTrx().getRevisionTimestamp());
      assertCurrentInstant(ctx, before, after);
    }
  }

  @Test
  @SuppressWarnings("NullAway") // The XML store accepts a null commit message.
  void currentDateTimeOpensTheCorrectXmlRevision() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final Instant reference = Instant.now().truncatedTo(ChronoUnit.MILLIS);
      final Instant present = reference.minusSeconds(60);
      final var collection =
          store.create("products", new DocumentParser("<value>past</value>"), null, reference.minusSeconds(120));
      try (final var session = collection.getDatabase().beginResourceSession("resource1");
          final var trx = session.beginNodeTrx()) {
        assertTrue(trx.moveToFirstChild());
        assertTrue(trx.moveToFirstChild());
        trx.setValue("present");
        trx.commit(null, present);
      }

      final Instant before = Instant.now().truncatedTo(ChronoUnit.MICROS);
      final XmlDBNode document =
          (XmlDBNode) new Query(chain, "xml:open('products','resource1',current-dateTime())").evaluate(ctx);
      final Instant after = Instant.now();
      assertNotNull(document);
      assertEquals(2, document.getTrx().getRevisionNumber());
      assertEquals(present, document.getTrx().getRevisionTimestamp());
      assertCurrentInstant(ctx, before, after);
    }
  }

  private static void assertCurrentInstant(final SirixQueryContext ctx, final Instant before, final Instant after) {
    final DateTime dateTime = ctx.getDateTime();
    final Instant instant = CONVERTER.convert(dateTime);
    assertTrue(!instant.isBefore(before) && !instant.isAfter(after),
        () -> dateTime + " must denote the current instant in " + ZoneId.systemDefault());
    final OffsetDateTime expected = instant.atZone(ZoneId.systemDefault()).toOffsetDateTime();
    assertEquals(new DateTime(expected.toString()).stringValue(), dateTime.stringValue());
  }
}
