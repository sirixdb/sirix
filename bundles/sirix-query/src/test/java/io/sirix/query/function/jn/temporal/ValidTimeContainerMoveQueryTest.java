package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.index.interval.json.ValidTimeMoveTestSupport;
import io.sirix.index.interval.json.ValidTimeMoveTestSupport.Keys;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ValidTimeContainerMoveQueryTest {
  private static final String POINT = "xs:dateTime('2024-01-01T00:00:00Z')";
  private static final Instant INSTANT = Instant.parse("2024-01-01T00:00:00Z");
  private static final WorkCapture POSTINGS = WorkCapture.of(EngineWorkCounters.VALID_TIME_POSTING_REFS);

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"vf,first", "vf,right", "vf,left", "vt,first", "vt,right", "vt,left"})
  void movedContainersKeepPublicTemporalConsumersExact(final String duplicate, final String mode) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : List.of("indexed", "plain")) {
        new Query(chain,
            "jn:store('moves','" + resource + "','" + ValidTimeMoveTestSupport.rows(duplicate) + "',"
                + (resource.equals("indexed")
                    ? "true()"
                    : "false()")
                + ",{\"validFromPath\":\"vf\",\"validToPath\":\"vt\",\"autoCreateValidTimeIndex\":"
                + (resource.equals("indexed")
                    ? "true()"
                    : "false()")
                + "})").evaluate(context);
        final var collection = requireNonNull(store.lookup("moves"));
        final var document = requireNonNull(collection.getDocument(resource));
        final Keys keys = ValidTimeMoveTestSupport.keys(document.getTrx());
        final var session = document.getResourceSession();
        try (var writer = session.beginNodeTrx()) {
          for (int step = 1; step <= 5; step++) {
            ValidTimeMoveTestSupport.mutate(writer, keys, mode, step);
            writer.commit();
          }
        }
      }
    }
    Databases.clearGlobalCaches();
    for (int step = 0; step <= 5; step++) {
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var context = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final var collection = requireNonNull(store.lookup("moves"));
        final int revision = step + 1;
        final var initial = requireNonNull(collection.getDocument("indexed", 1));
        final Keys keys = ValidTimeMoveTestSupport.keys(initial.getTrx());
        final var document = requireNonNull(collection.getDocument("indexed", revision));
        final var reader = document.getTrx();
        final long third = step >= 3
            ? ValidTimeMoveTestSupport.objectKey(reader, 3)
            : -1;
        final long fourth = step >= 5
            ? ValidTimeMoveTestSupport.objectKey(reader, 4)
            : -1;
        final long[] rootKeys = step == 1
            ? new long[0]
            : step >= 3
                ? new long[] {keys.record(), third}
                : new long[] {keys.record()};
        final long[] destinationKeys = step == 1
            ? new long[] {keys.record()}
            : step >= 5
                ? new long[] {fourth}
                : new long[0];
        assertArrayScope(chain, context, collection, reader, keys.root(), rootKeys,
            step >= 3 || (mode.equals("right") && step >= 2));
        assertArrayScope(chain, context, collection, reader, keys.destination(), destinationKeys,
            mode.equals("right") && step >= 1);
        final List<Long> closed = step == 1
            ? List.of()
            : step >= 3
                ? List.of(1L, 3L)
                : List.of(1L);
        final List<Long> documentOrder = step == 1
            ? List.of()
            : step >= 3
                ? List.of(3L, 1L)
                : List.of(1L);
        for (final String resource : List.of("indexed", "plain")) {
          final var represented = requireNonNull(collection.getDocument(resource, revision));
          final String transaction = "xs:dateTime('" + represented.getTrx().getRevisionTimestamp() + "')";
          final String source = "jn:open-bitemporal('moves','" + resource + "'," + transaction + "," + POINT + ")";
          final List<Long> expected = resource.equals("indexed")
              ? closed
              : documentOrder;
          assertDemand(chain, context, source, expected);
          for (final boolean start : List.of(false, true)) {
            for (final boolean strict : List.of(false, true)) {
              for (final boolean general : List.of(false, true)) {
                for (final boolean mirror : List.of(false, true)) {
                  final String bound = "xs:dateTime($x." + (start
                      ? "vf"
                      : "vt") + ")";
                  final String operator = general
                      ? (strict
                          ? "<"
                          : "<=")
                      : (strict
                          ? "lt"
                          : "le");
                  final String swapped = general
                      ? (strict
                          ? ">"
                          : ">=")
                      : (strict
                          ? "gt"
                          : "ge");
                  final String predicate = start != mirror
                      ? bound + " " + (mirror
                          ? swapped
                          : operator) + " " + POINT
                      : POINT + " " + (mirror
                          ? swapped
                          : operator) + " " + bound;
                  assertDemand(chain, context, "for $x in " + source + " where " + predicate + " return $x", expected);
                }
              }
            }
          }
          final String from = "jn:doc('moves','" + resource + "'," + revision + ")";
          final String comparisons = "xs:dateTime($x.vf) le " + POINT + " and " + POINT + " lt xs:dateTime($x.vt)";
          assertEquals(documentOrder,
              ids(new Query(chain, "for $x in " + from + "[] where " + comparisons + " return $x").execute(context)));
          assertEquals(List.of(1L, 3L), ids(requireNonNull(
              new Query(chain, "jn:valid-at('moves','" + resource + "'," + POINT + ")").execute(context))).stream()
                                                                                                          .sorted()
                                                                                                          .toList());
        }
        final var plainDocument = requireNonNull(collection.getDocument("plain", revision));
        final var plainKeys =
            ValidTimeMoveTestSupport.keys(requireNonNull(collection.getDocument("plain", 1)).getTrx());
        final var plainReader = plainDocument.getTrx();
        assertEquals(closed,
            ids(ValidTimeFilter.linearScanSequence(plainDocument, INSTANT,
                requireNonNull(plainDocument.getResourceSession().getResourceConfig().getValidTimeConfig()))).stream()
                                                                                                             .sorted()
                                                                                                             .toList());
        assertTrue(plainReader.moveTo(plainKeys.destination()));
        final var plainDestination = (JsonDBItem) JsonItemFactory.INSTANCE.getSequence(plainReader, collection);
        final List<Long> destination = step == 1
            ? List.of(1L)
            : step >= 5
                ? List.of(4L)
                : List.of();
        assertEquals(destination, ids(ValidTimeFilter.linearScanSequence(plainDestination, INSTANT,
            requireNonNull(plainDestination.getResourceSession().getResourceConfig().getValidTimeConfig()))));
      }
      Databases.clearGlobalCaches();
    }
  }

  private static void assertArrayScope(final SirixCompileChain chain, final SirixQueryContext context,
      final JsonDBCollection collection, final JsonNodeReadOnlyTrx reader, final long array, final long[] expected,
      final boolean unordered) throws Exception {
    assertTrue(reader.moveTo(array));
    final var document = (JsonDBItem) JsonItemFactory.INSTANCE.getSequence(reader, collection);
    final var capture = POSTINGS.call(() -> ValidTimeIntervalIndex.keys(document, INSTANT, false));
    assertArrayEquals(expected, capture.result());
    capture.work()
           .assertExactly(EngineWorkCounters.VALID_TIME_POSTING_REFS, expected.length + 1 + (unordered
               ? 1
               : 0), "moved-array membership, duplicate verification and the persisted order marker");
    assertArrayEquals(expected, ValidTimeIntervalIndex.keys(document, INSTANT, true));
    final Sequence sequence = requireNonNull(ValidTimeIntervalIndex.sequence(document, INSTANT,
        requireNonNull(document.getResourceSession().getResourceConfig().getValidTimeConfig()), false, false, null));
    assertInstanceOf(ValidTimeKeySequence.class, sequence);
    final List<Long> expectedIds = new ArrayList<>();
    for (final long key : expected) {
      assertTrue(reader.moveTo(key));
      expectedIds.add(id((Item) JsonItemFactory.INSTANCE.getSequence(reader, collection)));
    }
    assertEquals(expected.length, sequence.size().longValue());
    assertEquals(expectedIds, ids(sequence));
    if (expected.length == 0) {
      assertNull(sequence.get(Int32.ONE));
    } else {
      assertEquals(expectedIds.getFirst().longValue(), id(requireNonNull(sequence.get(Int32.ONE))));
    }
    context.bind(new QNm("scope"), document);
    assertDemand(chain, context, "declare variable $scope external; jn:scan-valid-time-index($scope," + POINT + ")",
        expectedIds);
  }

  private static void assertDemand(final SirixCompileChain chain, final SirixQueryContext context, final String source,
      final List<Long> expected) {
    final @Nullable Sequence sequence = new Query(chain, source).execute(context);
    assertEquals(expected, ids(sequence), source);
    if (sequence == null) {
      assertTrue(expected.isEmpty(), source);
    } else {
      assertEquals(expected.size(), sequence.size().longValue(), source);
      if (expected.isEmpty()) {
        assertNull(sequence.get(Int32.ONE));
      } else {
        assertEquals(expected.getFirst().longValue(), id(requireNonNull(sequence.get(Int32.ONE))), source);
        assertEquals(expected.getLast().longValue(), id(requireNonNull(sequence.get(new Int32(expected.size())))),
            source);
        assertNull(sequence.get(new Int32(expected.size() + 1)));
      }
    }
    final int split = source.indexOf(';');
    final String declarations = split < 0
        ? ""
        : source.substring(0, split + 1);
    final String expression = source.substring(split + 1);
    assertEquals(expected.size(), ((Numeric) requireNonNull(
        new Query(chain, declarations + "count(" + expression + ")").execute(context))).intValue(), source);
    assertEquals(!expected.isEmpty(), ((Bool) requireNonNull(
        new Query(chain, declarations + "exists(" + expression + ")").execute(context))).booleanValue(), source);
  }

  private static List<Long> ids(final @Nullable Sequence sequence) {
    final List<Long> ids = new ArrayList<>();
    if (sequence == null) {
      return ids;
    }
    try (Iter iterator = sequence.iterate()) {
      Item item;
      while ((item = iterator.next()) != null) {
        ids.add(id(item));
      }
    }
    return ids;
  }

  private static long id(final Item item) {
    return ((Numeric) requireNonNull(((Object) item).get(new QNm("id")))).longValue();
  }
}
