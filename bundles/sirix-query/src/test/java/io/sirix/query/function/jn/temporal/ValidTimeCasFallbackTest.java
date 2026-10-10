package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.util.serialize.SubtreePrinter;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeCasFallbackTest {
  private static final String POINT = "xs:dateTime('2024-01-01T00:00:00Z')";
  private static final Instant INSTANT = Instant.parse("2024-01-01T00:00:00Z");

  @TempDir
  Path directory;

  private record Resource(Path catalogues, List<Instant> revisionTimes) {
  }

  @ParameterizedTest
  @ValueSource(strings = {"vf", "vt", "both"})
  void obsoleteAutoIndexesRetainClosedRowsAndDemandTimeCastErrors(final String endpoint) throws Exception {
    final Resource resource = createObsoleteAutoIndexes(endpoint);
    final byte[] firstCatalogue = Files.readAllBytes(resource.catalogues().resolve("1.xml"));
    assertFalse(Files.exists(resource.catalogues().resolve("2.xml")));
    final QNm castError = assertThrows(QueryException.class, () -> new DateTime("2023-01-01T00:00:00.Z")).getCode();
    final long[] history;
    try (var database = Databases.openJsonDatabase(directory.resolve("coverage"));
        var session = database.beginResourceSession("rows")) {
      assertEquals(2, session.getMostRecentRevisionNumber());
      history = session.getHistoryTimestamps();
    }

    final List<String> sources = new ArrayList<>();
    for (final Instant time : resource.revisionTimes()) {
      sources.add("jn:open-bitemporal('coverage','rows',xs:dateTime('" + time + "')," + POINT + ")");
    }
    sources.add("jn:valid-at('coverage','rows'," + POINT + ")");
    for (final int revision : List.of(1, 2)) {
      sources.add("jn:scan-valid-time-index(jn:doc('coverage','rows'," + revision + ")," + POINT + ")");
    }
    for (final String firstSource : sources) {
      Databases.clearGlobalCaches();
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var context = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        assertClosed(chain, context, firstSource);
        final var collection = requireNonNull(store.lookup("coverage"));
        final var session = collection.getDatabase().beginResourceSession("rows");
        for (final int revision : List.of(1, 2)) {
          final var controller = session.getRtxIndexController(revision);
          assertEquals(2, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS));
          assertEquals(0, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.VALIDTIME));
          final var document = requireNonNull(collection.getDocument("rows", revision));
          assertNull(ValidTimeIntervalIndex.sequence(document, INSTANT,
              requireNonNull(session.getResourceConfig().getValidTimeConfig()), false, false));
        }
        assertComparisons(chain, context, firstSource, endpoint, castError);
        final String outsidePoint = "xs:dateTime('2022-01-01T00:00:00Z')";
        final String outsideSource = firstSource.replace(POINT, outsidePoint);
        for (final String field : List.of("vf", "vt")) {
          final String comparison = field.equals("vf")
              ? "xs:dateTime($x.vf) lt " + outsidePoint
              : outsidePoint + " lt xs:dateTime($x.vt)";
          final String query = "count(for $x in " + outsideSource + " where " + comparison + " return $x)";
          assertEquals(0, ((Numeric) requireNonNull(new Query(chain, query).execute(context))).longValue(), query);
        }
        if (firstSource.equals(sources.get(2))) {
          for (final int revision : List.of(1, 2)) {
            assertComparisons(chain, context, "jn:doc('coverage','rows'," + revision + ")[]", endpoint, castError);
            for (final boolean upperFirst : List.of(false, true)) {
              final String lower = "xs:dateTime($x.vf) le " + POINT;
              final String upper = POINT + " lt xs:dateTime($x.vt)";
              final String rows = "for $x in jn:doc('coverage','rows'," + revision + ")[] where " + (upperFirst
                  ? upper + " and " + lower
                  : lower + " and " + upper) + " return $x";
              assertCastOnSecondDemand(chain, context, rows, castError);
            }
          }
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        assertArrayEquals(history, session.getHistoryTimestamps());
        assertArrayEquals(firstCatalogue, Files.readAllBytes(resource.catalogues().resolve("1.xml")));
        assertFalse(Files.exists(resource.catalogues().resolve("2.xml")));
        assertFalse(Files.exists(resource.catalogues().resolve("3.xml")));
      }
    }
  }

  private Resource createObsoleteAutoIndexes(final String endpoint) throws Exception {
    final String from = endpoint.equals("vt")
        ? "2023-01-01T00:00:00Z"
        : "2023-01-01T00:00:00.Z";
    final String to = endpoint.equals("vf")
        ? "2025-01-01T00:00:00Z"
        : "2025-01-01T00:00:00.Z";
    final String json = "[{\"id\":1,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
        + "{\"id\":2,\"vf\":\"" + from + "\",\"vt\":\"" + to + "\"},"
        + "{\"id\":3,\"vf\":\"2030-01-01T00:00:00Z\",\"vt\":\"2031-01-01T00:00:00Z\"}]";
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('coverage','rows','" + json
          + "',true(),{\"validFromPath\":\"vf\",\"validToPath\":\"vt\"})").evaluate(context);
    }
    final Path catalogues;
    final List<Instant> times = new ArrayList<>();
    try (var database = Databases.openJsonDatabase(directory.resolve("coverage"));
        var session = database.beginResourceSession("rows")) {
      try (var writer = session.beginNodeTrx()) {
        writer.commit();
      }
      for (final int revision : List.of(1, 2)) {
        final var indexes = session.getRtxIndexController(revision).getIndexes();
        assertEquals(2, indexes.getNrOfIndexDefsWithType(IndexType.CAS));
        assertEquals(1, indexes.getNrOfIndexDefsWithType(IndexType.VALIDTIME));
        final Set<String> fields = new TreeSet<>();
        for (final var definition : indexes.getIndexDefs()) {
          if (definition.isCasIndex()) {
            assertEquals(Type.DATI, definition.getContentType());
            for (final var path : definition.getPaths()) {
              fields.add(requireNonNull(path.tail()).getLocalName());
            }
          }
        }
        assertEquals(Set.of("vf", "vt"), fields);
        try (var reader = session.beginNodeReadOnlyTrx(revision)) {
          times.add(reader.getRevisionTimestamp());
        }
      }
      catalogues =
          session.getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    }
    // Revision 2 inherits the unchanged catalogue from revision 1.
    final Path catalogue = catalogues.resolve("1.xml");
    final Node<?> persisted;
    try (var input = Files.newInputStream(catalogue)) {
      persisted = requireNonNull(IndexController.deserialize(input).getFirstChild());
    }
    boolean changed = false;
    final QNm formatName = new QNm("validTimeFormat");
    for (Node<?> definition = persisted.getFirstChild(); definition != null; definition =
        definition.getNextSibling()) {
      if (definition.getAttribute(formatName) != null) {
        definition.deleteAttribute(formatName);
        definition.setAttribute(formatName, new Str("5"));
        changed = true;
      }
    }
    assertTrue(changed);
    try (var output = new PrintStream(Files.newOutputStream(catalogue))) {
      final SubtreePrinter printer = new SubtreePrinter(output);
      printer.print(persisted);
      printer.end();
    }
    Databases.clearGlobalCaches();
    return new Resource(catalogues, times);
  }

  private static void assertClosed(final SirixCompileChain chain, final SirixQueryContext context,
      final String source) {
    final Sequence rows = requireNonNull(new Query(chain, source).execute(context));
    assertEquals(2, rows.size().longValue(), source);
    assertEquals(1, id(requireNonNull(rows.get(Int32.ONE))), source);
    assertEquals(List.of(1L, 2L), ids(rows), source);
    assertEquals(2, ((Numeric) requireNonNull(new Query(chain, "count(" + source + ")").execute(context))).longValue(),
        source);
  }

  private static void assertComparisons(final SirixCompileChain chain, final SirixQueryContext context,
      final String source, final String endpoint, final QNm castError) {
    for (final String field : List.of("vf", "vt")) {
      for (final boolean strict : List.of(false, true)) {
        for (final boolean general : List.of(false, true)) {
          for (final boolean mirror : List.of(false, true)) {
            final String predicate = comparisonPredicate(field, strict, general, mirror);
            final String rows = "for $x in " + source + " where " + predicate + " return $x";
            if (endpoint.equals("both") || endpoint.equals(field)) {
              assertCastOnSecondDemand(chain, context, rows, castError);
            } else {
              final List<Long> expected = source.endsWith("[]") && field.equals("vt")
                  ? List.of(1L, 2L, 3L)
                  : List.of(1L, 2L);
              assertEquals(expected, ids(requireNonNull(new Query(chain, rows).execute(context))), rows);
            }
          }
        }
      }
    }
  }

  private static String comparisonPredicate(final String field, final boolean strict, final boolean general,
      final boolean mirror) {
    final boolean fieldOnLeft = field.equals("vf") != mirror;
    final String operator = general
        ? (strict
            ? "<"
            : "<=")
        : (strict
            ? "lt"
            : "le");
    final String reversed = general
        ? (strict
            ? ">"
            : ">=")
        : (strict
            ? "gt"
            : "ge");
    final String bound = "xs:dateTime($x." + field + ")";
    return fieldOnLeft
        ? bound + " " + (mirror
            ? reversed
            : operator) + " " + POINT
        : POINT + " " + (mirror
            ? reversed
            : operator) + " " + bound;
  }

  private static void assertCastOnSecondDemand(final SirixCompileChain chain, final SirixQueryContext context,
      final String query, final QNm castError) {
    final Sequence rows = requireNonNull(new Query(chain, query).execute(context));
    assertEquals(1, id(requireNonNull(rows.get(Int32.ONE))), query);
    assertEquals(1,
        id(requireNonNull(
            requireNonNull(new Query(chain, "subsequence((" + query + "),1,1)").execute(context)).get(Int32.ONE))),
        query);
    assertEquals(castError, assertThrows(QueryException.class, () -> rows.get(new Int32(2)), query).getCode());
    assertEquals(castError, assertThrows(QueryException.class, rows::size, query).getCode());
    assertEquals(castError, assertThrows(QueryException.class,
        () -> new Query(chain, "count(" + query + ")").execute(context), query).getCode());
    try (Iter iterator = rows.iterate()) {
      assertEquals(1, id(requireNonNull(iterator.next())), query);
      assertEquals(castError, assertThrows(QueryException.class, iterator::next, query).getCode());
    }
  }

  private static List<Long> ids(final Sequence sequence) {
    final List<Long> ids = new ArrayList<>();
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
