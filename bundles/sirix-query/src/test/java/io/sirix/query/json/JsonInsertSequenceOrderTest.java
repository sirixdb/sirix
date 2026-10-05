package io.sirix.query.json;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** JSON sequences inserted as array members must retain the order inside the new member. */
final class JsonInsertSequenceOrderTest {
  private static final String SOURCE = "jn:doc('order','resource')";
  private static final String ITEMS = "(1, {\"id\":2}, [3,4], \"five\")";
  private static final String MEMBER = "[1,{\"id\":2},[3,4],\"five\"]";

  @TempDir
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(VersioningType.values())
                 .flatMap(versioning -> Stream.concat(
                     Stream.of(-1, 0, 1, 2).map(position -> Arguments.of(versioning, "[0,9]", position)),
                     Stream.of(-1, 0).map(position -> Arguments.of(versioning, "[]", position))));
  }

  @ParameterizedTest(name = "{0} {1} position {2}")
  @MethodSource("cases")
  void xqueryUpdatePreservesOrder(final VersioningType versioning, final String initial, final int position) {
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var context = SirixQueryContext.createWithJsonStore(store)) {
      store.create("order", "resource", initial);
      new Query(chain, "insert json " + ITEMS + " into " + SOURCE + (position == -1
          ? ""
          : " at position " + position)).evaluate(context);
      assertEquals(expected(initial, position), serialize(chain, context, SOURCE));
    }
    assertReopened(versioning, initial, position);
  }

  @ParameterizedTest(name = "{0} {1} position {2}")
  @MethodSource("cases")
  void directTransactionBindingPreservesOrder(final VersioningType versioning, final String initial,
      final int position) {
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var context = SirixQueryContext.createWithJsonStore(store)) {
      final var collection = store.create("order", "resource", initial);
      final var session = ((JsonDBArray) collection.getDocument("resource")).getResourceSession();
      try (final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        final JsonDBArray array = new JsonDBArray(trx, collection);
        final Sequence member = new Query(chain, "[1, {\"id\":2}, [3,4], \"five\"]").evaluate(context);
        if (position == -1) {
          array.append(member);
        } else {
          array.insert(position, member);
        }
        assertEquals(expected(initial, position), serialize(array));
        trx.commit();
      }
    }
    assertReopened(versioning, initial, position);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void invalidReplacementBoundsPreservePendingEdits(final VersioningType versioning) {
    try (final var store = openStore(versioning)) {
      final var collection = store.create("order", "resource", "[0,9]");
      final var session = ((JsonDBArray) collection.getDocument("resource")).getResourceSession();
      try (final JsonNodeTrx trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        final JsonDBArray array = new JsonDBArray(trx, collection);
        array.append(new Int32(11));
        assertThrows(QueryException.class, () -> array.replaceAt(3, new Int32(99)));
        assertThrows(QueryException.class, () -> array.replaceAt(-1, new Int32(99)));
        assertThrows(QueryException.class, () -> array.insert(-1, new Int32(99)));
        assertThrows(QueryException.class, () -> array.insert(4, new Int32(99)));
        assertEquals("[0,9,11]", serialize(array));
        trx.commit();
      }
    }
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var context = SirixQueryContext.createWithJsonStore(store)) {
      assertEquals("[0,9,11]", serialize(chain, context, "jn:doc('order','resource',2)"));
      assertEquals("[0,9]", serialize(chain, context, "jn:doc('order','resource',1)"));
    }
  }

  private static String expected(final String initial, final int position) {
    if (initial.equals("[]")) {
      return "[" + MEMBER + "]";
    }
    return switch (position) {
      case 0 -> "[" + MEMBER + ",0,9]";
      case 1 -> "[0," + MEMBER + ",9]";
      case -1, 2 -> "[0,9," + MEMBER + "]";
      default -> throw new IllegalArgumentException("Unsupported position: " + position);
    };
  }

  private BasicJsonDBStore openStore(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }

  private void assertReopened(final VersioningType versioning, final String initial, final int position) {
    try (final var store = openStore(versioning);
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var context = SirixQueryContext.createWithJsonStore(store)) {
      assertEquals(expected(initial, position), serialize(chain, context, SOURCE));
      assertEquals(expected(initial, position), serialize(chain, context, "jn:doc('order','resource',2)"));
      assertEquals(initial, serialize(chain, context, "jn:doc('order','resource',1)"));
    }
  }

  private static String serialize(final SirixCompileChain chain, final SirixQueryContext context,
      final String expression) {
    return serialize(new Query(chain, expression).evaluate(context));
  }

  private static String serialize(final Sequence sequence) {
    final StringWriter output = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(sequence);
    }
    return output.toString();
  }
}
