package io.sirix.query.json;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Omitted counts must have the same observable semantics as stored counts after a cold reopen. */
final class UnstoredChildCountTest {
  private static final String NUMBERS = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]";

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void arrayAccessAndQueries(final VersioningType versioning) {
    create(versioning, NUMBERS);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("counts");
      final JsonDBArray stored = (JsonDBArray) collection.getDocument("stored");
      final JsonDBArray omitted = (JsonDBArray) collection.getDocument("omitted");
      // Probe before len(): unknown does not mean empty, and at() need not count first.
      assertEquals("23", serialize(omitted.at(23)));
      assertEquals(24, omitted.len());
      assertEquals(stored.length(), omitted.length());
      for (int i = 0; i < 24; i++) {
        assertEquals(Integer.toString(i), serialize(omitted.at(i)));
        assertEquals(serialize(stored.at(i)), serialize(omitted.at(new Int32(i))));
      }
      assertNull(omitted.at(-1));
      assertNull(omitted.at(24));
      final var slice = omitted.range(new Int32(3), new Int32(22));
      assertEquals(19, slice.len());
      assertEquals(19, slice.length().intValue());
      for (int i = 0; i < slice.len(); i++) {
        assertEquals(Integer.toString(i + 3), serialize(slice.at(i)));
      }
      assertEquals(24, omitted.values().size());
      for (final String suffix : new String[] {"[]", "[0]", "[23]", "[-1]"}) {
        assertEquals(query(chain, context, "jn:doc('counts','stored')" + suffix),
            query(chain, context, "jn:doc('counts','omitted')" + suffix));
      }
      assertEquals("24", query(chain, context, "count(jn:doc('counts','omitted')[])"));
      assertEquals("24", query(chain, context, "sdb:child-count(jn:doc('counts','omitted'))"));
      assertEquals("276", query(chain, context, "sum(jn:doc('counts','omitted')[])"));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void lengthIsCachedWithoutMaterializingOrMovingTheCursor(final VersioningType versioning) {
    create(versioning, NUMBERS);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.lookup("counts");
      for (final String resource : new String[] {"stored", "omitted"}) {
        try (var session = collection.getDatabase().beginResourceSession(resource);
            var reader = session.beginNodeReadOnlyTrx()) {
          reader.moveToFirstChild();
          final JsonNodeReadOnlyTrx observed = mock(JsonNodeReadOnlyTrx.class, delegatesTo(reader));
          final JsonDBArray array = new JsonDBArray(observed, collection);
          verify(observed, never()).moveToFirstChild();
          verify(observed, never()).moveToRightSibling();
          assertEquals(24, array.len());
          // Leave the shared cursor on an element, then require both size APIs to preserve it.
          assertEquals("10", serialize(array.at(10)));
          clearInvocations(observed);
          for (int i = 0; i < 24; i++) {
            assertEquals(24, array.len());
            assertEquals(24, array.length().intValue());
          }
          verify(observed, never()).moveTo(anyLong());
          verify(observed, never()).moveToFirstChild();
          verify(observed, never()).moveToRightSibling();
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void objectNamesNestedArraysAndEmptyContainers(final VersioningType versioning) {
    create(versioning, "{\"numbers\":" + NUMBERS + ",\"empty\":[],\"object\":{}}");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("counts");
      final JsonDBObject object = (JsonDBObject) collection.getDocument("omitted");
      assertEquals(3, object.len());
      assertEquals(3, object.length().intValue());
      final var names = new JsonObjectKeyDBArray(object.getTrx(), collection);
      assertEquals(3, names.len());
      assertEquals("\"numbers\"", serialize(names.at(0)));
      assertEquals(3, names.values().size());
      final var empty = (JsonDBArray) object.get(new QNm("empty"));
      assertEquals(0, empty.len());
      assertEquals(0, empty.length().intValue());
      assertNull(empty.at(0));
      assertEquals(0, ((JsonDBObject) object.get(new QNm("object"))).len());
      for (final String suffix : new String[] {".numbers[]", ".numbers[-1]", "=>>numbers[]", ".empty[]"}) {
        assertEquals(query(chain, context, "jn:doc('counts','stored')" + suffix),
            query(chain, context, "jn:doc('counts','omitted')" + suffix));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void writerCountsFollowEditsThroughOtherWrappers(final VersioningType versioning) {
    create(versioning, "[1,2,3]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.lookup("counts");
      try (var session = collection.getDatabase().beginResourceSession("omitted");
          var writer = session.beginNodeTrx()) {
        writer.moveToFirstChild();
        final JsonDBArray array = new JsonDBArray(writer, collection);
        final JsonDBArray other = new JsonDBArray(writer, collection);
        assertEquals(3, array.len());
        other.insert(3, new Int32(4));
        assertEquals(4, array.len());
        other.replaceAt(0, new Int32(9));
        assertEquals("9", serialize(array.at(0)));
        other.remove(1);
        assertEquals(3, array.length().intValue());
        assertEquals("3", serialize(array.at(1)));
        assertThrows(QueryException.class, () -> other.insert(4, new Int32(0)));
        writer.rollback();
      }
    }
  }

  private void create(final VersioningType versioning, final String json) {
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.create("counts");
      for (final boolean stored : new boolean[] {true, false}) {
        final String resource = stored
            ? "stored"
            : "omitted";
        collection.getDatabase()
                  .createResource(ResourceConfiguration.newBuilder(resource)
                                                       .storageType(StorageType.FILE_CHANNEL)
                                                       .versioningApproach(versioning)
                                                       .storeChildCount(stored)
                                                       .buildPathSummary(false)
                                                       .storeDiffs(false)
                                                       .build());
        try (var session = collection.getDatabase().beginResourceSession(resource);
            var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
      }
    }
  }

  private static String query(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    return serialize(new Query(chain, text).execute(context));
  }

  private static String serialize(final Sequence sequence) {
    final StringWriter output = new StringWriter();
    try (var writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(sequence);
    }
    return output.toString();
  }
}
