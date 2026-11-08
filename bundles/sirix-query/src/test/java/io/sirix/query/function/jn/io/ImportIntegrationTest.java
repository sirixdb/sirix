package io.sirix.query.function.jn.io;

import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.sirix.access.ResourceConfiguration;
import io.sirix.io.StorageType;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.serialize.JsonSerializer;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImportIntegrationTest {
  @TempDir
  Path directory;

  @Test
  void stringAndItemImportsPreserveIdentityAndHistory() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("versions", "doc", "{\"stable\":{\"id\":1},\"changed\":1,\"removed\":true}");
      final JsonDBItem stable = (JsonDBItem) new Query(chain, "jn:doc('versions','doc').stable").evaluate(context);
      final long key = stable.getNodeKey();
      final String next = "{\"stable\":{\"id\":1},\"changed\":2,\"added\":[1,2]}";
      final JsonDBItem imported = (JsonDBItem) new Query(chain,
          "jn:import('versions','doc','" + next + "')").evaluate(context);
      final var session = imported.getResourceSession();
      assertEquals(2, session.getMostRecentRevisionNumber());
      assertJson(next, session, 2);
      assertJson("{\"stable\":{\"id\":1},\"changed\":1,\"removed\":true}", session, 1);
      final JsonDBItem unchanged = (JsonDBItem) new Query(chain, "jn:doc('versions','doc').stable").evaluate(context);
      assertEquals(key, unchanged.getNodeKey());

      new Query(chain, "jn:import('versions','doc'," + next + ")").evaluate(context);
      assertEquals(3, session.getMostRecentRevisionNumber(), "Even an identical import commits exactly one revision");
      assertJson(next, session, 3);
    }
  }

  @Test
  void unchangedDescendantSurvivesReplacementOfItsUnmatchedParent() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("versions", "doc", "{\"outer\":{\"stable\":{\"id\":1},\"a\":2,\"b\":3}}");
      final JsonDBItem stable = (JsonDBItem) new Query(chain, "jn:doc('versions','doc').outer.stable").evaluate(context);
      final long key = stable.getNodeKey();
      final String next = "{\"outer\":{\"stable\":{\"id\":1},\"x\":4,\"y\":5,\"z\":6}}";
      new Query(chain, "jn:import('versions','doc','" + next + "')").evaluate(context);
      final JsonDBItem unchanged = (JsonDBItem) new Query(chain, "jn:doc('versions','doc').outer.stable").evaluate(context);
      assertEquals(key, unchanged.getNodeKey());
      assertJson(next, unchanged.getResourceSession(), 2);
    }
  }

  @Test
  void rejectsMissingResourcesAndInvalidContentWithoutMutation() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.create("versions", "doc", "[1,2,3]");
      final QueryException missing = assertThrows(QueryException.class,
          () -> new Query(chain, "jn:import('versions','missing',[4,5,6])").evaluate(context));
      assertTrue(missing.getMessage().contains("missing"));
      assertFalse(collection.getDatabase().existsResource("missing"));
      final JsonDBItem imported = collection.getDocument("doc");
      final var session = imported.getResourceSession();
      assertEquals(1, session.getMostRecentRevisionNumber());
      assertJson("[1,2,3]", session, 1);
      assertThrows(RuntimeException.class,
          () -> new Query(chain, "jn:import('versions','doc','{broken')").evaluate(context));
      assertEquals(1, session.getMostRecentRevisionNumber());
      assertJson("[1,2,3]", session, 1);
    }
  }

  @ParameterizedTest
  @CsvSource({
      "9007199254740992,9007199254740993",
      "18446744073709551616,18446744073709551617",
      "0.12345678901234567890,0.12345678901234567891"
  })
  void importsExactNumericPayloads(final String oldNumber, final String newNumber) {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String oldContent = "{\"n\":" + oldNumber + ",\"array\":[" + oldNumber + "]}";
      final String newContent = "{\"n\":" + newNumber + ",\"array\":[" + newNumber + "]}";
      store.create("versions", "doc", oldContent);
      final JsonDBItem imported = (JsonDBItem) new Query(chain,
          "jn:import('versions','doc','" + newContent + "')").evaluate(context);
      assertJsonText(newContent, imported.getResourceSession(), 2);
      assertJsonText(oldContent, imported.getResourceSession(), 1);
    }
  }

  @Test
  void renamesNullValuedFields() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("versions", "doc", "{\"field\":null}");
      final JsonDBItem imported = (JsonDBItem) new Query(chain,
          "jn:import('versions','doc','{\"fields\":null}')").evaluate(context);
      assertJsonText("{\"fields\":null}", imported.getResourceSession(), 2);
      assertJsonText("{\"field\":null}", imported.getResourceSession(), 1);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"123\"", "\"hello\"", "\"a\\\"b\\nc\\\\d\""})
  void storedStringItemsRemainJsonStrings(final String content) {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.create("versions", "doc", "{}");
      collection.getDatabase().createResource(ResourceConfiguration.newBuilder("source")
          .storageType(StorageType.FILE_CHANNEL).build());
      try (final var session = collection.getDatabase().beginResourceSession("source");
          final var writer = session.beginNodeTrx()) {
        writer.insertStringValueAsFirstChild(JsonParser.parseString(content).getAsString());
        writer.commit();
      }
      final JsonDBItem imported = (JsonDBItem) new Query(chain,
          "jn:import('versions','doc',jn:doc('versions','source'))").evaluate(context);
      assertJsonText(content, imported.getResourceSession(), 2);
      assertJsonText("{}", imported.getResourceSession(), 1);
    }
  }

  private static void assertJsonText(final String expected, final JsonResourceSession session,
      final int revision) {
    final StringBuilder output = new StringBuilder();
    new JsonSerializer.Builder(session, output, revision).build().call();
    assertEquals(expected, output.toString());
  }

  private static void assertJson(final String expected, final JsonResourceSession session,
      final int revision) {
    final StringBuilder output = new StringBuilder();
    new JsonSerializer.Builder(session, output, revision).build().call();
    assertEquals(JsonParser.parseString(expected), JsonParser.parseString(output.toString()));
  }
}
