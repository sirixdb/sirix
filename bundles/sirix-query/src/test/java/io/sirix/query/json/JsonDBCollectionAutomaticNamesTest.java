package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.MutationGate;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.service.json.shredder.JsonShredder;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Several JSON items added to an existing collection get automatic names that are always new
 * (#1478): an existing resource is never written into, whatever names the collection already holds
 * and whatever a concurrent caller creates meanwhile.
 */
final class JsonDBCollectionAutomaticNamesTest {
  @TempDir
  Path directory;

  /**
   * {@code resource2} and {@code resource21} exist, so the former naming - the size, then the same
   * name plus {@code 1} - resolved to an existing resource.
   */
  @ParameterizedTest
  @ValueSource(strings = {"jn:store", "jn:load", "addAll"})
  void automaticNamesSkipEveryExistingName(final String entryPoint) throws IOException {
    final Path first = Files.writeString(directory.resolve("first.json"), "{\"v\":\"a\"}");
    final Path second = Files.writeString(directory.resolve("second.json"), "{\"v\":\"b\"}");
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("db", "resource2", "{\"v\":\"kept\"}");
      run(chain, context, "jn:store('db','resource21','{\"v\":\"kept\"}',false())");
      switch (entryPoint) {
        case "jn:store" -> run(chain, context, "jn:store('db',(),('{\"v\":\"a\"}','{\"v\":\"b\"}'),false())");
        case "jn:load" ->
          run(chain, context, "jn:load('db',(),('" + first.toUri() + "','" + second.toUri() + "'),false())");
        case "addAll" -> collection.addAll(List.of(reader("{\"v\":\"a\"}"), reader("{\"v\":\"b\"}")),
            new ArrayObject(new QNm[0], new Sequence[0]));
        default -> throw new IllegalArgumentException(entryPoint);
      }
      assertEquals(List.of("resource2", "resource21", "resource3", "resource4"), resources(store));
      for (final String kept : List.of("resource2", "resource21")) {
        assertEquals(1, revisions(store, kept), kept);
        assertEquals("{\"v\":\"kept\"}", run(chain, context, "jn:doc('db','" + kept + "')"));
      }
      assertEquals("{\"v\":\"a\"}", run(chain, context, "jn:doc('db','resource3')"));
      assertEquals("{\"v\":\"b\"}", run(chain, context, "jn:doc('db','resource4')"));
    }
  }

  /** A single unnamed item also takes a free name: {@code resource3} (size + 1) exists here. */
  @Test
  void aSingleUnnamedItemTakesAFreeName() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("db", "resource1", "{\"v\":\"kept\"}");
      run(chain, context, "jn:store('db','resource3','{\"v\":\"kept\"}',false())");
      collection.add("{\"v\":\"added\"}");
      assertEquals(List.of("resource1", "resource3", "resource4"), resources(store));
      assertEquals(1, revisions(store, "resource3"));
      assertEquals("{\"v\":\"kept\"}", run(chain, context, "jn:doc('db','resource3')"));
      assertEquals("{\"v\":\"added\"}", run(chain, context, "jn:doc('db','resource4')"));
    }
  }

  /** A name another caller creates while this one waits for it is skipped, not written into. */
  @Test
  void aNameCreatedConcurrentlyIsSkipped() throws Exception {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("db", "resource1", "{\"v\":\"first\"}");
      final Path databaseFile = collection.getDatabase().getDatabaseConfig().getDatabaseFile();
      final Thread caller;
      // resource1 exists, so the caller's first candidate is resource1 and its second resource2.
      try (var gate = MutationGate.acquireResource(databaseFile, "resource2")) {
        caller = new Thread(() -> collection.addAll(List.of(reader("{\"v\":\"added\"}")),
            new ArrayObject(new QNm[0], new Sequence[0])));
        caller.start();
        while (caller.getState() != Thread.State.WAITING && caller.isAlive()) {
          Thread.onSpinWait();
        }
        collection.add("resource2", reader("{\"v\":\"concurrent\"}"));
      }
      caller.join();
      assertEquals(List.of("resource1", "resource2", "resource3"), resources(store));
      assertEquals(1, revisions(store, "resource2"));
      assertEquals("{\"v\":\"concurrent\"}", run(chain, context, "jn:doc('db','resource2')"));
      assertEquals("{\"v\":\"added\"}", run(chain, context, "jn:doc('db','resource3')"));
    }
  }

  private static JsonReader reader(final String json) {
    return JsonShredder.createStringReader(json);
  }

  private static List<String> resources(final BasicJsonDBStore store) {
    return store.lookup("db")
                .getDatabase()
                .listResources()
                .stream()
                .map(path -> path.getFileName().toString())
                .sorted()
                .toList();
  }

  private static int revisions(final BasicJsonDBStore store, final String resource) {
    final JsonResourceSession session = store.lookup("db").getDatabase().beginResourceSession(resource);
    return session.getMostRecentRevisionNumber();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(context, writer);
    }
    return out.toString();
  }
}
