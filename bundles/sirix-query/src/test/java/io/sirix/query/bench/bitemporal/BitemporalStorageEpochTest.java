package io.sirix.query.bench.bitemporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.trx.page.StorageEpochTestSupport;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.query.bench.bitemporal.BitemporalSchema.Event;
import io.sirix.query.bench.bitemporal.BitemporalSchema.Tier;
import io.sirix.service.json.serialize.JsonSerializer;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/** The real SH1 interval-edit stream, with default hashing and versioning on plain transactions. */
@Isolated
final class BitemporalStorageEpochTest {
  private static final List<String> RESOURCES =
      List.of(BitemporalSchema.CONTRACTS, BitemporalSchema.PRODUCTS, BitemporalSchema.SUPPLIERS);

  @TempDir
  Path directory;

  @AfterEach
  void restore() {
    StorageEpochTestSupport.setPlainTransactionBound(0);
    Databases.clearGlobalCaches();
  }

  @Test
  void allPublicationsMatchTheUnboundedTwinAfterColdReopen() throws Exception {
    final List<Event>[] events = generateEvents(directory.resolve("events.jsonl"));

    final Path plain = directory.resolve("plain");
    assertEquals(0, load(plain, events, Integer.MAX_VALUE));
    final List<String> expected = coldPublications(plain);
    final Path bounded = directory.resolve("bounded");
    assertTrue(load(bounded, events, StorageEpochTestSupport.IMPORT_BOUND) > 0);
    assertEquals(expected, coldPublications(bounded), "every resource at every publication");
  }

  static List<Event>[] generateEvents(final Path eventsPath) throws Exception {
    // Exercise the generator/loader internals without their command-line campaign path restriction.
    final Class<?> generatorType = nestedClass(BitemporalGenerateMain.class, "Generator");
    final Constructor<?> generatorConstructor = generatorType.getDeclaredConstructor(Tier.class, Path.class);
    generatorConstructor.setAccessible(true);
    invoke(method(generatorType, "generate"), generatorConstructor.newInstance(Tier.DEVELOPMENT, eventsPath));
    @SuppressWarnings("unchecked")
    final List<Event>[] events = (List<Event>[]) invoke(
        method(BitemporalSirixLoadMain.class, "readEvents", Path.class, Tier.class), null,
        eventsPath, Tier.DEVELOPMENT);
    assertEquals(4_776, List.of(events).stream().mapToInt(List::size).sum());
    return events;
  }

  static int load(final Path path, final List<Event>[] events, final int bound) throws Exception {
    StorageEpochTestSupport.setPlainTransactionBound(bound);
    final Class<?> stateType = nestedClass(BitemporalSirixLoadMain.class, "RelationState");
    final Constructor<?> stateConstructor = stateType.getDeclaredConstructor(String.class, int.class);
    stateConstructor.setAccessible(true);
    final Method loadInitial = method(BitemporalSirixLoadMain.class, "loadInitial",
        Database.class, stateType, List.class);
    final Method apply = method(stateType, "apply", JsonNodeTrx.class, Event.class);
    final int[] counts = {Tier.DEVELOPMENT.contracts(), Tier.DEVELOPMENT.products(), Tier.DEVELOPMENT.suppliers()};
    int rotations = 0;
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      invoke(method(BitemporalSirixLoadMain.class, "createResources", Database.class), null, database);
      for (int resource = 0; resource < RESOURCES.size(); resource++) {
        final String name = RESOURCES.get(resource);
        final Object state = stateConstructor.newInstance(name, counts[resource]);
        invoke(loadInitial, null, database, state, filter(events[0], name));
        for (int epoch = 1; epoch < BitemporalSchema.PUBLICATIONS; epoch++) {
          try (final var session = database.beginResourceSession(name); final var wtx = session.beginNodeTrx()) {
            for (final Event event : filter(events[epoch], name)) {
              invoke(apply, state, wtx, event);
            }
            rotations += StorageEpochTestSupport.rotatedEpochs(wtx);
            wtx.commit("SH1 E" + epoch, BitemporalSchema.systemTime(epoch));
          }
        }
      }
    }
    return rotations;
  }

  private static List<Event> filter(final List<Event> events, final String resource) {
    return events.stream().filter(event -> event.table().equals(resource)).toList();
  }

  static List<String> coldPublications(final Path path) throws Exception {
    Databases.clearGlobalCaches();
    final List<String> digests = new ArrayList<>(RESOURCES.size() * BitemporalSchema.PUBLICATIONS);
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (final var database = Databases.openJsonDatabase(path)) {
      for (final String resource : RESOURCES) {
        try (final JsonResourceSession session = database.beginResourceSession(resource)) {
          assertEquals(BitemporalSchema.PUBLICATIONS, session.getMostRecentRevisionNumber());
          for (int epoch = 0; epoch < BitemporalSchema.PUBLICATIONS; epoch++) {
            final StringWriter json = new StringWriter();
            JsonSerializer.newBuilder(session, json, epoch + 1).build().call();
            digests.add(resource + '/' + epoch + ':'
                + HexFormat.of().formatHex(digest.digest(json.toString().getBytes(StandardCharsets.UTF_8))));
          }
        }
      }
    }
    return digests;
  }

  private static Class<?> nestedClass(final Class<?> owner, final String name) {
    for (final Class<?> nested : owner.getDeclaredClasses()) {
      if (nested.getSimpleName().equals(name)) {
        return nested;
      }
    }
    throw new IllegalArgumentException("Missing nested class " + name);
  }

  private static Method method(final Class<?> owner, final String name, final Class<?>... parameters)
      throws NoSuchMethodException {
    final Method method = owner.getDeclaredMethod(name, parameters);
    method.setAccessible(true);
    return method;
  }

  private static Object invoke(final Method method, final Object receiver, final Object... arguments) throws Exception {
    try {
      return method.invoke(receiver, arguments);
    } catch (final InvocationTargetException failure) {
      if (failure.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw failure;
    }
  }
}
