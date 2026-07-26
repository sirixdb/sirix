/*
 * Copyright (c) 2024, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.service.json.serialize.JsonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Corner-case coverage for {@link ParallelJsonShredder}: ordering, the all-or-nothing rollback
 * contract, fail-fast collision detection, reader lifecycle, and argument validation. The happy-path
 * test also exercises real concurrency (many partitions, default pool).
 */
final class ParallelJsonShredderTest {

  private static final Path DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String BASE = "records";

  private static final Function<String, ResourceConfiguration> CONFIG =
      name -> ResourceConfiguration.newBuilder(name).build();

  private Database<JsonResourceSession> database;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DB_PATH));
    database = Databases.openJsonDatabase(DB_PATH);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.close();
    }
    JsonTestHelper.deleteEverything();
  }

  // ---------------------------------------------------------------------------------------------
  // Happy path + ordering
  // ---------------------------------------------------------------------------------------------

  @Test
  void shardsEachPartitionIntoItsOwnResourceInOrder() {
    final int n = 17; // > cores, so the pool actually queues/parallelises and order can't be luck
    final List<Callable<JsonReader>> parts = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      parts.add(readerOf("[{\"shard\":" + i + "}]"));
    }

    final List<String> names = ParallelJsonShredder.shredPartitioned(database, parts, BASE, CONFIG, 1024, 0);

    assertEquals(n, names.size());
    assertEquals(n, database.listResources().size());
    for (int i = 0; i < n; i++) {
      assertEquals(BASE + "-" + i, names.get(i), "partition i must map to resource baseName-i");
      assertEquals("[{\"shard\":" + i + "}]", serialize(names.get(i)),
          "resource baseName-i must hold exactly partition i's content (order preserved)");
    }
  }

  @Test
  void singlePartitionProducesOneResource() {
    final List<String> names =
        ParallelJsonShredder.shredPartitioned(database, List.of(readerOf("[1,2,3]")), BASE, CONFIG, 0, 1);
    assertEquals(List.of("records-0"), names);
    assertEquals("[1,2,3]", serialize("records-0"));
  }

  @Test
  void emptyPartitionsIsANoOp() {
    final List<String> names = ParallelJsonShredder.shredPartitioned(database, List.of(), BASE, CONFIG, 0, 4);
    assertTrue(names.isEmpty());
    assertEquals(0, database.listResources().size());
  }

  @Test
  void concurrencyOverridesAreClamped() {
    // maxConcurrency far above the partition count, and the <=0 "use all cores" path, must both work.
    // Distinct base names keep the two runs on the same database without interfering.
    assertEquals(3, ParallelJsonShredder.shredPartitioned(database,
        List.of(readerOf("[1]"), readerOf("[2]"), readerOf("[3]")), "hi", CONFIG, 0, 9999).size());
    assertEquals(3, ParallelJsonShredder.shredPartitioned(database,
        List.of(readerOf("[1]"), readerOf("[2]"), readerOf("[3]")), "lo", CONFIG, 0, -5).size());
    assertEquals(6, database.listResources().size());
  }

  // ---------------------------------------------------------------------------------------------
  // Atomicity / rollback
  // ---------------------------------------------------------------------------------------------

  @Test
  void anyPartitionFailureRollsBackEveryCreatedResource() {
    final List<Callable<JsonReader>> parts = Arrays.asList(
        readerOf("[{\"shard\":0}]"),
        () -> { throw new IOException("boom in partition 1"); }, // worker-time failure
        readerOf("[{\"shard\":2}]"));

    final SirixException ex = assertThrows(SirixException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, parts, BASE, CONFIG, 0, 4));
    // original cause is preserved
    assertInstanceOf(IOException.class, rootCause(ex));

    // All-or-nothing: not a single resource survives — including the partitions that DID shred fine.
    assertFalse(database.existsResource("records-0"), "successfully-shredded shard must be rolled back");
    assertFalse(database.existsResource("records-1"));
    assertFalse(database.existsResource("records-2"));
    assertEquals(0, database.listResources().size());
  }

  @Test
  void nullReaderFromPartitionRollsBack() {
    final List<Callable<JsonReader>> parts = Arrays.asList(readerOf("[1]"), () -> null);
    assertThrows(SirixException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, parts, BASE, CONFIG, 0, 2));
    assertEquals(0, database.listResources().size());
  }

  // ---------------------------------------------------------------------------------------------
  // Fail-fast: collision + config integrity (no mutation)
  // ---------------------------------------------------------------------------------------------

  @Test
  void existingResourceNameFailsFastWithoutTouchingAnything() {
    // Pre-existing resource that collides with records-1.
    database.createResource(ResourceConfiguration.newBuilder("records-1").build());

    final List<Callable<JsonReader>> parts = List.of(readerOf("[0]"), readerOf("[1]"), readerOf("[2]"));
    assertThrows(IllegalStateException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, parts, BASE, CONFIG, 0, 4));

    // No new resources, and the pre-existing one is untouched.
    assertFalse(database.existsResource("records-0"));
    assertFalse(database.existsResource("records-2"));
    assertTrue(database.existsResource("records-1"));
    assertEquals(1, database.listResources().size());
  }

  @Test
  void configFactoryNameMismatchFailsAndRollsBack() {
    // A config whose name does not match the requested resource would silently write the wrong files.
    final Function<String, ResourceConfiguration> badConfig =
        name -> ResourceConfiguration.newBuilder("totally-different").build();
    final List<Callable<JsonReader>> parts = List.of(readerOf("[1]"), readerOf("[2]"));
    assertThrows(SirixException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, parts, BASE, badConfig, 0, 2));
    assertEquals(0, database.listResources().size());
  }

  // ---------------------------------------------------------------------------------------------
  // Argument validation
  // ---------------------------------------------------------------------------------------------

  @Test
  void nullArgumentsRejected() {
    final List<Callable<JsonReader>> ok = List.of(readerOf("[1]"));
    assertThrows(NullPointerException.class,
        () -> ParallelJsonShredder.shredPartitioned(null, ok, BASE, CONFIG, 0, 1));
    assertThrows(NullPointerException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, null, BASE, CONFIG, 0, 1));
    assertThrows(NullPointerException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, ok, null, CONFIG, 0, 1));
    assertThrows(NullPointerException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, ok, BASE, null, 0, 1));
  }

  @Test
  void nullPartitionEntryRejected() {
    final List<Callable<JsonReader>> parts = Arrays.asList(readerOf("[1]"), null);
    assertThrows(NullPointerException.class,
        () -> ParallelJsonShredder.shredPartitioned(database, parts, BASE, CONFIG, 0, 2));
    assertEquals(0, database.listResources().size(), "a null entry must abort before any resource is created");
  }

  // ---------------------------------------------------------------------------------------------
  // Reader lifecycle
  // ---------------------------------------------------------------------------------------------

  @Test
  void partitionReadersAreClosed() {
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Callable<JsonReader> tracked = () -> new JsonReader(new StringReader("[1]") {
      @Override
      public void close() {
        closed.set(true);
        super.close();
      }
    });
    ParallelJsonShredder.shredPartitioned(database, List.of(tracked), BASE, CONFIG, 0, 1);
    assertTrue(closed.get(), "the partition's JsonReader must be closed by the shredder");
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Callable<JsonReader> readerOf(final String json) {
    return () -> new JsonReader(new StringReader(json));
  }

  private String serialize(final String resourceName) {
    try (final JsonResourceSession session = database.beginResourceSession(resourceName)) {
      final StringWriter writer = new StringWriter();
      new JsonSerializer.Builder(session, writer).build().call();
      return writer.toString();
    }
  }

  private static Throwable rootCause(Throwable t) {
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t;
  }
}
