/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.brackit.query.atomic.QNm;
import io.sirix.access.InMemoryLifetimeFixture.Model;
import io.sirix.api.Database;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.lang.ref.Reference;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import static io.sirix.access.InMemoryLifetimeFixture.commitFirstValue;
import static io.sirix.access.InMemoryLifetimeFixture.commitNextValue;
import static io.sirix.access.InMemoryLifetimeFixture.createDatabase;
import static io.sirix.access.InMemoryLifetimeFixture.createResource;
import static io.sirix.access.InMemoryLifetimeFixture.hasContent;
import static io.sirix.access.InMemoryLifetimeFixture.open;
import static io.sirix.access.InMemoryLifetimeFixture.resourceNames;
import static io.sirix.access.InMemoryLifetimeFixture.valueAt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * TXN-615 acceptance criteria requiring child JVMs: AC10 (abrupt end), AC13 (memory released at
 * close and removal), and failed initial-scan admission. These run in ordinary CI; a child loads
 * unmutated classes, so it cannot observe PIT mutations in its parent. The in-process
 * database-instance contracts remain in {@link DatabasesInMemoryLifetimeTest}.
 */
final class InMemoryLifetimeForkedJvmTest {

  private static final String VOLATILE = "volatile";

  private static final String DURABLE = "durable";

  @TempDir
  Path directory;

  @AfterEach
  void removeDatabase() {
    Databases.removeDatabase(directory.resolve("database"));
  }

  private Path databasePath() {
    return directory.resolve("database");
  }

  static Stream<Arguments> modelsAndResourceAdmissions() {
    return Stream.of(Model.values())
                 .flatMap(model -> Stream.of("exists", "list", "open", "create")
                                         .map(admission -> Arguments.of(model, admission)));
  }

  @ParameterizedTest
  @MethodSource("modelsAndResourceAdmissions")
  void failedInitialScanIsRetriedBeforeResourceAdmission(final Model model, final String admission) throws Exception {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"), "needs POSIX permissions");
    final Process process = child("-Xmx256m", AbruptExitChild.class, databasePath().toString(), model.name());
    try {
      assertEquals("HALTING", childResult(process));
      assertTrue(process.waitFor(60, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
    } finally {
      process.destroyForcibly();
    }
    final Path resources = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    final Set<PosixFilePermission> readable = Files.getPosixFilePermissions(resources);
    Files.setPosixFilePermissions(resources, PosixFilePermissions.fromString("--x--x--x"));
    try {
      assumeTrue(!Files.isReadable(resources), "running as a user who ignores permissions");
      try (Database<? extends ResourceSession<?, ?>> database = open(model, databasePath())) {
        try {
          assertThrows(SirixIOException.class, () -> database.existsResource(VOLATILE));
        } finally {
          Files.setPosixFilePermissions(resources, readable);
        }
        switch (admission) {
          case "exists" -> assertFalse(database.existsResource(VOLATILE));
          case "list" -> assertEquals(Set.of(DURABLE), resourceNames(database));
          case "open" -> assertThrows(SirixUsageException.class, () -> database.beginResourceSession(VOLATILE));
          case "create" -> createResource(database, VOLATILE, StorageType.IN_MEMORY);
          default -> throw new IllegalArgumentException(admission);
        }
        if (!admission.equals("create")) {
          createResource(database, VOLATILE, StorageType.IN_MEMORY);
        }
        try (ResourceSession<?, ?> session = database.beginResourceSession(VOLATILE)) {
          assertEquals(0, session.getMostRecentRevisionNumber());
          assertFalse(hasContent(session));
          final long key = commitFirstValue(session, model, "fresh");
          assertEquals("fresh", valueAt(session, model, 1, key));
        }
        assertTrue(database.existsResource(VOLATILE));
        assertEquals(Set.of(DURABLE, VOLATILE), resourceNames(database));
        try (ResourceSession<?, ?> session = database.beginResourceSession(DURABLE)) {
          assertEquals(2, session.getMostRecentRevisionNumber());
          assertTrue(hasContent(session));
        }
      }
    } finally {
      Files.setPosixFilePermissions(resources, readable);
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void closedSessionsDoNotRetainCommittedHistories(final Model model) throws Exception {
    for (final String mode : new String[] {"close-retain-sessions", "remove-retain-sessions"}) {
      final Process process =
          child("-Xmx160m", MemoryChild.class, directory.resolve(mode).toString(), model.name(), mode);
      try {
        assertEquals("COMPLETED", childResult(process));
        assertTrue(process.waitFor(300, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue());
      } finally {
        process.destroyForcibly();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void unreadableSettingsRemovalReleasesMemory(final Model model) throws Exception {
    final String mode = "remove-broken-settings-retain-sessions";
    final Process process =
        child("-Xmx160m", MemoryChild.class, directory.resolve(mode).toString(), model.name(), mode);
    try {
      assertEquals("COMPLETED", childResult(process));
      assertTrue(process.waitFor(300, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
    } finally {
      process.destroyForcibly();
    }
  }

  /**
   * AC10, clause 6: a process that ends without closing leaves nothing a later process exposes as an
   * IN_MEMORY resource, and the persistent neighbour keeps its revisions and files.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void aFreshProcessAfterAnAbruptExitSeesOnlyThePersistentResource(final Model model) throws Exception {
    final Process child = child("-Xmx256m", AbruptExitChild.class, databasePath().toString(), model.name());
    try {
      assertEquals("HALTING", childResult(child));
      assertTrue(child.waitFor(60, TimeUnit.SECONDS));
    } finally {
      child.destroyForcibly();
    }
    final Path durableFiles = databasePath().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                                            .resolve(DURABLE)
                                            .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath());
    final long durableBytes = directorySize(durableFiles);
    assertTrue(durableBytes > 0, "the child committed the persistent resource");
    try (Database<? extends ResourceSession<?, ?>> reopened = open(model, databasePath())) {
      assertEquals(Set.of(DURABLE), resourceNames(reopened));
      assertFalse(reopened.existsResource(VOLATILE));
      try (ResourceSession<?, ?> durable = reopened.beginResourceSession(DURABLE)) {
        assertEquals(2, durable.getMostRecentRevisionNumber());
        assertTrue(hasContent(durable));
      }
      createResource(reopened, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> recreated = reopened.beginResourceSession(VOLATILE)) {
        assertEquals(0, recreated.getMostRecentRevisionNumber());
      }
    }
    assertEquals(durableBytes, directorySize(durableFiles), "the persistent resource's files are untouched");
  }

  /**
   * AC13, clause 3: the final close releases the IN_MEMORY state's memory. A child with a small heap
   * fills and closes many databases; a control child with the same heap that keeps them open must run
   * out of memory, which proves the heap bound bites.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void finalCloseReleasesTheMemoryOfTheInMemoryState(final Model model) throws Exception {
    // ExitOnOutOfMemoryError: an exhausted heap in any thread ends the child with exit code 3.
    final Process releasing =
        child("-Xmx160m", MemoryChild.class, directory.resolve("release").toString(), model.name(), "close");
    try {
      assertEquals("COMPLETED", childResult(releasing), "closing each database releases its state");
      assertTrue(releasing.waitFor(300, TimeUnit.SECONDS));
      assertEquals(0, releasing.exitValue());
    } finally {
      releasing.destroyForcibly();
    }
    final Process retaining =
        child("-Xmx160m", MemoryChild.class, directory.resolve("retain").toString(), model.name(), "keep-open");
    try {
      assertTrue(retaining.waitFor(300, TimeUnit.SECONDS));
      assertEquals(OUT_OF_MEMORY_EXIT, retaining.exitValue(),
          "control: retaining every database must exhaust the same heap");
    } finally {
      retaining.destroyForcibly();
    }
  }

  /**
   * AC13, clause 8: removing an IN_MEMORY resource from an open database discards its state at once.
   * The child keeps one database open and creates, fills and removes many resources in it, under the
   * heap the control of {@link #finalCloseReleasesTheMemoryOfTheInMemoryState} exhausts by retaining
   * the same amount of state.
   */
  @ParameterizedTest
  @EnumSource(Model.class)
  void removalReleasesTheMemoryOfTheRemovedResource(final Model model) throws Exception {
    final Process removing =
        child("-Xmx160m", MemoryChild.class, directory.resolve("remove").toString(), model.name(), "remove");
    try {
      assertEquals("COMPLETED", childResult(removing), "removing each resource releases its state");
      assertTrue(removing.waitFor(300, TimeUnit.SECONDS));
      assertEquals(0, removing.exitValue());
    } finally {
      removing.destroyForcibly();
    }
  }

  /** The JVM's exit code under -XX:+ExitOnOutOfMemoryError. */
  private static final int OUT_OF_MEMORY_EXIT = 3;

  private Process child(final String heap, final Class<?> main, final String... arguments) throws IOException {
    final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final List<String> command = new ArrayList<>(List.of(java, heap, "-XX:+ExitOnOutOfMemoryError", "--enable-preview",
        "--enable-native-access=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector", "-Dsirix.allocator.maxSize=64M",
        "-Djava.io.tmpdir=" + directory, "-cp", System.getProperty("java.class.path"), main.getName()));
    command.addAll(List.of(arguments));
    return new ProcessBuilder(command).redirectError(directory.resolve("child-" + System.nanoTime() + ".log").toFile())
                                      .start();
  }

  private static String childResult(final Process process) throws Exception {
    // The bound detects a broken child, not database latency.
    return CompletableFuture.supplyAsync(() -> {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        String last = "child exited without a result";
        while ((line = reader.readLine()) != null) {
          last = line;
          if (line.equals("HALTING") || line.equals("COMPLETED")) {
            return line;
          }
        }
        return last;
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    }).get(300, TimeUnit.SECONDS);
  }

  private static long directorySize(final Path path) throws IOException {
    try (var files = Files.walk(path)) {
      long size = 0;
      for (final Path file : files.toList()) {
        if (Files.isRegularFile(file)) {
          size += Files.size(file);
        }
      }
      return size;
    }
  }

  /** Child for AC10: commit to an IN_MEMORY and a persistent resource, then halt without closing. */
  static final class AbruptExitChild {
    public static void main(final String[] arguments) {
      final Path path = Path.of(arguments[0]);
      final Model model = Model.valueOf(arguments[1]);
      createDatabase(model, path);
      final Database<? extends ResourceSession<?, ?>> database = open(model, path);
      createResource(database, DURABLE, StorageType.FILE_CHANNEL);
      createResource(database, VOLATILE, StorageType.IN_MEMORY);
      try (ResourceSession<?, ?> durable = database.beginResourceSession(DURABLE);
          ResourceSession<?, ?> volatileSession = database.beginResourceSession(VOLATILE)) {
        final long first = commitFirstValue(durable, model, "d0");
        commitNextValue(durable, model, first, "d1");
        commitFirstValue(volatileSession, model, "v0");
      }
      System.out.println("HALTING");
      System.out.flush();
      // An abrupt end: no close, no shutdown hooks.
      Runtime.getRuntime().halt(0);
    }
  }

  /**
   * Child for AC13: fill many IN_MEMORY databases, closing each ({@code close}) or keeping all open
   * ({@code keep-open}); or fill as many IN_MEMORY resources in one open database, removing each
   * ({@code remove}).
   */
  static final class MemoryChild {
    /** Committed per database; together the databases need several times the child's heap. */
    private static final int VALUES = 512;

    private static final int VALUE_CHARS = 16 * 1024;

    private static final int DATABASES = 40;

    // The fence keeps retained sessions alive through the entire loop before this child halts.
    @SuppressWarnings("ReachabilityFenceUsage")
    public static void main(final String[] arguments) throws IOException {
      final Path root = Path.of(arguments[0]);
      final Model model = Model.valueOf(arguments[1]);
      final String mode = arguments[2];
      final boolean close = mode.equals("close") || mode.equals("close-retain-sessions");
      final boolean breakSettings = mode.equals("remove-broken-settings-retain-sessions");
      final boolean remove = mode.equals("remove") || mode.equals("remove-retain-sessions") || breakSettings;
      final boolean retainSessions = mode.endsWith("-retain-sessions");
      final Database<? extends ResourceSession<?, ?>> shared;
      if (remove) {
        createDatabase(model, root.resolve("database"));
        shared = open(model, root.resolve("database"));
      } else {
        shared = null;
      }
      // Kept reachable on purpose in the control mode.
      @SuppressWarnings("ModifiedButNotUsed")
      final List<Database<? extends ResourceSession<?, ?>>> kept = new ArrayList<>();
      final List<ResourceSession<?, ?>> closedSessions = new ArrayList<>();
      for (int i = 0; i < DATABASES; i++) {
        final Database<? extends ResourceSession<?, ?>> database;
        final String name;
        if (shared != null) {
          database = shared;
          name = breakSettings
              ? VOLATILE
              : VOLATILE + "-" + i;
        } else {
          final Path path = root.resolve("database-" + i);
          createDatabase(model, path);
          database = open(model, path);
          name = VOLATILE;
        }
        createResource(database, name, StorageType.IN_MEMORY);
        try (ResourceSession<?, ?> session = database.beginResourceSession(name);
            NodeTrx writer = session.beginNodeTrx()) {
          fill(writer, model, new SplittableRandom(i));
          writer.commit();
          if (retainSessions) {
            closedSessions.add(session);
          }
        }
        if (breakSettings) {
          Files.writeString(database.getDatabaseConfig()
                                    .getDatabaseFile()
                                    .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                                    .resolve(name)
                                    .resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()),
              "invalid");
        }
        if (remove) {
          database.removeResource(name);
        } else if (close) {
          database.close();
        } else {
          kept.add(database);
        }
      }
      Reference.reachabilityFence(closedSessions);
      System.out.println("COMPLETED");
      System.out.flush();
      Runtime.getRuntime().halt(0);
    }

    private static void fill(final NodeTrx writer, final Model model, final SplittableRandom random) {
      if (model == Model.JSON) {
        final JsonNodeTrx json = (JsonNodeTrx) writer;
        json.insertArrayAsFirstChild();
        json.insertStringValueAsFirstChild(incompressible(random));
        for (int i = 1; i < VALUES; i++) {
          json.insertStringValueAsRightSibling(incompressible(random));
        }
      } else {
        final XmlNodeTrx xml = (XmlNodeTrx) writer;
        xml.insertElementAsFirstChild(new QNm("root"));
        xml.insertElementAsFirstChild(new QNm("item"));
        xml.insertTextAsFirstChild(incompressible(random));
        for (int i = 1; i < VALUES; i++) {
          xml.moveToParent();
          xml.insertElementAsRightSibling(new QNm("item"));
          xml.insertTextAsFirstChild(incompressible(random));
        }
      }
    }

    /** Random letters and digits: no page encoding or compressor can shrink the state much. */
    private static String incompressible(final SplittableRandom random) {
      final char[] chars = new char[VALUE_CHARS];
      for (int i = 0; i < chars.length; i++) {
        chars[i] = ALPHABET.charAt(random.nextInt(ALPHABET.length()));
      }
      return new String(chars);
    }

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  }
}
