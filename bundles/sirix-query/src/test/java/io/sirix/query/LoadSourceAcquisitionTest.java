package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.node.BasicXmlDBStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.json.BasicJsonDBStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class LoadSourceAcquisitionTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void xmlAcquiresAllSourcesBeforeChangingAnyTarget(final boolean existing, final boolean named) throws Exception {
    final Path available = Files.writeString(directory.resolve("available.xml"), "<loaded/>");
    final Path missing = directory.resolve("missing.xml");
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      if (existing) {
        store.create("database", "keep", new DocumentParser("<original/>"));
      }
      final String target = named
          ? "'keep'"
          : "()";
      assertThrows(QueryException.class,
          () -> new Query(chain,
              "xn:load('database'," + target + ",('" + available.toUri() + "','" + missing.toUri() + "'))").execute(
                  context));
      if (existing) {
        final var database = store.lookup("database").getDatabase();
        assertEquals(1, database.listResources().size());
        try (var session = database.beginResourceSession("keep"); var trx = session.beginNodeReadOnlyTrx()) {
          assertEquals(1, session.getMostRecentRevisionNumber());
          assertTrue(trx.moveToFirstChild());
          assertEquals("original", trx.getName().getLocalName());
        }
      } else {
        assertNull(store.lookup("database"), "failed acquisition must not create a database");
      }
    }
  }

  @Test
  void aMissingJsonSourcePreservesTheReplacementTargetsHistory() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final var database = store.create("database", "keep", "{\"value\":\"original\"}").getDatabase();
      try (var session = database.beginResourceSession("keep"); var writer = session.beginNodeTrx()) {
        writer.commit();
      }
      assertThrows(QueryException.class, () -> new Query(chain,
          "jn:load('database','keep','" + directory.resolve("missing.json").toUri() + "')").execute(context));
      assertEquals(1, database.listResources().size());
      try (var session = database.beginResourceSession("keep")) {
        assertEquals(2, session.getMostRecentRevisionNumber());
        for (int revision = 1; revision <= 2; revision++) {
          try (var trx = session.beginNodeReadOnlyTrx(revision)) {
            assertStoredValue(trx, "original");
          }
        }
      }
    }
  }

  @Test
  void replacementConsumesEachSourceFromItsFirstAcquisition() throws Exception {
    final AtomicInteger firstRequests = new AtomicInteger();
    final AtomicInteger secondRequests = new AtomicInteger();
    final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/input", exchange -> {
      final boolean first = exchange.getRequestURI().getQuery().equals("first");
      final int request = (first
          ? firstRequests
          : secondRequests).incrementAndGet();
      final byte[] body = (first
          ? "{\"value\":\"first\"}"
          : "{\"value\":\"second\"}").getBytes(StandardCharsets.UTF_8);
      try {
        // One-shot sources are valid inputs: reopening them must not lose the original response.
        exchange.sendResponseHeaders(request == 1
            ? 200
            : 404, body.length);
        try (var response = exchange.getResponseBody()) {
          response.write(body);
        }
      } finally {
        exchange.close();
      }
    });
    server.start();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("database", "keep", "{\"value\":\"original\"}");
      final String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/input?";
      new Query(chain, "jn:load('database',(),('" + uri + "first','" + uri + "second'))").execute(context);
      assertEquals(1, firstRequests.get(), "each source must be acquired exactly once");
      assertEquals(1, secondRequests.get(), "each source must be acquired exactly once");
      final var database = store.lookup("database").getDatabase();
      assertTrue(database.existsResource("keep"), "loading into an existing database keeps its resources (#1384)");
      try (var first = database.beginResourceSession("resource1");
          var second = database.beginResourceSession("resource2");
          var firstTrx = first.beginNodeReadOnlyTrx();
          var secondTrx = second.beginNodeReadOnlyTrx()) {
        assertStoredValue(firstTrx, "first");
        assertStoredValue(secondTrx, "second");
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  void sourceAcquisitionFailurePreservesExistingData() throws Exception {
    final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/available", exchange -> {
      try {
        exchange.sendResponseHeaders(200, 2);
        try (var response = exchange.getResponseBody()) {
          response.write("{}".getBytes(StandardCharsets.UTF_8));
        }
      } finally {
        exchange.close();
      }
    });
    server.start();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("database", "keep", "{\"value\":\"original\"}");
      final String uri = "http://127.0.0.1:" + server.getAddress().getPort();
      assertThrows(QueryException.class,
          () -> new Query(chain, "jn:load('database',(),('" + uri + "/available','" + uri + "/missing'))").execute(
              context));
      final var database = store.lookup("database").getDatabase();
      assertEquals(1, database.listResources().size(), "failed acquisition must leave the old database intact");
      try (var session = database.beginResourceSession("keep"); var trx = session.beginNodeReadOnlyTrx()) {
        assertEquals(1, session.getMostRecentRevisionNumber());
        assertStoredValue(trx, "original");
      }
    } finally {
      server.stop(0);
    }
  }

  /**
   * Loading several sources into an existing collection acquires all of them before writing; when a
   * later source cannot be acquired, every source already acquired is released, not left open.
   */
  @Test
  @EnabledOnOs(OS.LINUX)
  void acquiredSourcesAreReleasedWhenALaterSourceFails() throws Exception {
    final Path available = directory.resolve("available.json");
    Files.writeString(available, "{\"value\":\"loaded\"}");
    final Path missing = directory.resolve("missing.json");
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory.resolve("db")).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("database", "keep", "{\"value\":\"original\"}");
      assertThrows(QueryException.class, () -> new Query(chain,
          "jn:load('database',(),('" + available.toUri() + "','" + missing.toUri() + "'))").execute(context));
      assertFalse(openFiles().contains(available.toRealPath()), "an acquired source must be released");
      assertEquals(1, store.lookup("database").getDatabase().listResources().size());
    }
  }

  /** The files this process holds open, from its descriptor table. */
  private static Set<Path> openFiles() throws IOException {
    final Set<Path> files = new HashSet<>();
    try (final DirectoryStream<Path> descriptors = Files.newDirectoryStream(Path.of("/proc/self/fd"))) {
      for (final Path descriptor : descriptors) {
        try {
          files.add(Files.readSymbolicLink(descriptor));
        } catch (final IOException closedMeanwhile) {
          // The descriptor was closed while listing; it holds nothing open.
        }
      }
    }
    return files;
  }

  private static void assertStoredValue(final JsonNodeReadOnlyTrx trx, final String expected) {
    assertTrue(trx.moveToDocumentRoot());
    assertTrue(trx.moveToFirstChild());
    assertTrue(trx.moveToFirstChild());
    assertEquals(expected, trx.getValue());
  }
}
