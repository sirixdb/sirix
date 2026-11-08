package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.json.BasicJsonDBStore;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
final class LoadSourceAcquisitionTest {
  @TempDir
  Path directory;

  @Test
  void replacementConsumesEachSourceFromItsFirstAcquisition() throws Exception {
    final AtomicInteger firstRequests = new AtomicInteger();
    final AtomicInteger secondRequests = new AtomicInteger();
    final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/input", exchange -> {
      final boolean first = exchange.getRequestURI().getQuery().equals("first");
      final int request = (first ? firstRequests : secondRequests).incrementAndGet();
      final byte[] body = (first ? "{\"value\":\"first\"}" : "{\"value\":\"second\"}").getBytes(StandardCharsets.UTF_8);
      try {
        // One-shot sources are valid inputs: reopening them must not lose the original response.
        exchange.sendResponseHeaders(request == 1 ? 200 : 404, body.length);
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
      assertFalse(database.existsResource("keep"), "a successful replacement removes the old resource");
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
          () -> new Query(chain, "jn:load('database',(),('" + uri + "/available','" + uri + "/missing'))")
              .execute(context));
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

  private static void assertStoredValue(final JsonNodeReadOnlyTrx trx, final String expected) {
    assertTrue(trx.moveToDocumentRoot());
    assertTrue(trx.moveToFirstChild());
    assertTrue(trx.moveToFirstChild());
    assertEquals(expected, trx.getValue());
  }
}
