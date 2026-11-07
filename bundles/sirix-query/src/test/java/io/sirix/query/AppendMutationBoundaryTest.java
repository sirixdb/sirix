package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.stream.JsonReader;
import com.sun.net.httpserver.HttpServer;
import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
final class AppendMutationBoundaryTest {
  private static final MutationAuthorizer CREATE_ONLY = new MutationAuthorizer() {
    @Override
    public void authorizeWrite(final String database, final String resource) {
      throw new IllegalStateException("MODIFY refused");
    }

    @Override
    public void authorizeDelete(final String database) {
      throw new IllegalStateException("DELETE refused");
    }

    @Override
    public void authorizeCreate(final String database, final String resource) {
    }
  };

  @TempDir
  Path directory;

  @Test
  void batchRechecksPermissionWhenLaterTargetIsReplaced() throws Exception {
    final Object options = new ArrayObject(new QNm[0], new Sequence[0]);
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.create("database", "keep", "{}");
      final AtomicBoolean replaced = new AtomicBoolean();
      final StringReader first = new StringReader("{}") {
        @Override
        public int read(final char[] buffer, final int offset, final int length) throws IOException {
          if (replaced.compareAndSet(false, true)) {
            try (var database = Databases.openJsonDatabase(directory.resolve("database"))) {
              if (database.existsResource("resource2")) {
                database.removeResource("resource2");
              }
              assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource2").build()));
              try (final JsonResourceSession session = database.beginResourceSession("resource2");
                  final JsonNodeTrx trx = session.beginNodeTrx();
                  final JsonReader reader = new JsonReader(new StringReader("{\"protected\":true}"))) {
                trx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO);
                trx.commit();
              }
            }
          }
          return super.read(buffer, offset, length);
        }
      };
      try (final JsonReader firstReader = new JsonReader(first);
          final JsonReader secondReader = new JsonReader(new StringReader("{}"))) {
        final IllegalStateException refused = assertThrows(IllegalStateException.class,
            () -> MutationAuthorization.where(CREATE_ONLY).run(
                () -> collection.addAll(List.of(firstReader, secondReader), options)));
        assertEquals("MODIFY refused", refused.getMessage());
      }
      assertTrue(replaced.get());
      try (var database = Databases.openJsonDatabase(directory.resolve("database"));
          final JsonResourceSession session = database.beginResourceSession("resource2")) {
        assertEquals(1, session.getMostRecentRevisionNumber(), "the replacement must receive no append commit");
      }
    }
  }

  @Test
  void xmlBatchRechecksPermissionWhenLaterTargetIsReplaced() throws Exception {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      final var collection = store.create("database", new DocumentParser("<keep/>"));
      final NodeSubtreeParser first = handler -> {
        try (var database = Databases.openXmlDatabase(directory.resolve("database"))) {
          if (database.existsResource("resource3")) {
            database.removeResource("resource3");
          }
          assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource3").build()));
          try (final XmlResourceSession session = database.beginResourceSession("resource3");
              final XmlNodeTrx trx = session.beginNodeTrx()) {
            trx.insertElementAsFirstChild(new QNm("protected"));
            trx.commit();
          }
        }
        new DocumentParser("<first/>").parse(handler);
      };
      final IllegalStateException refused = assertThrows(IllegalStateException.class,
          () -> MutationAuthorization.where(CREATE_ONLY).run(
              () -> collection.addAll(List.of(first, new DocumentParser("<second/>")))));
      assertEquals("MODIFY refused", refused.getMessage());
      try (var database = Databases.openXmlDatabase(directory.resolve("database"));
          final XmlResourceSession session = database.beginResourceSession("resource3")) {
        assertEquals(1, session.getMostRecentRevisionNumber(), "the replacement must receive no append commit");
      }
    }
  }

  @Test
  void loadOpensLaterUriAfterEarlierResourceIsCommitted() throws Exception {
    final AtomicBoolean ordered = new AtomicBoolean();
    final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/input", exchange -> {
      if (exchange.getRequestURI().getQuery().equals("second")) {
        try (var database = Databases.openJsonDatabase(directory.resolve("database"))) {
          if (database.existsResource("resource1")) {
            try (final JsonResourceSession session = database.beginResourceSession("resource1")) {
              ordered.set(session.getMostRecentRevisionNumber() == 1);
            }
          }
        }
      }
      final byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      try (var response = exchange.getResponseBody()) {
        response.write(body);
      }
    });
    server.start();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      store.create("database", "keep", "{}");
      final String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/input?";
      new Query(chain, "jn:load('database',(),('" + uri + "first','" + uri + "second'),false())").execute(context);
      assertTrue(ordered.get(), "each source must be opened only when its resource is ready to be written");
      assertEquals(3, store.lookup("database").getDatabase().listResources().size());
    } finally {
      server.stop(0);
    }
  }
}
