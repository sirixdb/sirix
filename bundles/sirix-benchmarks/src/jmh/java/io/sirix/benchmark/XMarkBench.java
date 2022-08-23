package io.sirix.benchmark;

import org.brackit.xquery.XQuery;
import org.brackit.xquery.node.parser.DocumentParser;
import org.openjdk.jmh.annotations.*;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.node.BasicXmlDBStore;
import org.sirix.xquery.node.XmlDBCollection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms1G", "-Xmx5G" })
public class XMarkBench {

  public static final String USER_HOME = System.getProperty("user.home");

  public static final Path SIRIX_DATA_LOCATION = Paths.get(USER_HOME, "sirix-data");

  private static final Path DATABASE_PATH = SIRIX_DATA_LOCATION.resolve("xml-xmark-database");

  @State(Scope.Thread)
  public static class MyState {

    private static final Path XMARK_AUCTION;
    private static final Path QUERY_DIR;

    static {
      final Path XMARK = Paths.get("src", "jmh", "resources", "xmark");
      XMARK_AUCTION = XMARK.resolve("auction.xml");
      QUERY_DIR = XMARK.resolve("queries").resolve("orig");
    }

    private Database<XmlResourceManager> database;
    private XmlResourceManager manager;
    private BasicXmlDBStore store;
    private XmlDBCollection collection;
    private SirixQueryContext ctx;
    private PrintStream buffer;

    @Setup(Level.Trial)
    public void doSetup() throws FileNotFoundException {
      if (Files.exists(DATABASE_PATH))
        Databases.removeDatabase(DATABASE_PATH);

      final var dbConfig = new DatabaseConfiguration(DATABASE_PATH);
      Databases.createXmlDatabase(dbConfig);
      database = Databases.openXmlDatabase(DATABASE_PATH);
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .useTextCompression(false)
                                                   .useDeweyIDs(true)
                                                   .build());
      manager = database.openResourceManager("resource");

      try (final var wtx = manager.beginNodeTrx(); final var fis = new FileInputStream(XMARK_AUCTION.toFile())) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createFileReader(fis));
        wtx.commit();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      final var parser = new DocumentParser(XMARK_AUCTION.toFile());
      parser.setRetainWhitespace(true);
      store = BasicXmlDBStore.newBuilder().build();
      collection = store.create("testCollection", parser);
      ctx = SirixQueryContext.createWithNodeStore(store);
      ctx.setContextItem(collection.getDocument());
      buffer = createBuffer();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      buffer.close();
      ctx.close();
      collection.close();
      store.close();
      manager.close();
      database.close();

      Databases.removeDatabase(DATABASE_PATH);
    }

    private PrintStream createBuffer() {
      final var out = new ByteArrayOutputStream();
      return new PrintStream(out);
    }
  }

  private XQuery xquery(final MyState state, final String query) {
    return new XQuery(SirixCompileChain.createWithNodeStore(state.store), query);
  }

  @Benchmark
  public void xmark01(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q01.xq"), StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark02(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q02.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark03(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q03.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark04(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q04.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark05(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q05.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark06(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q06.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark07(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q07.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark08(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q08.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark09(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q09.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark10(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q10.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark11(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q11.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark12(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q12.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }


  @Benchmark
  public void xmark13(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q13.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }


  @Benchmark
  public void xmark14(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q14.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }


  @Benchmark
  public void xmark15(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q15.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }


  @Benchmark
  public void xmark16(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q16.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark17(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q17.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark18(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q18.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark19(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q19.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }

  @Benchmark
  public void xmark20(MyState state) throws Exception {
    final var queryString = Files.readString(MyState.QUERY_DIR.resolve("q20.xq"),  StandardCharsets.UTF_8);
    final var query = xquery(state, queryString);
    query.serialize(state.ctx, state.buffer);
  }
}
