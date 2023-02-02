package org.sirix.xquery.function.jn.io;

import org.brackit.xquery.XQuery;
import org.brackit.xquery.util.io.IOUtils;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Sequence;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.utils.JsonDocumentCreator;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBObject;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public final class SimpleQueryIntegrationTest {

  private static final String simpleJson = "{\"sirix\":{\"revisionNumber\":1}}";

  private static final String json =
      "{\"sirix\":[{\"revisionNumber\":1,\"revision\":{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}},{\"revisionNumber\":2,\"revision\":{\"tadaaa\":\"todooo\",\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}}]}";

  private static final String expectedJson =
      "[{\"revisionNumber\":1,\"revision\":{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}},{\"revisionNumber\":2,\"revision\":{\"tadaaa\":\"todooo\",\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}}]";

  private static final String expectedAllTimesTimeTravelQueryResult =
      "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]} {\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]} {\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String expectedLastTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String expectedNextTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String expectedFutureTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]} {\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String expectedPastTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]} {\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String expectedPastOrSelfTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]} {\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]} {\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private final Path sirixPath = PATHS.PATH1.getFile();

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testSimple() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var storeQuery = "jn:store('json-path1','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('json-path1','mydoc.jn')[[0]]";
      final var seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("\"bla\"", buf.toString());
    }
  }

  @Test
  public void testSimpleSecond() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var storeQuery = "jn:store('json-path1','mydoc.jn','" + json + "')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('json-path1','mydoc.jn').sirix";
      final var seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }

      assertEquals(expectedJson, buf.toString());
    }
  }

  @Test
  public void testSimpleDeref() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var storeQuery = "jn:store('json-path1','mydoc.jn','" + simpleJson + "')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('json-path1','mydoc.jn').sirix.revisionNumber";
      final var seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }

      assertEquals("1", buf.toString());
    }
  }

  @Test
  public void testComplexSecond() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var storeQuery = "jn:store('json-path1','mydoc.jn','" + json + "')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('json-path1','mydoc.jn').sirix[[0]].revisionNumber";
      final var seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }

      assertEquals("1", buf.toString());
    }
  }

  @Test
  public void testArrays() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final String storeQuery = "jn:store('json-path1','mydoc.jn','[\"foo\",[[\"bar\"]]]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final String openQuery = "jn:doc('json-path1','mydoc.jn')[[1]][[0]][[0]]";
      final Sequence seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final PrintStream buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }

      assertEquals("\"bar\"", buf.toString());

      // // Use XQuery to load a JSON database/resource.
      // System.out.println("Opening document again:");
      // final String query = "jn:doc('json-path1','mydoc.jn')[[0:1]]";
      // System.out.println(openQuery);
      // final Sequence seqSlice = new XQuery(chain, query).evaluate(ctx);
      //
      // assertNotNull(seqSlice);
      //
      // final PrintStream bufSlice = IOUtils.createBuffer();
      // new StringSerializer(bufSlice).serialize(seq);
      // assertEquals("[\"foo\",[[\"bar\"]]]", buf.toString());
    }
  }

  @Test
  public void testTimeTravelAllTimes() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:all-times(jn:doc('json-path1','mydoc.jn'))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedAllTimesTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelFirst() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:first(jn:doc('json-path1','mydoc.jn'))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(JsonDocumentCreator.JSON, buf.toString());
    }
  }

  @Test
  public void testTimeTravelLast() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:last(jn:doc('json-path1','mydoc.jn'))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedLastTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelNext() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:next(jn:doc('json-path1','mydoc.jn',1))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedNextTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelPrevious() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:previous(jn:doc('json-path1','mydoc.jn',2))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(JsonDocumentCreator.JSON, buf.toString());
    }
  }

  @Test
  public void testTimeTravelFuture() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:future(jn:doc('json-path1','mydoc.jn',1))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedFutureTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelFutureOrSelf() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:future(jn:doc('json-path1','mydoc.jn',1),true())";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedAllTimesTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelPast() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:past(jn:doc('json-path1','mydoc.jn',3))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedPastTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelPastOrSelf() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:past(jn:doc('json-path1','mydoc.jn',3),true())";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(allTimesSeq);
      }

      assertEquals(expectedPastOrSelfTimeTravelQueryResult, buf.toString());
    }
  }

  private void setupRevisions(final SirixQueryContext ctx, final SirixCompileChain chain) throws IOException {
    final var storeQuery = "jn:store('json-path1','mydoc.jn','" + JsonDocumentCreator.JSON + "')";
    new XQuery(chain, storeQuery).evaluate(ctx);

    final var openDocQuery = "jn:doc('json-path1','mydoc.jn')";
    final var object = (JsonDBObject) new XQuery(chain, openDocQuery).evaluate(ctx);

    try (final var wtx = object.getTrx().getResourceSession().beginNodeTrx()) {
      wtx.moveTo(3);

      try (final var reader = JsonShredder.createStringReader("{\"foo\":\"bar\"}")) {
        wtx.insertSubtreeAsFirstChild(reader);

        wtx.moveTo(11);
        wtx.remove();
        wtx.commit();
      }

      assert wtx.getRevisionNumber() == 4;
    }
  }

  // @Test
  // public void testVersionedTwitter1() throws IOException, InterruptedException {
  // try (final var store = BasicJsonDBStore.newBuilder().build();
  // final var ctx = SirixQueryContext.createWithJsonStore(store);
  // final var chain = SirixCompileChain.createWithJsonStore(store)) {
  // final var twitterFilePath = JSON.resolve("twitter.json").toString();
  // final var storeQuery = "jn:load('json-path1','mydoc.jn','" + twitterFilePath + "').statuses";
  // final var sequence = (JsonDBArray) new XQuery(chain, storeQuery).execute(ctx);
  //
  // TimeUnit.SECONDS.sleep(5);
  //
  // final var rtx = sequence.getTrx();
  // final Instant now;
  //
  // try (final var wtx = rtx.getResourceManager().beginNodeTrx()) {
  // wtx.moveTo(rtx.getNodeKey());
  // wtx.moveToFirstChild();
  // while (wtx.hasRightSibling())
  // wtx.moveToRightSibling();
  // wtx.insertSubtreeAsRightSibling(JsonShredder.createFileReader(JSON.resolve("twitterTweet1.json")));
  // TimeUnit.SECONDS.sleep(5);
  // wtx.insertSubtreeAsRightSibling(JsonShredder.createFileReader(JSON.resolve("twitterTweet2.json")));
  // TimeUnit.SECONDS.sleep(2);
  // now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
  // TimeUnit.SECONDS.sleep(5);
  //
  // while (wtx.hasLeftSibling())
  // wtx.moveToLeftSibling();
  //
  // wtx.remove();
  // wtx.commit();
  // }
  //
  // final var retrieveQuery =
  // Files.readString(JSON.resolve("query.xq")).replace("$$$$",
  // DateTimeFormatter.ISO_INSTANT.format(now));
  //
  // final var buf = IOUtils.createBuffer();
  // final var serializer = new StringSerializer(buf);
  // new XQuery(chain, retrieveQuery).serialize(ctx, serializer);
  //
  // assertEquals("2018-02-25T19:31:07 2018-02-26T06:42:50 2018-08-16T21:10:50:557000",
  // buf.toString());
  // }
  // }
  //
  // @Test
  // public void testVersionedTwitter2() throws IOException, InterruptedException {
  // try (final var store = BasicJsonDBStore.newBuilder().build();
  // final var ctx = SirixQueryContext.createWithJsonStore(store);
  // final var chain = SirixCompileChain.createWithJsonStore(store)) {
  // final var twitterFilePath = JSON.resolve("twitter.json").toString();
  // final var storeQuery = "jn:load('json-path1','mydoc.jn','" + twitterFilePath + "').statuses";
  // final var sequence = (JsonDBArray) new XQuery(chain, storeQuery).execute(ctx);
  //
  // TimeUnit.SECONDS.sleep(5);
  //
  // final var rtx = sequence.getTrx();
  // final Instant now;
  //
  // try (final var wtx = rtx.getResourceManager().beginNodeTrx()) {
  // wtx.moveTo(rtx.getNodeKey());
  // wtx.moveToFirstChild();
  // while (wtx.hasRightSibling())
  // wtx.moveToRightSibling();
  // wtx.insertSubtreeAsRightSibling(JsonShredder.createFileReader(JSON.resolve("twitterTweet1.json")));
  // TimeUnit.SECONDS.sleep(5);
  // wtx.insertSubtreeAsRightSibling(JsonShredder.createFileReader(JSON.resolve("twitterTweet2.json")));
  // TimeUnit.SECONDS.sleep(2);
  // now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
  // TimeUnit.SECONDS.sleep(5);
  //
  // while (wtx.hasLeftSibling())
  // wtx.moveToLeftSibling();
  //
  // wtx.remove();
  // wtx.commit();
  // }
  //
  // final var retrieveQuery = Files.readString(JSON.resolve("temporal-query.xq"))
  // .replace("$$$$", DateTimeFormatter.ISO_INSTANT.format(now));
  //
  // final var buf = IOUtils.createBuffer();
  // final var serializer = new StringSerializer(buf);
  // new XQuery(chain, retrieveQuery).serialize(ctx, serializer);
  //
  // System.out.println(buf.toString());
  // // assertEquals("2018-02-25T19:31:07 2018-02-26T06:42:50 2018-08-16T21:10:50:557000",
  // // buf.toString());
  // }
  // }
  //
//   @Test
//   public void testTwitter() throws IOException {
//   try (final var store = BasicJsonDBStore.newBuilder().build();
//   final var ctx = SirixQueryContext.createWithJsonStore(store);
//   final var chain = SirixCompileChain.createWithJsonStore(store)) {
//   final var twitterFilePath = JSON.resolve("twitter.json").toString();
//   final var storeQuery = "let $created := jn:load('json-path1','mydoc.jn','" + twitterFilePath
//   + "').statuses[[0]].created_at return xs:dateTime($created)";
//
//   final var buf = IOUtils.createBuffer();
//   final var serializer = new StringSerializer(buf);
//   new XQuery(chain, storeQuery).serialize(ctx, serializer);
//
//   assertEquals("2018-08-16T21:10:50:557000", buf.toString());
//   }
//   }
}
