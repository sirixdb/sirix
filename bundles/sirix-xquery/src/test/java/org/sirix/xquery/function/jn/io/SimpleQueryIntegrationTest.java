package org.sirix.xquery.function.jn.io;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.util.io.IOUtils;
import org.brackit.xquery.util.serialize.StringSerializer;
import org.brackit.xquery.xdm.Sequence;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.utils.JsonDocumentCreator;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBObject;
import junit.framework.TestCase;

public final class SimpleQueryIntegrationTest extends TestCase {

  private static final String mSimpleJson = "{\"sirix\":{\"revisionNumber\":1}}";

  private static final String mJson =
      "{\"sirix\":[{\"revisionNumber\":1,\"revision\":{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}},{\"revisionNumber\":2,\"revision\":{\"tadaaa\":\"todooo\",\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}}]}";

  private static final String mExpectedJson =
      "[{\"revisionNumber\":1,\"revision\":{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}},{\"revisionNumber\":2,\"revision\":{\"tadaaa\":\"todooo\",\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}}]";

  private static final String mExpectedAllTimesTimeTravelQueryResult =
      "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String mExpectedLastTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String mExpectedNextTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String mExpectedFutureTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String mExpectedPastTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private static final String mExpectedPastOrSelfTimeTravelQueryResult =
      "{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\"},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}{\"foo\":[{\"foo\":\"bar\"},\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  private Path sirixPath = PATHS.PATH1.getFile();

  @Override
  protected void setUp() throws Exception {
    JsonTestHelper.deleteEverything();
  }

  @Override
  protected void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testSimple() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('mycol.jn','mydoc.jn')[[0]]";
      final var seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("bla", buf.toString());
    }
  }

  @Test
  public void testSimpleSecond() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var storeQuery = "jn:store('mycol.jn','mydoc.jn','" + mJson + "')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('mycol.jn','mydoc.jn')=>sirix";
      final var seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals(mExpectedJson, buf.toString());
    }
  }

  @Test
  public void testSimpleDeref() {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      final var storeQuery = "jn:store('mycol.jn','mydoc.jn','" + mSimpleJson + "')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('mycol.jn','mydoc.jn')=>sirix=>revisionNumber";
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
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var storeQuery = "jn:store('mycol.jn','mydoc.jn','" + mJson + "')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final var openQuery = "jn:doc('mycol.jn','mydoc.jn')=>sirix[[0]]=>revisionNumber";
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
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"foo\",[[\"bar\"]]]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      final String openQuery = "jn:doc('mycol.jn','mydoc.jn')[[1]][[0]][[0]]";
      final Sequence seq = new XQuery(chain, openQuery).evaluate(ctx);

      assertNotNull(seq);

      final PrintStream buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(seq);
      }
      assertEquals("bar", buf.toString());

      // // Use XQuery to load a JSON database/resource.
      // System.out.println("Opening document again:");
      // final String query = "jn:doc('mycol.jn','mydoc.jn')[[0:1]]";
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
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:all-times(jn:doc('mycol.jn','mydoc.jn'))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedAllTimesTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelFirst() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:first(jn:doc('mycol.jn','mydoc.jn'))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(JsonDocumentCreator.JSON, buf.toString());
    }
  }

  @Test
  public void testTimeTravelLast() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:last(jn:doc('mycol.jn','mydoc.jn'))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedLastTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelNext() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:next(jn:doc('mycol.jn','mydoc.jn',1))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedNextTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelPrevious() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:previous(jn:doc('mycol.jn','mydoc.jn',2))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(JsonDocumentCreator.JSON, buf.toString());
    }
  }

  @Test
  public void testTimeTravelFuture() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:future(jn:doc('mycol.jn','mydoc.jn',1))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedFutureTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelFutureOrSelf() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:future(jn:doc('mycol.jn','mydoc.jn',1),true())";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedAllTimesTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelPast() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:past(jn:doc('mycol.jn','mydoc.jn',3))";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedPastTimeTravelQueryResult, buf.toString());
    }
  }

  @Test
  public void testTimeTravelPastOrSelf() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      setupRevisions(ctx, chain);

      final var allTimesQuery = "jn:past(jn:doc('mycol.jn','mydoc.jn',3),true())";
      final var allTimesSeq = new XQuery(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }
      assertEquals(mExpectedPastOrSelfTimeTravelQueryResult, buf.toString());
    }
  }

  private void setupRevisions(final SirixQueryContext ctx, final SirixCompileChain chain) throws IOException {
    final var storeQuery = "jn:store('mycol.jn','mydoc.jn','" + JsonDocumentCreator.JSON + "')";
    new XQuery(chain, storeQuery).evaluate(ctx);

    final var openDocQuery = "jn:doc('mycol.jn','mydoc.jn')";
    final var object = (JsonDBObject) new XQuery(chain, openDocQuery).evaluate(ctx);

    try (final var wtx = object.getTrx().getResourceManager().beginNodeTrx()) {
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
  // final var storeQuery = "jn:load('mycol.jn','mydoc.jn','" + twitterFilePath + "')=>statuses";
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
  // final var storeQuery = "jn:load('mycol.jn','mydoc.jn','" + twitterFilePath + "')=>statuses";
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
  // @Test
  // public void testTwitter() throws IOException {
  // try (final var store = BasicJsonDBStore.newBuilder().build();
  // final var ctx = SirixQueryContext.createWithJsonStore(store);
  // final var chain = SirixCompileChain.createWithJsonStore(store)) {
  // final var twitterFilePath = JSON.resolve("twitter.json").toString();
  // final var storeQuery = "let $created := jn:load('mycol.jn','mydoc.jn','" + twitterFilePath
  // + "')=>statuses[[0]]=>created_at return xs:dateTime($created)";
  //
  // final var buf = IOUtils.createBuffer();
  // final var serializer = new StringSerializer(buf);
  // new XQuery(chain, storeQuery).serialize(ctx, serializer);
  //
  // assertEquals("2018-08-16T21:10:50:557000", buf.toString());
  // }
  // }
}
