/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import com.google.gson.stream.JsonReader;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.PageKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The read policy end to end: a point lookup loads a chunk-framed page without expanding the records
 * it did not ask for, and the answers are the ones an eagerly decoded page gives.
 *
 * <p>
 * <b>Why this test has to exist separately.</b> The chunked writer is off by default, so every other
 * suite in the repository reads monolith bodies, where the lazy request degrades to an eager decode
 * and proves nothing. Nothing else in the tree would notice if the policy stopped requesting
 * laziness, or if the request stopped reaching the deserializer — the answers would stay correct and
 * the feature would simply be gone. That is precisely how the column read path was silently disabled
 * twice, so the counters are asserted here, not just printed.
 */
@DisplayName("Chunk-framed body lazy read policy")
final class ChunkedBodyLazyReadPolicyTest {

  /**
   * Wide enough to span several record pages, so a point lookup is genuinely opening a page for one
   * of its thousand records rather than for most of them.
   */
  private static String json(final int records) {
    final StringBuilder out = new StringBuilder(records * 96);
    out.append("{\"records\":[");
    for (int i = 0; i < records; i++) {
      if (i > 0) {
        out.append(',');
      }
      out.append("{\"name\":\"person")
         .append(i)
         .append("\",\"age\":")
         .append(20 + (i % 60))
         .append(",\"dept\":\"dept")
         .append(i % 7)
         .append("\",\"active\":")
         .append((i & 1) == 0)
         .append(",\"score\":")
         .append(i * 3)
         .append('}');
    }
    return out.append("],\"schema\":\"v2\"}").toString();
  }

  private static final String JSON = json(400);

  private boolean previouslyEnabled;
  private boolean previousDiag;
  private boolean previousPoison;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    previouslyEnabled = ChunkedBodyConfig.enabled();
    previousDiag = ChunkedBodyConfig.setDiagForTesting(true);
    // Poisoned, because this is the only test that reaches a lazily loaded page through the whole
    // reader stack rather than through the page's own API. Every consumer the plan's A3 names --
    // the cursor's three inlined slot locates, the write singleton binder, the flyweight factory's
    // caller -- is exercised here, and a missing gate on any of them reads 0xCC and fails loudly
    // instead of returning bytes that happen to look plausible.
    previousPoison = ChunkedBodyConfig.setPoisonForTesting(true);
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setDiagForTesting(previousDiag);
    ChunkedBodyConfig.setPoisonForTesting(previousPoison);
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("point lookups load lazily and answer exactly as an eagerly decoded page does")
  void pointLookupsLoadLazily() throws Exception {
    PageKind.resetChunkedBodyStats();
    shred(JsonTestHelper.PATHS.PATH1.getFile(), true);
    assertTrue(PageKind.chunkedBodiesWritten() > 0, "the writer never produced a chunked body");

    // The point lookups come FIRST, before anything reads the resource for any other reason. The
    // record-page cache lives in the buffer manager, not in the session, so a serialization run
    // beforehand would leave every page resident and this test would measure zero loads of any
    // kind — which is exactly what it did until the ordering was fixed.
    ChunkedBodyConfig.resetDiag();
    final List<String> byPointLookup = readByPointLookup(JsonTestHelper.PATHS.PATH1.getFile());
    final long lazyLoads = ChunkedBodyConfig.lazyLoads();
    System.out.println("[chunked-policy] " + PROBE_KEYS + " probed keys: lazyLoads=" + lazyLoads
        + " chunkMaterializations=" + ChunkedBodyConfig.chunkMaterializations() + " eagerFallbacks="
        + ChunkedBodyConfig.eagerFallbacks());
    assertTrue(lazyLoads > 0, "no page was loaded lazily — the policy never reached the deserializer");
    // Deliberately not asserted at zero fallbacks. Even a resource written by a single shred holds
    // pages whose reference names an older fragment — the shredder's commit leaves a delta over the
    // empty page the resource was created with — and those are combined, hence eager by A6. What
    // matters is that the pages without a chain did take the lazy path, which the count above says.
    assertTrue(lazyLoads > ChunkedBodyConfig.eagerFallbacks(),
        "more point-lookup loads fell back to eager than were served lazily");

    // The whole document still reads back as the document: a lazily loaded page that later has to
    // serve every one of its records must end up indistinguishable from one decoded whole.
    JSONAssert.assertEquals(JSON, serialize(JsonTestHelper.PATHS.PATH1.getFile()), true);

    // The same keys against a monolith-bodied twin of the same document: same answers, and by
    // construction not one lazy load.
    shred(JsonTestHelper.PATHS.PATH2.getFile(), false);
    ChunkedBodyConfig.resetDiag();
    final List<String> byMonolith = readByPointLookup(JsonTestHelper.PATHS.PATH2.getFile());
    assertEquals(byMonolith, byPointLookup, "a lazily loaded page answered a point lookup differently");
    assertEquals(0, ChunkedBodyConfig.lazyLoads(), "a monolith body was somehow loaded lazily");
    assertTrue(byPointLookup.size() > 100, "the probe keys resolved to only " + byPointLookup.size() + " nodes");
  }

  /**
   * A page with a fragment chain is combined, and combine reads all of it — so the policy declines
   * laziness there and says so. Amendment A6: this commit scopes the point-read path to
   * single-fragment pages.
   */
  @Test
  @DisplayName("a point lookup on a multi-fragment page falls back to an eager load and counts it")
  void multiFragmentPagesFallBackToEager() throws Exception {
    final Path file = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(file));
    ChunkedBodyConfig.setEnabledForTesting(true);
    try (final var db = Databases.openJsonDatabase(file)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .byteHandlerPipeline(new ByteHandlerPipeline())
                                             .build());
      try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = session.beginNodeTrx()) {
        try (final Reader src = new StringReader(JSON); final JsonReader jsonReader = new JsonReader(src)) {
          new JsonShredder.Builder(trx, jsonReader, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
      }
      // A second revision leaves a delta fragment over the first, which is what makes the chain.
      try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.insertObjectRecordAsFirstChild("addedInRevisionTwo", new StringValue("added"));
        trx.commit();
      }
    }

    ChunkedBodyConfig.resetDiag();
    final List<String> seen = readByPointLookup(file);
    final long fallbacks = ChunkedBodyConfig.eagerFallbacks();
    System.out.println("[chunked-policy] revision 2, " + seen.size() + " nodes probed: lazyLoads="
        + ChunkedBodyConfig.lazyLoads() + " eagerFallbacks=" + fallbacks);
    assertTrue(fallbacks > 0, "a two-revision resource produced no combined page, so A6 went untested");
  }

  /**
   * How many node keys the probe walks. Fixed, and chosen without opening the resource: deriving the
   * range from {@code getMaxNodeKey()} would need a session, and that session would warm the very
   * pages whose first load this test exists to observe.
   */
  private static final int PROBE_KEYS = 600;

  /**
   * Walk a spread of node keys in one session and describe each node the cursor lands on.
   *
   * <p>
   * A fresh shred numbers its nodes densely from 1, so a fixed stride finds real nodes on every
   * record page without asking the resource anything first. Keys past the end simply do not resolve,
   * identically in both twins.
   */
  private static List<String> readByPointLookup(final Path file) throws Exception {
    final List<String> seen = new ArrayList<>();
    final Random random = new Random(20260817L);
    try (final var db = Databases.openJsonDatabase(file);
        final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      for (int i = 0; i < PROBE_KEYS; i++) {
        final long key = 1 + random.nextInt(3000);
        if (rtx.moveTo(key)) {
          seen.add(key + ":" + rtx.getKind() + ":" + describe(rtx));
        }
      }
    }
    return seen;
  }

  /** Whatever the cursor can say about the node it sits on, as a comparable string. */
  private static String describe(final JsonNodeReadOnlyTrx rtx) {
    final StringBuilder out = new StringBuilder(64);
    out.append("parent=").append(rtx.getParentKey());
    if (rtx.hasFirstChild()) {
      out.append(",firstChild=").append(rtx.getFirstChildKey());
    }
    if (rtx.hasRightSibling()) {
      out.append(",rightSibling=").append(rtx.getRightSiblingKey());
    }
    if (rtx.isObjectKey()) {
      out.append(",name=").append(rtx.getName());
    } else if (rtx.isStringValue()) {
      out.append(",value=").append(rtx.getValue());
    } else if (rtx.isNumberValue()) {
      out.append(",number=").append(rtx.getNumberValue());
    } else if (rtx.isBooleanValue()) {
      out.append(",boolean=").append(rtx.getBooleanValue());
    }
    return out.toString();
  }

  private static void shred(final Path file, final boolean chunkedBody) throws Exception {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunkedBody);
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(file));
      try (final var db = Databases.openJsonDatabase(file)) {
        db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                               .byteHandlerPipeline(new ByteHandlerPipeline())
                                               .build());
        try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
            final var trx = session.beginNodeTrx()) {
          try (final Reader src = new StringReader(JSON); final JsonReader jsonReader = new JsonReader(src)) {
            new JsonShredder.Builder(trx, jsonReader, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
          }
        }
      }
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
  }

  private static String serialize(final Path file) throws Exception {
    try (final var db = Databases.openJsonDatabase(file);
        final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer out = new StringWriter()) {
      new JsonSerializer.Builder(session, out).build().call();
      return out.toString();
    }
  }
}
