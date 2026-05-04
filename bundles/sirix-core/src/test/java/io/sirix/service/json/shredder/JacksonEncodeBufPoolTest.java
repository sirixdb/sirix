/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.service.json.shredder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.InsertPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the iter#23 thread-local {@code encodeBuf} pool keeps
 * {@link JacksonJsonShredder} steady-state allocation low across repeated
 * shred calls on the same thread.
 *
 * <p>Strategy: reflectively read {@code ENCODE_BUF_POOL} after a first shred
 * (which possibly grows the buffer); a fresh shredder instance built later
 * on the same thread must rent the very same array object — this proves the
 * pool reuse rather than relying on coarse heap-size counters.
 */
final class JacksonEncodeBufPoolTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** Builds a 10K-record JSON array; values pre-sized to force a one-time pool grow. */
  private static String build10kJsonArray(final int valueLen) {
    final StringBuilder val = new StringBuilder(valueLen);
    for (int i = 0; i < valueLen; i++) val.append('x');
    final String v = val.toString();
    final StringBuilder sb = new StringBuilder(10_000 * (valueLen + 16));
    sb.append('[');
    for (int i = 0; i < 10_000; i++) {
      if (i > 0) sb.append(',');
      sb.append("{\"k\":\"").append(v).append("\"}");
    }
    sb.append(']');
    return sb.toString();
  }

  @Test
  @DisplayName("repeated shreds on the same thread reuse the same pooled byte[]")
  void poolReuseObserved() throws Exception {
    // Pick a value length that forces growth past the initial 256-byte pool buffer.
    final String payload = build10kJsonArray(/*valueLen=*/512);

    final Database<JsonResourceSession> db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
      runShred(session, payload, /*asFirstChild=*/true);

      final byte[] poolAfterFirst = readPool();
      assertNotNull(poolAfterFirst, "pool should be initialised after first shred");
      assertTrue(poolAfterFirst.length >= 512,
          "pool should have grown to fit value length, got " + poolAfterFirst.length);

      // Second shred — encoder hot path should rent the SAME array.
      runShred(session, payload, /*asFirstChild=*/false);
      final byte[] poolAfterSecond = readPool();
      assertSame(poolAfterFirst, poolAfterSecond,
          "pool buffer must be reused across shreds on the same thread");

      runShred(session, payload, /*asFirstChild=*/false);
      final byte[] poolAfterThird = readPool();
      assertSame(poolAfterFirst, poolAfterThird,
          "pool buffer must remain stable on subsequent shreds");
    }
  }

  private static byte[] readPool() throws Exception {
    final Field f = JacksonJsonShredder.class.getDeclaredField("ENCODE_BUF_POOL");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    final ThreadLocal<byte[]> pool = (ThreadLocal<byte[]>) f.get(null);
    return pool.get();
  }

  private static void runShred(final JsonResourceSession session, final String payload,
      final boolean asFirstChild) throws IOException {
    try (final var trx = session.beginNodeTrx(0, AfterCommitState.KEEP_OPEN)) {
      try (final JsonParser parser = new JsonFactory().createParser(new StringReader(payload))) {
        final var pos = asFirstChild ? InsertPosition.AS_FIRST_CHILD : InsertPosition.AS_LAST_CHILD;
        new JacksonJsonShredder.Builder(trx, parser, pos)
            .build()
            .call();
      }
      trx.commit();
    }
  }
}
