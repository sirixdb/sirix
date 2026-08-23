/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.service.json.shredder;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end coverage for the fused parser-to-page primitive number path. */
final class JacksonPrimitiveNumberFastPathTest {

  private static final String RESOURCE = "primitive-number-fast-path";
  private static final String JSON = """
      {
        "intMin": -2147483648,
        "longAboveInt": 2147483648,
        "big": 9223372036854775808,
        "decimal": 1.25,
        "intMax": 2147483647,
        "longMin": -9223372036854775808
      }
      """;

  @TempDir
  Path temporaryDirectory;

  @Test
  void mixedPrimitiveAndFallbackValuesSurviveCarrierReuseAndColdReopen() throws Exception {
    final Path databasePath = temporaryDirectory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .storeDiffs(false)
                                                   .buildPathSummary(true)
                                                   .buildPathStatistics(true)
                                                   .useDeweyIDs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx();
          var parser = JacksonJsonShredder.createStringParser(JSON)) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      }
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild(), "missing root object after cold reopen");
      assertTrue(rtx.moveToFirstChild(), "missing first fused number field after cold reopen");

      assertNumber(rtx, "intMin", Integer.class, Integer.MIN_VALUE);
      assertTrue(rtx.moveToRightSibling());
      assertNumber(rtx, "longAboveInt", Long.class, 2_147_483_648L);
      assertTrue(rtx.moveToRightSibling());
      assertNumber(rtx, "big", BigInteger.class, new BigInteger("9223372036854775808"));
      assertTrue(rtx.moveToRightSibling());
      assertNumber(rtx, "decimal", BigDecimal.class, new BigDecimal("1.25"));
      assertTrue(rtx.moveToRightSibling());
      assertNumber(rtx, "intMax", Integer.class, Integer.MAX_VALUE);
      assertTrue(rtx.moveToRightSibling());
      assertNumber(rtx, "longMin", Long.class, Long.MIN_VALUE);
    }
  }

  private static void assertNumber(final JsonNodeReadOnlyTrx rtx, final String expectedName,
      final Class<? extends Number> expectedType, final Number expectedValue) {
    assertEquals(NodeKind.OBJECT_NAMED_NUMBER, rtx.getKind());
    assertEquals(expectedName, rtx.getName().getLocalName());
    final Number actual = rtx.getNumberValue();
    assertEquals(expectedType, actual.getClass(), "wrong numeric wire type for " + expectedName);
    assertEquals(expectedValue, actual, "wrong numeric value for " + expectedName);
  }
}
