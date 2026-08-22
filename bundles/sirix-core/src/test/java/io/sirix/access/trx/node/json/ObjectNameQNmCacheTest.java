/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.QNm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ObjectNameQNmCacheTest {

  @Test
  void internsEqualNamesRegardlessOfStringIdentity() {
    final var cache = new JsonNodeTrxImpl.ObjectNameQNmCache();
    final String firstName = new String("arbitrary-field");
    final String equalName = new String("arbitrary-field");

    final QNm first = cache.qNmFor(firstName);
    final QNm second = cache.qNmFor(equalName);

    assertSame(first, second);
    assertEquals(1, cache.size());
  }

  @Test
  void resetsAtEntryLimitAndThenReusesTheNewGeneration() {
    final var cache = new JsonNodeTrxImpl.ObjectNameQNmCache();
    final QNm oldGeneration = cache.qNmFor("field-0");
    for (int i = 1; i < JsonNodeTrxImpl.ObjectNameQNmCache.MAX_CACHED_NAMES; i++) {
      cache.qNmFor("field-" + i);
    }
    assertEquals(JsonNodeTrxImpl.ObjectNameQNmCache.MAX_CACHED_NAMES, cache.size());

    final QNm firstAfterReset = cache.qNmFor("next-schema-field");

    assertEquals(1, cache.size());
    assertSame(firstAfterReset, cache.qNmFor(new String("next-schema-field")));
    assertNotSame(oldGeneration, cache.qNmFor("field-0"));
    assertEquals(2, cache.size());
  }

  @Test
  void doesNotRetainOversizedArbitraryNames() {
    final var cache = new JsonNodeTrxImpl.ObjectNameQNmCache();
    final String longName = "x".repeat(JsonNodeTrxImpl.ObjectNameQNmCache.MAX_CACHED_NAME_LENGTH + 1);

    final QNm first = cache.qNmFor(longName);
    final QNm second = cache.qNmFor(new String(longName));

    assertNotSame(first, second);
    assertEquals(first, second);
    assertEquals(0, cache.size());
  }

  @Test
  void rejectsNullNames() {
    final var cache = new JsonNodeTrxImpl.ObjectNameQNmCache();

    assertThrows(NullPointerException.class, () -> cache.qNmFor(null));
  }
}
