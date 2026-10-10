package io.sirix.query.function.jn.temporal;

import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

final class ValidTimeRangeCacheTest {
  @Test
  void immutableKeysAreFencedByEveryRangeAndRevisionComponent() {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final long[] keys = {3, 7};
    ValidTimeRangeCache.put(session, 1, 2, 3, 4, 5, 3, keys);
    keys[0] = 100;
    assertArrayEquals(new long[] {3, 7}, ValidTimeRangeCache.get(session, 1, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(mock(JsonResourceSession.class), 1, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, 2, 2, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, 1, 3, 3, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, 1, 2, 4, 4, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, 1, 2, 3, 3, 5, 3));
    assertNull(ValidTimeRangeCache.get(session, 1, 2, 3, 4, 6, 3));
    for (int mode = 0; mode < 3; mode++) {
      assertNull(ValidTimeRangeCache.get(session, 1, 2, 3, 4, 5, mode));
    }
    ValidTimeRangeCache.put(session, 2, 2, 3, 4, 5, 3, new long[] {8});
    assertNull(ValidTimeRangeCache.get(session, 1, 2, 3, 4, 5, 3));
    assertArrayEquals(new long[] {8}, ValidTimeRangeCache.get(session, 2, 2, 3, 4, 5, 3));
  }
}
