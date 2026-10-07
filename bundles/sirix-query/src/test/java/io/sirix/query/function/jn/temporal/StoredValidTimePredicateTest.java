package io.sirix.query.function.jn.temporal;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class StoredValidTimePredicateTest {
  @TempDir
  Path directory;

  @Test
  void memoizedUtcBoundsAndGeneralFallbackKeepNanosecondStrictness() {
    final String[] bounds =
        {"1969-12-31T23:59:59Z", "1970-01-01T00:00:00Z", "2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z",
            "2024-01-01T01:00:00+01:00", "2024-01-01T00:00:00.000000001Z", "2024-01-01T00:00:00.123456789Z",
            "2024-01-01T00:00:00", "0000-01-01T00:00:00Z", "2016-12-31T23:59:60Z", "invalid"};
    final Instant[] probes =
        {Instant.MIN, Instant.parse("1969-12-31T23:59:58.999999999Z"), Instant.parse("1969-12-31T23:59:59Z"),
            Instant.parse("1969-12-31T23:59:59.000000001Z"), Instant.EPOCH, Instant.parse("2024-01-01T00:00:00Z"),
            Instant.parse("2024-01-01T00:00:00.000000001Z"), Instant.parse("2024-01-01T00:00:00.123456789Z"),
            Instant.parse("2024-01-01T00:00:00.999999999Z"), Instant.parse("2024-01-01T00:00:01Z"), Instant.MAX};
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      for (int index = 0; index < bounds.length; index++) {
        final String from = bounds[index];
        final String to = bounds[(index + 1) % bounds.length];
        final var collection = store.create("dates", "r" + index, "{\"vf\":\"" + from + "\",\"vt\":\"" + to + "\"}");
        final JsonDBObject stored = (JsonDBObject) collection.getDocument("r" + index);
        final var general =
            new ArrayObject(new QNm[] {new QNm("vf"), new QNm("vt")}, new Sequence[] {new Str(from), new Str(to)});
        for (final Instant probe : probes) {
          for (int strict = 0; strict < 4; strict++) {
            assertEquals(
                ValidTimeIndexScan.isValidAtTime(general, probe, "vf", "vt", (strict & 1) != 0, (strict & 2) != 0),
                ValidTimeIndexScan.isValidAtTime(stored, probe, "vf", "vt", (strict & 1) != 0, (strict & 2) != 0),
                from + "/" + to + " at " + probe + " strict=" + strict);
          }
        }
      }
    }
  }
}
