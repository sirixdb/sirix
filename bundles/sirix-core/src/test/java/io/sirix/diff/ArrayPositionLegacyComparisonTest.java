package io.sirix.diff;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Full-output byte digests captured from the unmodified serializer at 858d0bb8a after directly
 * comparing all 216 outputs byte-for-byte with the memoized serializer. The checked-in manifest
 * records the exact source hash. This complements the literal byte golden in
 * {@link JsonDiffSerializerArrayPositionTest} without duplicating the old production class.
 */
final class ArrayPositionLegacyComparisonTest {

  @Test
  void outputBytesMatchUnmodifiedBaseline() throws Exception {
    final List<String> documents = List.of("[]", "[0,1,2,3]", "[[],[1,2],[[3],[4,5]]]",
        "[null,true,false,\"quote\\\"slash/雪\",3.125]", "{\"items\":[0,{\"nested\":[1,2]},[3,4]],\"other\":[5,6]}",
        "[{\"child\":{\"array\":[6,7]}},{\"items\":[3,{\"x\":[4,5]}]}]");
    final List<String> expectedDigests;
    try (final var reader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(getClass().getResourceAsStream("/json/diff-array-positions-baseline.sha256")),
        StandardCharsets.UTF_8))) {
      expectedDigests = reader.lines().filter(line -> !line.startsWith("#")).toList();
    }
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    final HexFormat hex = HexFormat.of();
    int comparisons = 0;
    try {
      for (final boolean pathSummary : new boolean[] {false, true}) {
        for (final boolean dewey : new boolean[] {false, true}) {
          for (final String document : documents) {
            JsonTestHelper.deleteEverything();
            final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                                      .storageType(StorageType.FILE_CHANNEL)
                                                                      .storeDiffs(false)
                                                                      .buildPathSummary(pathSummary)
                                                                      .useDeweyIDs(dewey)
                                                                      .build();
            try (
                final var db =
                    JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
                final var session = db.beginResourceSession(JsonTestHelper.RESOURCE);
                final var wtx = session.beginNodeTrx()) {
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document), JsonNodeTrx.Commit.NO);
              wtx.commit();
              wtx.moveToDocumentRoot();
              final List<Long> keys = new ArrayList<>();
              final DescendantAxis axis = new DescendantAxis(wtx);
              while (axis.hasNext()) {
                keys.add(axis.nextLong());
              }
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
              if (wtx.isArray()) {
                wtx.insertNumberValueAsFirstChild(-7);
              }
              wtx.commit();
              for (int order = 0; order < 3; order++) {
                final List<DiffTuple> tuples = new ArrayList<>();
                for (final long key : keys) {
                  for (final DiffFactory.DiffType kind : new DiffFactory.DiffType[] {DiffFactory.DiffType.INSERTED,
                      DiffFactory.DiffType.DELETED, DiffFactory.DiffType.UPDATED, DiffFactory.DiffType.REPLACEDNEW,
                      DiffFactory.DiffType.SAME, DiffFactory.DiffType.SAMEHASH, DiffFactory.DiffType.REPLACEDOLD}) {
                    tuples.add(new DiffTuple(kind, key, key, null));
                  }
                }
                if (order == 1) {
                  Collections.reverse(tuples);
                } else if (order == 2) {
                  Collections.shuffle(tuples, new Random(541));
                }
                for (int mode = 0; mode < 3; mode++) {
                  final var fixed = new JsonDiffSerializer(db.getName(), session, 1, 2, new ArrayList<>(tuples));
                  final String actual = mode == 2
                      ? fixed.serializeSidecar()
                      : fixed.serialize(mode == 1);
                  assertEquals(expectedDigests.get(comparisons),
                      hex.formatHex(digest.digest(actual.getBytes(StandardCharsets.UTF_8))), "document=" + document
                          + " pathSummary=" + pathSummary + " dewey=" + dewey + " order=" + order + " mode=" + mode);
                  comparisons++;
                }
              }
            }
          }
        }
      }
    } finally {
      JsonTestHelper.deleteEverything();
    }
    assertEquals(expectedDigests.size(), comparisons, "every baseline output must be checked");
  }
}
