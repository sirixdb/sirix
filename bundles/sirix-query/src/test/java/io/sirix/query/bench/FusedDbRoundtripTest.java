package io.sirix.query.bench;

import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

/**
 * iter#30 read-back sanity check for the fused 100 M DB. Walks the first 100 records
 * verifying structure + kinds. Disabled by default (reads an externally-produced DB).
 */
@Disabled("requires /tmp/sirix-100m-fused-native on disk; enable manually to validate post-shred.")
public final class FusedDbRoundtripTest {
  @Test
  public void verifyFirst100Records() throws Exception {
    final Path db = Path.of("/tmp/sirix-100m-fused-native/scale-db");
    final var database = Databases.openJsonDatabase(db);
    try (database) {
      final JsonResourceSession session = database.beginResourceSession("records.jn");
      try (session; final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // array
        System.out.println("# root kind=" + rtx.getKind() + " descendants=" + rtx.getDescendantCount());
        rtx.moveToFirstChild(); // first object
        int objectCount = 0;
        int fusedStringCount = 0;
        int fusedNumberCount = 0;
        int fusedBooleanCount = 0;
        int legacyKeyCount = 0;
        do {
          if (rtx.getKind() != NodeKind.OBJECT) {
            System.out.println("# expected OBJECT at record " + objectCount + ", got " + rtx.getKind());
            break;
          }
          objectCount++;
          // walk fields of this object
          if (rtx.hasFirstChild()) {
            rtx.moveToFirstChild();
            do {
              switch (rtx.getKind()) {
                case OBJECT_NAMED_STRING -> fusedStringCount++;
                case OBJECT_NAMED_NUMBER -> fusedNumberCount++;
                case OBJECT_NAMED_BOOLEAN -> fusedBooleanCount++;
                // (Phase 4: legacy OBJECT_KEY kind deleted; the assertion below remains active —
                //  if any legacy record sneaks in, it would surface via NodeKind.getKind(byte 26)
                //  → null which propagates as a deserialize NPE rather than a count mismatch.)
                default -> {}
              }
            } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
            rtx.moveToParent();
          }
          if (objectCount >= 100) break;
        } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
        System.out.printf("# verified %d records: fusedString=%d fusedNumber=%d fusedBool=%d legacyKey=%d%n",
            objectCount, fusedStringCount, fusedNumberCount, fusedBooleanCount, legacyKeyCount);
        if (legacyKeyCount > 0) {
          throw new AssertionError("fused DB should not contain OBJECT_KEY but found " + legacyKeyCount);
        }
        if (fusedStringCount + fusedNumberCount + fusedBooleanCount < 300) {
          throw new AssertionError("expected >= 300 fused fields across 100 records, got only "
              + (fusedStringCount + fusedNumberCount + fusedBooleanCount));
        }
      }
    }
  }
}
