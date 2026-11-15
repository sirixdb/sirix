package io.sirix.format;

import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Store written by unchanged 7cd676bcd, with five indexes and three revisions. */
public final class LegacyPageReadersFixtureTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void baseStoreReadsEveryRevisionAndAppendsWithoutRewritingOldPages() throws Exception {
    final Path directory = temporary.getRoot().toPath().resolve("database");
    try (final InputStream resource = getClass().getResourceAsStream("/format/readers-base-7cd676bcd.zip");
        final ZipInputStream archive = new ZipInputStream(resource)) {
      ZipEntry entry;
      while ((entry = archive.getNextEntry()) != null) {
        final Path destination = directory.resolve(entry.getName()).normalize();
        assertTrue(destination.startsWith(directory));
        if (entry.isDirectory()) {
          Files.createDirectories(destination);
        } else {
          Files.createDirectories(destination.getParent());
          Files.copy(archive, destination);
        }
      }
    }
    final Path data = directory.resolve("resources/fixture/data/sirix.data");
    final byte[] before = Files.readAllBytes(data);
    int usedEnd = before.length;
    while (usedEnd > 12_288 && before[usedEnd - 1] == 0) {
      usedEnd--;
    }
    try (final var database = Databases.openJsonDatabase(directory);
        final var session = database.beginResourceSession("fixture")) {
      assertTrue(session.getResourceConfig().verifyChecksumsOnRead);
      assertEquals(3, session.getMostRecentRevisionNumber());
      final String[] expected = {"first", "revised", "third"};
      for (int revision = 1; revision <= 3; revision++) {
        try (final var trx = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(expected[revision - 1], firstString(trx));
          // Walk the rest as well, forcing every document page through checksum verification.
          trx.moveToDocumentRoot();
          final DescendantAxis axis = new DescendantAxis(trx);
          while (axis.hasNext()) {
            axis.nextLong();
          }
        }
      }
      try (final var trx = session.beginNodeTrx()) {
        assertEquals("third", firstString(trx));
        trx.setStringValue("fourth");
        trx.commit();
      }
      assertEquals(4, session.getMostRecentRevisionNumber());
    }
    final byte[] after = Files.readAllBytes(data);
    // Superblock/beacons occupy the first 12 KiB and intentionally change at commit.
    assertArrayEquals(Arrays.copyOfRange(before, 12_288, usedEnd), Arrays.copyOfRange(after, 12_288, usedEnd));
    try (final var database = Databases.openJsonDatabase(directory);
        final var session = database.beginResourceSession("fixture");
        final var current = session.beginNodeReadOnlyTrx(4);
        final var original = session.beginNodeReadOnlyTrx(1)) {
      assertEquals("fourth", firstString(current));
      assertEquals("first", firstString(original));
    }
  }

  private static String firstString(final JsonNodeReadOnlyTrx trx) {
    trx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(trx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (trx.isStringValue()) {
        return trx.getValue();
      }
    }
    throw new AssertionError("Fixture contains no string value");
  }
}
