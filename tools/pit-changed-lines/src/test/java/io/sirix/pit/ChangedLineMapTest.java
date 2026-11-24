package io.sirix.pit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.pit.ChangedLineMap.ChangedLineMapException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The changed-line map the lane writes, read as CONV-205 requires: a line is in scope only when the
 * map lists its source file with that line, and a map this reader cannot fully understand fails
 * instead of filtering on a guess.
 */
final class ChangedLineMapTest {

  @TempDir
  private Path directory;

  private Path write(final String... records) throws IOException {
    final Path path = directory.resolve("changed-lines.tsv");
    Files.writeString(path, String.join("\n", records) + '\n');
    return path;
  }

  @Test
  void listsOnlyTheLinesItWasGiven() throws IOException {
    final ChangedLineMap map =
        ChangedLineMap.read(write(ChangedLineMap.HEADER, "io.sirix.api\tNodeTrx.java\t24 36 120"));

    assertTrue(map.contains("io.sirix.api", "NodeTrx.java", 24));
    assertTrue(map.contains("io.sirix.api", "NodeTrx.java", 36));
    assertTrue(map.contains("io.sirix.api", "NodeTrx.java", 120));
    assertFalse(map.contains("io.sirix.api", "NodeTrx.java", 23), "line before the first");
    assertFalse(map.contains("io.sirix.api", "NodeTrx.java", 30), "line between two changed ones");
    assertFalse(map.contains("io.sirix.api", "NodeTrx.java", 121), "line after the last");
    assertEquals(1, map.sources());
  }

  @Test
  void findsALineWhateverOrderTheRecordListsThem() throws IOException {
    // The format does not promise ascending lines, and a lookup is a binary search.
    final ChangedLineMap map = ChangedLineMap.read(write(ChangedLineMap.HEADER, "p\tA.java\t36 24 120 7"));

    for (final int line : new int[] {7, 24, 36, 120}) {
      assertTrue(map.contains("p", "A.java", line), "line " + line);
    }
    assertFalse(map.contains("p", "A.java", 8));
    assertFalse(map.contains("p", "A.java", 119));
  }

  @Test
  void separatesSourcesByPackageAndFileName() throws IOException {
    final ChangedLineMap map = ChangedLineMap.read(write(ChangedLineMap.HEADER, "io.sirix.api\tNodeTrx.java\t24",
        "io.sirix.api.xml\tNodeTrx.java\t36", "\tRoot.java\t7"));

    assertTrue(map.contains("io.sirix.api", "NodeTrx.java", 24));
    assertFalse(map.contains("io.sirix.api.xml", "NodeTrx.java", 24),
        "a line of one package's file is not a line of another's");
    assertTrue(map.contains("io.sirix.api.xml", "NodeTrx.java", 36));
    assertTrue(map.contains("", "Root.java", 7), "the default package reads as an empty field");
    assertFalse(map.contains("io.sirix.api", "Other.java", 24), "a file the map does not list");
    assertEquals(3, map.sources());
  }

  @Test
  void acceptsASourceWithoutChangedLines() throws IOException {
    final ChangedLineMap map =
        ChangedLineMap.read(write(ChangedLineMap.HEADER, "io.sirix.api\tNodeTrx.java\t", "io.sirix\tFoo.java\t3"));

    assertFalse(map.contains("io.sirix.api", "NodeTrx.java", 1),
        "a renamed-only file contributes no changed line, so none of its mutants is in scope");
    assertTrue(map.contains("io.sirix", "Foo.java", 3));
  }

  @Test
  void rejectsAnAbsentMap() {
    final Path missing = directory.resolve("absent.tsv");

    final ChangedLineMapException error =
        assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(missing));

    final String message = error.getMessage();
    assertNotNull(message);
    assertTrue(message.contains("cannot read"), message);
  }

  @Test
  void rejectsAMapWithoutTheKnownHeader() throws IOException {
    final Path path = write("# sirix-changed-lines 2", "io.sirix\tFoo.java\t3");

    assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(path));
  }

  @Test
  void rejectsAnEmptyMap() throws IOException {
    final Path path = write(ChangedLineMap.HEADER);

    final ChangedLineMapException error = assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(path));

    final String message = error.getMessage();
    assertNotNull(message);
    assertTrue(message.contains("lists no source"), message);
  }

  @Test
  void rejectsARecordWithTheWrongNumberOfFields() throws IOException {
    final Path path = write(ChangedLineMap.HEADER, "io.sirix\tFoo.java");

    final ChangedLineMapException error = assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(path));

    final String message = error.getMessage();
    assertNotNull(message);
    assertTrue(message.contains("at line 2"), message);
  }

  @Test
  void rejectsAnEmptyFileName() throws IOException {
    final Path path = write(ChangedLineMap.HEADER, "io.sirix\t\t3");

    assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(path));
  }

  @Test
  void rejectsALineNumberThatIsNotAPositiveInteger() throws IOException {
    for (final String lines : new String[] {"3 x", "0", "-4"}) {
      final Path path = write(ChangedLineMap.HEADER, "io.sirix\tFoo.java\t" + lines);

      assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(path), "lines: " + lines);
    }
  }

  @Test
  void rejectsTheSameSourceTwice() throws IOException {
    final Path path = write(ChangedLineMap.HEADER, "io.sirix\tFoo.java\t3", "io.sirix\tFoo.java\t4");

    final ChangedLineMapException error = assertThrows(ChangedLineMapException.class, () -> ChangedLineMap.read(path));

    final String message = error.getMessage();
    assertNotNull(message);
    assertTrue(message.contains("duplicate source"), message);
  }
}
