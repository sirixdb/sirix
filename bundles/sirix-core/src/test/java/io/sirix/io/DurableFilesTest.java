/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TXN-616 platform barrier: runs the real barrier of whichever platform executes it (directory
 * force on Linux, {@code F_FULLFSYNC} on macOS, {@code FlushFileBuffers} and {@code MoveFileExW}
 * write-through on Windows), and pins the failure contract: a barrier that cannot run fails, naming
 * the directory, and is never skipped.
 */
@Isolated
final class DurableFilesTest {

  @TempDir
  Path temp;

  @Test
  void barrierOnADirectoryCompletesAndIsReported() throws IOException {
    final Path directory = Files.createDirectory(temp.resolve("dir"));
    Files.createFile(directory.resolve("created"));
    final List<Path> barriers = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      DurableFiles.syncDirectory(directory);
    }
    assertEquals(List.of(directory), barriers);
  }

  @Test
  void barrierOnAMissingDirectoryFailsNamingIt() {
    final Path missing = temp.resolve("missing");
    final List<Path> barriers = new ArrayList<>();
    final SirixIOException failure;
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      failure = assertThrows(SirixIOException.class, () -> DurableFiles.syncDirectory(missing));
    }
    assertTrue(failure.getMessage().contains(missing.toString()), failure.getMessage());
    assertTrue(barriers.isEmpty(), "a failed barrier must not be reported as completed");
  }

  @Test
  void batchedBarriersSyncEachDirectoryOnce() throws IOException {
    final Path first = Files.createDirectory(temp.resolve("first"));
    final Path second = Files.createDirectory(temp.resolve("second"));
    final List<Path> barriers = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      DurableFiles.syncDirectories(List.of(first, second, first.resolve("..").resolve("first")));
    }
    assertEquals(List.of(first, second), barriers);
  }

  @Test
  void replaceSwapsContentsAndSyncsTheDirectory() throws IOException {
    final Path target = Files.writeString(temp.resolve("settings"), "old", StandardCharsets.UTF_8);
    final Path source = Files.writeString(temp.resolve("settings.tmp"), "new", StandardCharsets.UTF_8);
    final List<Path> barriers = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      DurableFiles.replace(source, target);
    }
    assertEquals("new", Files.readString(target, StandardCharsets.UTF_8));
    assertFalse(Files.exists(source));
    assertEquals(List.of(temp.toAbsolutePath()), barriers);
  }

  @Test
  void renameAcrossDirectoriesSyncsBoth() throws IOException {
    final Path from = Files.createDirectory(temp.resolve("from"));
    final Path to = Files.createDirectory(temp.resolve("to"));
    final Path moved = Files.createDirectory(from.resolve("resource"));
    Files.writeString(moved.resolve("file"), "content", StandardCharsets.UTF_8);
    final List<Path> barriers = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      DurableFiles.rename(moved, to.resolve("tombstone"));
    }
    assertEquals("content", Files.readString(to.resolve("tombstone").resolve("file"), StandardCharsets.UTF_8));
    assertFalse(Files.exists(moved));
    assertEquals(List.of(to.toAbsolutePath(), from.toAbsolutePath()), barriers);
  }

  @Test
  void renameRefusesAnExistingTargetAndChangesNothing() throws IOException {
    final Path source = Files.writeString(temp.resolve("source"), "source", StandardCharsets.UTF_8);
    final Path target = Files.writeString(temp.resolve("target"), "target", StandardCharsets.UTF_8);
    assertThrows(FileAlreadyExistsException.class, () -> DurableFiles.rename(source, target));
    assertEquals("source", Files.readString(source, StandardCharsets.UTF_8));
    assertEquals("target", Files.readString(target, StandardCharsets.UTF_8));
  }

  @Test
  void renameOfAMissingSourceFails() {
    assertThrows(NoSuchFileException.class, () -> DurableFiles.rename(temp.resolve("absent"), temp.resolve("target")));
  }

  @Test
  void createDirectoriesSyncsTheParentOfEveryNewDirectory() throws IOException {
    final Path leaf = temp.resolve("a").resolve("b").resolve("c");
    final List<Path> barriers = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      DurableFiles.createDirectories(leaf);
      DurableFiles.createDirectories(leaf);
    }
    assertTrue(Files.isDirectory(leaf));
    final Path a = temp.toAbsolutePath().resolve("a");
    assertEquals(List.of(temp.toAbsolutePath(), a, a.resolve("b")), barriers,
        "only the new names need a barrier, and an existing directory needs none");
  }

  @Test
  void forceFileForcesAnExistingFileAndNamesAMissingOne() throws IOException {
    DurableFiles.forceFile(Files.writeString(temp.resolve("file"), "bytes", StandardCharsets.UTF_8));
    final Path missing = temp.resolve("missing");
    final SirixIOException failure = assertThrows(SirixIOException.class, () -> DurableFiles.forceFile(missing));
    assertTrue(failure.getMessage().contains(missing.toString()), failure.getMessage());
  }

  @Test
  void forceFileForcesTheFilesOwnWritableChannelAndReportsIt() throws IOException {
    final Path file = Files.writeString(temp.resolve("file"), "bytes", StandardCharsets.UTF_8);
    final RecordingChannelOpener opener = new RecordingChannelOpener();
    final List<Path> forced = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(new DurableFiles.BarrierObserver() {
      @Override
      public void afterBarrier(final Path directory) {
        throw new AssertionError("a file force is not a directory barrier: " + directory);
      }

      @Override
      public void afterForce(final Path path) {
        forced.add(path);
      }
    })) {
      DurableFiles.forceFile(file, opener);
    }
    assertEquals(List.of("open " + file + " WRITE", "force " + file + " true", "close " + file), opener.calls);
    assertEquals(List.of(file), forced);
  }

  @Test
  void observationRestoresThePreviousObserver() throws IOException {
    final Path directory = Files.createDirectory(temp.resolve("dir"));
    final List<Path> outer = new ArrayList<>();
    final List<Path> inner = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(outer::add)) {
      try (final DurableFiles.Observation nested = DurableFiles.observeBarriers(inner::add)) {
        DurableFiles.syncDirectory(directory);
      }
      DurableFiles.syncDirectory(directory);
    }
    DurableFiles.syncDirectory(directory);
    assertEquals(List.of(directory), inner);
    assertEquals(List.of(directory), outer);
  }
}
