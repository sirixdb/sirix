/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;
import io.sirix.crash.DirectoryCrashModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mockStatic;

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void crossDirectoryMovesAreRefusedWithoutChangingNames(final boolean replace) throws IOException {
    final Path from = Files.createDirectory(temp.resolve("from"));
    final Path to = Files.createDirectory(temp.resolve("to"));
    final Path source = Files.writeString(from.resolve("source"), "source");
    final Path target = Files.writeString(to.resolve("target"), "target");
    assertThrows(IllegalArgumentException.class, () -> {
      if (replace) {
        DurableFiles.replace(source, target);
      } else {
        DurableFiles.rename(source, target);
      }
    });
    assertEquals("source", Files.readString(source));
    assertEquals("target", Files.readString(target));
  }

  @Test
  void siblingRenamePreservesTheDirectoryContents() throws IOException {
    final Path source = Files.createDirectory(temp.resolve("resource"));
    Files.writeString(source.resolve("file"), "content");
    final Path target = temp.resolve("tombstone");
    final List<Path> barriers = new ArrayList<>();
    try (final DurableFiles.Observation ignored = DurableFiles.observeBarriers(barriers::add)) {
      DurableFiles.rename(source, target);
    }
    assertFalse(Files.exists(source));
    assertEquals("content", Files.readString(target.resolve("file")));
    assertEquals(List.of(temp), barriers);
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
    assertEquals(List.of(temp.toAbsolutePath(), a, a.resolve("b"), a.resolve("b")), barriers,
        "each call must make the accepted leaf name durable");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void directoryCreationRetrySurvivesAnAncestorBarrierFailure(final int failedAncestor) throws IOException {
    final Path root = Files.createDirectory(temp.resolve("fs"));
    assumeTrue(DirectoryCrashModel.supported(root));
    final Path leaf = root.resolve("a/b/c");
    final Path directory = switch (failedAncestor) {
      case 0 -> root;
      case 1 -> root.resolve("a");
      default -> root.resolve("a/b");
    };
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, temp.resolve("links"))) {
      try (final MockedStatic<DurableFiles> ignored = mockStatic(DurableFiles.class, invocation -> {
        if (invocation.getMethod().getName().equals("syncDirectory") && directory.equals(invocation.getArgument(0))) {
          throw DurableFiles.barrierFailure(directory, new IOException("directory force refused"));
        }
        return invocation.callRealMethod();
      })) {
        assertThrows(SirixIOException.class, () -> DurableFiles.createDirectories(leaf));
      }
      DurableFiles.createDirectories(leaf);
      model.materialize(temp.resolve("crashed"));
    }
    assertTrue(Files.isDirectory(temp.resolve("crashed/a/b/c")),
        "a successful retry must publish the complete directory chain");
  }

  @Test
  void existingDirectoryRetryMakesItsNameDurable() throws IOException {
    final Path root = Files.createDirectory(temp.resolve("fs"));
    assumeTrue(DirectoryCrashModel.supported(root));
    final Path leaf = root.resolve("existing");
    try (final DirectoryCrashModel model = DirectoryCrashModel.start(root, temp.resolve("links"))) {
      Files.createDirectory(leaf);
      try (final MockedStatic<DurableFiles> ignored = mockStatic(DurableFiles.class, invocation -> {
        if (invocation.getMethod().getName().equals("syncDirectory") && root.equals(invocation.getArgument(0))) {
          throw DurableFiles.barrierFailure(root, new IOException("directory force refused"));
        }
        return invocation.callRealMethod();
      })) {
        assertThrows(SirixIOException.class, () -> DurableFiles.createDirectories(leaf));
      }
      DurableFiles.createDirectories(leaf);
      model.materialize(temp.resolve("crashed"));
    }
    assertTrue(Files.isDirectory(temp.resolve("crashed/existing")));
  }

  /**
   * TXN-616: rollback errors must not replace the directory barrier's original failure. Inject at the
   * shared barrier boundary: Windows uses kernel32 rather than a directory FileChannel.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedCreationRetainsBothBarrierAndCleanupFailures(final boolean directory) {
    final Path entry = temp.resolve("entry");
    final IOException barrierFailure = new IOException("directory force refused");
    final IOException cleanupFailure = new IOException("entry deletion refused");
    try (final MockedStatic<DurableFiles> barriers = mockStatic(DurableFiles.class, invocation -> {
      if (invocation.getMethod().getName().equals("syncDirectory") && temp.equals(invocation.getArgument(0))) {
        throw DurableFiles.barrierFailure(temp, barrierFailure);
      }
      return invocation.callRealMethod();
    }); final MockedStatic<Files> files = mockStatic(Files.class, invocation -> {
      if (invocation.getMethod().getName().equals("delete") && entry.equals(invocation.getArgument(0))) {
        throw cleanupFailure;
      }
      return invocation.callRealMethod();
    })) {
      final SirixIOException failure = assertThrows(SirixIOException.class, () -> {
        if (directory) {
          DurableFiles.createDirectories(entry);
        } else {
          DurableFiles.createFile(entry);
        }
      });
      assertEquals(barrierFailure, failure.getCause());
      assertEquals(List.of(cleanupFailure), List.of(failure.getSuppressed()));
      assertTrue(Files.exists(entry), "the refused cleanup leaves the entry for a later attempt");
    }
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
