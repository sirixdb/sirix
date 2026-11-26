/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;
import io.sirix.utils.OS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * TXN-616 clauses 1 and 2: on POSIX systems the barrier is a force of a channel opened read-only on
 * the directory itself, and a durable rename forces each directory whose entries it changed.
 */
final class PosixDirectoryBarriersTest {

  @TempDir
  Path temp;

  private RecordingChannelOpener opener;
  private PosixDirectoryBarriers barriers;

  @BeforeEach
  void setUp() {
    assumeFalse(OS.isWindows(), "Windows cannot open a directory as a channel; WindowsDirectoryBarriersTest");
    opener = new RecordingChannelOpener();
    barriers = new PosixDirectoryBarriers(opener);
  }

  @Test
  void barrierForcesTheDirectoryItself() {
    barriers.syncDirectory(temp);
    assertEquals(List.of("open " + temp + " READ", "force " + temp + " true", "close " + temp), opener.calls);
  }

  @Test
  void barrierThatCannotOpenTheDirectoryFailsNamingIt() {
    final IOException cause = new IOException("refused");
    final PosixDirectoryBarriers failing = new PosixDirectoryBarriers((path, option) -> {
      throw cause;
    });
    final SirixIOException failure = assertThrows(SirixIOException.class, () -> failing.syncDirectory(temp));
    assertTrue(failure.getMessage().contains(temp.toString()), failure.getMessage());
    assertSame(cause, failure.getCause());
  }

  @Test
  void replaceForcesTheDirectoryOnce() throws IOException {
    final Path target = Files.writeString(temp.resolve("settings"), "old", StandardCharsets.UTF_8);
    final Path source = Files.writeString(temp.resolve("settings.tmp"), "new", StandardCharsets.UTF_8);
    barriers.move(source, target, true);
    assertEquals("new", Files.readString(target, StandardCharsets.UTF_8));
    assertEquals(List.of("open " + temp + " READ", "force " + temp + " true", "close " + temp), opener.calls);
  }

  @Test
  void renameAcrossDirectoriesForcesTargetThenSource() throws IOException {
    final Path from = Files.createDirectory(temp.resolve("from"));
    final Path to = Files.createDirectory(temp.resolve("to"));
    final Path moved = Files.createDirectory(from.resolve("resource"));
    barriers.move(moved, to.resolve("tombstone"), false);
    assertTrue(Files.isDirectory(to.resolve("tombstone")));
    assertEquals(List.of("open " + to + " READ", "force " + to + " true", "close " + to, "open " + from + " READ",
        "force " + from + " true", "close " + from), opener.calls);
  }

  @Test
  void renameOntoAnExistingNameIsRefusedWithoutForcing() throws IOException {
    final Path source = Files.writeString(temp.resolve("source"), "source", StandardCharsets.UTF_8);
    final Path target = Files.writeString(temp.resolve("target"), "target", StandardCharsets.UTF_8);
    assertThrows(FileAlreadyExistsException.class, () -> barriers.move(source, target, false));
    assertTrue(Files.exists(source));
    assertEquals("target", Files.readString(target, StandardCharsets.UTF_8));
    assertFalse(opener.calls.stream().anyMatch(call -> call.startsWith("force")), opener.calls.toString());
  }
}
