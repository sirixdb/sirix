/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TXN-616 clause 3 on Windows, run on every platform: the Windows barriers drive kernel32 through
 * {@link Kernel32Calls}, here bound to a Java fake that records each call. The real bindings run in
 * {@link DurableFilesTest} on the Windows CI lane.
 */
final class WindowsDirectoryBarriersTest {

  private static final long HANDLE = 0x1234L;

  @TempDir
  Path temp;

  /** A kernel32 stand-in with the downcalls' exact method types. */
  static final class FakeKernel32 {
    static final MemoryLayout STATE = MemoryLayout.structLayout(ValueLayout.JAVA_INT.withName("GetLastError"));
    static final VarHandle LAST_ERROR = STATE.varHandle(MemoryLayout.PathElement.groupElement("GetLastError"));

    final List<String> calls = new ArrayList<>();
    long createResult = HANDLE;
    int createError;
    int flushResult = 1;
    int flushError;
    int moveResult = 1;
    int moveError;
    RuntimeException throwFromCreate;
    RuntimeException throwFromMove;

    MemorySegment createFileW(final MemorySegment state, final MemorySegment path, final int access, final int share,
        final MemorySegment security, final int disposition, final int flags, final MemorySegment template) {
      if (throwFromCreate != null) {
        throw throwFromCreate;
      }
      calls.add("CreateFileW " + text(path) + " access=" + Integer.toHexString(access) + " share="
          + Integer.toHexString(share) + " security=" + security.address() + " disposition=" + disposition + " flags="
          + Integer.toHexString(flags) + " template=" + template.address());
      LAST_ERROR.set(state, 0L, createError);
      return MemorySegment.ofAddress(createResult);
    }

    int flushFileBuffers(final MemorySegment state, final MemorySegment handle) {
      calls.add("FlushFileBuffers " + Long.toHexString(handle.address()));
      LAST_ERROR.set(state, 0L, flushError);
      return flushResult;
    }

    int closeHandle(final MemorySegment handle) {
      calls.add("CloseHandle " + Long.toHexString(handle.address()));
      return 1;
    }

    int moveFileExW(final MemorySegment state, final MemorySegment from, final MemorySegment to, final int flags) {
      if (throwFromMove != null) {
        throw throwFromMove;
      }
      calls.add("MoveFileExW " + text(from) + " -> " + text(to) + " flags=" + Integer.toHexString(flags));
      LAST_ERROR.set(state, 0L, moveError);
      return moveResult;
    }

    Kernel32Calls bind() throws ReflectiveOperationException {
      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      final MethodHandle create = lookup
                                        .findVirtual(FakeKernel32.class, "createFileW",
                                            MethodType.methodType(MemorySegment.class, MemorySegment.class,
                                                MemorySegment.class, int.class, int.class, MemorySegment.class,
                                                int.class, int.class, MemorySegment.class))
                                        .bindTo(this);
      final MethodHandle flush = lookup
                                       .findVirtual(FakeKernel32.class, "flushFileBuffers",
                                           MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class))
                                       .bindTo(this);
      final MethodHandle close =
          lookup.findVirtual(FakeKernel32.class, "closeHandle", MethodType.methodType(int.class, MemorySegment.class))
                .bindTo(this);
      final MethodHandle move = lookup
                                      .findVirtual(FakeKernel32.class, "moveFileExW",
                                          MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class,
                                              MemorySegment.class, int.class))
                                      .bindTo(this);
      return new Kernel32Calls(create, flush, close, move, STATE, LAST_ERROR);
    }

    private static String text(final MemorySegment wide) {
      return wide.getString(0, StandardCharsets.UTF_16LE);
    }
  }

  private static String win32(final Path path) {
    return WindowsDirectoryBarriers.win32Path(path.toAbsolutePath().normalize().toString());
  }

  @Test
  void directoryBarrierFlushesABackupSemanticsWriteHandleAndClosesIt() throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    new WindowsDirectoryBarriers(fake.bind()).syncDirectory(temp);
    assertEquals(List.of("CreateFileW " + win32(temp) + " access=40000000 share=7 security=0 disposition=3"
        + " flags=2000000 template=0", "FlushFileBuffers 1234", "CloseHandle 1234"), fake.calls);
  }

  @Test
  void directoryThatCannotBeOpenedFailsTheBarrierNamingIt() throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    fake.createResult = WindowsDirectoryBarriers.INVALID_HANDLE_VALUE;
    fake.createError = WindowsDirectoryBarriers.ERROR_PATH_NOT_FOUND;
    final SirixIOException failure =
        assertThrows(SirixIOException.class, () -> new WindowsDirectoryBarriers(fake.bind()).syncDirectory(temp));
    assertTrue(failure.getMessage().contains(temp.toString()), failure.getMessage());
    assertInstanceOf(NoSuchFileException.class, failure.getCause());
    assertTrue(failure.getCause().getMessage().contains("CreateFileW failed with Windows error 3"),
        failure.getCause().getMessage());
    assertEquals(1, fake.calls.size(), "nothing is flushed or closed without a handle: " + fake.calls);
  }

  @Test
  void rejectedFlushFailsTheBarrierAndStillClosesTheHandle() throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    fake.flushResult = 0;
    fake.flushError = WindowsDirectoryBarriers.ERROR_ACCESS_DENIED;
    final SirixIOException failure =
        assertThrows(SirixIOException.class, () -> new WindowsDirectoryBarriers(fake.bind()).syncDirectory(temp));
    assertTrue(failure.getMessage().contains(temp.toString()), failure.getMessage());
    assertInstanceOf(AccessDeniedException.class, failure.getCause());
    assertTrue(failure.getCause().getMessage().contains("FlushFileBuffers failed with Windows error 5"),
        failure.getCause().getMessage());
    assertEquals("CloseHandle 1234", fake.calls.getLast());
  }

  @Test
  void downcallFailureFailsTheBarrierNamingTheDirectory() throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    final IllegalStateException cause = new IllegalStateException("linker");
    fake.throwFromCreate = cause;
    final SirixIOException failure =
        assertThrows(SirixIOException.class, () -> new WindowsDirectoryBarriers(fake.bind()).syncDirectory(temp));
    assertTrue(failure.getMessage().contains(temp.toString()), failure.getMessage());
    assertSame(cause, failure.getCause());
  }

  @Test
  void replaceIsAWriteThroughMoveThatReplaces() throws IOException, ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    final Path source = temp.resolve("settings.tmp");
    final Path target = temp.resolve("settings");
    new WindowsDirectoryBarriers(fake.bind()).move(source, target, true);
    assertEquals(List.of("MoveFileExW " + win32(source) + " -> " + win32(target) + " flags=9"), fake.calls);
  }

  @Test
  void renameIsAWriteThroughMoveThatNeverReplaces() throws IOException, ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    final Path source = temp.resolve("resource");
    final Path target = temp.resolve("tombstone");
    new WindowsDirectoryBarriers(fake.bind()).move(source, target, false);
    assertEquals(List.of("MoveFileExW " + win32(source) + " -> " + win32(target) + " flags=8"), fake.calls);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void crossDirectoryMovesAreRefusedBeforeCallingKernel32(final boolean replace) throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    final WindowsDirectoryBarriers barriers = new WindowsDirectoryBarriers(fake.bind());
    assertThrows(IllegalArgumentException.class,
        () -> barriers.move(temp.resolve("from/source"), temp.resolve("to/target"), replace));
    assertTrue(fake.calls.isEmpty());
  }

  @Test
  void failedMovesSurfaceAsTheExceptionsFilesMoveWouldThrow() throws ReflectiveOperationException {
    assertMoveFailure(WindowsDirectoryBarriers.ERROR_FILE_NOT_FOUND, NoSuchFileException.class);
    assertMoveFailure(WindowsDirectoryBarriers.ERROR_PATH_NOT_FOUND, NoSuchFileException.class);
    assertMoveFailure(WindowsDirectoryBarriers.ERROR_ACCESS_DENIED, AccessDeniedException.class);
    assertMoveFailure(WindowsDirectoryBarriers.ERROR_FILE_EXISTS, FileAlreadyExistsException.class);
    assertMoveFailure(WindowsDirectoryBarriers.ERROR_ALREADY_EXISTS, FileAlreadyExistsException.class);
    // A sharing violation (32) stays a plain FileSystemException, which settings replacement retries.
    assertMoveFailure(32, FileSystemException.class);
  }

  private void assertMoveFailure(final int error, final Class<? extends FileSystemException> expected)
      throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    fake.moveResult = 0;
    fake.moveError = error;
    final Path source = temp.resolve("source");
    final Path target = temp.resolve("target");
    final FileSystemException failure = assertThrows(FileSystemException.class,
        () -> new WindowsDirectoryBarriers(fake.bind()).move(source, target, false));
    assertEquals(expected, failure.getClass());
    assertEquals(source.toString(), failure.getFile());
    assertEquals(target.toString(), failure.getOtherFile());
    assertTrue(failure.getReason().contains("MoveFileExW failed with Windows error " + error), failure.getReason());
  }

  @Test
  void moveDowncallFailureIsAnIOException() throws ReflectiveOperationException {
    final FakeKernel32 fake = new FakeKernel32();
    final IllegalStateException cause = new IllegalStateException("linker");
    fake.throwFromMove = cause;
    final IOException failure = assertThrows(IOException.class,
        () -> new WindowsDirectoryBarriers(fake.bind()).move(temp.resolve("a"), temp.resolve("b"), true));
    assertSame(cause, failure.getCause());
  }

  @Test
  void longPathsUseTheExtendedLengthSpelling() {
    final String plain = "C:\\" + "d".repeat(WindowsDirectoryBarriers.MAX_PLAIN_PATH - 3);
    assertEquals(WindowsDirectoryBarriers.MAX_PLAIN_PATH, plain.length());
    assertEquals(plain, WindowsDirectoryBarriers.win32Path(plain));
    final String longer = plain + "e";
    assertEquals("\\\\?\\" + longer, WindowsDirectoryBarriers.win32Path(longer));
    final String unc = "\\\\server\\share\\" + "d".repeat(WindowsDirectoryBarriers.MAX_PLAIN_PATH);
    assertEquals("\\\\?\\UNC\\server\\share\\" + "d".repeat(WindowsDirectoryBarriers.MAX_PLAIN_PATH),
        WindowsDirectoryBarriers.win32Path(unc));
    final String prefixed = "\\\\?\\" + longer;
    assertEquals(prefixed, WindowsDirectoryBarriers.win32Path(prefixed));
  }
}
