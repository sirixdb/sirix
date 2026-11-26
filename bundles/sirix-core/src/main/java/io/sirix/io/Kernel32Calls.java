/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.util.Objects.requireNonNull;

/**
 * The kernel32 functions {@link WindowsDirectoryBarriers} calls, as method handles with the types
 * of the FFM downcalls (FFM, no JNI). Every handle except {@code closeHandle} takes the call-state
 * segment first, from which {@code lastError} reads {@code GetLastError}.
 *
 * <p>
 * {@link Native#KERNEL32} binds the real functions and is initialized only on Windows. Tests on any
 * platform construct an instance from Java methods of the same types, which is how the Windows
 * logic is exercised where kernel32 does not exist.
 */
final class Kernel32Calls {

  /**
   * {@code HANDLE CreateFileW(LPCWSTR, DWORD, DWORD, LPSECURITY_ATTRIBUTES, DWORD, DWORD, HANDLE)}:
   * {@code (MemorySegment state, MemorySegment path, int access, int share, MemorySegment security,
   * int disposition, int flags, MemorySegment template) -> MemorySegment}.
   */
  final MethodHandle createFileW;
  /**
   * {@code BOOL FlushFileBuffers(HANDLE)}:
   * {@code (MemorySegment state, MemorySegment handle) -> int}.
   */
  final MethodHandle flushFileBuffers;
  /** {@code BOOL CloseHandle(HANDLE)}: {@code (MemorySegment handle) -> int}. */
  final MethodHandle closeHandle;
  /**
   * {@code BOOL MoveFileExW(LPCWSTR, LPCWSTR, DWORD)}:
   * {@code (MemorySegment state, MemorySegment from, MemorySegment to, int flags) -> int}.
   */
  final MethodHandle moveFileExW;
  /** The layout of the call-state segment. */
  final MemoryLayout stateLayout;
  /**
   * Reads the int {@code GetLastError} member of a call-state segment: {@code (MemorySegment, long)}.
   */
  final VarHandle lastError;

  Kernel32Calls(final MethodHandle createFileW, final MethodHandle flushFileBuffers, final MethodHandle closeHandle,
      final MethodHandle moveFileExW, final MemoryLayout stateLayout, final VarHandle lastError) {
    this.createFileW = requireNonNull(createFileW);
    this.flushFileBuffers = requireNonNull(flushFileBuffers);
    this.closeHandle = requireNonNull(closeHandle);
    this.moveFileExW = requireNonNull(moveFileExW);
    this.stateLayout = requireNonNull(stateLayout);
    this.lastError = requireNonNull(lastError);
  }

  /** The real kernel32 bindings; referencing this class outside Windows fails its initialization. */
  static final class Native {

    static final Kernel32Calls KERNEL32;

    static {
      final Linker linker = Linker.nativeLinker();
      final SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32.dll", Arena.global());
      final Linker.Option captureLastError = Linker.Option.captureCallState("GetLastError");
      final MemoryLayout stateLayout = Linker.Option.captureStateLayout();
      KERNEL32 = new Kernel32Calls(linker.downcallHandle(kernel32.find("CreateFileW").orElseThrow(),
          FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS),
          captureLastError),
          linker.downcallHandle(kernel32.find("FlushFileBuffers").orElseThrow(),
              FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS), captureLastError),
          linker.downcallHandle(kernel32.find("CloseHandle").orElseThrow(),
              FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)),
          linker.downcallHandle(kernel32.find("MoveFileExW").orElseThrow(),
              FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                  ValueLayout.JAVA_INT),
              captureLastError),
          stateLayout, stateLayout.varHandle(MemoryLayout.PathElement.groupElement("GetLastError")));
    }

    private Native() {}
  }
}
