/*
 * Copyright (c) 2026, Sirix.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 */
package io.sirix.io.filechannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * Best-effort Linux file-access and bounded read-ahead advice.
 *
 * <p>
 * Range advice overlaps reads for already requested offsets with decoding earlier pages. It does
 * not read page objects, bypass checksum verification, or change the channel position. Whole-file
 * sequential and random advice are separate opt-in policies.
 *
 * <p>
 * The file descriptor has to be extracted from the {@link FileChannel}. Java does not expose the fd
 * publicly; we use {@code sun.misc.Unsafe} field-offset reads rather than
 * {@code setAccessible(true)} so we don't need a runtime
 * {@code --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED}
 * flag. Unsafe still works in Java 25 (deprecated for removal but not removed).
 *
 * <p>
 * Safe under all error paths: any throwable is swallowed and the advice is treated as a no-op.
 * Sirix continues to function correctly without the hint, just with standard kernel readahead
 * behaviour.
 */
final class PosixFadvise {

  private static final Logger LOGGER = LoggerFactory.getLogger(PosixFadvise.class);

  private static final int POSIX_FADV_RANDOM = 1;
  private static final int POSIX_FADV_SEQUENTIAL = 2;
  private static final int POSIX_FADV_WILLNEED = 3;

  private static final MemorySegment POSIX_FADVISE;
  private static final boolean SUPPORTED;

  /**
   * Constant call adapter; the platform symbol and file descriptors remain runtime state. Native
   * Image initializes this holder at run time by default, because the 25.0.x LTS line rejects a
   * build-time downcall handle with a {@code linkToNative} compilation error. An optimized build on a
   * GraalVM 25 innovation release 25.1.3 or later may preinitialize this pure adapter; see
   * {@code docs/NATIVE_IMAGE.md}.
   */
  private static final class AdviceCall {
    private static final MethodHandle HANDLE =
        Linker.nativeLinker().downcallHandle(FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_INT));
  }

  /** True once we've logged the "disabled" diagnostic to avoid log spam. */
  private static volatile boolean loggedDisabled = false;

  /** True once we've logged a fadvise success to avoid log spam. */
  private static volatile boolean loggedEnabled = false;

  /**
   * {@code sun.misc.Unsafe} handle. Used to read private fields on {@link FileDescriptor} and
   * {@code sun.nio.ch.FileChannelImpl} without tripping the module access check that reflection
   * {@code setAccessible(true)} requires. Deprecated for removal but still present in Java 25.
   */
  private static final Object UNSAFE;

  /** Byte offset of {@link FileDescriptor#fd} in the object layout. */
  private static final long FD_INT_OFFSET;

  /** Byte offset of {@code sun.nio.ch.FileChannelImpl#fd} in the object layout. */
  private static final long CHANNEL_FD_OFFSET;

  /** {@code Unsafe.getInt(Object, long)}. */
  private static final Method UNSAFE_GET_INT;

  /** {@code Unsafe.getObject(Object, long)}. */
  private static final Method UNSAFE_GET_OBJECT;
  private static final Class<?> CHANNEL_IMPL;

  static {
    boolean supported = false;
    MemorySegment h = null;

    final String os = System.getProperty("os.name", "").toLowerCase();
    if (os.contains("linux")) {
      try {
        final Linker linker = Linker.nativeLinker();
        h = linker.defaultLookup()
                  .find("posix_fadvise")
                  .orElseThrow(() -> new RuntimeException("posix_fadvise not found"));
        AdviceCall.HANDLE.type();
        supported = true;
      } catch (final Throwable t) {
        LOGGER.debug("posix_fadvise not available on this Linux: {}", t.toString());
      }
    }

    POSIX_FADVISE = h;
    SUPPORTED = supported;

    Object unsafe = null;
    long fdIntOffset = -1L;
    long channelFdOffset = -1L;
    Method getInt = null;
    Method getObject = null;
    Class<?> channelImpl = null;
    try {
      final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      final Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      unsafe = theUnsafe.get(null);
      final Method objectFieldOffset = unsafeClass.getMethod("objectFieldOffset", Field.class);
      getInt = unsafeClass.getMethod("getInt", Object.class, long.class);
      getObject = unsafeClass.getMethod("getObject", Object.class, long.class);

      final Field fdField = FileDescriptor.class.getDeclaredField("fd");
      fdIntOffset = (long) objectFieldOffset.invoke(unsafe, fdField);

      final Class<?> chanImpl = Class.forName("sun.nio.ch.FileChannelImpl");
      final Field chanFdField = chanImpl.getDeclaredField("fd");
      channelFdOffset = (long) objectFieldOffset.invoke(unsafe, chanFdField);
      channelImpl = chanImpl;
    } catch (final Throwable t) {
      LOGGER.debug("Unsafe-based FD reflection unavailable: {}", t.toString());
      unsafe = null;
      fdIntOffset = -1L;
      channelFdOffset = -1L;
      getInt = null;
      getObject = null;
    }
    UNSAFE = unsafe;
    FD_INT_OFFSET = fdIntOffset;
    CHANNEL_FD_OFFSET = channelFdOffset;
    UNSAFE_GET_INT = getInt;
    UNSAFE_GET_OBJECT = getObject;
    CHANNEL_IMPL = channelImpl;
  }

  private PosixFadvise() {
    // utility
  }

  /**
   * Hint {@code POSIX_FADV_SEQUENTIAL} on the whole file behind {@code channel}.
   *
   * <p>
   * Best-effort — returns silently on any failure. Callers must not depend on the advice taking
   * effect.
   */
  static void adviseSequential(final FileChannel channel) {
    final int rawFd = extractFd(channel);
    if (rawFd < 0) {
      return;
    }
    try {
      // (int fd, off_t offset = 0, off_t len = 0 = "whole file", int advice)
      final int rc = (int) AdviceCall.HANDLE.invokeExact(POSIX_FADVISE, rawFd, 0L, 0L, POSIX_FADV_SEQUENTIAL);
      if (rc == 0) {
        logEnabledOnce();
      } else {
        LOGGER.debug("posix_fadvise SEQUENTIAL returned rc={} for fd={}", rc, rawFd);
      }
    } catch (final Throwable t) {
      LOGGER.debug("posix_fadvise SEQUENTIAL failed: {}", t.toString());
    }
  }

  /**
   * Hint {@code POSIX_FADV_RANDOM} on the whole file behind {@code channel}. Use for files that are
   * primarily seeked rather than scanned linearly. Best-effort.
   */
  static void adviseRandom(final FileChannel channel) {
    final int rawFd = extractFd(channel);
    if (rawFd < 0) {
      return;
    }
    try {
      final int rc = (int) AdviceCall.HANDLE.invokeExact(POSIX_FADVISE, rawFd, 0L, 0L, POSIX_FADV_RANDOM);
      if (rc != 0) {
        LOGGER.debug("posix_fadvise RANDOM returned rc={} for fd={}", rc, rawFd);
      }
    } catch (final Throwable t) {
      LOGGER.debug("posix_fadvise RANDOM failed: {}", t.toString());
    }
  }

  /**
   * Extract the raw integer file descriptor from a {@link FileChannel}. Returns {@code -1} if the
   * platform, JDK version, or strict module enforcement prevents it.
   */
  static int extractFd(final FileChannel channel) {
    if (!SUPPORTED || channel == null || UNSAFE == null || FD_INT_OFFSET < 0 || CHANNEL_FD_OFFSET < 0
        || UNSAFE_GET_INT == null || UNSAFE_GET_OBJECT == null || CHANNEL_IMPL == null
        || !CHANNEL_IMPL.isInstance(channel) || !channel.isOpen()) {
      logDisabledOnce();
      return -1;
    }
    try {
      final Object fdObj = UNSAFE_GET_OBJECT.invoke(UNSAFE, channel, CHANNEL_FD_OFFSET);
      if (!(fdObj instanceof FileDescriptor fd)) {
        return -1;
      }
      final int raw = (int) UNSAFE_GET_INT.invoke(UNSAFE, fd, FD_INT_OFFSET);
      return raw;
    } catch (final Throwable t) {
      LOGGER.debug("extractFd failed: {}", t.toString());
      return -1;
    }
  }

  /** Best-effort bounded read-ahead; never changes the channel position or stages page objects. */
  static boolean adviseWillNeed(final int fd, final long offset, final long length) {
    if (!SUPPORTED || fd < 0 || offset < 0 || length <= 0 || offset > Long.MAX_VALUE - length) {
      return false;
    }
    try {
      final int rc = (int) AdviceCall.HANDLE.invokeExact(POSIX_FADVISE, fd, offset, length, POSIX_FADV_WILLNEED);
      return rc == 0;
    } catch (final Throwable failure) {
      return false;
    }
  }

  private static void logDisabledOnce() {
    if (!loggedDisabled) {
      loggedDisabled = true;
      LOGGER.debug("posix_fadvise disabled (not Linux, Unsafe unavailable, or FFI unavailable)");
      if (Boolean.getBoolean("sirix.fadvise.diag")) {
        System.err.println("# posix_fadvise DISABLED — SUPPORTED=" + SUPPORTED + " UNSAFE=" + (UNSAFE != null)
            + " FD_INT_OFFSET=" + FD_INT_OFFSET + " CHANNEL_FD_OFFSET=" + CHANNEL_FD_OFFSET);
      }
    }
  }

  private static void logEnabledOnce() {
    if (!loggedEnabled) {
      loggedEnabled = true;
      LOGGER.info("posix_fadvise SEQUENTIAL hint applied to sirix.data file channel");
      if (Boolean.getBoolean("sirix.fadvise.diag")) {
        System.err.println("# posix_fadvise SEQUENTIAL hint applied to sirix.data file channel");
      }
    }
  }
}
