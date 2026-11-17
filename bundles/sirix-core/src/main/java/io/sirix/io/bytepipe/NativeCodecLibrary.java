/* Copyright (c) 2026, SirixDB. All rights reserved. */
package io.sirix.io.bytepipe;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.lang.foreign.Arena;
import java.lang.foreign.Linker.Option;
import java.lang.foreign.SymbolLookup;
import java.security.MessageDigest;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared, content-addressed loading of the bundled native codec adapters. */
public final class NativeCodecLibrary {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeCodecLibrary.class);

  private NativeCodecLibrary() {}

  private static final class Loaded {
    private static final @Nullable SymbolLookup LOOKUP = load();

    private static @Nullable SymbolLookup load() {
      try {
        final Path path = extractNativeLib();
        return path == null
            ? null
            : SymbolLookup.libraryLookup(path, Arena.global());
      } catch (final Throwable failure) {
        LOGGER.warn("Native codec adapters unavailable; using Java fallback", failure);
        return null;
      }
    }
  }

  /** Process-lifetime library, or null on platforms without a bundled adapter. */
  public static @Nullable SymbolLookup lookup() {
    return Loaded.LOOKUP;
  }

  /**
   * Extract the embedded {@code libsirix_lz77.so} resource to a temp file on first use. Returns the
   * path for {@link SymbolLookup#libraryLookup}.
   *
   * <p>
   * Returns {@code null} if the resource is missing for the running platform — caller falls back to
   * Java decoder.
   */
  private static @Nullable Path extractNativeLib() throws Exception {
    final String os = System.getProperty("os.name", "").toLowerCase();
    final String arch = System.getProperty("os.arch", "").toLowerCase();

    final String resourcePath;
    final String libName;
    if (os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64"))) {
      resourcePath = "/native/linux-x86_64/libsirix_lz77.so";
      libName = "libsirix_lz77.so";
    } else if (os.contains("linux") && arch.equals("aarch64")) {
      resourcePath = "/native/linux-aarch64/libsirix_lz77.so";
      libName = "libsirix_lz77.so";
    } else {
      LOGGER.info("NativeCodecLibrary: no prebuilt native lib for {} {}", os, arch);
      return null;
    }

    try (InputStream in = NativeCodecLibrary.class.getResourceAsStream(resourcePath)) {
      if (in == null) {
        LOGGER.info("NativeCodecLibrary: resource {} not found on classpath", resourcePath);
        return null;
      }
      final byte[] lib = in.readAllBytes();
      // Content-addressed, so two builds of the library never contend for one name and a staged
      // file never has to be rewritten in place. See publishNativeLib for why in-place is fatal.
      final String stagedName = libName.substring(0, libName.length() - 3) + '-' + digestOf(lib) + ".so";
      final String dirName = "sirix-native-" + System.getProperty("user.name", "default");
      // More than one candidate because falling back to the Java decoder costs ~1.8× on every
      // region decode, and the usual reason for landing there is mundane: a read-only or
      // noexec /tmp. Losing that much throughput to an unwritable directory, silently, is not an
      // acceptable default — so try the obvious alternatives before giving up, and say so when we
      // do (at WARN, not DEBUG: this is a performance cliff, not a detail).
      // Deliberately only these two: the temp directory, then the user's home. Writing into the
      // process's working directory would be a surprising side effect of opening a database.
      final String[] roots = {System.getProperty("java.io.tmpdir"), System.getProperty("user.home")};
      Exception lastFailure = null;
      for (final String root : roots) {
        if (root == null || root.isEmpty()) {
          continue;
        }
        try {
          final Path dir = Path.of(root, dirName);
          Files.createDirectories(dir);
          return publishNativeLib(dir.resolve(stagedName), lib);
        } catch (final Exception e) {
          lastFailure = e;
        }
      }
      LOGGER.warn("NativeCodecLibrary: could not stage {} into any of {} — falling back to the "
          + "pure-Java decoder, which decompresses roughly 1.8x slower. Point java.io.tmpdir at "
          + "a writable directory to recover it.", libName, String.join(", ", roots), lastFailure);
      return null;
    }
  }

  /**
   * Make {@code target} exist with exactly {@code lib}'s bytes, without ever writing into a file
   * another process may already have mapped.
   *
   * <p>
   * The staging directory is shared by every JVM running as this user, and the previous version of
   * this method wrote the library straight into it with {@code Files.write}, which truncates and
   * rewrites in place. A second JVM starting while a first had the library {@code dlopen}ed therefore
   * pulled the mapped pages out from under it, and the first JVM's next downcall jumped into unmapped
   * memory — a hard {@code SIGSEGV} at a near-null pc, not a catchable exception, because
   * {@link Option#critical(boolean) critical} linkage omits the thread-state transition that would
   * let the VM report it. It reproduced whenever more than one test JVM ran at once, which is the
   * ordinary case for a parallel Gradle build.
   *
   * <p>
   * Two things make this safe. The name is content-addressed, so an existing file of the right size
   * already holds the right bytes and is simply reused. And a file that has to be created is written
   * under a unique temporary name and moved into place with {@link StandardCopyOption#ATOMIC_MOVE}: a
   * rename swaps the directory entry, so a process holding the previous inode keeps a valid mapping
   * until it lets go. Losing the race is not an error — the winner staged identical bytes, so the
   * loser drops its copy and uses theirs.
   */
  private static Path publishNativeLib(final Path target, final byte[] lib) throws Exception {
    if (Files.isReadable(target) && Files.size(target) == lib.length) {
      return target;
    }
    final Path staging = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
    try {
      Files.write(staging, lib);
      Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE);
      return target;
    } catch (final Exception e) {
      Files.deleteIfExists(staging);
      // Another JVM published the same content first — its bytes are ours, by construction.
      if (Files.isReadable(target) && Files.size(target) == lib.length) {
        return target;
      }
      throw e;
    }
  }

  /** Short, stable content digest for the staged file name. Runs once per JVM. */
  private static String digestOf(final byte[] lib) throws Exception {
    final byte[] hash = MessageDigest.getInstance("SHA-256").digest(lib);
    final StringBuilder out = new StringBuilder(16);
    for (int i = 0; i < 8; i++) {
      out.append(Character.forDigit((hash[i] >> 4) & 0xF, 16));
      out.append(Character.forDigit(hash[i] & 0xF, 16));
    }
    return out.toString();
  }

}
