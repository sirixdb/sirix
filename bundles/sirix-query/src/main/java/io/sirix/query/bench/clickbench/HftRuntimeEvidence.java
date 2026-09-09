/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public final class HftRuntimeEvidence {

  private static final byte[] CLASSPATH_MAGIC = "SIRIX-HFT-CLASSPATH-V1\0".getBytes(StandardCharsets.UTF_8);
  private static final String BUILD_IDENTITY_RESOURCE = "META-INF/sirix-hft-build.properties";
  static final int MAX_BUILD_IDENTITY_BYTES = 4 * 1024;

  private HftRuntimeEvidence() {
    throw new AssertionError("no instances");
  }

  public static Build capture(final Class<?> mainClass) {
    final EmbeddedBuild embedded = requiredBuild(mainClass);
    final String artifactSha256 = artifactSha256(mainClass);
    if (!artifactSha256.equals(embedded.artifactSha256())) {
      throw new IllegalStateException("embedded HFT artifact " + embedded.artifactSha256()
          + " does not match the effective runtime artifact " + artifactSha256);
    }
    final boolean gcLogging = unifiedLoggingEnabled("gc");
    final boolean safepointLogging = unifiedLoggingEnabled("safepoint");
    if (!gcLogging || !safepointLogging) {
      throw new IllegalStateException("HFT evidence requires effective gc and safepoint unified logging on stdout");
    }
    return new Build(embedded.gitSha(), artifactSha256, gcLogging, safepointLogging);
  }

  private static String artifactSha256(final Class<?> mainClass) {
    if (mainClass == null || mainClass.getProtectionDomain() == null
        || mainClass.getProtectionDomain().getCodeSource() == null) {
      throw new IllegalArgumentException("an executable main class with a code source is required");
    }
    final String classpath = System.getProperty("java.class.path", "");
    try {
      final Path mainLocation =
          Path.of(mainClass.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath().normalize();
      boolean mainLocationPresent = false;
      for (final String rawEntry : classpath.split(java.util.regex.Pattern.quote(File.pathSeparator), -1)) {
        if (!rawEntry.isBlank() && Path.of(rawEntry).toAbsolutePath().normalize().equals(mainLocation)) {
          mainLocationPresent = true;
          break;
        }
      }
      if (!mainLocationPresent) {
        throw new IllegalStateException("the executable main class is outside the effective runtime classpath");
      }
      return runtimeClasspathSha256(classpath);
    } catch (final URISyntaxException error) {
      throw new IllegalStateException("Cannot resolve executable main-class location", error);
    }
  }

  static String runtimeClasspathSha256(final String classpath) {
    if (classpath == null || classpath.isBlank()) {
      throw new IllegalArgumentException("runtime classpath must not be empty");
    }
    try {
      final List<byte[]> entries = new ArrayList<>();
      for (final String rawEntry : classpath.split(java.util.regex.Pattern.quote(File.pathSeparator), -1)) {
        if (rawEntry.isBlank()) {
          throw new IllegalArgumentException("runtime classpath contains an empty entry");
        }
        final Path entry = Path.of(rawEntry).toAbsolutePath().normalize();
        if (Files.isRegularFile(entry)) {
          entries.add(hashFileEntry(entry));
        } else if (Files.isDirectory(entry)) {
          entries.add(hashDirectoryEntry(entry));
        } else {
          throw new IllegalArgumentException("runtime classpath entry is unreadable: " + entry);
        }
      }
      final MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
      aggregate.update(CLASSPATH_MAGIC);
      for (final byte[] entry : entries) {
        aggregate.update(entry);
      }
      return HexFormat.of().formatHex(aggregate.digest());
    } catch (final IOException error) {
      throw new IllegalStateException("Cannot hash the effective runtime classpath", error);
    } catch (final NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 is unavailable", error);
    }
  }

  private static byte[] hashFileEntry(final Path file) throws IOException, NoSuchAlgorithmException {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update((byte) 'F');
    digest.update((byte) 0);
    updateFile(digest, file);
    return digest.digest();
  }

  private static byte[] hashDirectoryEntry(final Path directory) throws IOException, NoSuchAlgorithmException {
    final List<Path> files;
    try (var walk = Files.walk(directory)) {
      files =
          walk.filter(Files::isRegularFile)
              .filter(path -> !directory.relativize(path)
                                        .toString()
                                        .replace(File.separatorChar, '/')
                                        .equals(BUILD_IDENTITY_RESOURCE))
              .sorted(
                  Comparator.comparing(path -> directory.relativize(path).toString().replace(File.separatorChar, '/')))
              .toList();
    }
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update((byte) 'D');
    digest.update((byte) 0);
    for (final Path file : files) {
      final byte[] name =
          directory.relativize(file).toString().replace(File.separatorChar, '/').getBytes(StandardCharsets.UTF_8);
      updateLong(digest, name.length);
      digest.update(name);
      updateLong(digest, Files.size(file));
      updateFile(digest, file);
    }
    return digest.digest();
  }

  private static void updateFile(final MessageDigest digest, final Path file) throws IOException {
    try (InputStream source = Files.newInputStream(file)) {
      final byte[] buffer = new byte[64 * 1024];
      int read;
      while ((read = source.read(buffer)) >= 0) {
        if (read > 0) {
          digest.update(buffer, 0, read);
        }
      }
    }
  }

  private static void updateLong(final MessageDigest digest, final long value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      digest.update((byte) (value >>> shift));
    }
  }

  private static EmbeddedBuild requiredBuild(final Class<?> mainClass) {
    final String expected = System.getProperty("sirix.hft.gitSha", "");
    if (!expected.matches("[0-9a-f]{40}")) {
      throw new IllegalStateException("-Dsirix.hft.gitSha must name the clean 40-character build commit");
    }
    final EmbeddedBuild embedded = embeddedBuild(mainClass);
    if (!embedded.clean() || !embedded.gitSha().equals(expected)) {
      throw new IllegalStateException(
          "embedded HFT build commit " + embedded.gitSha() + " is not a clean build of " + expected);
    }
    final String actual = gitOutput("rev-parse", "HEAD");
    if (!actual.matches("[0-9a-f]{40}") || !actual.equals(embedded.gitSha())) {
      throw new IllegalStateException(
          "HFT worktree commit " + actual + " does not match embedded build " + embedded.gitSha());
    }
    if (!gitOutput("status", "--porcelain", "--untracked-files=normal").isEmpty()) {
      throw new IllegalStateException("HFT evidence requires a clean tracked worktree");
    }
    return embedded;
  }

  private static EmbeddedBuild embeddedBuild(final Class<?> mainClass) {
    final Properties properties = new Properties();
    try (InputStream source = buildIdentitySource(codeSource(mainClass))) {
      if (source == null) {
        throw new IllegalStateException("HFT build identity is missing from the executable main-class CodeSource");
      }
      properties.load(source);
    } catch (final IOException error) {
      throw new IllegalStateException("Cannot read HFT build identity resource", error);
    }
    final String gitSha = properties.getProperty("gitSha", "");
    final String clean = properties.getProperty("clean", "");
    final String artifactSha256 = properties.getProperty("artifactSha256", "");
    if (!gitSha.matches("[0-9a-f]{40}") || !(clean.equals("true") || clean.equals("false"))
        || !artifactSha256.matches("[0-9a-f]{64}")) {
      throw new IllegalStateException("HFT build identity resource is malformed");
    }
    return new EmbeddedBuild(gitSha, Boolean.parseBoolean(clean), artifactSha256);
  }

  private static Path codeSource(final Class<?> mainClass) {
    if (mainClass == null || mainClass.getProtectionDomain() == null
        || mainClass.getProtectionDomain().getCodeSource() == null) {
      throw new IllegalArgumentException("an executable main class with a code source is required");
    }
    try {
      return Path.of(mainClass.getProtectionDomain().getCodeSource().getLocation().toURI())
                 .toAbsolutePath()
                 .normalize();
    } catch (final URISyntaxException error) {
      throw new IllegalStateException("Cannot resolve executable main-class location", error);
    }
  }

  private static InputStream buildIdentitySource(final Path codeSource) throws IOException {
    return buildIdentitySource(codeSource, System.getProperty("java.class.path", ""));
  }

  static InputStream buildIdentitySource(final Path codeSource, final String classpath) throws IOException {
    if (Files.isSymbolicLink(codeSource)) {
      throw new IllegalStateException("executable main-class CodeSource must not be a symbolic link: " + codeSource);
    }
    final InputStream codeSourceIdentity = codeSourceBuildIdentitySource(codeSource);
    if (codeSourceIdentity != null || !Files.isDirectory(codeSource)) {
      return boundedBuildIdentitySource(codeSourceIdentity);
    }

    if (classpath == null || classpath.isBlank()) {
      throw new IllegalArgumentException("runtime classpath must not be empty");
    }
    final Path normalizedCodeSource = codeSource.toAbsolutePath().normalize();
    Path standaloneIdentity = null;
    boolean codeSourcePresent = false;
    for (final String rawEntry : classpath.split(java.util.regex.Pattern.quote(File.pathSeparator), -1)) {
      if (rawEntry.isBlank()) {
        throw new IllegalArgumentException("runtime classpath contains an empty entry");
      }
      final Path entry = Path.of(rawEntry).toAbsolutePath().normalize();
      if (entry.equals(normalizedCodeSource)) {
        codeSourcePresent = true;
        continue;
      }
      if (Files.isSymbolicLink(entry)) {
        throw new IllegalStateException("runtime classpath entries must not be symbolic links: " + entry);
      }
      if (!Files.isDirectory(entry)) {
        if (!Files.isRegularFile(entry)) {
          throw new IllegalArgumentException("runtime classpath entry is unreadable: " + entry);
        }
        continue;
      }
      final Path candidate = entry.resolve(BUILD_IDENTITY_RESOURCE);
      if (!Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)) {
        continue;
      }
      if (!Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)
          || !isStandaloneIdentityDirectory(entry, candidate)) {
        throw new IllegalStateException(
            "HFT build identity outside the executable CodeSource must be in a standalone classpath directory: "
                + entry);
      }
      if (standaloneIdentity != null) {
        throw new IllegalStateException(
            "multiple standalone HFT build identities are present on the effective runtime classpath");
      }
      standaloneIdentity = candidate;
    }
    if (!codeSourcePresent) {
      throw new IllegalStateException("the executable main class is outside the effective runtime classpath");
    }
    return standaloneIdentity == null
        ? null
        : boundedBuildIdentitySource(Files.newInputStream(standaloneIdentity));
  }

  private static boolean isStandaloneIdentityDirectory(final Path directory, final Path identity) throws IOException {
    if (Files.isSymbolicLink(directory)) {
      return false;
    }
    try (var walk = Files.walk(directory)) {
      return walk.allMatch(path -> path.equals(directory)
          || (!Files.isSymbolicLink(path) && (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
              || path.equals(identity) && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))));
    }
  }

  private static InputStream codeSourceBuildIdentitySource(final Path codeSource) throws IOException {
    if (Files.isDirectory(codeSource)) {
      final Path identity = codeSource.resolve(BUILD_IDENTITY_RESOURCE);
      if (!Files.exists(identity, LinkOption.NOFOLLOW_LINKS)) {
        return null;
      }
      if (!Files.isRegularFile(identity, LinkOption.NOFOLLOW_LINKS)) {
        throw new IllegalStateException("HFT build identity must be a regular non-symbolic file: " + identity);
      }
      return Files.newInputStream(identity);
    }
    if (!Files.isRegularFile(codeSource)) {
      throw new IllegalStateException("executable main-class CodeSource is unreadable: " + codeSource);
    }
    final JarFile jar = new JarFile(codeSource.toFile());
    final JarEntry identity = jar.getJarEntry(BUILD_IDENTITY_RESOURCE);
    if (identity == null || identity.isDirectory()) {
      jar.close();
      return null;
    }
    final InputStream source = jar.getInputStream(identity);
    return new InputStream() {
      @Override
      public int read() throws IOException {
        return source.read();
      }

      @Override
      public int read(final byte[] bytes, final int offset, final int length) throws IOException {
        return source.read(bytes, offset, length);
      }

      @Override
      public void close() throws IOException {
        try {
          source.close();
        } finally {
          jar.close();
        }
      }
    };
  }

  private static InputStream boundedBuildIdentitySource(final InputStream source) throws IOException {
    if (source == null) {
      return null;
    }
    try (source) {
      final byte[] identity = source.readNBytes(MAX_BUILD_IDENTITY_BYTES + 1);
      if (identity.length > MAX_BUILD_IDENTITY_BYTES) {
        throw new IllegalStateException("HFT build identity exceeds " + MAX_BUILD_IDENTITY_BYTES + " bytes");
      }
      return new ByteArrayInputStream(identity);
    }
  }

  private static String gitOutput(final String... arguments) {
    final String[] command = new String[arguments.length + 1];
    command[0] = "git";
    System.arraycopy(arguments, 0, command, 1, arguments.length);
    try {
      final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
      final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      if (process.waitFor() != 0) {
        throw new IllegalStateException("git command failed: " + output);
      }
      return output;
    } catch (final IOException error) {
      throw new IllegalStateException("git is required to bind HFT evidence", error);
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while binding HFT evidence", interrupted);
    }
  }

  private static boolean unifiedLoggingEnabled(final String tag) {
    return unifiedLoggingCaptured(ManagementFactory.getRuntimeMXBean().getInputArguments(), tag);
  }

  static boolean unifiedLoggingCaptured(final List<String> arguments, final String tag) {
    if (arguments == null) {
      throw new IllegalArgumentException("arguments must not be null");
    }
    if (tag == null || tag.isBlank() || tag.indexOf('+') >= 0 || tag.indexOf(',') >= 0) {
      throw new IllegalArgumentException("tag must be one non-empty unified-logging tag");
    }
    boolean stdoutEnabled = false;
    for (final String argument : arguments) {
      if ("-Xlog:disable".equals(argument)) {
        stdoutEnabled = false;
        continue;
      }
      if ("-Xlog".equals(argument)) {
        stdoutEnabled = true;
        continue;
      }
      if (!argument.startsWith("-Xlog:")) {
        continue;
      }
      final String[] fields = argument.substring("-Xlog:".length()).split(":", -1);
      final String selectors = fields[0].isBlank()
          ? "all=info"
          : fields[0];
      final String output = fields.length < 2 || fields[1].isBlank()
          ? "stdout"
          : fields[1];
      if (!"stdout".equals(output)) {
        continue;
      }
      Boolean optionEnabled = null;
      for (final String rawSelector : selectors.split(",", -1)) {
        final String[] parts = rawSelector.split("=", 2);
        final String selector = parts[0];
        if (selectorCoversTag(selector, tag)) {
          final String level = parts.length == 1
              ? "info"
              : parts[1].toLowerCase(java.util.Locale.ROOT);
          optionEnabled = "info".equals(level) || "debug".equals(level) || "trace".equals(level);
        }
      }
      if (optionEnabled != null) {
        stdoutEnabled = optionEnabled;
      }
    }
    return stdoutEnabled;
  }

  private static boolean selectorCoversTag(final String selector, final String tag) {
    return "all".equals(selector) || selector.equals(tag) || selector.equals(tag + "*");
  }

  public record Build(String gitSha, String artifactSha256, boolean gcLogging, boolean safepointLogging) {
  }

  private record EmbeddedBuild(String gitSha, boolean clean, String artifactSha256) {
  }
}
