/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HftRuntimeEvidenceTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void runtimeIdentityBindsEveryClasspathEntry() throws Exception {
    final Path classes = Files.createDirectory(temporaryDirectory.resolve("classes"));
    final Path mainClass = classes.resolve("Main.class");
    final Path dependency = temporaryDirectory.resolve("dependency.jar");
    Files.write(mainClass, new byte[] {1, 2, 3});
    Files.write(dependency, new byte[] {4, 5, 6});
    final String classpath = classes + File.pathSeparator + dependency;

    final String original = HftRuntimeEvidence.runtimeClasspathSha256(classpath);
    assertThrows(IllegalArgumentException.class, () -> HftRuntimeEvidence.runtimeClasspathSha256(
        classpath + File.pathSeparator + temporaryDirectory.resolve("missing-output")));
    assertNotEquals(original, HftRuntimeEvidence.runtimeClasspathSha256(dependency + File.pathSeparator + classes));

    Files.write(dependency, new byte[] {4, 5, 7});
    assertNotEquals(original, HftRuntimeEvidence.runtimeClasspathSha256(classpath));

    Files.write(dependency, new byte[] {4, 5, 6});
    Files.move(mainClass, classes.resolve("Renamed.class"));
    assertNotEquals(original, HftRuntimeEvidence.runtimeClasspathSha256(classpath));

    final String beforeAttestation = HftRuntimeEvidence.runtimeClasspathSha256(classpath);
    final Path attestation = classes.resolve("META-INF/sirix-hft-build.properties");
    Files.createDirectories(attestation.getParent());
    Files.writeString(attestation, "artifactSha256=stale\n");
    assertEquals(beforeAttestation, HftRuntimeEvidence.runtimeClasspathSha256(classpath));
  }

  @Test
  void discoversIdentityInStandaloneClasspathDirectory() throws Exception {
    final Path classes = classDirectory("classes");
    final Path identityDirectory = standaloneIdentityDirectory("identity", "standalone-identity");
    final String classpath = classes + File.pathSeparator + identityDirectory;

    try (InputStream source = HftRuntimeEvidence.buildIdentitySource(classes, classpath)) {
      assertEquals("standalone-identity", new String(source.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  @Test
  void rejectsAmbiguousStandaloneIdentities() throws Exception {
    final Path classes = classDirectory("classes");
    final Path first = standaloneIdentityDirectory("identity-one", "one");
    final Path second = standaloneIdentityDirectory("identity-two", "two");
    final String classpath = classes + File.pathSeparator + first + File.pathSeparator + second;

    final IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> HftRuntimeEvidence.buildIdentitySource(classes, classpath));
    assertTrue(error.getMessage().contains("multiple standalone HFT build identities"));
  }

  @Test
  void rejectsIdentityHiddenInOrdinaryClasspathOutput() throws Exception {
    final Path classes = classDirectory("classes");
    final Path dependencyClasses = classDirectory("dependency-classes");
    writeIdentity(dependencyClasses, "hidden-identity");
    final String classpath = classes + File.pathSeparator + dependencyClasses;

    final IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> HftRuntimeEvidence.buildIdentitySource(classes, classpath));
    assertTrue(error.getMessage().contains("standalone classpath directory"));
  }

  @Test
  void doesNotDiscoverIdentityOutsideEffectiveClasspath() throws Exception {
    final Path classes = classDirectory("classes");
    standaloneIdentityDirectory("outside-identity", "outside");

    try (InputStream source = HftRuntimeEvidence.buildIdentitySource(classes, classes.toString())) {
      assertNull(source);
    }
  }

  @Test
  void rejectsFallbackWhenCodeSourceIsOutsideEffectiveClasspath() throws Exception {
    final Path classes = classDirectory("classes");
    final Path otherClasses = classDirectory("other-classes");
    final Path identityDirectory = standaloneIdentityDirectory("identity", "standalone-identity");
    final String classpath = otherClasses + File.pathSeparator + identityDirectory;

    final IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> HftRuntimeEvidence.buildIdentitySource(classes, classpath));
    assertTrue(error.getMessage().contains("main class is outside the effective runtime classpath"));
  }

  @Test
  void rejectsSymlinkedIdentityCarrier() throws Exception {
    final Path classes = classDirectory("classes");
    final Path identityDirectory = standaloneIdentityDirectory("identity", "standalone-identity");
    final Path identityLink = Files.createSymbolicLink(temporaryDirectory.resolve("identity-link"), identityDirectory);
    final String classpath = classes + File.pathSeparator + identityLink;

    final IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> HftRuntimeEvidence.buildIdentitySource(classes, classpath));
    assertTrue(error.getMessage().contains("classpath entries must not be symbolic links"));
  }

  @Test
  void rejectsSymlinkedDirectCodeSource() throws Exception {
    final Path classes = classDirectory("classes");
    writeIdentity(classes, "direct-identity");
    final Path classesLink = Files.createSymbolicLink(temporaryDirectory.resolve("classes-link"), classes);

    final IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> HftRuntimeEvidence.buildIdentitySource(classesLink, classesLink.toString()));
    assertTrue(error.getMessage().contains("CodeSource must not be a symbolic link"));
  }

  @Test
  void boundsDirectAndStandaloneIdentityResources() throws Exception {
    final byte[] oversizedIdentity = new byte[HftRuntimeEvidence.MAX_BUILD_IDENTITY_BYTES + 1];
    final Path classes = classDirectory("classes");
    writeIdentity(classes, oversizedIdentity);
    assertOversizedIdentityRejected(classes, classes.toString());

    final Path otherClasses = classDirectory("other-classes");
    final Path identityDirectory = Files.createDirectory(temporaryDirectory.resolve("identity"));
    writeIdentity(identityDirectory, oversizedIdentity);
    assertOversizedIdentityRejected(otherClasses, otherClasses + File.pathSeparator + identityDirectory);
  }

  private static void assertOversizedIdentityRejected(final Path codeSource, final String classpath) {
    final IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> HftRuntimeEvidence.buildIdentitySource(codeSource, classpath));
    assertTrue(error.getMessage().contains("identity exceeds"));
  }

  private Path classDirectory(final String name) throws Exception {
    final Path directory = Files.createDirectory(temporaryDirectory.resolve(name));
    Files.write(directory.resolve("Main.class"), new byte[] {1, 2, 3});
    return directory;
  }

  private Path standaloneIdentityDirectory(final String name, final String identity) throws Exception {
    final Path directory = Files.createDirectory(temporaryDirectory.resolve(name));
    writeIdentity(directory, identity);
    return directory;
  }

  private static void writeIdentity(final Path directory, final String identity) throws Exception {
    writeIdentity(directory, identity.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static void writeIdentity(final Path directory, final byte[] identity) throws Exception {
    final Path file = directory.resolve("META-INF/sirix-hft-build.properties");
    Files.createDirectories(file.getParent());
    Files.write(file, identity);
  }

  @Test
  void requiresInfoOrMoreVerboseLoggingOnStdout() {
    assertTrue(HftRuntimeEvidence.unifiedLoggingCaptured(
        List.of("-Xlog:gc*,gc+heap=debug,safepoint:stdout:uptime,level,tags"), "gc"));
    assertFalse(HftRuntimeEvidence.unifiedLoggingCaptured(
        List.of("-Xlog:gc*,gc+heap=debug,safepoint:stderr:uptime,level,tags"), "safepoint"));
    assertTrue(HftRuntimeEvidence.unifiedLoggingCaptured(List.of("-Xlog"), "gc"));

    assertFalse(HftRuntimeEvidence.unifiedLoggingCaptured(List.of("-Xlog:gc=warning:stdout"), "gc"));
    assertFalse(HftRuntimeEvidence.unifiedLoggingCaptured(List.of("-Xlog:gc=error:stdout"), "gc"));
    assertFalse(HftRuntimeEvidence.unifiedLoggingCaptured(List.of("-Xlog:gc*:file=/tmp/gc.log"), "gc"));
    assertFalse(HftRuntimeEvidence.unifiedLoggingCaptured(List.of("-Xlog:gc*:stdout", "-Xlog:disable"), "gc"));
    assertFalse(HftRuntimeEvidence.unifiedLoggingCaptured(List.of("-Xlog:gc*:stdout", "-Xlog:gc*=off:stdout"), "gc"));
  }
}
