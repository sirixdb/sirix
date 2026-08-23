/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
