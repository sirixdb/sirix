/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.budget;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the native-image setting that cost every Q2 and Q3 query about 150 ms when it changed:
 * when the two FFM downcall adapters are initialized.
 *
 * <p>
 * Initialized at image build time, the adapters are constants the compiler can inline through;
 * initialized at run time, every native decode and every fadvise goes through a slower call path.
 * The portable default has to be run time, because the GraalVM 25.0.x LTS line rejects a build-time
 * downcall handle outright, so an optimized build opts in with
 * {@code -Pnative.preinitializeDowncalls=true}. None of that is Java code a compiler or a result
 * test looks at, and each piece fails silently:
 *
 * <ul>
 * <li>the argument names the adapters by binary class name, and Native Image does not complain
 * about a name that matches nothing, so renaming or inlining a holder turns the opt-in into a
 * no-op;</li>
 * <li>a holder may be initialized early only while it holds nothing but the call signature; a
 * library handle or a symbol address in it would be baked into the image heap;</li>
 * <li>listing a holder in a shared {@code native-image.properties} fails the build on the LTS
 * toolchain CI uses, which is how the setting came to be moved in the first place.</li>
 * </ul>
 *
 * <p>
 * <b>The build is asked, not read.</b> The script defines the argument once, adds it to the main
 * image, and hands this test what Gradle itself evaluated: the arguments the opt-in and the default
 * produce, the name of the property it consults, whether this build asked for the opt-in, and the
 * build-time initialization arguments the main and smoke-test images really end up with (see the
 * {@code test} block of {@code bundles/sirix-query/build.gradle}). Run the suite with
 * {@code -Pnative.preinitializeDowncalls=true} and the same assertions check the other state. The
 * shared {@code native-image.properties} files are parsed as properties, the way Native Image reads
 * them. Nothing here asserts on the text of a document: prose is reworded without changing a build.
 *
 * <p>
 * <b>What this cannot catch:</b> whether an image built with the option is actually fast, whether
 * it builds at all on a given GraalVM, or whether a binary someone measured was built with the
 * option. Nothing here compiles a native image. That is the job of a timed run on a real image;
 * this test only keeps the configuration such a run depends on from drifting unnoticed.
 */
final class NativeImageDowncallConfigTest {

  /** The Gradle property the documentation tells people to pass. */
  private static final String DOCUMENTED_PROPERTY = "native.preinitializeDowncalls";

  private static final String OPT_IN = "-P" + DOCUMENTED_PROPERTY + "=true";

  private static final String BUILD_TIME_FLAG = "--initialize-at-build-time=";

  /** The two adapters, by the binary names a builder argument has to use. */
  private static final List<String> HOLDERS =
      List.of("io.sirix.page.SirixLZ77NativeDecoder$DecodeCall", "io.sirix.io.filechannel.PosixFadvise$AdviceCall");

  private static final String ARGS_WHEN_OPTED_IN = "sirix.test.nativeImage.downcallArgs.optedIn";

  private static final String ARGS_BY_DEFAULT = "sirix.test.nativeImage.downcallArgs.default";

  private static final String OPT_IN_REQUESTED = "sirix.test.nativeImage.downcallArgs.requested";

  private static final String PROPERTY_CONSULTED = "sirix.test.nativeImage.downcallArgs.property";

  private static final String MAIN_IMAGE_ARGS = "sirix.test.nativeImage.buildTimeArgs.main";

  private static final String SMOKE_TEST_IMAGE_ARGS = "sirix.test.nativeImage.buildTimeArgs.smokeTest";

  @Test
  void theOptInProducesOneArgumentNamingExactlyTheTwoHolders() {
    assertEquals(List.of(BUILD_TIME_FLAG + String.join(",", HOLDERS)), evaluatedByTheBuild(ARGS_WHEN_OPTED_IN),
        OPT_IN + " must add one build-time initialization argument, for the two downcall holders and nothing else");
    assertEquals(List.of(), evaluatedByTheBuild(ARGS_BY_DEFAULT),
        "without the opt-in the build must add nothing: the portable configuration is the run-time one");
  }

  @Test
  void theMainImageInitializesTheHoldersEarlyOnlyWhenThisBuildAskedFor() {
    final List<String> early = holdersCoveredBy(buildTimeEntries(evaluatedByTheBuild(MAIN_IMAGE_ARGS)));
    if (Boolean.parseBoolean(evaluated(OPT_IN_REQUESTED))) {
      assertEquals(Set.copyOf(HOLDERS), Set.copyOf(early),
          "this build passed " + OPT_IN + ", so the main image must initialize both holders at build time");
    } else {
      assertEquals(List.of(), early, "this build did not pass " + OPT_IN + ", yet the main image initializes a "
          + "downcall holder at build time: the GraalVM 25.0.x LTS line rejects that with a linkToNative error");
    }
  }

  @Test
  void theSmokeTestImageNeverInitializesAHolderEarly() {
    assertEquals(List.of(), holdersCoveredBy(buildTimeEntries(evaluatedByTheBuild(SMOKE_TEST_IMAGE_ARGS))),
        "the smoke-test image is what CI builds on the LTS toolchain, opt-in or not; a build-time downcall holder "
            + "fails it with a linkToNative error");
  }

  @Test
  void noSharedConfigurationInitializesAHolderEarly() throws IOException {
    final Path repository = repositoryRoot();
    final List<Path> shared = new ArrayList<>();
    final List<Path> modules;
    try (Stream<Path> bundles = Files.list(repository.resolve("bundles"))) {
      modules = bundles.filter(Files::isDirectory).sorted().toList();
    }
    for (final Path module : modules) {
      // Only the sources: a module's build output carries copies of the same files.
      final Path nativeImage = module.resolve("src/main/resources/META-INF/native-image");
      if (Files.isDirectory(nativeImage)) {
        try (Stream<Path> files = Files.walk(nativeImage)) {
          files.filter(path -> path.getFileName().toString().equals("native-image.properties"))
               .sorted()
               .forEach(shared::add);
        }
      }
    }

    // The one shared configuration that carries a build-time list today. A scan that stopped
    // reaching it, or stopped parsing it the way Native Image does, would pass on anything.
    final Path core = repository.resolve(
        "bundles/sirix-core/src/main/resources/META-INF/native-image/io.sirix/sirix-core/native-image.properties");
    assertTrue(shared.contains(core),
        "the scan no longer reaches " + repository.relativize(core) + ", so it would pass on anything: " + shared);

    final List<String> offenders = new ArrayList<>();
    List<String> coreEntries = List.of();
    for (final Path file : shared) {
      final List<String> entries = buildTimeEntries(argsOf(file));
      if (file.equals(core)) {
        coreEntries = entries;
      }
      for (final String holder : holdersCoveredBy(entries)) {
        offenders.add(repository.relativize(file) + " initializes " + holder);
      }
    }
    assertFalse(coreEntries.isEmpty(), repository.relativize(core) + " yielded no build-time initialization entry, "
        + "so this scan reads nothing it could find a holder in");
    assertEquals(List.of(), offenders,
        "a shared native-image.properties applies to every image, the opt-in to one: a holder listed there, by "
            + "name, outer class or package, fails every build on the GraalVM 25.0.x LTS line");
  }

  /**
   * A rename of the Gradle property is the one drift {@code docs/NATIVE_IMAGE.md} cannot follow on
   * its own: the documented {@code -P} switch would go on being accepted and do nothing, and an
   * optimized build would silently keep the run-time adapters.
   */
  @Test
  void theDocumentedSwitchIsThePropertyTheBuildConsults() {
    assertEquals(DOCUMENTED_PROPERTY, evaluated(PROPERTY_CONSULTED),
        "the build consults a different property than the one the documentation tells people to pass (" + OPT_IN
            + "), so the documented switch does nothing and an optimized build silently keeps the run-time adapters");
  }

  @Test
  void eachHolderExistsAndHoldsNothingButItsCallSignature() throws ReflectiveOperationException {
    for (final String name : HOLDERS) {
      // initialize = false: the shape is what is checked, and linking the downcall needs a native
      // linker the test platform may not have.
      final Class<?> holder = Class.forName(name, false, NativeImageDowncallConfigTest.class.getClassLoader());

      assertTrue(holder.isMemberClass() && Modifier.isStatic(holder.getModifiers()),
          name + " must stay a static nested holder: its lazy initialization is what lets the enclosing class "
              + "load the library first and fall back when linkage is unsupported");
      // The holder's own fields. A synthetic one (javac's $assertionsDisabled, a coverage agent's
      // probe array on a pre-11 class file) is the compiler's or the instrumenter's, not source this
      // guard is about, and it is not state a build-time initialized image heap would carry.
      final List<Field> fields = Stream.of(holder.getDeclaredFields()).filter(field -> !field.isSynthetic()).toList();
      assertEquals(1, fields.size(), name + " must hold the call adapter and nothing else. Anything more is process "
          + "state that build-time initialization would bake into the image heap: " + fields);
      final Field handle = fields.get(0);
      assertEquals(MethodHandle.class, handle.getType(), name + "." + handle.getName());
      assertTrue(Modifier.isStatic(handle.getModifiers()) && Modifier.isFinal(handle.getModifiers()),
          name + "." + handle.getName() + " must be a static final constant, or invokeExact cannot inline through it");
      assertFalse(holder.isInterface() || holder.isEnum() || holder.isRecord(), name + " must be a plain class");
    }
  }

  /** One value the build evaluated and handed over; absent when the suite does not run through it. */
  private static String evaluated(final String property) {
    final String value = System.getProperty(property);
    assertNotNull(value, "-D" + property + " is missing. This guard checks what the Gradle build evaluates, so it "
        + "has to run through the sirix-query test task, which provides it");
    return value;
  }

  /** Builder arguments the build evaluated, as it would pass them: one per element. */
  private static List<String> evaluatedByTheBuild(final String property) {
    final String value = evaluated(property).strip();
    return value.isEmpty()
        ? List.of()
        : List.of(value.split("\\s+"));
  }

  /** The {@code Args} of a shared configuration, read as properties: continuation lines joined. */
  private static List<String> argsOf(final Path nativeImageProperties) throws IOException {
    final Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(nativeImageProperties, StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
    final String args = properties.getProperty("Args", "").strip();
    return args.isEmpty()
        ? List.of()
        : List.of(args.split("\\s+"));
  }

  /** Every class or package a list of builder arguments initializes at build time. */
  private static List<String> buildTimeEntries(final List<String> builderArguments) {
    final List<String> entries = new ArrayList<>();
    for (final String argument : builderArguments) {
      if (argument.startsWith(BUILD_TIME_FLAG)) {
        for (final String entry : argument.substring(BUILD_TIME_FLAG.length()).split(",")) {
          if (!entry.isBlank()) {
            entries.add(entry.strip());
          }
        }
      }
    }
    return entries;
  }

  /**
   * The holders those entries initialize early: named outright, or through an outer class or package.
   */
  private static List<String> holdersCoveredBy(final List<String> buildTimeEntries) {
    final List<String> covered = new ArrayList<>();
    for (final String holder : HOLDERS) {
      for (final String entry : buildTimeEntries) {
        if (holder.equals(entry) || holder.startsWith(entry + ".") || holder.startsWith(entry + "$")) {
          covered.add(holder);
          break;
        }
      }
    }
    return covered;
  }

  /**
   * The checkout this test runs in: the nearest ancestor of the working directory with a settings
   * file.
   */
  private static Path repositoryRoot() {
    Path directory = Path.of("").toAbsolutePath();
    while (directory != null && !Files.isRegularFile(directory.resolve("settings.gradle"))) {
      directory = directory.getParent();
    }
    assertNotNull(directory, "no settings.gradle above " + Path.of("").toAbsolutePath() + ": this guard reads the "
        + "shared native-image configuration of every module, so it has to run inside the checkout");
    return directory;
  }
}
