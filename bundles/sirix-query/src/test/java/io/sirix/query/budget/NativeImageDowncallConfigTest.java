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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * {@code -Pnative.preinitializeDowncalls=true}. The whole arrangement is three strings that must
 * agree and two classes that must keep a particular shape, and none of it is Java code a compiler
 * or a result test looks at:
 *
 * <ul>
 * <li>the argument names the adapters by binary class name, and Native Image does not complain
 * about a name that matches nothing, so renaming or inlining a holder turns the opt-in into a
 * silent no-op;</li>
 * <li>a holder may be initialized early only while it holds nothing but the call signature; a
 * library handle or a symbol address in it would be baked into the image heap;</li>
 * <li>listing a holder in a shared {@code native-image.properties} fails the build on the LTS
 * toolchain CI uses, which is how the setting came to be moved in the first place.</li>
 * </ul>
 *
 * <p>
 * <b>What this cannot catch:</b> whether an image built with the option is actually fast, or builds
 * at all on a given GraalVM. Nothing here compiles a native image. That is the job of a timed run
 * on a real image; this test only keeps the configuration such a run depends on from drifting
 * unnoticed.
 */
final class NativeImageDowncallConfigTest {

  private static final String OPT_IN_PROPERTY = "native.preinitializeDowncalls";

  private static final String BUILD_TIME_FLAG = "--initialize-at-build-time=";

  /** The two adapters, by the binary names the builder argument has to use. */
  private static final List<String> HOLDERS =
      List.of("io.sirix.page.SirixLZ77NativeDecoder$DecodeCall", "io.sirix.io.filechannel.PosixFadvise$AdviceCall");

  private static final String OPT_IN_ARGUMENT = BUILD_TIME_FLAG + String.join(",", HOLDERS);

  private static final Pattern BUILD_TIME_ENTRIES =
      Pattern.compile(Pattern.quote(BUILD_TIME_FLAG) + "([^\\s'\"\\\\]+)");

  private static final Path REPOSITORY = repositoryRoot();

  @Test
  void theDocumentationGivesTheArgumentTheBuildAdds() throws IOException {
    final List<String> documented = new ArrayList<>();
    for (final String line : Files.readAllLines(REPOSITORY.resolve("docs/NATIVE_IMAGE.md"), StandardCharsets.UTF_8)) {
      if (line.strip().startsWith(BUILD_TIME_FLAG) && coversAHolder(line.strip().substring(BUILD_TIME_FLAG.length()))) {
        documented.add(line.strip());
      }
    }
    assertEquals(List.of(OPT_IN_ARGUMENT), documented,
        "docs/NATIVE_IMAGE.md must state the opt-in argument exactly once, and exactly as the build adds it");
    assertTrue(Files.readString(REPOSITORY.resolve("docs/NATIVE_IMAGE.md")).contains("-P" + OPT_IN_PROPERTY + "=true"),
        "docs/NATIVE_IMAGE.md must name the property that switches the argument on");
  }

  @Test
  void theBuildAddsTheArgumentOnlyBehindTheOptInProperty() throws IOException {
    final List<String> lines =
        Files.readAllLines(REPOSITORY.resolve("bundles/sirix-query/build.gradle"), StandardCharsets.UTF_8);
    int gate = -1;
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).contains(OPT_IN_PROPERTY) && !lines.get(i).strip().startsWith("//")) {
        assertEquals(-1, gate, "the opt-in property must gate the argument in exactly one place");
        gate = i;
      }
    }
    assertTrue(gate >= 0, "bundles/sirix-query/build.gradle no longer reads -P" + OPT_IN_PROPERTY
        + ", so an optimized build silently keeps the run-time adapters");

    final String condition = lines.get(gate).strip();
    assertTrue(condition.startsWith("if (") && condition.endsWith("{"),
        "the property must guard a block, not be consulted in passing: " + condition);
    assertTrue(condition.contains("getOrElse('false')"),
        "the option must default to off: the portable configuration is the run-time one. Found: " + condition);
    // Single quotes matter: in a Groovy double-quoted string "$DecodeCall" is an interpolation, and
    // the argument would name a class that does not exist.
    assertEquals("buildArgs.add('" + OPT_IN_ARGUMENT + "')", lines.get(gate + 1).strip(),
        "the guarded block must add exactly the documented argument, single-quoted");
    assertEquals("}", lines.get(gate + 2).strip(), "the guarded block must add that argument and nothing else");
  }

  @Test
  void nothingInitializesAHolderAtBuildTimeExceptTheOptIn() throws IOException {
    final List<Path> configured = new ArrayList<>();
    final List<Path> modules;
    try (Stream<Path> bundles = Files.list(REPOSITORY.resolve("bundles"))) {
      modules = bundles.filter(Files::isDirectory).sorted().toList();
    }
    for (final Path module : modules) {
      if (Files.isRegularFile(module.resolve("build.gradle"))) {
        configured.add(module.resolve("build.gradle"));
      }
      // Only the sources: a module's build output carries copies of the same files.
      final Path shared = module.resolve("src/main/resources/META-INF/native-image");
      if (Files.isDirectory(shared)) {
        try (Stream<Path> files = Files.walk(shared)) {
          files.filter(path -> path.getFileName().toString().equals("native-image.properties"))
               .sorted()
               .forEach(configured::add);
        }
      }
    }
    try (Stream<Path> scripts = Files.list(REPOSITORY.resolve("native-image"))) {
      scripts.filter(path -> path.getFileName().toString().endsWith(".sh")).sorted().forEach(configured::add);
    }
    assertTrue(configured.stream().anyMatch(path -> path.endsWith("sirix-core/native-image.properties")),
        "the scan must reach sirix-core's shared configuration, or it proves nothing: " + configured);

    final List<String> offenders = new ArrayList<>();
    for (final Path file : configured) {
      for (final String entry : buildTimeEntries(file)) {
        final boolean isTheOptIn = file.endsWith("bundles/sirix-query/build.gradle") && HOLDERS.contains(entry);
        if (coversAHolder(entry) && !isTheOptIn) {
          offenders.add(REPOSITORY.relativize(file) + " -> " + entry);
        }
      }
    }
    assertEquals(List.of(), offenders,
        "a downcall holder is initialized at build time outside the opt-in, by name or through its package or "
            + "outer class. The GraalVM 25.0.x LTS line CI builds with rejects that with a linkToNative error");
  }

  @Test
  void sirixCoresSharedConfigurationIsReadAsNativeImageReadsIt() throws IOException {
    final List<String> entries = buildTimeEntries(REPOSITORY.resolve(
        "bundles/sirix-core/src/main/resources/META-INF/native-image/io.sirix/sirix-core/native-image.properties"));
    assertTrue(entries.contains("io.sirix.node.LE"),
        "the parser no longer sees sirix-core's build-time list, so the scan above would pass on anything: " + entries);
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
      final Field[] fields = holder.getDeclaredFields();
      assertEquals(1, fields.length, name + " must hold the call adapter and nothing else. Anything more is process "
          + "state that build-time initialization would bake into the image heap: " + List.of(fields));
      final Field handle = fields[0];
      assertEquals(MethodHandle.class, handle.getType(), name + "." + handle.getName());
      assertTrue(Modifier.isStatic(handle.getModifiers()) && Modifier.isFinal(handle.getModifiers()),
          name + "." + handle.getName() + " must be a static final constant, or invokeExact cannot inline through it");
      assertFalse(holder.isInterface() || holder.isEnum() || holder.isRecord(), name + " must be a plain class");
    }
  }

  /** Every class or package a file asks Native Image to initialize at build time. */
  private static List<String> buildTimeEntries(final Path file) throws IOException {
    final String text;
    if (file.getFileName().toString().endsWith(".properties")) {
      // Properties syntax joins the backslash-continued Args lines the way Native Image reads them.
      final Properties properties = new Properties();
      try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
        properties.load(reader);
      }
      text = properties.getProperty("Args", "");
    } else {
      text = Files.readString(file, StandardCharsets.UTF_8);
    }
    final List<String> entries = new ArrayList<>();
    final Matcher matcher = BUILD_TIME_ENTRIES.matcher(text);
    while (matcher.find()) {
      for (final String entry : matcher.group(1).split(",")) {
        if (!entry.isBlank()) {
          entries.add(entry.strip());
        }
      }
    }
    return entries;
  }

  /**
   * Whether initializing {@code entry} early initializes a holder: itself, its outer class or a
   * package.
   */
  private static boolean coversAHolder(final String entries) {
    for (final String entry : entries.split(",")) {
      for (final String holder : HOLDERS) {
        if (holder.equals(entry) || holder.startsWith(entry + ".") || holder.startsWith(entry + "$")) {
          return true;
        }
      }
    }
    return false;
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
    if (directory == null) {
      throw new IllegalStateException("no settings.gradle above " + Path.of("").toAbsolutePath()
          + ": this guard reads the build and the documentation, so it has to run inside the checkout");
    }
    return directory;
  }
}
