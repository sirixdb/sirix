package io.sirix.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Guards this module's test source set against classes with both standard JUnit 4 and Jupiter
 * annotation markers.
 *
 * <p>
 * This module runs on the JUnit Platform with both the Jupiter and the Vintage engine on the test
 * runtime classpath. Each engine discovers its own test methods and honours only its own lifecycle
 * annotations; a class can be discovered by both engines if it declares tests for both frameworks.
 * A class with only {@code org.junit.Test} methods and an {@code @BeforeEach} fixture can compile
 * and run without that fixture ever being invoked. This guard rejects mixed markers so a passing
 * test result cannot silently hide a skipped fixture.
 * </p>
 *
 * <p>
 * Each class compiled into this module's test output is judged together with its superclass chain,
 * because an engine runs inherited fixtures as the subclass' own. Enclosing classes are
 * deliberately not folded in: a {@code @Nested} class is itself a Jupiter construct and is judged
 * on its own markers. Only the annotations listed below are checked directly; composed annotations
 * and test interfaces are not traversed.
 * </p>
 */
final class JUnitFrameworkConsistencyTest {

  /**
   * {@code RunWith}, {@code Rule} and {@code ClassRule} count as JUnit 4 markers because Jupiter
   * ignores these runner and rule annotations.
   *
   * <p>
   * JUnit 4 markers are matched by annotation type name so {@code org.junit.Test} does not collide
   * with the imported Jupiter {@code @Test} this class is itself annotated with.
   * </p>
   */
  private static final List<String> JUNIT4_MARKERS =
      List.of("org.junit.Test", "org.junit.Before", "org.junit.After", "org.junit.BeforeClass", "org.junit.AfterClass",
          "org.junit.Ignore", "org.junit.Rule", "org.junit.ClassRule", "org.junit.runner.RunWith");

  private static final List<String> JUPITER_MARKERS = List.of(Test.class.getName(), BeforeEach.class.getName(),
      AfterEach.class.getName(), BeforeAll.class.getName(), AfterAll.class.getName(), Disabled.class.getName(),
      Nested.class.getName(), TestFactory.class.getName(), TestTemplate.class.getName(), RepeatedTest.class.getName(),
      ExtendWith.class.getName(), ParameterizedTest.class.getName());

  private static final String CLASS_FILE_SUFFIX = ".class";

  @Test
  void everyTestClassSettlesOnOneJUnitFramework() throws IOException, URISyntaxException {
    final Path testClasses = testClassesRoot();
    final List<Path> classFiles;
    try (final Stream<Path> walk = Files.walk(testClasses)) {
      classFiles = walk.filter(JUnitFrameworkConsistencyTest::isClassFile).sorted().toList();
    }

    assertFalse(classFiles.isEmpty(), () -> "no compiled test classes found under " + testClasses);

    final List<String> mixed = new ArrayList<>();
    final List<String> uninspectable = new ArrayList<>();
    for (final Path classFile : classFiles) {
      final String binaryName = binaryName(testClasses, classFile);
      try {
        // initialize=false: reading annotations must not run a test class' static initialiser.
        final Class<?> candidate =
            Class.forName(binaryName, false, JUnitFrameworkConsistencyTest.class.getClassLoader());
        final List<String> junit4 = markersOn(candidate, JUNIT4_MARKERS);
        final List<String> jupiter = markersOn(candidate, JUPITER_MARKERS);
        if (!junit4.isEmpty() && !jupiter.isEmpty()) {
          mixed.add(binaryName + ": JUnit 4 " + junit4 + " together with JUnit 5 " + jupiter);
        }
      } catch (final ClassNotFoundException | LinkageError e) {
        uninspectable.add(binaryName + " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ')');
      }
    }
    if (!uninspectable.isEmpty()) {
      // Skipping these quietly would let the guard go dark exactly where class loading breaks.
      fail("these test classes could not be inspected:" + System.lineSeparator()
          + String.join(System.lineSeparator(), uninspectable));
    }
    if (!mixed.isEmpty()) {
      fail("these test classes mix JUnit 4 and JUnit 5; the engine that claims the class ignores the other "
          + "framework's fixtures, so they are never invoked - settle each class on one framework (Jupiter):"
          + System.lineSeparator() + String.join(System.lineSeparator(), mixed));
    }
  }

  /**
   * Collects the simple names of every annotation out of {@code markers} that occurs on the class, on
   * one of its declared methods or on one of its declared fields, walking the superclass chain.
   */
  private static List<String> markersOn(final Class<?> candidate, final List<String> markers) {
    final List<String> found = new ArrayList<>(2);
    for (Class<?> type = candidate; type != null && type != Object.class; type = type.getSuperclass()) {
      final Method[] methods = type.getDeclaredMethods();
      final Field[] fields = type.getDeclaredFields();
      for (final String marker : markers) {
        final String simpleName = marker.substring(marker.lastIndexOf('.') + 1);
        if (!found.contains(simpleName) && declares(type, methods, fields, marker)) {
          found.add(simpleName);
        }
      }
    }
    return found;
  }

  private static boolean declares(final Class<?> type, final Method[] methods, final Field[] fields,
      final String marker) {
    if (hasAnnotation(type, marker)) {
      return true;
    }
    for (final Method method : methods) {
      if (hasAnnotation(method, marker)) {
        return true;
      }
    }
    for (final Field field : fields) {
      if (hasAnnotation(field, marker)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasAnnotation(final AnnotatedElement element, final String marker) {
    for (final Annotation annotation : element.getAnnotations()) {
      if (annotation.annotationType().getName().equals(marker)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isClassFile(final Path path) {
    return path.getFileName().toString().endsWith(CLASS_FILE_SUFFIX) && Files.isRegularFile(path);
  }

  private static String binaryName(final Path root, final Path classFile) {
    final String relative = root.relativize(classFile).toString();
    return relative.substring(0, relative.length() - CLASS_FILE_SUFFIX.length())
                   .replace(root.getFileSystem().getSeparator(), ".");
  }

  /** The directory this test class was loaded from, i.e. this module's compiled test output. */
  private static Path testClassesRoot() throws URISyntaxException {
    final CodeSource codeSource = JUnitFrameworkConsistencyTest.class.getProtectionDomain().getCodeSource();
    if (codeSource == null || codeSource.getLocation() == null) {
      throw new IllegalStateException("cannot locate the compiled test output of this module");
    }
    final Path root = Path.of(codeSource.getLocation().toURI());
    if (!Files.isDirectory(root)) {
      throw new IllegalStateException("expected a compiled test output directory but got " + root);
    }
    return root;
  }
}
