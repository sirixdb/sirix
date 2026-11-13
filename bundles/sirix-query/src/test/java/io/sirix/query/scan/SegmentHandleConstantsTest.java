/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Segment handles live only in classes a native image initializes at build time.
 *
 * <p>
 * A {@code VarHandle} read compiles to a plain load only where the compiler sees the handle as a
 * constant. HotSpot C2 trusts every static final, so on the JVM a handle may live anywhere; a
 * native image folds only the static finals of classes initialized at build time
 * ({@code SegmentAccess} is configured so in {@code native-image.properties}), and a segment handle
 * held, or aliased, by a class the image initializes at run time is invoked through the generic
 * method-handle path on every read: 105 ns and a boxed allocation per read against 2.3 ns for the
 * constant (GraalVM 25, measured in the derived-cache campaign), which cost the t1M warm joins Q4
 * 44 % and Q9 39 % natively while the same classes won on C2. Whether a class is initialized at
 * build time is decided by the image builder's simulation of its initializer, which a logger, a
 * counter or a system-property read silently turns off, so the rule is structural: a segment handle
 * lives in a class that {@code native-image.properties} lists for build-time initialization and
 * that holds nothing else ({@code SegmentAccess}, and {@code JoinKernels} for the join kernels,
 * whose read sites invoke the handle directly because C2 compiles a helper's inlined copy with the
 * helper's shared, polluted profile: the helper form cost t250k warm Q10 60 % on C2).
 * </p>
 *
 * <p>
 * {@code GroupLanes} is the third holder, and a helper by design: the group kernels reach their
 * accumulators through its accessors. What polluted {@code SegmentAccess}'s profile was that heap
 * and native segments both pass through it; {@code GroupLanes} is handed nothing but the native
 * memory of a {@code GroupWorkspace}, so its one handle site sees one segment class for the whole
 * process. That holds only while no caller passes it a heap array viewed as a segment. The
 * package-private {@code GroupKernelLanes} additionally holds a constant zero-address segment, the
 * mask every aggregation address is validated against, and a build-time debug switch for audited
 * group-memory kernels. It cannot own an allocation. Its explicit build-time initialization is
 * required even though the switch reads a system property. Its handle sites see that one segment
 * and nothing else, so they have no profile for another caller to pollute.
 * </p>
 *
 * <p>
 * The scan covers the derived-cache and query packages and loads the classes without initializing
 * them; only a class that declares a static {@code VarHandle} field is initialized, to read the
 * handle's coordinate types.
 * </p>
 */
final class SegmentHandleConstantsTest {

  /** Classes whose segment handles are build-time constants by configuration. */
  private static final Set<String> BUILD_TIME_INITIALIZED =
      Set.of("io.sirix.node.SegmentAccess", "io.sirix.query.compiler.translator.JoinKernels",
          "io.sirix.index.projection.GroupLanes", "io.sirix.index.projection.GroupKernelLanes");

  /** The shared native-image configurations that must list every class above. */
  private static final List<String> CONFIGURATIONS =
      List.of("META-INF/native-image/io.sirix/sirix-core/native-image.properties",
          "META-INF/native-image/io.sirix/query/native-image.properties");

  private static final List<String> PACKAGES = List.of("io.sirix.query", "io.sirix.index.projection");

  private static final String BUILD_TIME_FLAG = "--initialize-at-build-time=";

  @Test
  void segmentHandlesLiveOnlyInBuildTimeInitializedClasses() throws Exception {
    final ClassLoader loader = SegmentHandleConstantsTest.class.getClassLoader();
    final List<String> offenders = new ArrayList<>();
    final List<String> unloadable = new ArrayList<>();
    int classes = 0;
    int handles = 0;
    for (final String pkg : PACKAGES) {
      for (final String name : classNames(loader, pkg)) {
        final Class<?> type;
        final Field[] fields;
        try {
          type = Class.forName(name, false, loader);
          fields = type.getDeclaredFields();
        } catch (final ClassNotFoundException | LinkageError error) {
          unloadable.add(name);
          continue;
        }
        if (type.getNestHost() == SegmentHandleConstantsTest.class) {
          continue; // this test's own fixture
        }
        classes++;
        for (final Field field : fields) {
          if (!Modifier.isStatic(field.getModifiers()) || !VarHandle.class.isAssignableFrom(field.getType())) {
            continue;
          }
          handles++;
          if (isSegmentHandle(field) && !BUILD_TIME_INITIALIZED.contains(type.getName())) {
            offenders.add(type.getName() + "." + field.getName());
          }
        }
      }
    }
    assertTrue(classes > 200, "the scan reached " + classes + " classes; the package roots were not found");
    assertTrue(handles > 0, "the scan saw no static VarHandle field at all; the field walk is broken");
    assertTrue(unloadable.size() < classes / 10, "too many classes failed to load: " + unloadable);
    assertTrue(offenders.isEmpty(),
        "segment VarHandles outside the build-time-initialized classes (read through "
            + "SegmentAccess instead; a native image invokes a non-constant handle through the generic path on every "
            + "read): " + offenders);
  }

  @Test
  void everyAllowedClassIsConfiguredForBuildTimeInitializationAndHoldsOnlyAccessMetadata() throws Exception {
    final ClassLoader loader = SegmentHandleConstantsTest.class.getClassLoader();
    final Set<String> configured = configuredBuildTimeClasses(loader);
    for (final String name : BUILD_TIME_INITIALIZED) {
      assertTrue(configured.contains(name), name + " is allowed to hold segment handles but no shared "
          + "native-image.properties initializes it at build time: " + configured);
      assertClassHoldsOnlyAccessMetadata(name, loader);
    }
  }

  private static Set<String> configuredBuildTimeClasses(final ClassLoader loader) throws IOException {
    final Set<String> configured = new HashSet<>();
    for (final String configuration : CONFIGURATIONS) {
      final Enumeration<URL> resources = loader.getResources(configuration);
      assertTrue(resources.hasMoreElements(), configuration + " is not on the test class path");
      while (resources.hasMoreElements()) {
        final Properties properties = new Properties();
        try (InputStream in = resources.nextElement().openStream()) {
          properties.load(in);
        }
        for (final String arg : properties.getProperty("Args", "").split("\\s+")) {
          if (arg.startsWith(BUILD_TIME_FLAG)) {
            configured.addAll(List.of(arg.substring(BUILD_TIME_FLAG.length()).split(",")));
          }
        }
      }
    }
    return configured;
  }

  private static void assertClassHoldsOnlyAccessMetadata(final String name, final ClassLoader loader)
      throws ClassNotFoundException, IllegalAccessException {
    // Build-time initialization captures the class's static state in the image heap, so the class
    // may hold immutable access metadata only. The internal aggregation holder also captures
    // its build-time debug switch, a constant zero-address segment and the constant mask its
    // addresses are validated against; no owned memory.
    for (final Field field : Class.forName(name, false, loader).getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
        continue;
      }
      final boolean aggregationMetadata = isAggregationMetadata(name, field);
      assertTrue(
          Modifier.isFinal(field.getModifiers())
              && (VarHandle.class.isAssignableFrom(field.getType()) || aggregationMetadata),
          name + "." + field.getName() + " is static state in a build-time-initialized class; only final "
              + "access metadata belongs there");
      if (aggregationMetadata && field.getType() == long.class) {
        field.setAccessible(true);
        final long mask = field.getLong(null);
        // Lane-aligned, positive, and with room above it for the largest lane offset an int
        // lane expresses: a masked record plus any lane stays inside the spanning segment.
        assertEquals(0L, mask & 7L);
        assertTrue(mask > 0L && mask <= Long.MAX_VALUE - Long.BYTES - (0xFFFF_FFFFL << 3));
      }
      if (aggregationMetadata && field.getType() == MemorySegment.class) {
        field.setAccessible(true);
        final MemorySegment addressSpace = (MemorySegment) field.get(null);
        assertEquals(0L, addressSpace.address());
        assertEquals(Long.MAX_VALUE, addressSpace.byteSize());
        assertTrue(addressSpace.scope().isAlive());
      }
    }
  }

  private static boolean isAggregationMetadata(final String name, final Field field) {
    return name.equals("io.sirix.index.projection.GroupKernelLanes")
        && ((field.getName().equals("DEBUG") && field.getType() == boolean.class)
            || (field.getName().equals("ADDRESS_MASK") && field.getType() == long.class)
            || (field.getName().equals("ADDRESS_SPACE") && field.getType() == MemorySegment.class));
  }

  @Test
  void aSegmentHandleIsRecognisedAndAnArrayHandleIsNot() throws Exception {
    assertTrue(isSegmentHandle(Handles.class.getDeclaredField("SEGMENT")));
    assertFalse(isSegmentHandle(Handles.class.getDeclaredField("ARRAY")));
    assertEquals(2,
        Stream.of(Handles.class.getDeclaredFields())
              .filter(field -> VarHandle.class.isAssignableFrom(field.getType()))
              .count());
  }

  /** A fixture holding one handle of each kind; initialized only by the test that reads it. */
  private static final class Handles {
    static final VarHandle SEGMENT = ValueLayout.JAVA_LONG_UNALIGNED.varHandle();
    static final VarHandle ARRAY = MethodHandles.arrayElementVarHandle(long[].class);

    private Handles() {}
  }

  /** Whether the static handle field addresses a {@link MemorySegment} (its first coordinate). */
  private static boolean isSegmentHandle(final Field field) throws IllegalAccessException {
    field.setAccessible(true);
    final VarHandle handle = (VarHandle) field.get(null);
    if (handle == null) {
      return false;
    }
    final List<Class<?>> coordinates = handle.coordinateTypes();
    return !coordinates.isEmpty() && MemorySegment.class.isAssignableFrom(coordinates.get(0));
  }

  /** Every class under {@code pkg} on the loader's class path, from directories and jars. */
  private static List<String> classNames(final ClassLoader loader, final String pkg)
      throws IOException, URISyntaxException {
    final String prefix = pkg.replace('.', '/') + '/';
    final List<String> names = new ArrayList<>();
    final Enumeration<URL> roots = loader.getResources(prefix.substring(0, prefix.length() - 1));
    while (roots.hasMoreElements()) {
      final URL root = roots.nextElement();
      if ("file".equals(root.getProtocol())) {
        final Path dir = Path.of(root.toURI());
        try (Stream<Path> files = Files.walk(dir)) {
          files.filter(path -> path.toString().endsWith(".class"))
               .map(path -> pkg + '.' + dir.relativize(path).toString().replace('/', '.').replace('\\', '.'))
               .forEach(names::add);
        }
      } else if ("jar".equals(root.getProtocol())) {
        final JarURLConnection connection = (JarURLConnection) root.openConnection();
        connection.setUseCaches(false);
        try (JarFile jar = connection.getJarFile()) {
          for (final JarEntry entry : Collections.list(jar.entries())) {
            if (entry.getName().startsWith(prefix) && entry.getName().endsWith(".class")) {
              names.add(entry.getName().replace('/', '.'));
            }
          }
        }
      }
    }
    final List<String> classNames = new ArrayList<>(names.size());
    for (final String name : names) {
      final String className = name.substring(0, name.length() - ".class".length());
      if (!className.endsWith("module-info") && !className.endsWith("package-info")) {
        classNames.add(className);
      }
    }
    return classNames;
  }
}
