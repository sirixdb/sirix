/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The handles of the scratch borrow path stay constants in a native image.
 *
 * <p>
 * A native image folds a {@code VarHandle} only when its holder is initialized at build time;
 * otherwise every borrow would invoke its compare-and-sets through the generic method-handle path.
 * Build-time initialization captures the holder's static state in the image heap, so the holder may
 * hold nothing but final handles: one logger or counter added to it would either be frozen into the
 * image or make the builder initialize the class at run time after all. Both halves are checked
 * against the configuration the image build reads, not against a copy of it.
 */
final class ScratchHandlesTest {

  private static final String CONFIGURATION = "META-INF/native-image/io.sirix/sirix-core/native-image.properties";

  private static final String BUILD_TIME_FLAG = "--initialize-at-build-time=";

  @Test
  // String splitting follows the native-image property's whitespace and comma delimiters
  @SuppressWarnings("StringSplitter")
  void theHandleHolderIsInitializedAtBuildTimeAndHoldsOnlyFinalHandles() throws IOException {
    final ClassLoader loader = ScratchHandlesTest.class.getClassLoader();
    final Set<String> configured = new HashSet<>();
    final Enumeration<URL> resources = loader.getResources(CONFIGURATION);
    assertTrue(resources.hasMoreElements(), CONFIGURATION + " is not on the test class path");
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
    assertTrue(configured.contains(ScratchHandles.class.getName()),
        ScratchHandles.class.getName() + " is not initialized at build time: " + configured);

    int handles = 0;
    for (final Field field : ScratchHandles.class.getDeclaredFields()) {
      if (field.isSynthetic()) {
        continue;
      }
      assertTrue(Modifier.isStatic(field.getModifiers()), field.getName() + " is instance state");
      assertTrue(Modifier.isFinal(field.getModifiers()) && VarHandle.class.isAssignableFrom(field.getType()),
          field.getName() + " is static state in a build-time-initialized class; only final VarHandles belong there");
      handles++;
    }
    assertTrue(handles >= 6, "the field walk saw " + handles + " handles; the holder was not read");
  }

  @Test
  void theBorrowPathKeepsNoHandleOfItsOwn() {
    for (final Class<?> type : List.of(ScratchLease.class, ScratchCarrier.class, FrameScratch.class,
        ScratchSource.class)) {
      for (final Field field : type.getDeclaredFields()) {
        assertTrue(!VarHandle.class.isAssignableFrom(field.getType()),
            type.getSimpleName() + "." + field.getName() + " is a handle outside the build-time-initialized holder: "
                + "a native image would invoke it through the generic path on every borrow");
      }
    }
  }
}
