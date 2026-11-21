package io.sirix.access;

import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.io.StorageType;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * Recognises the on-disk footprint of IN_MEMORY resources (TXN-615). Their committed state lives
 * only in the open database instance; what they leave on disk (settings, index definitions,
 * update-diff sidecars, derived state) belongs to that state and goes with it.
 */
public final class InMemoryResources {

  /**
   * Reserved part of a resource directory name: an IN_MEMORY resource is retired by renaming its
   * directory to {@code <name>$retired-<uuid>} before deleting it, so the name is free at once and a
   * failed deletion leaves only an unlisted tombstone.
   */
  public static final String RETIRED_MARKER = "$retired-";

  private InMemoryResources() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Whether {@code resourceDirectory} belongs to an IN_MEMORY resource: readable persisted settings
   * prove it is one or is the derived resource of one, or its name marks it as a retired one. A
   * derived resource is recognised by its reserved name and its source's settings, never by its own
   * storage type. Missing, empty or unreadable settings prove nothing: such a directory counts as
   * persistent (TXN-611) and is never classified here.
   *
   * @param resourceDirectory a resource directory below a database's resources folder
   * @return {@code true} only if the directory provably belongs to an IN_MEMORY resource
   */
  public static boolean isInMemoryOwned(final Path resourceDirectory) {
    final Path fileName = requireNonNull(resourceDirectory).getFileName();
    if (fileName == null) {
      return false;
    }
    final String name = fileName.toString();
    if (isRetiredResourceName(name)) {
      return true;
    }
    if (!DerivedIndexMaintainer.isDerivedResourceName(name)) {
      return isInMemory(resourceDirectory);
    }
    final String sourceName = name.substring(0, name.length() - DerivedIndexMaintainer.SUFFIX.length());
    return !sourceName.isEmpty() && isInMemory(resourceDirectory.resolveSibling(sourceName));
  }

  /**
   * Whether {@code name} is reserved for the tombstone of a retired IN_MEMORY resource.
   *
   * @param name a resource directory name
   * @return {@code true} if the name carries {@link #RETIRED_MARKER}
   */
  public static boolean isRetiredResourceName(final String name) {
    return requireNonNull(name).contains(RETIRED_MARKER);
  }

  private static boolean isInMemory(final Path resourceDirectory) {
    if (!Files.isRegularFile(resourceDirectory.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()))) {
      return false;
    }
    try {
      return ResourceConfiguration.deserialize(resourceDirectory).storageType == StorageType.IN_MEMORY;
    } catch (final RuntimeException unreadable) {
      return false;
    }
  }
}
