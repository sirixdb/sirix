/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.crash;

import io.sirix.io.DurableFiles;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Crash model for directory entries (TXN-616): after a power loss, a directory holds the entries it
 * held at its last completed barrier, or at {@link #start} when no barrier on it completed since.
 *
 * <p>
 * The model isolates directory-entry durability from content durability, which
 * {@link PowerLossSimulationTest} covers: a file named in a surviving entry keeps its current
 * contents if it is still the same file, and the contents it had when the entry was recorded if it
 * has since been replaced or deleted. Files and directories are identified by their file key
 * (device and inode), so a rename keeps a directory's recorded entries. Every recorded file is
 * hard-linked into a scratch directory, which keeps its contents and stops its inode number being
 * reused.
 *
 * <p>
 * A test can also hand the model the contents of chosen files ({@link #governContents}): such a
 * file keeps the contents of its last {@link DurableFiles#forceFile} since the start, its contents
 * at the start if it was never forced since, and nothing if it is new and was never forced. This is
 * how the forces of settings, keysets, catalogues and backup copies are checked; the storage files
 * are forced through their own channels and are never governed.
 *
 * <p>
 * Barriers and forces are observed through {@link DurableFiles#observeBarriers}; code that changes
 * a name or writes a governed file without calling {@link DurableFiles} is exactly what this model
 * catches. Platforms without file keys (Windows) cannot run it: {@link #supported} tells the test
 * to skip.
 */
public final class DirectoryCrashModel implements AutoCloseable {

  private record Entry(String name, boolean directory, Object key, @Nullable Path link) {
  }

  private final Path root;
  private final Path links;
  private final Object rootKey;
  private final Map<Object, List<Entry>> atStart = new HashMap<>();
  private final Map<Object, List<Entry>> atLastBarrier = new HashMap<>();
  private final List<Path> barriers = new ArrayList<>();
  private final Map<Object, Path> contentAtStart = new HashMap<>();
  private final Map<Object, Path> contentAtLastForce = new HashMap<>();
  private Predicate<Path> governed = relative -> false;
  private Consumer<Path> afterEachBarrier = directory -> {
  };
  private final DurableFiles.Observation observation;
  private int linkCounter;

  private DirectoryCrashModel(final Path root, final Path links) throws IOException {
    this.root = root.toAbsolutePath().normalize();
    this.links = links;
    Files.createDirectories(links);
    this.rootKey = key(this.root);
    try (final Stream<Path> tree = Files.walk(this.root)) {
      for (final Path directory : tree.filter(Files::isDirectory).toList()) {
        atStart.put(key(directory), record(directory));
      }
    }
    this.observation = DurableFiles.observeBarriers(new DurableFiles.BarrierObserver() {
      @Override
      public void afterBarrier(final Path directory) {
        DirectoryCrashModel.this.afterBarrier(directory);
      }

      @Override
      public void afterForce(final Path file) {
        DirectoryCrashModel.this.afterForce(file);
      }
    });
  }

  /**
   * Makes the model govern the contents of every file whose path relative to the root matches
   * {@code relativePaths}, from now on. Files that match and already exist keep their current
   * contents as their contents at the start.
   */
  public synchronized void governContents(final Predicate<Path> relativePaths) throws IOException {
    governed = requireNonNull(relativePaths);
    try (final Stream<Path> tree = Files.walk(root)) {
      for (final Path file : tree.filter(Files::isRegularFile).toList()) {
        if (governed.test(root.relativize(file))) {
          contentAtStart.put(key(file), copy(file));
        }
      }
    }
  }

  /** Whether this platform exposes the file keys the model needs. */
  public static boolean supported(final Path anyFile) {
    try {
      return Files.readAttributes(anyFile, BasicFileAttributes.class).fileKey() != null;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Records the durable state of every directory under {@code root} and starts observing barriers.
   *
   * @param root an existing directory; its own entry in its parent is assumed durable
   * @param links an absent or empty scratch directory on the same filesystem as {@code root}, outside
   *        it
   */
  public static DirectoryCrashModel start(final Path root, final Path links) throws IOException {
    return new DirectoryCrashModel(requireNonNull(root), requireNonNull(links));
  }

  /**
   * Runs {@code action} after each recorded barrier under the root, for a test that crashes in the
   * middle of an operation (only one barrier observer can be installed at a time).
   */
  public synchronized void afterEachBarrier(final Consumer<Path> action) {
    afterEachBarrier = requireNonNull(action);
  }

  /** The directories whose barriers completed under the root, in order. */
  public synchronized List<Path> barriers() {
    return List.copyOf(barriers);
  }

  /**
   * Writes the state a crash at this instant leaves under {@code root} to {@code target}, with the
   * same relative layout.
   *
   * @param target an absent directory
   */
  public synchronized void materialize(final Path target) throws IOException {
    final Map<Object, Path> current = new HashMap<>();
    try (final Stream<Path> tree = Files.walk(root)) {
      for (final Path path : tree.toList()) {
        current.put(key(path), path);
      }
    }
    materialize(rootKey, target, target, current);
  }

  private void materialize(final Object directoryKey, final Path targetRoot, final Path target,
      final Map<Object, Path> current) throws IOException {
    Files.createDirectories(target);
    List<Entry> entries = atLastBarrier.get(directoryKey);
    if (entries == null) {
      entries = atStart.get(directoryKey);
    }
    if (entries == null) {
      return; // created after start and never synced: nothing inside it is durable
    }
    for (final Entry entry : entries) {
      final Path out = target.resolve(entry.name());
      if (entry.directory()) {
        materialize(entry.key(), targetRoot, out, current);
        continue;
      }
      if (governed.test(targetRoot.relativize(out))) {
        Path durable = contentAtLastForce.get(entry.key());
        if (durable == null) {
          durable = contentAtStart.get(entry.key());
        }
        if (durable == null) {
          Files.createFile(out); // created since the start and never forced
        } else {
          Files.copy(durable, out);
        }
        continue;
      }
      final Path same = current.get(entry.key());
      final Path source = same != null && Files.isRegularFile(same, LinkOption.NOFOLLOW_LINKS)
          ? same
          : requireNonNull(entry.link(), entry.name());
      Files.copy(source, out, StandardCopyOption.COPY_ATTRIBUTES);
    }
  }

  private synchronized void afterBarrier(final Path directory) {
    final Path absolute = directory.toAbsolutePath().normalize();
    if (!absolute.startsWith(root)) {
      return;
    }
    try {
      atLastBarrier.put(key(absolute), record(absolute));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    barriers.add(absolute);
    afterEachBarrier.accept(absolute);
  }

  private synchronized void afterForce(final Path file) {
    final Path absolute = file.toAbsolutePath().normalize();
    if (!absolute.startsWith(root) || !governed.test(root.relativize(absolute))) {
      return;
    }
    try {
      contentAtLastForce.put(key(absolute), copy(absolute));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Path copy(final Path file) throws IOException {
    final Path copy = links.resolve("content-" + linkCounter++);
    Files.copy(file, copy);
    return copy;
  }

  private List<Entry> record(final Path directory) throws IOException {
    final List<Entry> entries = new ArrayList<>();
    try (final Stream<Path> children = Files.list(directory)) {
      for (final Path child : children.sorted(Comparator.comparing(Path::toString)).toList()) {
        final BasicFileAttributes attributes =
            Files.readAttributes(child, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        final Object childKey = requireNonNull(attributes.fileKey(), "file keys unsupported");
        Path link = null;
        if (attributes.isRegularFile()) {
          link = links.resolve(Integer.toString(linkCounter++));
          Files.createLink(link, child);
        }
        entries.add(new Entry(child.getFileName().toString(), attributes.isDirectory(), childKey, link));
      }
    }
    return entries;
  }

  private static Object key(final Path path) throws IOException {
    return requireNonNull(Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey(),
        "file keys unsupported");
  }

  @Override
  public void close() {
    observation.close();
  }
}
