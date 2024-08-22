package io.sirix.utils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Static methods for file operations.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class SirixFiles {

	/**
	 * Utility methods.
	 */
	private SirixFiles() {
		throw new AssertionError("May not be instantiated!");
	}

	/**
	 * Recursively remove a directory.
	 *
	 * @param path
	 *            {@link Path} to the directory
	 * @throws UncheckedIOException
	 *             if any I/O operation fails
	 * @throws NullPointerException
	 *             if any of the arguments are {@code null}
	 */
	public static void recursiveRemove(final Path path) {
		if (Files.exists(path)) {
			try (var stream = Files.walk(path)) {
				stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
			} catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	public static boolean isDirectoryEmpty(Path path) {
		if (Files.isDirectory(path)) {
			try (final Stream<Path> entries = Files.list(path)) {
				return entries.findFirst().isEmpty();
			} catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		return false;
	}
}
