package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.fs.HierarchyFileVisitor.Builder;
import com.google.common.base.Optional;

/**
 * Parses a directory in the file system and creates a sirix {@link Database} with an initial
 * revision.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
@NonNull
public class FileHierarchyWalker {

  /**
   * Parse a directory and create a simple XML representation.
   *
   * @param path path to directory from which to shredder all content into sirix
   * @param database sirix {@IDatabase} to shred into
   * @param visitor an optional visitor
   * @throws SirixException if any sirix operation fails
   * @throws IOException if an I/O error occurs
   * @throws NullPointerException if one of the arguments is {@code null}
   */
  public static Map<Path, org.sirix.fs.FileSystemPath> parseDir(final Path path,
      final Database<XmlResourceManager> database, Optional<Visitor<XmlNodeTrx>> visitor) throws IOException {
    checkNotNull(visitor);
    checkNotNull(path);
    try (final XmlResourceManager resource = checkNotNull(database).openResourceManager("shredded");
        final XmlNodeTrx wtx = resource.beginNodeTrx()) {
      final Builder builder = new Builder(wtx);
      if (visitor.isPresent()) {
        builder.setVisitor(visitor.get());
      }
      Map<Path, org.sirix.fs.FileSystemPath> index = Collections.emptyMap();
      try (final HierarchyFileVisitor fileVisitor = HierarchyFileVisitor.getInstance(builder)) {
        Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, fileVisitor);
        index = fileVisitor.getIndex();
      }

      return index;
    }
  }
}
