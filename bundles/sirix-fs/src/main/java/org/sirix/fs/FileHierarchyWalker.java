package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nonnull;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeWriteTrx;
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
@Nonnull
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
  public static Map<Path, org.sirix.fs.Path> parseDir(final Path path, final Database database,
      Optional<Visitor<XdmNodeWriteTrx>> visitor) throws SirixException, IOException {
    checkNotNull(visitor);
    checkNotNull(path);
    try (
        final ResourceManager resource = checkNotNull(database)
            .getResourceManager(new ResourceManagerConfiguration.Builder("shredded").build());
        final XdmNodeWriteTrx wtx = resource.beginNodeWriteTrx()) {
      final Builder builder = new Builder(wtx);
      if (visitor.isPresent()) {
        builder.setVisitor(visitor.get());
      }
      Map<Path, org.sirix.fs.Path> index = Collections.emptyMap();
      try (final HierarchyFileVisitor fileVisitor = HierarchyFileVisitor.getInstance(builder)) {
        Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
            fileVisitor);
        index = fileVisitor.getIndex();
      }

      return index;
    }
  }
}
